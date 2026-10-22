"""Transform: build the dimension tables from the bronze snapshot."""
import logging
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from etl import reference as ref
from etl.scd import high_ts, low_ts, scd2_merge

log = logging.getLogger(__name__)

CUSTOMER_TYPE2 = ["address", "city", "region"]
CUSTOMER_TYPE1 = ["full_name", "email", "phone", "gender", "date_of_birth", "signup_date"]
PRODUCT_TYPE2 = ["category_id", "category_name", "parent_category_name", "list_price", "cost_price", "price_tier"]
PRODUCT_TYPE1 = ["sku", "product_name", "brand", "is_active"]


def _map(mapping: dict) -> Column:
    return F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])


def standardize_city(col: Column) -> Column:
    """Map free-text input (any casing, known aliases) to the canonical city name."""
    lookup = {**{city.lower(): city for city in ref.CITY_REGION}, **ref.CITY_ALIASES}
    city = F.regexp_replace(F.trim(col), r"\s+", " ")
    return F.coalesce(_map(lookup)[F.lower(city)], city)


def price_tier(col: Column) -> Column:
    expr = None
    for upper, label in ref.PRICE_TIERS:
        if upper is None:
            return expr.otherwise(F.lit(label))
        cond = col < F.lit(upper)
        expr = F.when(cond, F.lit(label)) if expr is None else expr.when(cond, F.lit(label))
    return expr


def with_unknown_member(dim: DataFrame, surrogate_key: str, natural_key: str) -> DataFrame:
    """Append the -1 'unknown' row that facts point to when a lookup fails."""
    values = []
    for field in dim.schema.fields:
        if field.name in (surrogate_key, natural_key):
            value = F.lit(ref.UNKNOWN_KEY)
        elif field.name == "valid_from":
            value = low_ts()
        elif field.name == "valid_to":
            value = high_ts()
        elif field.name == "is_current":
            value = F.lit(True)
        elif isinstance(field.dataType, StringType) and field.name != "row_hash":
            value = F.lit(ref.UNKNOWN_LABEL)
        else:
            value = F.lit(None)
        values.append(value.cast(field.dataType).alias(field.name))
    return dim.unionByName(dim.sparkSession.range(1).select(*values))


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------
def build_dim_date(spark: SparkSession, bronze: dict[str, DataFrame]) -> DataFrame:
    bounds = (bronze["orders"].select(F.col("order_date").alias("ts"))
              .unionByName(bronze["payments"].select(F.col("payment_date").alias("ts")))
              .agg(F.min("ts").alias("lo"), F.max("ts").alias("hi"))
              .first())
    start, end = date(bounds["lo"].year, 1, 1), date(bounds["hi"].year, 12, 31)

    d = F.col("full_date")
    iso_dow = ((F.dayofweek(d) + 5) % 7) + 1          # Spark: 1=Sunday -> ISO: 1=Monday
    holiday = _map(ref.HOLIDAYS)[F.date_format(d, "MM-dd")]
    return (spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS full_date")
            .select(
                F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
                d,
                F.dayofmonth(d).alias("day_of_month"),
                iso_dow.alias("day_of_week"),
                F.element_at(F.array(*[F.lit(n) for n in ref.DAY_NAMES]), iso_dow).alias("day_name"),
                F.weekofyear(d).alias("week_of_year"),
                F.month(d).alias("month"),
                F.concat(F.lit("Tháng "), F.month(d)).alias("month_name"),
                F.quarter(d).alias("quarter"),
                F.year(d).alias("year"),
                F.date_format(d, "yyyy-MM").alias("year_month"),
                F.concat(F.year(d), F.lit("-Q"), F.quarter(d)).alias("year_quarter"),
                (iso_dow >= 6).alias("is_weekend"),
                holiday.isNotNull().alias("is_holiday"),
                holiday.alias("holiday_name"),
                (F.month(d) == F.dayofmonth(d)).alias("is_double_day"),
            ))


# ---------------------------------------------------------------------------
# dim_customer (SCD2)
# ---------------------------------------------------------------------------
def clean_customers(customers: DataFrame) -> DataFrame:
    city = standardize_city(F.col("city"))
    phone = F.regexp_replace(F.col("phone"), r"[^0-9]", "")
    return customers.select(
        "customer_id",
        F.regexp_replace(F.trim("address"), r"\s+", " ").alias("address"),
        city.alias("city"),
        F.coalesce(_map(ref.CITY_REGION)[city], F.lit("Khác")).alias("region"),
        F.regexp_replace(F.trim("full_name"), r"\s+", " ").alias("full_name"),
        F.lower(F.trim("email")).alias("email"),
        F.when(phone != "", phone).alias("phone"),
        F.coalesce(_map(ref.GENDERS)[F.col("gender")], F.lit(ref.UNKNOWN_LABEL)).alias("gender"),
        "date_of_birth",
        F.to_date("created_at").alias("signup_date"),
        "updated_at",
    )


def build_dim_customer(bronze: dict[str, DataFrame], existing: DataFrame | None) -> tuple[DataFrame, dict]:
    dim, stats = scd2_merge(clean_customers(bronze["customers"]), existing,
                            natural_key="customer_id", surrogate_key="customer_key",
                            type2_cols=CUSTOMER_TYPE2, type1_cols=CUSTOMER_TYPE1,
                            effective_col="updated_at")
    return with_unknown_member(dim, "customer_key", "customer_id"), stats


# ---------------------------------------------------------------------------
# dim_product (SCD2) — products + 2-level category hierarchy flattened (snowflake -> star)
# ---------------------------------------------------------------------------
def clean_products(products: DataFrame, categories: DataFrame) -> DataFrame:
    cat, parent = categories.alias("cat"), categories.alias("parent")
    return (products.alias("p")
            .join(cat, F.col("p.category_id") == F.col("cat.category_id"), "left")
            .join(parent, F.col("cat.parent_category_id") == F.col("parent.category_id"), "left")
            .select(
                F.col("p.product_id").alias("product_id"),
                F.col("p.category_id").alias("category_id"),
                F.coalesce(F.col("cat.category_name"), F.lit(ref.UNKNOWN_LABEL)).alias("category_name"),
                F.coalesce(F.col("parent.category_name"), F.col("cat.category_name"),
                           F.lit(ref.UNKNOWN_LABEL)).alias("parent_category_name"),
                F.col("p.unit_price").alias("list_price"),
                F.col("p.cost_price").alias("cost_price"),
                price_tier(F.col("p.unit_price")).alias("price_tier"),
                F.upper(F.trim("p.sku")).alias("sku"),
                F.regexp_replace(F.trim("p.product_name"), r"\s+", " ").alias("product_name"),
                F.trim("p.brand").alias("brand"),
                F.col("p.is_active").alias("is_active"),
                # a category rename is also a change of the product's attributes
                F.greatest("p.updated_at", "cat.updated_at", "parent.updated_at").alias("updated_at"),
            ))


def build_dim_product(bronze: dict[str, DataFrame], existing: DataFrame | None) -> tuple[DataFrame, dict]:
    dim, stats = scd2_merge(clean_products(bronze["products"], bronze["categories"]), existing,
                            natural_key="product_id", surrogate_key="product_key",
                            type2_cols=PRODUCT_TYPE2, type1_cols=PRODUCT_TYPE1,
                            effective_col="updated_at")
    return with_unknown_member(dim, "product_key", "product_id"), stats


# ---------------------------------------------------------------------------
# Static dimensions (code lists)
# ---------------------------------------------------------------------------
def build_dim_payment_method(spark: SparkSession) -> DataFrame:
    rows = [(ref.UNKNOWN_KEY, "UNKNOWN", ref.UNKNOWN_LABEL, ref.UNKNOWN_LABEL), *ref.PAYMENT_METHODS]
    return spark.createDataFrame(
        rows, "payment_method_key int, payment_method_code string, payment_method_name string, payment_channel string")


def build_dim_order_status(spark: SparkSession) -> DataFrame:
    rows = [(ref.UNKNOWN_KEY, "UNKNOWN", ref.UNKNOWN_LABEL, ref.UNKNOWN_LABEL, False), *ref.ORDER_STATUSES]
    return spark.createDataFrame(
        rows, "order_status_key int, order_status_code string, order_status_name string, "
              "status_group string, is_final boolean")


def build_all(spark: SparkSession, bronze: dict[str, DataFrame],
              existing: dict[str, DataFrame | None]) -> tuple[dict[str, DataFrame], dict]:
    dim_customer, customer_stats = build_dim_customer(bronze, existing.get("dim_customer"))
    dim_product, product_stats = build_dim_product(bronze, existing.get("dim_product"))
    log.info("dim_customer SCD2: %s", customer_stats)
    log.info("dim_product  SCD2: %s", product_stats)
    dims = {
        "dim_date": build_dim_date(spark, bronze),
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_payment_method": build_dim_payment_method(spark),
        "dim_order_status": build_dim_order_status(spark),
    }
    return dims, {"dim_customer": customer_stats, "dim_product": product_stats}
