"""Transform: build the fact tables and resolve their dimension keys."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from etl.reference import UNKNOWN_KEY

MONEY = DecimalType(16, 2)


def lookup_scd(df: DataFrame, dim: DataFrame, natural_key: str, surrogate_key: str, event_ts: str,
               extra_cols: tuple[str, ...] = ()) -> DataFrame:
    """Point-in-time lookup: pick the dimension version valid when the event happened."""
    versions = dim.filter(F.col(surrogate_key) != UNKNOWN_KEY).select(
        F.col(natural_key).alias("_nk"), surrogate_key,
        F.col("valid_from").alias("_from"), F.col("valid_to").alias("_to"), *extra_cols)
    cond = ((F.col(natural_key) == F.col("_nk"))
            & (F.col(event_ts) >= F.col("_from"))
            & (F.col(event_ts) < F.col("_to")))
    return (df.join(F.broadcast(versions), cond, "left")
              .drop("_nk", "_from", "_to")
              .withColumn(surrogate_key, F.coalesce(F.col(surrogate_key), F.lit(UNKNOWN_KEY))))


def lookup_code(df: DataFrame, dim: DataFrame, code_col: str, dim_code_col: str, surrogate_key: str) -> DataFrame:
    codes = dim.select(F.col(dim_code_col).alias("_code"), surrogate_key)
    return (df.join(F.broadcast(codes), F.col(code_col) == F.col("_code"), "left")
              .drop("_code")
              .withColumn(surrogate_key, F.coalesce(F.col(surrogate_key), F.lit(UNKNOWN_KEY))))


def date_key(ts_col: str):
    return F.date_format(F.col(ts_col), "yyyyMMdd").cast("int")


# ---------------------------------------------------------------------------
# fact_sales — grain: one order line
# ---------------------------------------------------------------------------
def build_fact_sales(bronze: dict[str, DataFrame], dims: dict[str, DataFrame]) -> DataFrame:
    orders = bronze["orders"].select(
        "order_id", "customer_id", F.col("order_date").alias("order_ts"),
        F.col("status").alias("order_status"), "payment_method", "shipping_fee")
    items = bronze["order_items"].select(
        "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount")

    df = items.join(orders, "order_id", "inner")
    df = lookup_scd(df, dims["dim_customer"], "customer_id", "customer_key", "order_ts")
    df = lookup_scd(df, dims["dim_product"], "product_id", "product_key", "order_ts", extra_cols=("cost_price",))
    df = lookup_code(df, dims["dim_payment_method"], "payment_method", "payment_method_code", "payment_method_key")
    df = lookup_code(df, dims["dim_order_status"], "order_status", "order_status_code", "order_status_key")

    df = (df.withColumn("gross_amount", (F.col("quantity") * F.col("unit_price")).cast(MONEY))
            .withColumn("discount_amount", F.col("discount_amount").cast(MONEY))
            .withColumn("net_amount", (F.col("gross_amount") - F.col("discount_amount")).cast(MONEY))
            .withColumn("cost_amount", (F.col("quantity") * F.coalesce(F.col("cost_price"), F.lit(0))).cast(MONEY))
            .withColumn("gross_profit", (F.col("net_amount") - F.col("cost_amount")).cast(MONEY)))

    # Allocate the order-level shipping fee down to the line grain, proportional to net_amount.
    # Rounding residue goes to the last line so the allocation always sums to the order's fee.
    per_order = Window.partitionBy("order_id")
    last_line_first = Window.partitionBy("order_id").orderBy(F.col("order_item_id").desc())
    order_net = F.sum("net_amount").over(per_order)
    share = F.when(order_net != 0, F.col("net_amount") / order_net).otherwise(1 / F.count("*").over(per_order))
    df = df.withColumn("_alloc", F.round(F.col("shipping_fee") * share, 2).cast("decimal(14,2)"))
    df = df.withColumn(
        "shipping_fee_allocated",
        F.when(F.row_number().over(last_line_first) == 1,
               F.col("shipping_fee") - (F.sum("_alloc").over(per_order) - F.col("_alloc")))
         .otherwise(F.col("_alloc"))
         .cast("decimal(14,2)"))

    return df.select(
        "order_item_id", "order_id",
        date_key("order_ts").alias("date_key"),
        "customer_key", "product_key", "payment_method_key", "order_status_key",
        "order_ts", "quantity", "unit_price",
        "gross_amount", "discount_amount", "net_amount", "cost_amount", "gross_profit",
        "shipping_fee_allocated",
        F.current_timestamp().alias("etl_loaded_at"),
    )


# ---------------------------------------------------------------------------
# fact_payments — grain: one payment transaction
# ---------------------------------------------------------------------------
def build_fact_payments(bronze: dict[str, DataFrame], dims: dict[str, DataFrame]) -> DataFrame:
    df = (bronze["payments"]
          .join(bronze["orders"].select("order_id", "customer_id"), "order_id", "left")
          .select("payment_id", "order_id", "customer_id", "payment_method", "amount",
                  F.col("payment_date").alias("payment_ts"), F.col("status").alias("payment_status")))
    df = lookup_scd(df, dims["dim_customer"], "customer_id", "customer_key", "payment_ts")
    df = lookup_code(df, dims["dim_payment_method"], "payment_method", "payment_method_code", "payment_method_key")

    status = F.col("payment_status")
    net_collected = (F.when(status == "SUCCESS", F.col("amount"))
                      .when(status == "REFUNDED", -F.col("amount"))
                      .otherwise(F.lit(0)))
    return df.select(
        "payment_id", "order_id",
        date_key("payment_ts").alias("date_key"),
        "customer_key", "payment_method_key", "payment_status", "payment_ts",
        F.col("amount").cast(MONEY).alias("amount"),
        net_collected.cast(MONEY).alias("net_collected_amount"),
        F.current_timestamp().alias("etl_loaded_at"),
    )


def build_all(bronze: dict[str, DataFrame], dims: dict[str, DataFrame]) -> dict[str, DataFrame]:
    return {
        "fact_sales": build_fact_sales(bronze, dims),
        "fact_payments": build_fact_payments(bronze, dims),
    }
