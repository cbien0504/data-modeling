"""Data quality gate: reconcile the warehouse tables with the source before publishing.

If any check fails the run stops and the published warehouse stays untouched.
"""
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl.reference import UNKNOWN_KEY

log = logging.getLogger(__name__)


class DataQualityError(Exception):
    pass


def run_checks(bronze: dict[str, DataFrame], tables: dict[str, DataFrame]) -> list[dict]:
    results = []

    def check(name: str, passed: bool, detail: str) -> None:
        results.append({"check": name, "passed": bool(passed), "detail": detail})

    items, orders, payments = bronze["order_items"], bronze["orders"], bronze["payments"]
    fact_sales, fact_payments = tables["fact_sales"], tables["fact_payments"]

    # 1. Reconciliation of fact_sales with the source order lines
    src = items.agg(F.count("*").alias("rows"),
                    F.sum(F.col("quantity") * F.col("unit_price") - F.col("discount_amount")).alias("net")).first()
    is_unknown = (F.col("customer_key") == UNKNOWN_KEY) | (F.col("product_key") == UNKNOWN_KEY)
    tgt = fact_sales.agg(F.count("*").alias("rows"), F.sum("net_amount").alias("net"),
                         F.sum("shipping_fee_allocated").alias("shipping"),
                         F.sum(is_unknown.cast("int")).alias("unknown")).first()
    check("fact_sales rows = OLTP order_items", src["rows"] == tgt["rows"], f"{src['rows']} vs {tgt['rows']}")
    check("fact_sales net_amount = OLTP", src["net"] == tgt["net"], f"{src['net']} vs {tgt['net']}")
    check("fact_sales: no unknown customer/product key", tgt["unknown"] == 0, f"{tgt['unknown']} rows")

    shipping = (orders.join(items.select("order_id").distinct(), "order_id")
                .agg(F.sum("shipping_fee")).first()[0])
    check("shipping fee fully allocated", shipping == tgt["shipping"], f"{shipping} vs {tgt['shipping']}")

    # 2. Reconciliation of fact_payments
    src = payments.agg(F.count("*").alias("rows"), F.sum("amount").alias("amount")).first()
    tgt = fact_payments.agg(F.count("*").alias("rows"), F.sum("amount").alias("amount"),
                            F.sum((F.col("customer_key") == UNKNOWN_KEY).cast("int")).alias("unknown")).first()
    check("fact_payments rows = OLTP payments", src["rows"] == tgt["rows"], f"{src['rows']} vs {tgt['rows']}")
    check("fact_payments amount = OLTP", src["amount"] == tgt["amount"], f"{src['amount']} vs {tgt['amount']}")
    check("fact_payments: no unknown customer key", tgt["unknown"] == 0, f"{tgt['unknown']} rows")

    # 3. SCD2 integrity
    for dim_name, nk, sk, source in [("dim_customer", "customer_id", "customer_key", "customers"),
                                     ("dim_product", "product_id", "product_key", "products")]:
        dim = tables[dim_name]
        dup_sk = dim.groupBy(sk).count().filter("count > 1").count()
        check(f"{dim_name}: surrogate key unique", dup_sk == 0, f"{dup_sk} duplicates")
        multi_current = dim.filter("is_current").groupBy(nk).count().filter("count > 1").count()
        check(f"{dim_name}: at most one current version per key", multi_current == 0, f"{multi_current} keys")
        missing = bronze[source].select(nk).join(dim.filter("is_current"), nk, "left_anti").count()
        check(f"{dim_name}: every source key has a current version", missing == 0, f"{missing} keys")
        bad_range = dim.filter(F.col("valid_from") > F.col("valid_to")).count()
        check(f"{dim_name}: valid_from <= valid_to", bad_range == 0, f"{bad_range} rows")

    # 4. Every fact date exists in dim_date
    for fact_name in ("fact_sales", "fact_payments"):
        orphans = (tables[fact_name].select("date_key").distinct()
                   .join(tables["dim_date"], "date_key", "left_anti").count())
        check(f"{fact_name}: date_key covered by dim_date", orphans == 0, f"{orphans} dates")

    for r in results:
        log.info("DQ %-4s %-55s %s", "PASS" if r["passed"] else "FAIL", r["check"], r["detail"])
    failed = [r["check"] for r in results if not r["passed"]]
    if failed:
        raise DataQualityError(f"{len(failed)} data quality check(s) failed: {failed}")
    return results
