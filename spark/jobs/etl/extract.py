"""Extract: snapshot every OLTP table into the bronze layer of the lake (Parquet).

Keeping a raw, immutable copy per run decouples the (slow, load-sensitive) read from
the production database from the transformations, and makes every run reproducible.
"""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.config import LAKE_PATH, OLTP, OLTP_SCHEMA
from etl.jdbc import read_query, read_table

log = logging.getLogger(__name__)

# table -> (primary key, number of parallel JDBC partitions)
TABLES = {
    "categories": ("category_id", 1),
    "customers": ("customer_id", 1),
    "products": ("product_id", 1),
    "orders": ("order_id", 4),
    "order_items": ("order_item_id", 4),
    "payments": ("payment_id", 4),
}


def bronze_path(table: str, run_date) -> str:
    return f"{LAKE_PATH}/bronze/{table}/extract_date={run_date}"


def _read_source(spark: SparkSession, table: str, pk: str, partitions: int) -> DataFrame:
    options = {}
    if partitions > 1:
        # Split the read on the primary key range so several executors pull in parallel
        bounds = read_query(spark, OLTP, f"SELECT min({pk}) AS lo, max({pk}) AS hi "
                                         f"FROM {OLTP_SCHEMA}.{table}").first()
        if bounds["lo"] is not None:
            options = {"partitionColumn": pk, "lowerBound": bounds["lo"],
                       "upperBound": bounds["hi"], "numPartitions": partitions}
    return read_table(spark, OLTP, f"{OLTP_SCHEMA}.{table}", **options)


def extract_all(spark: SparkSession, run_date) -> dict[str, int]:
    counts = {}
    for table, (pk, partitions) in TABLES.items():
        path = bronze_path(table, run_date)
        (_read_source(spark, table, pk, partitions)
            .withColumn("_extracted_at", F.current_timestamp())
            .write.mode("overwrite").parquet(path))
        counts[table] = spark.read.parquet(path).count()
        log.info("extract  %-12s -> %-55s %8d rows", table, path, counts[table])
    return counts


def read_bronze(spark: SparkSession, run_date) -> dict[str, DataFrame]:
    return {table: spark.read.parquet(bronze_path(table, run_date)) for table in TABLES}
