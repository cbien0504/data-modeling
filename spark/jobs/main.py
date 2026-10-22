"""OLTP -> OLAP pipeline.

    extract   : OLTP (PostgreSQL) --JDBC--> bronze Parquet snapshot (/opt/lake/bronze)
    transform : bronze -> dimensions (SCD1/SCD2) + facts (point-in-time key lookup)
    quality   : reconcile with the source; stop before publishing if anything is off
    load      : write stg.* then publish dwh.* in a single transaction

Usage (inside the spark-master container):
    /opt/submit.sh [--run-date YYYY-MM-DD] [--skip-extract]
"""
import argparse
import logging
import sys
import uuid
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl import config, dimensions, extract, facts, load, quality
from etl.jdbc import read_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(name)s - %(message)s")
log = logging.getLogger("etl")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-date", type=date.fromisoformat, default=config.now_vn().date(),
                   help="logical date of the run, names the bronze snapshot (default: today)")
    p.add_argument("--skip-extract", action="store_true",
                   help="reuse the bronze snapshot of --run-date instead of reading the OLTP database")
    return p.parse_args()


def read_existing_dims(spark: SparkSession) -> dict[str, DataFrame]:
    """Current content of the SCD2 dimensions (without the -1 unknown member)."""
    return {
        name: read_table(spark, config.DWH, f"dwh.{name}").filter(F.col(sk) > 0)
        for name, sk in [("dim_customer", "customer_key"), ("dim_product", "product_key")]
    }


def main() -> int:
    args = parse_args()
    run_id, started_at = str(uuid.uuid4()), config.now_vn()
    spark = SparkSession.builder.appName("oltp-to-olap").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    metrics: dict = {}
    log.info("Run %s for %s", run_id, args.run_date)

    try:
        if not args.skip_extract:
            metrics["extracted_rows"] = extract.extract_all(spark, args.run_date)
        bronze = extract.read_bronze(spark, args.run_date)

        dims, metrics["scd2"] = dimensions.build_all(spark, bronze, read_existing_dims(spark))
        tables = {**dims, **facts.build_all(bronze, dims)}
        for df in tables.values():
            df.cache()

        metrics["quality_checks"] = quality.run_checks(bronze, tables)
        metrics["rows"] = {name: df.count() for name, df in tables.items()}

        load.write_staging(tables)
        load.publish(tables)
        load.log_run(run_id, args.run_date, started_at, "SUCCESS", metrics)
        log.info("Run %s finished: %s", run_id, metrics["rows"])
        return 0
    except Exception as exc:  # log every failure to audit.etl_run_log, then fail the job
        log.exception("Run %s failed", run_id)
        try:
            load.log_run(run_id, args.run_date, started_at, "FAILED", metrics, error=str(exc)[:4000])
        except Exception:
            log.exception("Could not write the failure to audit.etl_run_log")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
