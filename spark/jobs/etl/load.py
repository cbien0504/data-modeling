"""Load: write to the staging schema, then publish to `dwh` atomically.

Spark's JDBC writer cannot run a multi-table transaction, so the pipeline writes each
table into `stg` first, then swaps the content of every `dwh` table inside ONE
PostgreSQL transaction. Readers see either the previous load or the new one, never a
half-loaded warehouse, and the foreign keys between facts and dimensions hold.
"""
import json
import logging

import psycopg2
from psycopg2 import sql
from pyspark.sql import DataFrame

from etl.config import DWH, now_vn
from etl.jdbc import write_table

log = logging.getLogger(__name__)

# Dimensions before facts (foreign keys)
PUBLISH_ORDER = ["dim_date", "dim_customer", "dim_product", "dim_payment_method", "dim_order_status",
                 "fact_sales", "fact_payments"]


def write_staging(tables: dict[str, DataFrame]) -> None:
    for name in PUBLISH_ORDER:
        write_table(tables[name], DWH, f"stg.{name}")
        log.info("staged   stg.%s", name)


def publish(tables: dict[str, DataFrame]) -> None:
    conn = psycopg2.connect(**DWH.connect_kwargs())
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql.SQL("TRUNCATE {}").format(
                sql.SQL(", ").join(sql.Identifier("dwh", t) for t in PUBLISH_ORDER)))
            for name in PUBLISH_ORDER:
                cols = sql.SQL(", ").join(map(sql.Identifier, tables[name].columns))
                cur.execute(sql.SQL("INSERT INTO {target} ({cols}) SELECT {cols} FROM {source}").format(
                    target=sql.Identifier("dwh", name), cols=cols, source=sql.Identifier("stg", name)))
                log.info("publish  dwh.%-20s %8d rows", name, cur.rowcount)
        conn.autocommit = True
        with conn.cursor() as cur:
            for name in PUBLISH_ORDER:
                cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier("dwh", name)))
    finally:
        conn.close()


def log_run(run_id: str, run_date, started_at, status: str, metrics: dict, error: str | None = None) -> None:
    conn = psycopg2.connect(**DWH.connect_kwargs())
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit.etl_run_log (run_id, run_date, started_at, finished_at, status, metrics, error) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (run_id, run_date, started_at, now_vn(), status, json.dumps(metrics, default=str), error))
    finally:
        conn.close()
