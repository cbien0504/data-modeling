"""Helpers shared by the data generator and the change simulator."""
import io
import logging
import os
import unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2

TZ = ZoneInfo("Asia/Ho_Chi_Minh")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("datagen")


def connect():
    return psycopg2.connect(
        host=os.getenv("OLTP_HOST", "oltp-db"),
        port=int(os.getenv("OLTP_PORT", "5432")),
        dbname=os.getenv("OLTP_DB", "shop_oltp"),
        user=os.getenv("OLTP_USER", "oltp"),
        password=os.getenv("OLTP_PASSWORD", "oltp"),
    )


def now_vn() -> datetime:
    """Current wall-clock time in Vietnam, as a naive datetime (matches TIMESTAMP columns)."""
    return datetime.now(TZ).replace(tzinfo=None, microsecond=0)


def strip_accents(text: str) -> str:
    text = text.replace("đ", "d").replace("Đ", "D")
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def round_vnd(amount: float, step: int = 1000) -> int:
    return max(step, int(round(amount / step)) * step)


def _copy_value(value) -> str:
    if value is None:
        return r"\N"
    return str(value).replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


def copy_rows(cur, table: str, columns: list[str], rows) -> None:
    """Bulk insert with COPY — much faster than INSERT for large volumes."""
    buf = io.StringIO()
    for row in rows:
        buf.write("\t".join(_copy_value(v) for v in row))
        buf.write("\n")
    buf.seek(0)
    cur.copy_expert(f"COPY sales.{table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT text)", buf)


def reset_sequence(cur, table: str, column: str) -> None:
    cur.execute(
        f"SELECT setval(pg_get_serial_sequence('sales.{table}', '{column}'), "
        f"COALESCE((SELECT max({column}) FROM sales.{table}), 0) + 1, false)"
    )
