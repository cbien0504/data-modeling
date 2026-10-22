"""Runtime configuration, read from environment variables (see docker-compose.yml)."""
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

# Vietnam has no DST, so a fixed offset is exact and avoids depending on OS tzdata.
VN_TZ = timezone(timedelta(hours=7))

LAKE_PATH = os.getenv("LAKE_PATH", "/opt/lake")
OLTP_SCHEMA = "sales"


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    db: str
    user: str
    password: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}?reWriteBatchedInserts=true"

    def connect_kwargs(self) -> dict:
        return {"host": self.host, "port": self.port, "dbname": self.db,
                "user": self.user, "password": self.password}


def _pg(prefix: str, host: str, db: str, user: str) -> PgConfig:
    return PgConfig(
        host=os.getenv(f"{prefix}_HOST", host),
        port=int(os.getenv(f"{prefix}_PORT", "5432")),
        db=os.getenv(f"{prefix}_DB", db),
        user=os.getenv(f"{prefix}_USER", user),
        password=os.getenv(f"{prefix}_PASSWORD", user),
    )


OLTP = _pg("OLTP", host="oltp-db", db="shop_oltp", user="oltp")
DWH = _pg("DWH", host="dwh-db", db="shop_dwh", user="dwh")


def now_vn() -> datetime:
    return datetime.now(VN_TZ).replace(tzinfo=None, microsecond=0)
