"""Generic Slowly Changing Dimension merge (Type 2 with Type 1 attributes).

    type2_cols : a change creates a new version (old one is closed with valid_to)
    type1_cols : a change overwrites the value on every version of the key

Each version covers the half-open interval [valid_from, valid_to). The first version
of a key starts at LOW_TS so facts older than the first load still find it; the open
version ends at HIGH_TS. A fact is matched to the version valid at its event time.
"""
import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

LOW_TS = "1900-01-01 00:00:00"
HIGH_TS = "9999-12-31 00:00:00"


def low_ts():
    return F.to_timestamp(F.lit(LOW_TS))


def high_ts():
    return F.to_timestamp(F.lit(HIGH_TS))


def row_hash(cols: list[str]):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)


def scd2_merge(source: DataFrame, existing: DataFrame | None, *, natural_key: str, surrogate_key: str,
               type2_cols: list[str], type1_cols: list[str], effective_col: str) -> tuple[DataFrame, dict]:
    """Merge the latest source snapshot into the existing dimension; returns (full dimension, stats)."""
    attr_cols = [*type2_cols, *type1_cols]
    out_cols = [surrogate_key, natural_key, *attr_cols, "row_hash", "valid_from", "valid_to", "is_current"]
    src = source.select(natural_key, *attr_cols, effective_col).withColumn("row_hash", row_hash(type2_cols))

    if existing is None or existing.isEmpty():
        dim = (src.withColumn(surrogate_key, F.row_number().over(Window.orderBy(natural_key)))
                  .withColumn("valid_from", low_ts())
                  .withColumn("valid_to", high_ts())
                  .withColumn("is_current", F.lit(True))
                  .select(out_cols))
        return dim, {"mode": "initial", "new_keys": src.count(), "changed_keys": 0}

    existing = existing.select(out_cols)
    max_key = existing.agg(F.max(surrogate_key)).first()[0] or 0
    history = existing.filter(~F.col("is_current"))
    current = existing.filter(F.col("is_current"))

    joined = src.alias("s").join(current.alias("c"),
                                 F.col(f"s.{natural_key}") == F.col(f"c.{natural_key}"), "full_outer")
    in_src = F.col(f"s.{natural_key}").isNotNull()
    in_dim = F.col(f"c.{natural_key}").isNotNull()
    changed = in_src & in_dim & ~F.col("s.row_hash").eqNullSafe(F.col("c.row_hash"))
    # Guard against clock skew: a new version never starts before the one it replaces
    effective = F.greatest(F.col(f"s.{effective_col}"), F.col("c.valid_from"))

    # Unchanged keys, and keys deleted at the source (kept as-is for history)
    unchanged = joined.filter(in_dim & ~changed).select("c.*")
    expired = joined.filter(changed).select(
        *[F.col(f"c.{c}") for c in out_cols if c not in ("valid_to", "is_current")],
        effective.alias("valid_to"),
        F.lit(False).alias("is_current"),
    )
    payload = [F.col(f"s.{c}") for c in (natural_key, *attr_cols, "row_hash")]
    new_versions = joined.filter(changed).select(*payload, effective.alias("valid_from"))
    new_keys = joined.filter(in_src & ~in_dim).select(*payload, low_ts().alias("valid_from"))
    inserts = (new_versions.unionByName(new_keys)
               .withColumn(surrogate_key,
                           F.lit(max_key) + F.row_number().over(Window.orderBy(natural_key, "valid_from")))
               .withColumn("valid_to", high_ts())
               .withColumn("is_current", F.lit(True)))

    dim = (history
           .unionByName(unchanged.select(out_cols))
           .unionByName(expired.select(out_cols))
           .unionByName(inserts.select(out_cols)))

    if type1_cols:
        latest = src.select(F.col(natural_key).alias("_t1_key"),
                            *[F.col(c).alias(f"_t1_{c}") for c in type1_cols])
        dim = dim.join(latest, dim[natural_key] == latest["_t1_key"], "left")
        dim = dim.select(*[
            F.when(F.col("_t1_key").isNotNull(), F.col(f"_t1_{c}")).otherwise(F.col(c)).alias(c)
            if c in type1_cols else F.col(c)
            for c in out_cols
        ])

    stats = {"mode": "incremental", "new_keys": new_keys.count(), "changed_keys": new_versions.count()}
    return dim, stats
