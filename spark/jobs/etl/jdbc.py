"""Thin wrappers around Spark's JDBC data source."""
from pyspark.sql import DataFrame, SparkSession

from etl.config import PgConfig


def _reader(spark: SparkSession, pg: PgConfig):
    return (spark.read.format("jdbc")
            .option("url", pg.jdbc_url)
            .option("user", pg.user)
            .option("password", pg.password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "10000"))


def read_table(spark: SparkSession, pg: PgConfig, table: str, **options) -> DataFrame:
    reader = _reader(spark, pg).option("dbtable", table)
    for key, value in options.items():
        reader = reader.option(key, str(value))
    return reader.load()


def read_query(spark: SparkSession, pg: PgConfig, query: str) -> DataFrame:
    return _reader(spark, pg).option("query", query).load()


def write_table(df: DataFrame, pg: PgConfig, table: str, mode: str = "overwrite") -> None:
    (df.write.format("jdbc")
       .option("url", pg.jdbc_url)
       .option("user", pg.user)
       .option("password", pg.password)
       .option("driver", "org.postgresql.Driver")
       .option("dbtable", table)
       .option("batchsize", "5000")
       .mode(mode)
       .save())
