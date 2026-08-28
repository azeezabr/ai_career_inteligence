from pyspark.sql import SparkSession, functions as F

CATALOG = "skill_iq_catalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
SOURCE_SYSTEM = "theirstack"


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def ensure_gold_schema(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")


def silver_table(name: str) -> str:
    return f"{CATALOG}.{SILVER_SCHEMA}.{name}"


def gold_table(name: str) -> str:
    return f"{CATALOG}.{GOLD_SCHEMA}.{name}"


def with_audit_cols(df):
    """business_date = the ETL run date this Gold row was computed on (lineage/debugging),
    distinct from posting_date, which is the row's actual business grain date."""
    return df.withColumn("business_date", F.current_date()).withColumn("source_system", F.lit(SOURCE_SYSTEM))
