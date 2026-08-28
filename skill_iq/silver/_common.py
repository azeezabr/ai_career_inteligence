"""
Shared utilities for all Silver-layer transforms.

Bronze (`bronze.job_postings`) is now the flattened, typed layer -- one row
per posting already, ~90 columns (see bronze/ingest_job_postings.py). This
module used to do the explode/flatten work itself; now it's just a thin
"read Bronze, optionally filter incrementally" passthrough plus the shared
surrogate-key/audit-column helpers every Silver job uses.
"""

from pyspark.sql import SparkSession, DataFrame, functions as F

CATALOG = "skill_iq_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
SOURCE_SYSTEM = "theirstack"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.job_postings"


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_dbutils(spark: SparkSession):
    """Plain .py files run as a Databricks Job spark_python_task do NOT get
    `dbutils` auto-injected into scope the way notebooks do -- it has to be
    constructed explicitly. Centralized here so every script that needs a
    secret uses the same pattern instead of reinventing it."""
    from pyspark.dbutils import DBUtils
    return DBUtils(spark)


# Secret scope/key naming convention used across this project -- keep these
# in sync with whatever `databricks secrets create-scope`/`put-secret`
# commands you actually ran.
SECRET_SCOPE = "skill-iq-secrets"
SECRET_KEYS = {"openai": "openai-api-key", "anthropic": "anthropic-api-key"}


def load_llm_api_key(spark: SparkSession) -> None:
    """Fetches the API key for whichever provider silver/llm_enrichment.py
    is configured to use (LLM_PROVIDER env var, default "openai") from
    Databricks Secrets, and sets it into os.environ so llm_enrichment's
    _client() can pick it up. No-ops if the env var is already set (e.g.
    if you set it another way, or are running this outside Databricks
    entirely for a local test)."""
    import os
    from silver.llm_enrichment import PROVIDER

    env_var = "ANTHROPIC_API_KEY" if PROVIDER == "anthropic" else "OPENAI_API_KEY"
    if os.environ.get(env_var):
        return  # already set -- don't overwrite

    secret_key = SECRET_KEYS.get(PROVIDER, SECRET_KEYS["openai"])
    dbutils = get_dbutils(spark)
    os.environ[env_var] = dbutils.secrets.get(scope=SECRET_SCOPE, key=secret_key)


def ensure_silver_schema(spark: SparkSession) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")


def bronze_postings(spark: SparkSession, incremental_since: str | None = None) -> DataFrame:
    """Reads Bronze as-is. incremental_since (ISO date string) optionally
    limits to postings discovered after that date, for incremental Silver
    refreshes rather than a full recompute."""
    df = spark.table(BRONZE_TABLE)
    if incremental_since:
        df = df.where(F.to_date(F.col("discovered_at")) > F.lit(incremental_since))
    return df


def surrogate_key(*cols):
    """Deterministic MD5-based surrogate key so dims are stable across reruns.
    Accepts column name strings and/or Column expressions interchangeably."""
    resolved = [c if not isinstance(c, str) else F.col(c) for c in cols]
    return F.md5(F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("")) for c in resolved]))


def with_audit_cols(df: DataFrame) -> DataFrame:
    """Adds created_date/source_system columns present on every Silver table per the table design."""
    return (
        df.withColumn("created_date", F.current_timestamp())
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    )
