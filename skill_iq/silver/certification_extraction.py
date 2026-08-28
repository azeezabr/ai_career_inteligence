"""
Silver Layer :: certification_extraction (staging)
------------------------------------------------------
Runs LLM certification extraction EXACTLY ONCE per distinct posting and
caches the result as (job_id, cert_name, cert_provider) rows in a staging
table. `dim_certifications.py` (dedup to build the dim) and
`bridge_tables.py`'s job<->certification bridge both read FROM this table
instead of calling the LLM themselves.

This exists because the two consumers used to each independently call
`apply_certification_extraction` on the same postings -- silently doubling
API cost and runtime with no benefit. Run this task before both.

Provider (OpenAI by default, or Anthropic) is configured in
llm_enrichment.py via the LLM_PROVIDER env var -- see that module's
docstring. The API key itself is fetched automatically from Databricks
Secrets (scope "skill-iq-secrets", key "openai-api-key" or
"anthropic-api-key" depending on provider) via `load_llm_api_key()` when
this runs -- no manual `dbutils.secrets.get()` cell needed, since this is
a plain .py file (spark_python_task), not a notebook, and doesn't get
`dbutils` auto-injected the way a notebook would.
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, bronze_postings, load_llm_api_key, CATALOG, SILVER_SCHEMA
from silver.llm_enrichment import apply_certification_extraction

# leading underscore: internal staging table, not part of the published
# Silver table design -- not meant to be queried by the API/Gold layer.
TABLE = f"{CATALOG}.{SILVER_SCHEMA}._staging_job_certifications"


def build(spark):
    postings = bronze_postings(spark).select("job_id", "description").dropDuplicates(["job_id"])
    extracted = apply_certification_extraction(postings)

    staging = (
        extracted.select("job_id", F.explode_outer("certifications").alias("c"))
        .where(F.col("c.name").isNotNull())
        .select(
            "job_id",
            F.trim(F.col("c.name")).alias("cert_name"),
            F.trim(F.col("c.provider")).alias("cert_provider"),
        )
        .dropDuplicates(["job_id", "cert_name"])
    )

    (
        staging.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_silver_schema(spark)
    load_llm_api_key(spark)  # fetches OPENAI_API_KEY/ANTHROPIC_API_KEY from Databricks Secrets automatically
    build(spark)
