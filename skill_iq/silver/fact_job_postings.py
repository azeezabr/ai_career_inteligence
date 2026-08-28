"""
Silver Layer :: fact job_postings
----------------------------------
Grain: one row per job posting (job_id). Holds FKs to company/location/role
dims; skills and certifications are many-to-many and live in the bridge
tables (bridge_job_skills, bridge_job_certifications) built in
bridge_tables.py.
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, bronze_postings, surrogate_key, CATALOG, SILVER_SCHEMA
from silver.role_classifier import classify_role_column

TABLE = f"{CATALOG}.{SILVER_SCHEMA}.job_postings"


def build(spark):
    postings = bronze_postings(spark)

    fact = (
        postings
        .withColumn("company_id", F.coalesce(F.col("co_id"), surrogate_key("co_domain", "co_name")))
        .withColumn("location_id", F.coalesce(F.col("loc_id").cast("string"), surrogate_key("location")))
        .withColumn("role_id", surrogate_key(classify_role_column()))
        .select(
            "job_id",
            F.col("job_title").alias("title"),
            "url",
            "company_id",
            "location_id",
            "role_id",
            F.col("date_posted").alias("posted_at"),
            F.col("remote").alias("is_remote"),
            F.to_date("_bronze_processed_at").alias("_batch_date"),
            F.col("_raw_ingested_at").alias("_ingested_at"),
        )
        .dropDuplicates(["job_id"])
    )

    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("_batch_date")
        .saveAsTable(TABLE)
    )


spark = get_spark()
ensure_silver_schema(spark)
build(spark)
