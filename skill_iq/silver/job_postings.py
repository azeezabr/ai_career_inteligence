"""
Silver Layer :: job_postings
------------------------------
Grain: one row per job posting (job_id). Schema per the table design:
job_id, role_id, company_id, location_id, job_title, normalized_title,
job_description, seniority, date_posted, date_reposted, closed_at,
is_open, remote, hybrid, min_salary_usd, max_salary_usd, avg_salary_usd,
salary_currency, created_date, created_by, source_system.

Real Bronze schema note: salary fields (min/max/avg_annual_salary_usd),
`seniority`, `remote`, `hybrid`, `reposted`/`date_reposted`, and `closed_at`
are all real source columns now -- no assumptions or Claude enrichment
needed for any of them. `role_id` is the one derived field, via the
rule-based classifier in role_classifier.py (not Claude -- see that
module's docstring for why).

`job_id` is a native bigint from Bronze (theirstack's own posting ID), not
a hash -- carried straight through.
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, bronze_postings, surrogate_key, with_audit_cols, CATALOG, SILVER_SCHEMA
from silver.role_classifier import classify_role_column

TABLE = f"{CATALOG}.{SILVER_SCHEMA}.job_postings"


def build(spark):
    postings = bronze_postings(spark)

    fact = (
        postings
        .withColumn("company_id", F.coalesce(F.col("co_id"), surrogate_key("co_domain", "co_name")))
        .withColumn("location_id", F.coalesce(F.col("loc_id").cast("string"), surrogate_key("location")))
        .withColumn("role_id", surrogate_key(classify_role_column()))
        .withColumn("is_open", F.col("closed_at").isNull())
        .withColumn("created_by", F.lit("lakeflow_job"))
        .withColumn("_batch_date", F.to_date(F.col("discovered_at")))
        .select(
            "job_id", "role_id", "company_id", "location_id",
            F.col("job_title"),
            F.col("normalized_title"),
            F.col("description").alias("job_description"),
            F.col("url"),
            F.col("seniority"),
            F.col("date_posted"),
            F.col("date_reposted"),
            F.col("closed_at"),
            "is_open",
            F.col("remote"),
            F.col("hybrid"),
            F.col("min_annual_salary_usd").alias("min_salary_usd"),
            F.col("max_annual_salary_usd").alias("max_salary_usd"),
            F.col("avg_annual_salary_usd").alias("avg_salary_usd"),
            F.col("salary_currency"),
            "created_by", "_batch_date",
        )
        .dropDuplicates(["job_id"])
    )
    fact = with_audit_cols(fact)

    (
        fact.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_batch_date")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_silver_schema(spark)
    build(spark)
