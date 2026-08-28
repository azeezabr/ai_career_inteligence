"""
Gold Layer :: cert_daily
--------------------------
Schema matches the solution doc's Table Designs diagram EXACTLY:
cert_daily_id, business_date, cert_name, cert_provider, industry,
posting_date, posting_appear_count, total_posting_count, source_system.
No role dimension -- same reasoning as skill_daily.py: GET
/api/certifications' role-scoped occurrence_pct is computed by the API
directly from gold.job_posting_lines instead. See api/app/db/queries.py.

Grain: one row per (posting_date, industry, cert_name).
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from gold._common import get_spark, ensure_gold_schema, silver_table, gold_table, with_audit_cols

TABLE = gold_table("cert_daily")


def build(spark):
    jp = spark.table(silver_table("job_postings")).alias("jp")
    companies = spark.table(silver_table("companies")).alias("c")

    postings = (
        jp.join(companies, F.col("jp.company_id") == F.col("c.company_id"), "left")
        .select(
            F.col("jp.job_id").alias("job_id"),
            F.to_date(F.col("jp.date_posted")).alias("posting_date"),
            F.coalesce(F.col("c.industry"), F.lit("Unknown")).alias("industry"),
        )
    )
    bridge = spark.table(silver_table("bridge_job_certifications"))
    certs = spark.table(silver_table("certifications"))

    postings_with_cert = (
        postings.join(bridge, "job_id")
        .join(certs, "cert_id")
        .select(
            "job_id", "posting_date", "industry",
            F.col("name").alias("cert_name"),
            F.col("provider").alias("cert_provider"),
        )
    )

    appear = postings_with_cert.groupBy(
        "posting_date", "industry", "cert_name", "cert_provider"
    ).agg(F.countDistinct("job_id").alias("posting_appear_count"))

    totals = postings.groupBy("posting_date", "industry").agg(
        F.countDistinct("job_id").alias("total_posting_count")
    )

    result = (
        appear.join(totals, ["posting_date", "industry"])
        .withColumn("cert_daily_id", F.md5(F.concat_ws("||", "posting_date", "industry", "cert_name")))
    )
    result = with_audit_cols(result).select(
        "cert_daily_id", "business_date", "cert_name", "cert_provider", "industry",
        "posting_date", "posting_appear_count", "total_posting_count", "source_system",
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("posting_date")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_gold_schema(spark)
    build(spark)
