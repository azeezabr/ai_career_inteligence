"""
Gold Layer :: skill_daily
---------------------------
Schema matches the solution doc's Table Designs diagram EXACTLY:
skills_daily_id, business_date, skill_name, skill_category, industry,
posting_date, posting_appear_count, total_posting_count, source_system.
No role dimension -- this table is global across roles per (posting_date,
industry, skill_name).

IMPORTANT: because this grain has no role dimension, it CANNOT answer
"what % of Data Engineer postings mention Python" -- that information is
destroyed the moment postings from every role get aggregated together.
GET /api/skills' role-scoped occurrence_pct/trend is therefore computed
by the API directly from gold.job_posting_lines (which retains role_name
per posting), NOT from this table. See api/app/db/queries.py.

This table still gets built (matches the documented design, may serve
other reporting needs), it's just not what the role-scoped API endpoints
query.

Grain: one row per (posting_date, industry, skill_name).
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from gold._common import get_spark, ensure_gold_schema, silver_table, gold_table, with_audit_cols

TABLE = gold_table("skill_daily")


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
    bridge = spark.table(silver_table("bridge_job_skills"))
    skills = spark.table(silver_table("skills"))

    postings_with_skill = (
        postings.join(bridge, "job_id")
        .join(skills, "skill_id")
        .select(
            "job_id", "posting_date", "industry",
            F.col("name").alias("skill_name"),
            F.col("category").alias("skill_category"),
        )
    )

    appear = postings_with_skill.groupBy(
        "posting_date", "industry", "skill_name", "skill_category"
    ).agg(F.countDistinct("job_id").alias("posting_appear_count"))

    totals = postings.groupBy("posting_date", "industry").agg(
        F.countDistinct("job_id").alias("total_posting_count")
    )

    result = (
        appear.join(totals, ["posting_date", "industry"])
        .withColumn("skills_daily_id", F.md5(F.concat_ws("||", "posting_date", "industry", "skill_name")))
    )
    result = with_audit_cols(result).select(
        "skills_daily_id", "business_date", "skill_name", "skill_category", "industry",
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
