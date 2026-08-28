"""
Gold Layer :: job_posting_lines
--------------------------------
Not itemized in the written "Table Designs" section but shown in the Data
Flow Architecture diagram under Gold, and needed to back GET /api/jobs
(the endpoint returns per-posting fields like `description`, `url`,
`hybrid` that skill_daily/cert_daily/role_daily don't carry).

Fully denormalized, one row per job posting, with role/industry attached
for filtering and skill/cert name arrays for the skill=/certificate=
query params on /api/jobs.
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from gold._common import get_spark, ensure_gold_schema, silver_table, gold_table, with_audit_cols

TABLE = gold_table("job_posting_lines")


def build(spark):
    jp = spark.table(silver_table("job_postings")).alias("jp")
    roles = spark.table(silver_table("roles")).alias("r")
    companies = spark.table(silver_table("companies")).alias("c")
    locations = spark.table(silver_table("locations")).alias("l")
    bridge_skills = spark.table(silver_table("bridge_job_skills"))
    skills = spark.table(silver_table("skills"))
    bridge_certs = spark.table(silver_table("bridge_job_certifications"))
    certs = spark.table(silver_table("certifications"))

    skill_names = (
        bridge_skills.join(skills, "skill_id")
        .groupBy("job_id")
        .agg(F.collect_set("name").alias("skill_names"))
    )
    cert_names = (
        bridge_certs.join(certs, "cert_id")
        .groupBy("job_id")
        .agg(F.collect_set("name").alias("cert_names"))
    )

    lines = (
        jp.join(roles, F.col("jp.role_id") == F.col("r.role_id"))
        .join(companies, F.col("jp.company_id") == F.col("c.company_id"), "left")
        .join(locations, F.col("jp.location_id") == F.col("l.location_id"), "left")
        .join(skill_names, F.col("jp.job_id") == skill_names.job_id, "left")
        .join(cert_names, F.col("jp.job_id") == cert_names.job_id, "left")
        .select(
            F.col("jp.job_id").alias("job_id"),
            F.col("jp.job_title").alias("title"),
            F.col("jp.job_description").alias("description"),
            F.col("jp.url").alias("url"),
            F.col("c.name").alias("company"),
            F.col("c.domain").alias("company_domain"),
            F.coalesce(F.col("c.industry"), F.lit("Unknown")).alias("industry"),
            F.col("r.name").alias("role_name"),
            F.col("jp.seniority").alias("seniority"),
            F.col("jp.remote").alias("remote"),
            F.col("jp.hybrid").alias("hybrid"),
            F.col("jp.is_open").alias("is_open"),
            F.col("jp.min_salary_usd").alias("min_salary_usd"),
            F.col("jp.max_salary_usd").alias("max_salary_usd"),
            F.col("jp.avg_salary_usd").alias("avg_salary_usd"),
            F.col("jp.date_posted").alias("date_posted"),
            F.to_date(F.col("jp.date_posted")).alias("posting_date"),
            F.col("l.country_name").alias("country"),
            F.coalesce(F.col("skill_names"), F.array()).alias("skill_names"),
            F.coalesce(F.col("cert_names"), F.array()).alias("cert_names"),
        )
    )
    lines = with_audit_cols(lines)

    (
        lines.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("posting_date")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_gold_schema(spark)
    build(spark)
