"""
Gold Layer :: role_daily
--------------------------
Schema matches the solution doc's Table Designs diagram EXACTLY:
role_daily_id, business_date, role_name, job_postings_count,
open_roles_count, min_salary, max_salary, median_salary, source_system.

IMPORTANT: this schema has NO posting_date or industry column at all --
so despite the "_daily" name, this is a cumulative AS-OF-`business_date`
snapshot per role (all postings ever seen for that role, as of whenever
this Gold job last ran), not a genuine per-day time series. It cannot
support date_range windowing, prior-period deltas, or industry filtering.

GET /api/skills' metrics block (job_postings, open_roles,
median_salary_usd, all date_range/industry-scoped, with prior-period
deltas) is therefore computed by the API directly from
gold.job_posting_lines instead of this table. See api/app/db/queries.py.
This table still gets built to match the documented design; it's just not
what the API's role-scoped metrics query.

Grain: one row per (business_date, role_name).
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from gold._common import get_spark, ensure_gold_schema, silver_table, gold_table, with_audit_cols

TABLE = gold_table("role_daily")


def build(spark):
    jp = spark.table(silver_table("job_postings")).alias("jp")
    roles = spark.table(silver_table("roles")).alias("r")

    postings = (
        jp.join(roles, F.col("jp.role_id") == F.col("r.role_id"))
        .select(
            F.col("jp.job_id").alias("job_id"),
            F.col("r.name").alias("role_name"),
            F.col("jp.is_open").alias("is_open"),
            F.col("jp.min_salary_usd").alias("min_salary_usd"),
            F.col("jp.max_salary_usd").alias("max_salary_usd"),
            F.col("jp.avg_salary_usd").alias("avg_salary_usd"),
        )
    )

    result = postings.groupBy("role_name").agg(
        F.countDistinct("job_id").alias("job_postings_count"),
        F.countDistinct(F.when(F.col("is_open"), F.col("job_id"))).alias("open_roles_count"),
        F.min("min_salary_usd").alias("min_salary"),
        F.max("max_salary_usd").alias("max_salary"),
        F.expr("percentile_approx(avg_salary_usd, 0.5)").alias("median_salary"),
    )
    result = with_audit_cols(result).withColumn(
        "role_daily_id", F.md5(F.concat_ws("||", F.col("business_date").cast("string"), "role_name"))
    ).select(
        "role_daily_id", "business_date", "role_name",
        "job_postings_count", "open_roles_count", "min_salary", "max_salary", "median_salary", "source_system",
    )

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_gold_schema(spark)
    build(spark)
