"""
Silver Layer :: bridge tables
------------------------------
Many-to-many links between job_postings and skills/certifications/locations.
Schema per the table design (each): job_id, {skill_id|cert_id|location_id},
created_date, source_system.

Real Bronze schema note: job<->skills is now built directly from
technology_slugs/keyword_slugs (matching dim_skills.py's slug->skill_id
derivation exactly), and job<->locations directly from loc_id (matching
dim_locations.py). job<->certifications reads from
`silver._staging_job_certifications` (written once by
certification_extraction.py) rather than calling Claude itself -- run
certification_extraction.py before this script.
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, bronze_postings, surrogate_key, with_audit_cols, CATALOG, SILVER_SCHEMA

BRIDGE_SKILLS = f"{CATALOG}.{SILVER_SCHEMA}.bridge_job_skills"
BRIDGE_CERTS = f"{CATALOG}.{SILVER_SCHEMA}.bridge_job_certifications"
BRIDGE_LOCATIONS = f"{CATALOG}.{SILVER_SCHEMA}.bridge_job_locations"
STAGING_CERTS = f"{CATALOG}.{SILVER_SCHEMA}._staging_job_certifications"


def build_bridge_job_skills(spark):
    postings = bronze_postings(spark)

    tech = postings.select("job_id", F.explode_outer("technology_slugs").alias("slug"))
    keyword = postings.select("job_id", F.explode_outer("keyword_slugs").alias("slug"))

    bridge = (
        tech.unionByName(keyword)
        .where(F.col("slug").isNotNull())
        .withColumn("skill_id", surrogate_key(F.lower(F.col("slug"))))
        .select("job_id", "skill_id")
        .dropDuplicates()
    )
    _overwrite(bridge, BRIDGE_SKILLS)


def build_bridge_job_certifications(spark):
    staging = spark.table(STAGING_CERTS)
    bridge = (
        staging.withColumn("cert_id", surrogate_key(F.lower(F.col("cert_name"))))
        .select("job_id", "cert_id")
        .dropDuplicates()
    )
    _overwrite(bridge, BRIDGE_CERTS)


def build_bridge_job_locations(spark):
    postings = bronze_postings(spark)
    bridge = (
        postings.where(F.col("location").isNotNull())
        .withColumn("location_id", F.coalesce(F.col("loc_id").cast("string"), surrogate_key("location")))
        .select("job_id", "location_id")
        .dropDuplicates()
    )
    _overwrite(bridge, BRIDGE_LOCATIONS)


def _overwrite(df, table):
    df = with_audit_cols(df)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


if __name__ == "__main__":
    spark = get_spark()
    ensure_silver_schema(spark)
    build_bridge_job_skills(spark)
    build_bridge_job_certifications(spark)
    build_bridge_job_locations(spark)
