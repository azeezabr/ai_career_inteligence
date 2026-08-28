"""
Silver Layer :: dim certifications -- name + issuing provider.

Reads from `silver._staging_job_certifications` (written once by
certification_extraction.py) rather than calling Claude itself -- run
certification_extraction.py first. This is the one dim ultimately backed
by Claude, since Bronze has no `certification_slugs` or equivalent
structured field (unlike skills, which come straight from
`technology_slugs`/`keyword_slugs`).
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, surrogate_key, with_audit_cols, CATALOG, SILVER_SCHEMA

TABLE = f"{CATALOG}.{SILVER_SCHEMA}.certifications"
STAGING_TABLE = f"{CATALOG}.{SILVER_SCHEMA}._staging_job_certifications"


def build(spark):
    staging = spark.table(STAGING_TABLE)

    certifications = (
        staging.select(
            F.col("cert_name").alias("name"),
            F.col("cert_provider").alias("provider"),
        )
        .dropDuplicates(["name"])
        .withColumn("cert_id", surrogate_key(F.lower(F.col("name"))))
    )
    certifications = with_audit_cols(certifications).select(
        "cert_id", "name", "provider", "created_date", "source_system"
    )

    (
        certifications.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_silver_schema(spark)
    build(spark)
