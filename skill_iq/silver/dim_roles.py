"""
Silver Layer :: dim roles
--------------------------
`roles` is a small, controlled taxonomy (NOT one row per distinct posting
title) -- it must match the fixed enum the frontend/API contract expects:
data_engineer | data_scientist | ml_engineer | data_analyst | software_engineer
(+ "other" as a catch-all for postings Claude can't confidently classify).

job_postings.role_id references this table; the per-posting classification
into one of these values happens in role_classifier.py (a rule-based
keyword match, not Claude -- see that module's docstring). This job just
seeds/maintains the dimension itself (schema: role_id, name, created_date,
source_system per the Silver table design).
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, surrogate_key, with_audit_cols, CATALOG, SILVER_SCHEMA

TABLE = f"{CATALOG}.{SILVER_SCHEMA}.roles"

CANONICAL_ROLES = [
    "data_engineer", "data_scientist", "ml_engineer", "data_analyst", "software_engineer", "other",
]


def build(spark):
    seed = spark.createDataFrame([(r,) for r in CANONICAL_ROLES], ["name"])
    roles = with_audit_cols(seed.withColumn("role_id", surrogate_key("name"))).select(
        "role_id", "name", "created_date", "source_system"
    )

    (
        roles.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_silver_schema(spark)
    build(spark)
