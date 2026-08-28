"""
Silver Layer :: dim skills
--------------------------
Real Bronze schema note: `technology_slugs` and `keyword_slugs` are already
present as source data -- no Claude extraction needed anymore. Per your
call: `technology_slugs` -> category "technology", `keyword_slugs` ->
category "methodology" (broader/methodology terms). No "platform" values
are produced by this split (the 3-value category enum still allows it --
retag specific slugs to "platform" here later if you want that
distinction, e.g. cloud provider slugs like "aws"/"gcp"/"azure").

Schema matches the Silver table design: skill_id, name, description,
category, created_date, updated_date, source_system. `description` is
left NULL; `name` is a human-readable label derived from the slug
(hyphens -> spaces, title-cased) since Bronze only gives us slugs, not
display names.
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, bronze_postings, surrogate_key, with_audit_cols, CATALOG, SILVER_SCHEMA

TABLE = f"{CATALOG}.{SILVER_SCHEMA}.skills"


def _slug_to_name(col):
    return F.initcap(F.regexp_replace(col, "[-_]", " "))


def build(spark):
    postings = bronze_postings(spark)

    tech = (
        postings.select(F.explode_outer("technology_slugs").alias("slug"))
        .where(F.col("slug").isNotNull())
        .select("slug", F.lit("technology").alias("category"))
    )
    keyword = (
        postings.select(F.explode_outer("keyword_slugs").alias("slug"))
        .where(F.col("slug").isNotNull())
        .select("slug", F.lit("methodology").alias("category"))
    )

    # a slug appearing in both lists is unusual but not impossible --
    # technology wins the category tie (more specific classification)
    combined = tech.unionByName(keyword).groupBy("slug").agg(F.max("category").alias("category"))

    skills = (
        combined.withColumn("name", _slug_to_name(F.col("slug")))
        .withColumn("description", F.lit(None).cast("string"))
        .withColumn("skill_id", surrogate_key(F.lower(F.col("slug"))))
    )
    skills = with_audit_cols(skills).withColumn("updated_date", F.col("created_date")).select(
        "skill_id", "name", "description", "category", "created_date", "updated_date", "source_system"
    )

    (
        skills.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_silver_schema(spark)
    build(spark)
