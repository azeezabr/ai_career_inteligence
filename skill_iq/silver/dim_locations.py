"""
Silver Layer :: dim locations
--------------------------------
Schema per the table design: location_id, name, state, state_code,
country_name, country_code, continent_code, continent_name, latitude,
longitude, source_system.

Real Bronze schema note: full geo enrichment (`loc_id`, `loc_admin1_name`,
`loc_continent`, `loc_country_name`, `latitude`, `longitude`, etc.) is
already present as source data (a GeoNames-style reference join theirstack
does upstream) -- no Claude normalization needed anymore. `location_id`
uses Bronze's `loc_id` directly when present, falling back to a hash of
the raw location string for the rare row missing it.

`continent_code` has no direct Bronze column (`loc_continent` is a name
like "North America", not a 2-letter code) -- derived via a small lookup
in `_CONTINENT_CODES` below.
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, bronze_postings, surrogate_key, with_audit_cols, CATALOG, SILVER_SCHEMA

TABLE = f"{CATALOG}.{SILVER_SCHEMA}.locations"

_CONTINENT_CODES = {
    "North America": "NA", "South America": "SA", "Europe": "EU",
    "Asia": "AS", "Africa": "AF", "Oceania": "OC", "Antarctica": "AN",
}


def build(spark):
    postings = bronze_postings(spark)

    continent_map = F.create_map(*[F.lit(x) for pair in _CONTINENT_CODES.items() for x in pair])

    locations = (
        postings.where(F.col("location").isNotNull())
        .select(
            F.coalesce(F.col("loc_id").cast("string"), surrogate_key("location")).alias("location_id"),
            F.coalesce(F.col("loc_display_name"), F.col("location")).alias("name"),
            F.col("loc_admin1_name").alias("state"),
            F.coalesce(F.col("loc_admin1_code"), F.col("state_code")).alias("state_code"),
            F.coalesce(F.col("loc_country_name"), F.col("country")).alias("country_name"),
            F.col("country_code").alias("country_code"),
            F.coalesce(continent_map[F.col("loc_continent")], F.lit(None)).alias("continent_code"),
            F.col("loc_continent").alias("continent_name"),
            F.col("latitude"),
            F.col("longitude"),
        )
        .dropDuplicates(["location_id"])
    )
    locations = with_audit_cols(locations).select(
        "location_id", "name", "state", "state_code", "country_name", "country_code",
        "continent_code", "continent_name", "latitude", "longitude", "source_system",
    )

    (
        locations.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TABLE)
    )


if __name__ == "__main__":
    spark = get_spark()
    ensure_silver_schema(spark)
    build(spark)
