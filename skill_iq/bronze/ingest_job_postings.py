"""
Bronze Layer :: job_postings (flattened)
-------------------------------------------
Databricks job (Lakeflow Jobs task) that ingests **daily** JSON drops from
the Unity Catalog Volume `skill_iq_catalog.landing.theirstack_raw` and
flattens each posting into the exact ~90-column Bronze schema you provided
(via `df.printSchema()` on the real table). Landing files are one flat job
record per row (JSON Lines) -- there is NO `{"data": [...]}` wrapper.

*** SOURCE FIELD MAPPING: CONFIRMED against a real raw record ***
`_COLUMN_MAP` below was checked against an actual raw JSON record from
`skill_iq_catalog.landing.theirstack_raw`. Real mistakes this caught, in
order of discovery:
1. `job_title` (not `title`) is the real field name.
2. `company_object` is the nested company struct; `company`/`company_domain`
   are SEPARATE flat top-level strings (an earlier version had that backwards).
3. `locations` is an ARRAY of location-reference objects, not a single
   nested struct -- `_path_exists`/`_resolve_column` support numeric path
   segments as array indices (e.g. `"locations.0.id"`) to handle this.
4. There is NO `{"data": [...], "total_count": N}` wrapper on landing
   files -- that shape describes theirstack's API *response envelope*
   only; what's actually written to the volume is already one flat record
   per row. An earlier version of this file exploded a `data` field that
   doesn't exist, which is what threw `UNRESOLVED_COLUMN.WITH_SUGGESTION`
   the first time this ran for real (the suggested columns in that error
   -- `id`, `url`, `cities`, `remote`, `closed_at` -- are exactly the real
   top-level job fields, which is what confirmed the fix).

Paths marked "unconfirmed" in `_COLUMN_MAP`'s comments were present in the
Bronze target schema but absent from that one sample record (commonly
sparse fields like `date_reposted` on a posting that's never been reposted).

Note: the sample record also included a top-level `ingestion_date` field
not present in the confirmed Bronze schema you gave me -- it's not
selected here since it's not in the target schema, but it's available if
you'd like it added (could be a more reliable `_batch_date` signal than
`discovered_at`, worth considering).

Design notes
------------
* Uses Auto Loader (cloudFiles) for incremental, checkpointed ingestion.
* Landing volume is FLAT (no date-partitioned folders, confirmed) --
  `_raw_ingested_at` (when Auto Loader saw the file) and `_bronze_processed_at`
  (when this flatten ran) both use current_timestamp(); the file's own
  modification time isn't otherwise tracked since there's no folder-date to
  reconcile against.
* `_source` is a static literal identifying the ingestion pipeline/source
  system, matching the audit-column pattern already established on this table.

Run as a Databricks Workflow (Lakeflow Jobs) task:
    databricks bundle deploy
    databricks bundle run skill_iq_bronze_ingest_daily
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StringType, LongType, IntegerType, DoubleType, BooleanType,
    DateType, TimestampType, ArrayType, StructType,
)

CATALOG = "skill_iq_catalog"
LANDING_SCHEMA = "landing"
LANDING_VOLUME = "theirstack_raw"
CHECKPOINT_VOLUME = "pipeline_checkpoints"  # sibling volume -- create once:
                                             # CREATE VOLUME IF NOT EXISTS skill_iq_catalog.landing.pipeline_checkpoints;
BRONZE_SCHEMA = "bronze"

TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.job_postings"

LANDING_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{CHECKPOINT_VOLUME}/raw_job_postings/checkpoint/"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{CHECKPOINT_VOLUME}/raw_job_postings/schema/"

SOURCE_NAME = "theirstack"


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def ensure_catalog_objects(spark: SparkSession) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")


def _path_exists(struct_type, dotted_path: str) -> bool:
    """Walks a StructType (or ArrayType of StructType) checking each dotted
    segment is actually present. A purely numeric segment (e.g. the "0" in
    "locations.0.id") is treated as an array index into the CURRENT
    ArrayType's element type. This -- not a try/except around .getField()
    -- is what makes a missing source field degrade to NULL instead of
    crashing the job: Spark's .getField() builds a lazy, unresolved
    expression, so the AnalysisException for a genuinely missing field only
    surfaces later at .select()/analysis time, by which point a try/except
    around the expression-building code has already exited. Checking the
    schema up front, before building any expression, is the only way to
    actually catch this."""
    current = struct_type
    for segment in dotted_path.split("."):
        if isinstance(current, ArrayType):
            if not segment.isdigit():
                return False
            current = current.elementType
            continue
        if not isinstance(current, StructType) or segment not in current.fieldNames():
            return False
        current = current[segment].dataType
    return True


def _resolve_column(struct_type, dotted_path: str, cast):
    """Builds the getField()/getItem() chain starting from a top-level
    column (the first path segment becomes F.col(...)), not from a
    pre-supplied struct column -- landing files have no wrapping struct;
    each row IS the job record directly."""
    if not _path_exists(struct_type, dotted_path):
        return F.lit(None).cast(cast)
    segments = dotted_path.split(".")
    col = F.col(segments[0])
    current_type = struct_type[segments[0]].dataType
    for segment in segments[1:]:
        if isinstance(current_type, ArrayType):
            # NOT col.getItem(index) -- that throws INVALID_ARRAY_INDEX
            # under ANSI mode (Databricks' default) when a row's array is
            # genuinely empty at runtime, even though the schema confirms
            # the array TYPE exists (schema existence != per-row element
            # count). F.get() is what Spark's own error message points to:
            # it's ANSI-safe and returns NULL for an out-of-bounds index
            # instead of raising SparkArrayIndexOutOfBoundsException.
            col = F.get(col, int(segment))
            current_type = current_type.elementType
        else:
            col = col.getField(segment)
            current_type = current_type[segment].dataType
    return col.cast(cast)


# (bronze_column_name, CONFIRMED source path, spark_type) -- verified
# against a real raw record from skill_iq_catalog.landing.theirstack_raw.
# Paths marked "unconfirmed" below were absent from that one sample record
# (commonly sparse -- e.g. a posting with no reposting history has no
# `date_reposted`) so their path is still a best guess; everything else
# is confirmed.
_COLUMN_MAP = [
    ("job_id", "id", LongType()),
    ("job_title", "job_title", StringType()),                    # CONFIRMED (was "title" -- wrong)
    ("normalized_title", "normalized_title", StringType()),      # CONFIRMED (empty string, not null, when unset)
    ("seniority", "seniority", StringType()),                    # CONFIRMED
    ("date_posted", "date_posted", DateType()),                  # CONFIRMED
    ("discovered_at", "discovered_at", TimestampType()),         # CONFIRMED
    ("closed_at", "closed_at", TimestampType()),                 # unconfirmed -- absent on this open posting
    ("reposted", "reposted", BooleanType()),                     # CONFIRMED
    ("date_reposted", "date_reposted", DateType()),               # unconfirmed -- absent, not reposted
    ("has_blurred_data", "has_blurred_data", BooleanType()),     # CONFIRMED
    ("easy_apply", "easy_apply", BooleanType()),                 # unconfirmed -- absent on this record
    ("employment_statuses", "employment_statuses", ArrayType(StringType())),  # CONFIRMED
    ("url", "url", StringType()),                                 # CONFIRMED
    ("source_url", "source_url", StringType()),                  # CONFIRMED
    ("final_url", "final_url", StringType()),                    # unconfirmed -- absent, url==source_url here
    ("location", "location", StringType()),                      # CONFIRMED
    ("short_location", "short_location", StringType()),          # CONFIRMED
    ("long_location", "long_location", StringType()),            # CONFIRMED
    ("latitude", "latitude", DoubleType()),                       # CONFIRMED (job-level, distinct from locations[0].latitude)
    ("longitude", "longitude", DoubleType()),                     # CONFIRMED
    ("postal_code", "postal_code", StringType()),                # unconfirmed -- absent on this record
    ("state_code", "state_code", StringType()),                  # CONFIRMED
    ("country", "country", StringType()),                        # CONFIRMED
    ("country_code", "country_code", StringType()),              # CONFIRMED
    ("country_codes", "country_codes", ArrayType(StringType())), # CONFIRMED (empty array here, field present)
    ("countries", "countries", ArrayType(StringType())),         # CONFIRMED
    ("cities", "cities", ArrayType(StringType())),                # CONFIRMED
    ("continents", "continents", ArrayType(StringType())),       # CONFIRMED
    ("remote", "remote", BooleanType()),                          # CONFIRMED
    ("hybrid", "hybrid", BooleanType()),                          # CONFIRMED
    # loc_* -- CONFIRMED: `locations` is an ARRAY of location-reference
    # objects (GeoNames-style), not a single nested struct as originally
    # guessed. Taking element 0 as the primary location; postings with
    # multiple locations only get the first one represented here.
    ("loc_id", "locations.0.id", LongType()),
    ("loc_name", "locations.0.name", StringType()),
    ("loc_type", "locations.0.type", StringType()),
    ("loc_feature_code", "locations.0.feature_code", StringType()),
    ("loc_admin1_name", "locations.0.admin1_name", StringType()),
    ("loc_admin1_code", "locations.0.admin1_code", StringType()),
    ("loc_admin2_name", "locations.0.admin2_name", StringType()), # unconfirmed -- absent on this record's location element
    ("loc_continent", "locations.0.continent", StringType()),
    ("loc_display_name", "locations.0.display_name", StringType()),
    ("loc_country_name", "locations.0.country_name", StringType()),
    ("salary_string", "salary_string", StringType()),            # unconfirmed -- absent (unstructured salary in description prose instead)
    ("salary_currency", "salary_currency", StringType()),        # unconfirmed
    ("min_annual_salary", "min_annual_salary", DoubleType()),    # unconfirmed
    ("max_annual_salary", "max_annual_salary", DoubleType()),    # unconfirmed
    ("min_annual_salary_usd", "min_annual_salary_usd", DoubleType()),  # unconfirmed
    ("max_annual_salary_usd", "max_annual_salary_usd", DoubleType()),  # unconfirmed
    ("avg_annual_salary_usd", "avg_annual_salary_usd", DoubleType()),  # unconfirmed
    ("technology_slugs", "technology_slugs", ArrayType(StringType())),  # CONFIRMED (job-level stack, distinct from company_object's)
    ("keyword_slugs", "keyword_slugs", ArrayType(StringType())),        # CONFIRMED
    ("description", "description", StringType()),                # CONFIRMED
    # legacy flat fields -- CONFIRMED top-level, NOT nested under company_object
    ("company", "company", StringType()),
    ("company_domain", "company_domain", StringType()),
    # co_* -- CONFIRMED: nested under `company_object`, not `company`
    ("co_id", "company_object.id", StringType()),
    ("co_name", "company_object.name", StringType()),
    ("co_domain", "company_object.domain", StringType()),
    ("co_url", "company_object.url", StringType()),
    ("co_linkedin_url", "company_object.linkedin_url", StringType()),
    ("co_logo", "company_object.logo", StringType()),
    ("co_industry", "company_object.industry", StringType()),
    ("co_industry_id", "company_object.industry_id", IntegerType()),
    ("co_country", "company_object.country", StringType()),
    ("co_country_code", "company_object.country_code", StringType()),
    ("co_city", "company_object.city", StringType()),
    ("co_postal_code", "company_object.postal_code", StringType()),
    ("co_employee_count", "company_object.employee_count", IntegerType()),
    ("co_employee_count_range", "company_object.employee_count_range", StringType()),
    ("co_founded_year", "company_object.founded_year", IntegerType()),
    ("co_annual_revenue_usd", "company_object.annual_revenue_usd", DoubleType()),
    ("co_annual_revenue_readable", "company_object.annual_revenue_usd_readable", StringType()),  # CONFIRMED, name differs from bronze col
    ("co_total_funding_usd", "company_object.total_funding_usd", DoubleType()),
    ("co_funding_stage", "company_object.funding_stage", StringType()),
    ("co_last_funding_date", "company_object.last_funding_round_date", StringType()),  # CONFIRMED, name differs from bronze col
    ("co_last_funding_amount", "company_object.last_funding_amount", StringType()),    # unconfirmed -- absent on this record
    ("co_is_recruiting_agency", "company_object.is_recruiting_agency", BooleanType()),
    ("co_publicly_traded_symbol", "company_object.publicly_traded_symbol", StringType()),
    ("co_publicly_traded_exchange", "company_object.publicly_traded_exchange", StringType()),
    ("co_alexa_ranking", "company_object.alexa_ranking", IntegerType()),
    ("co_num_jobs", "company_object.num_jobs", IntegerType()),
    ("co_num_jobs_last_30_days", "company_object.num_jobs_last_30_days", IntegerType()),
    ("co_num_technologies", "company_object.num_technologies", IntegerType()),
    ("co_apollo_id", "company_object.apollo_id", StringType()),
    ("co_linkedin_id", "company_object.linkedin_id", StringType()),
    ("co_yc_batch", "company_object.yc_batch", StringType()),    # unconfirmed -- absent on this record
    ("co_technology_slugs", "company_object.technology_slugs", ArrayType(StringType())),
    ("co_company_keywords", "company_object.company_keywords", ArrayType(StringType())),
    ("co_company_tags", "company_object.company_tags", ArrayType(StringType())),
    ("co_possible_domains", "company_object.possible_domains", ArrayType(StringType())),
    ("co_long_description", "company_object.long_description", StringType()),
]


def run(spark: SparkSession) -> None:
    ensure_catalog_objects(spark)

    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "true")
        .load(LANDING_PATH)
    )

    # CONFIRMED (previously wrong): landing files are NOT wrapped in
    # {"data": [...], "total_count": N} -- that shape only describes the
    # theirstack API *response envelope*; what's actually written to the
    # landing volume is one flat job record per row (JSON Lines), matching
    # the raw sample record checked earlier. No explode needed -- Bronze
    # reads the top-level schema directly.
    row_schema = raw_stream.schema

    select_exprs = [
        _resolve_column(row_schema, path, cast).alias(col)
        for col, path, cast in _COLUMN_MAP
    ]
    select_exprs += [
        F.current_timestamp().alias("_raw_ingested_at"),
        F.current_timestamp().alias("_bronze_processed_at"),
        F.lit(SOURCE_NAME).alias("_source"),
    ]

    flattened = raw_stream.select(*select_exprs).where(F.col("job_id").isNotNull())

    query = (
        flattened.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(TABLE)
    )
    query.awaitTermination()


if __name__ == "__main__":
    run(get_spark())
