"""
Silver Layer :: dim companies (SCD Type 1)
--------------------------------------------
Schema per the table design: company_id, name, domain, industry,
employee_count, country, created_date, updated_date, source_system.

Real Bronze schema note: `co_industry`, `co_employee_count`, and `co_country`
are already present as source data (theirstack's company enrichment is
flattened into Bronze with a `co_` prefix) -- no Claude classification
needed here anymore. `company_id` uses Bronze's `co_id` directly when
present (it's theirstack's own company identifier), falling back to a
domain+name hash only for the rare row missing it.

SCD Type 1 = no history, attributes overwritten in place. Implemented as a
Delta MERGE so unchanged rows aren't rewritten and `created_date` is
preserved across reruns (only `updated_date` moves forward).
"""

import sys, os
sys.path.insert(0, os.path.join(next(p for p in sys.path if p.endswith("/nvers")), "skill_iq"))

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from silver._common import get_spark, ensure_silver_schema, bronze_postings, surrogate_key, CATALOG, SILVER_SCHEMA, SOURCE_SYSTEM

TABLE = f"{CATALOG}.{SILVER_SCHEMA}.companies"


def build(spark):
    postings = bronze_postings(spark)

    companies_new = (
        postings.where(F.col("co_name").isNotNull() | F.col("company").isNotNull())
        .select(
            F.coalesce(F.col("co_id"), surrogate_key("co_domain", "co_name")).alias("company_id"),
            F.coalesce(F.col("co_name"), F.col("company")).alias("name"),
            F.coalesce(F.col("co_domain"), F.col("company_domain")).alias("domain"),
            F.col("co_industry").alias("industry"),
            F.col("co_employee_count").cast("long").alias("employee_count"),
            F.coalesce(F.col("co_country"), F.col("country")).alias("country"),
        )
        .dropDuplicates(["company_id"])
        .withColumn("updated_date", F.current_timestamp())
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    )

    if spark.catalog.tableExists(TABLE):
        target = DeltaTable.forName(spark, TABLE)
        (
            target.alias("t")
            .merge(companies_new.alias("s"), "t.company_id = s.company_id")
            .whenMatchedUpdate(
                set={
                    "name": "s.name",
                    "domain": "s.domain",
                    "industry": "s.industry",
                    "employee_count": "s.employee_count",
                    "country": "s.country",
                    "updated_date": "s.updated_date",
                    "source_system": "s.source_system",
                }
            )
            .whenNotMatchedInsert(
                values={
                    "company_id": "s.company_id",
                    "name": "s.name",
                    "domain": "s.domain",
                    "industry": "s.industry",
                    "employee_count": "s.employee_count",
                    "country": "s.country",
                    "created_date": "current_timestamp()",
                    "updated_date": "s.updated_date",
                    "source_system": "s.source_system",
                }
            )
            .execute()
        )
    else:
        (
            companies_new.withColumn("created_date", F.col("updated_date"))
            .select(
                "company_id", "name", "domain", "industry", "employee_count",
                "country", "created_date", "updated_date", "source_system",
            )
            .write.format("delta")
            .mode("overwrite")
            .saveAsTable(TABLE)
        )


if __name__ == "__main__":
    spark = get_spark()
    ensure_silver_schema(spark)
    build(spark)
