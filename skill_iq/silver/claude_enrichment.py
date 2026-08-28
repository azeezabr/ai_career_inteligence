"""
Claude / Agent Bricks Enrichment :: certifications only
-----------------------------------------------------------
Earlier versions of this module also classified company industry, geocoded
locations, and extracted role/seniority/skills per posting. The real
Bronze schema already provides all of that as source data
(co_industry, loc_*, seniority, normalized_title, technology_slugs,
keyword_slugs) -- so those extraction tasks were removed. Role
classification into the 5-value canonical taxonomy is now a cheap
rule-based mapping (see silver/role_classifier.py), not an LLM call.

The one thing Bronze has NO field for is certifications -- there's no
`certification_slugs` or similar column, so they're still extracted from
free-text `description` via Claude, mirroring the "Agent call" arrow to
Agent Bricks in the architecture diagram. In Databricks this is wired up
as an Agent Bricks endpoint; the function below calls the Anthropic
Messages API directly so it also runs standalone (e.g. unit tests) --
swap `_call_model` for a Model Serving client call if you're invoking a
deployed Agent Bricks endpoint instead of the raw API.

Fails soft (never raises) so a single bad extraction can't break the
Silver pipeline.
"""

import json
import os
from functools import lru_cache

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

MODEL = "claude-haiku-4-5-20251001"  # cheap/fast model; high-volume extraction task

CERTIFICATION_SCHEMA = StructType([
    StructField(
        "certifications",
        ArrayType(StructType([
            StructField("name", StringType()),
            StructField("provider", StringType()),
        ])),
    ),
])

_SYSTEM_PROMPT = """Extract named certifications/credentials mentioned in a job posting description.
Return ONLY minified JSON, no prose, matching exactly:
{"certifications": [{"name": string, "provider": string}]}

Rules:
- Only explicit named certifications (e.g. "AWS Certified Solutions Architect", "PMP", "CPA").
- provider is the issuing organization (e.g. "AWS", "PMI", "AICPA").
- Empty array if none are mentioned.
- Never invent information not present in the text."""


@lru_cache(maxsize=1)
def _client():
    import anthropic

    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


def _extract_one(description: str) -> dict:
    if not description:
        return {"certifications": []}
    try:
        resp = _client().messages.create(
            model=MODEL, max_tokens=300, system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": description[:4000]}],
        )
        result = json.loads(resp.content[0].text.strip())
        return {"certifications": result.get("certifications", [])}
    except Exception:
        return {"certifications": []}


def _extract_pandas(descriptions: pd.Series) -> pd.DataFrame:
    records = [_extract_one(d) for d in descriptions.fillna("")]
    return pd.DataFrame.from_records(records, columns=["certifications"])


def apply_certification_extraction(df):
    """Attach `certifications` (array<struct<name,provider>>) to a DataFrame with a `description` column."""

    @pandas_udf(CERTIFICATION_SCHEMA)
    def _udf(description: pd.Series) -> pd.DataFrame:
        return _extract_pandas(description)

    from pyspark.sql import functions as F
    return df.withColumn("cert_extraction", _udf(F.col("description"))).withColumn(
        "certifications", F.col("cert_extraction.certifications")
    ).drop("cert_extraction")
