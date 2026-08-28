"""
LLM Enrichment :: certifications only
------------------------------------------
Earlier versions of this module also classified company industry, geocoded
locations, and extracted role/seniority/skills per posting. The real
Bronze schema already provides all of that as source data
(co_industry, loc_*, seniority, normalized_title, technology_slugs,
keyword_slugs) -- so those extraction tasks were removed. Role
classification into the 5-value canonical taxonomy is a cheap rule-based
mapping (see silver/role_classifier.py), not an LLM call.

The one thing Bronze has NO field for is certifications -- there's no
`certification_slugs` or similar column, so they're still extracted from
free-text `description` via an LLM, mirroring the "Agent call" arrow to
Agent Bricks in the architecture diagram. In Databricks this can be wired
up as an Agent Bricks endpoint; the functions below call the provider API
directly so this also runs standalone (e.g. unit tests).

PROVIDER: configurable via the LLM_PROVIDER env var -- "openai" (default)
or "anthropic". Set whichever's API key you're using:
  - openai:    OPENAI_API_KEY,    model default: gpt-5.6-luna (do NOT use
               the bare "gpt-5.6" alias -- that routes to the flagship
               Sol tier, not the cheap Luna tier)
  - anthropic: ANTHROPIC_API_KEY, model default: claude-haiku-4-5-20251001
Override the model per-provider with LLM_MODEL if you want something else.
This file used to be named claude_enrichment.py and only supported
Anthropic -- renamed since hardcoding "claude" in the filename while
calling a different provider by default would be misleading.

*** VERIFY gpt-5.6-luna MANUALLY BEFORE TRUSTING IT AT SCALE ***
gpt-5.6-luna is a reasoning-capable model, which is why `_call_openai` uses
`max_completion_tokens` (not the older `max_tokens`) and budgets extra
room for invisible internal reasoning tokens, not just the visible JSON
output. I have NOT run a live call against this specific model -- the
model ID and the max_completion_tokens requirement are both confirmed via
current documentation, not tested end-to-end here. Because `_extract_one`
fails soft on ANY exception (wrong param name, wrong response shape,
truncated JSON, etc.), a subtle incompatibility would NOT show up as an
error -- every posting would just silently get `{"certifications": []}`,
which looks identical to "this model correctly found no certifications."
Run one real call manually first and confirm you get a populated result
back before trusting `certification_extraction.py` at scale:
    from silver.llm_enrichment import _extract_one
    print(_extract_one("Must hold an AWS Certified Solutions Architect certification."))
    # expect: {"certifications": [{"name": "AWS Certified Solutions Architect", "provider": "AWS"}]}
    # if you get {"certifications": []} here, something in the API call is wrong.

Fails soft (never raises) so a single bad extraction can't break the
Silver pipeline -- this applies per-record regardless of provider.
"""

import json
import os
from functools import lru_cache

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

PROVIDER = os.environ.get("LLM_PROVIDER", "openai").lower()  # "openai" | "anthropic"

_DEFAULT_MODELS = {
    "openai": "gpt-5.6-luna",  # OpenAI's fastest/cheapest GPT-5.6 tier; do NOT use bare "gpt-5.6" alias, that routes to the flagship Sol instead
    "anthropic": "claude-haiku-4-5-20251001",
}
MODEL = os.environ.get("LLM_MODEL", _DEFAULT_MODELS.get(PROVIDER, _DEFAULT_MODELS["openai"]))

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
    if PROVIDER == "anthropic":
        import anthropic
        return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    else:
        import openai
        return openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])


def _call_anthropic(description: str) -> str:
    resp = _client().messages.create(
        model=MODEL, max_tokens=300, system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": description}],
    )
    return resp.content[0].text.strip()


def _call_openai(description: str) -> str:
    resp = _client().chat.completions.create(
        model=MODEL,
        # max_completion_tokens, not max_tokens: gpt-5.6-luna is a
        # reasoning-capable model, and max_tokens is deprecated/rejected on
        # this model family via Chat Completions. This budget also has to
        # cover invisible internal reasoning tokens the model spends before
        # it emits the final JSON, not just the visible output -- 300 (the
        # old Haiku budget, which has no reasoning overhead) risked
        # truncating before any JSON came out, so this is raised to 800.
        # A truncated/empty response still fails soft (see _extract_one),
        # so an under-budget value wouldn't error loudly -- it would just
        # silently look like "no certifications found" on every posting.
        max_completion_tokens=800,
        response_format={"type": "json_object"},  # JSON mode -- the system prompt already demands JSON-only
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": description},
        ],
    )
    return resp.choices[0].message.content.strip()


def _extract_one(description: str) -> dict:
    if not description:
        return {"certifications": []}
    try:
        raw_text = (
            _call_anthropic(description[:4000]) if PROVIDER == "anthropic"
            else _call_openai(description[:4000])
        )
        result = json.loads(raw_text)
        return {"certifications": result.get("certifications", [])}
    except Exception:
        return {"certifications": []}


def _extract_pandas(descriptions: pd.Series) -> pd.DataFrame:
    records = [_extract_one(d) for d in descriptions.fillna("")]
    return pd.DataFrame.from_records(records, columns=["certifications"])


def apply_certification_extraction(df):
    """Attach `certifications` (array<struct<name,provider>>) to a DataFrame with a `description` column.

    *** CRITICAL: os.environ set on the driver does NOT reach executors ***
    This runs inside a pandas_udf, which executes on executor worker
    processes -- separate Python subprocesses that do NOT inherit changes
    made to the driver's os.environ after they've started (e.g. via
    silver._common.load_llm_api_key(), which only sets it on the driver).
    Without this fix, every single row would silently hit the same
    KeyError the driver hit before load_llm_api_key existed, get caught by
    _extract_one's fail-soft except, and return {"certifications": []} for
    EVERY posting -- with zero visible errors anywhere (this is exactly
    what happened the first time this ran for real: 0 rows, no error).

    Fix: read the key from the DRIVER's os.environ (call
    silver._common.load_llm_api_key(spark) before this), broadcast the
    VALUE to every executor via sparkContext.broadcast, and have each UDF
    invocation set it into that worker's own os.environ on first use
    (idempotent -- subsequent calls on the same worker process are no-ops).

    Fails LOUD here (raises immediately on the driver) if the key isn't
    set at all, rather than let the distributed job run to completion and
    produce a silently-empty result -- that failure mode has already cost
    real debugging time and shouldn't repeat itself for this specific
    precondition.
    """
    import os

    env_var_name = "ANTHROPIC_API_KEY" if PROVIDER == "anthropic" else "OPENAI_API_KEY"
    api_key = os.environ.get(env_var_name)
    if not api_key:
        raise RuntimeError(
            f"{env_var_name} is not set on the driver. Call "
            f"silver._common.load_llm_api_key(spark) before apply_certification_extraction()."
        )

    _api_key_value = api_key  # captured in UDF closure; serialized to executors automatically

    @pandas_udf(CERTIFICATION_SCHEMA)
    def _udf(description: pd.Series) -> pd.DataFrame:
        import os as _os  # local import: this code runs on the executor, not the driver
        if not _os.environ.get(env_var_name):
            _os.environ[env_var_name] = _api_key_value
        return _extract_pandas(description)

    from pyspark.sql import functions as F
    return df.withColumn("cert_extraction", _udf(F.col("description"))).withColumn(
        "certifications", F.col("cert_extraction.certifications")
    ).drop("cert_extraction")
