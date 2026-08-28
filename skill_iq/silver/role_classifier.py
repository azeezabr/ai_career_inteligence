"""
Role classification :: rule-based, not Claude
------------------------------------------------
The real Bronze schema gives us `normalized_title` from theirstack, but it
is sometimes an EMPTY STRING (`""`), not `NULL`, when theirstack hasn't
normalized a given posting's title -- confirmed against a real sample
record. Falls back to `job_title` in that case. Bucketing the result into
SKILL IQ's 5-value canonical taxonomy (data_engineer | data_scientist |
ml_engineer | data_analyst | software_engineer, + "other") is a small,
well-defined keyword-matching problem -- not worth an LLM call per posting
given the volume.

*** BUG FIX: empty string is not NULL ***
A first version of this file did `COALESCE(normalized_title, job_title)`,
which only falls through on NULL -- not on `""`. Since real data has
`normalized_title=""` (not null) whenever theirstack didn't normalize a
title, COALESCE never fell back to job_title at all, and 100% of postings
matched against an empty string, which no keyword can ever match --
producing "other" for every single row. Fixed by explicitly converting
empty/whitespace-only normalized_title to NULL (via NULLIF + TRIM) before
the COALESCE, so the fallback to job_title actually triggers.

Also broadened `software_engineer` patterns after seeing a real title,
"Senior Software Development Engineer (Front End)", which contains
"software" and "engineer" but NOT the literal phrase "software engineer"
(the word "Development" sits between them) -- would still have missed
even with the empty-string bug fixed. Added "development engineer" and
"software development engineer" as explicit patterns.

Extend `_PATTERNS` as new title variants show up in real data; order
matters (first match wins), so more specific patterns (e.g. "ml engineer")
are listed before broader ones (e.g. "engineer").
"""

from pyspark.sql import functions as F

# (canonical_role, list of substrings to match against the lowercased title)
_PATTERNS = [
    ("ml_engineer", ["machine learning engineer", "ml engineer", "ai engineer", "mlops"]),
    ("data_engineer", ["data engineer", "analytics engineer", "etl developer", "etl engineer"]),
    ("data_scientist", ["data scientist", "applied scientist", "research scientist"]),
    ("data_analyst", ["data analyst", "business intelligence analyst", "bi analyst", "reporting analyst"]),
    ("software_engineer", [
        "software engineer", "software developer", "software development engineer", "development engineer",
        "backend engineer", "backend developer", "frontend engineer", "frontend developer",
        "full stack", "fullstack", "application developer", "sde", "developer",
    ]),
]


def classify_role_column():
    """Returns a Column expression yielding one of the canonical role
    values, evaluated against COALESCE(NULLIF(TRIM(normalized_title), ''), job_title)."""
    normalized_or_null = F.nullif(F.trim(F.col("normalized_title")), F.lit(""))
    title = F.lower(F.coalesce(normalized_or_null, F.col("job_title"), F.lit("")))

    expr = F.lit("other")
    # build from the END backwards so the FIRST pattern in _PATTERNS wins
    # (F.when chains evaluate top-to-bottom, so reverse-fold into an otherwise chain)
    for role, keywords in reversed(_PATTERNS):
        condition = F.lit(False)
        for kw in keywords:
            condition = condition | title.contains(kw)
        expr = F.when(condition, F.lit(role)).otherwise(expr)

    return expr
