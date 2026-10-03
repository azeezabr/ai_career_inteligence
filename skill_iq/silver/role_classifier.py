"""
Role classification :: rule-based, not Claude
------------------------------------------------
The real Bronze schema gives us `normalized_title` from theirstack, but it
is sometimes an EMPTY STRING (`""`), not `NULL`, when theirstack hasn't
normalized a given posting's title -- confirmed against a real sample
record. Falls back to `job_title` in that case. Bucketing the result into
SKILL IQ's canonical taxonomy is a small, well-defined keyword-matching
problem -- not worth an LLM call per posting given the volume.

*** SCOPE CHANGE (2026-10-02): project is scoped to exactly 3 roles ***
Earlier versions of this taxonomy had 5 values (data_engineer |
data_scientist | ml_engineer | data_analyst | software_engineer). Per
explicit decision, SKILL IQ only needs: data_engineer | ai_engineer |
data_architect (+ "other" as the catch-all for everything else). Anything
that previously matched data_scientist, software_engineer, or the old
ml_engineer's non-AI keywords ("mlops", "machine learning engineer" without
"ai") now simply falls into "other" -- that's intentional narrowing, not a
bug. "ai_engineer" absorbs what the old ml_engineer bucket matched for AI
titles. "data_architect" is a brand-new bucket with no prior patterns;
extend it as real "Data Architect"-style titles are seen (none were in the
145-row Bronze sample checked on 2026-09-26, so this is a best-effort
starter list pending real examples).

*** BUG FIX: empty string is not NULL ***
A first version of this file did `COALESCE(normalized_title, job_title)`,
which only falls through on NULL -- not on `""`. Since real data has
`normalized_title=""` (not null) whenever theirstack didn't normalize a
title, COALESCE never fell back to job_title at all, and 100% of postings
matched against an empty string, which no keyword can ever match --
producing "other" for every single row. Fixed by explicitly converting
empty/whitespace-only normalized_title to NULL (via NULLIF + TRIM) before
the COALESCE, so the fallback to job_title actually triggers.

Extend `_PATTERNS` as new title variants show up in real data; order
matters (first match wins), so more specific patterns are listed before
broader ones.
"""

from pyspark.sql import functions as F

# (canonical_role, list of substrings to match against the lowercased title)
_PATTERNS = [
    ("data_architect", [
        "data architect", "information architect", "analytics architect",
        "cloud data architect", "enterprise data architect",
    ]),
    ("ai_engineer", [
        "ai engineer", "artificial intelligence engineer", "machine learning engineer",
        "ml engineer", "genai engineer", "generative ai engineer", "ai/ml engineer",
    ]),
    ("data_engineer", [
        "data engineer", "analytics engineer", "etl developer", "etl engineer", "data platform engineer",
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