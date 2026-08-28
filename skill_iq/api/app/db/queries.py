"""
All SQL lives here, kept out of the routers. Every query is parameterized
(no string-interpolated user input) and targets the Gold tables named in
semantic_model.yml.

*** DESIGN NOTE: everything role/industry/date-scoped reads from
gold.job_posting_lines, not skill_daily/cert_daily/role_daily ***
Those three tables now match the solution doc's Table Designs diagram
EXACTLY (no role_name column on skill_daily/cert_daily, no posting_date or
industry on role_daily) -- see each gold/*.py file's docstring. Without a
role dimension, "% of postings mentioning Python" computed from
skill_daily would be aggregated across every role, not scoped to the one
selected -- which the API's role-scoped occurrence_pct explicitly requires
(and the mockup's "Top Skills — Data Engineer" visibly demonstrates).
job_posting_lines is the one Gold table that retains role_name, industry,
and posting_date per row (plus skill_names/cert_names arrays), so it's
what every role-scoped query below actually uses. This does mean scanning
and exploding arrays per request instead of reading pre-aggregated daily
rows -- slower per-request, but it's the only way to honor the documented
skill_daily/cert_daily/role_daily schemas exactly while still supporting
the documented API behavior.

Trend & delta methodology (see semantic_model.yml `trend` / `metric_delta`):
  - skill/cert `trend`: split the selected window in half at `window_midpoint`;
    trend_delta_pp = recent-half occurrence_pct - prior-half occurrence_pct;
    classified via core.semantic_model.classify_trend (default: >=+2pp
    trending, <=-2pp declining, else stable).
  - `metrics.*.delta_pct`: compares the FULL selected window against the
    immediately preceding period of the same length (None when
    date_range=all, since there's no fixed-length prior period to compare).
"""

from app.core.date_ranges import Window
from app.core.semantic_model import entity_table, canonical_roles, classify_trend
from app.db.connection import run_query


# ---------------------------------------------------------------- 1. filters

def get_filters(role: str | None, industry: str | None) -> dict:
    table = entity_table("job_posting")  # job_posting_lines
    roles = canonical_roles()  # full canonical list, base case

    all_industries_rows = run_query(f"SELECT DISTINCT industry FROM {table} ORDER BY 1")
    all_industries = [{"value": "all", "label": "All Industries"}] + [
        {"value": r["industry"], "label": r["industry"]} for r in all_industries_rows if r["industry"]
    ]

    if industry and industry != "all":
        scoped = run_query(
            f"SELECT DISTINCT role_name FROM {table} WHERE industry = %s", (industry,)
        )
        scoped_values = {r["role_name"] for r in scoped}
        roles = [r for r in roles if r["value"] in scoped_values]

    if role:
        scoped = run_query(
            f"SELECT DISTINCT industry FROM {table} WHERE role_name = %s", (role,)
        )
        scoped_values = {r["industry"] for r in scoped if r["industry"]}
        industries = [{"value": "all", "label": "All Industries"}] + [
            i for i in all_industries[1:] if i["value"] in scoped_values
        ]
    else:
        industries = all_industries

    return {"roles": roles, "industries": industries}


# ------------------------------------------------------------ shared: metrics

def _industry_clause(industry: str) -> tuple[str, list]:
    if industry and industry != "all":
        return "AND industry = %s", [industry]
    return "", []


def _metrics_over_window(role: str, industry: str, start, end) -> dict:
    table = entity_table("job_posting")  # job_posting_lines
    ind_clause, ind_params = _industry_clause(industry)

    row = run_query(
        f"""
        SELECT
            COUNT(DISTINCT job_id) AS job_postings,
            COUNT(DISTINCT CASE WHEN is_open THEN job_id END) AS open_roles,
            percentile_approx(avg_salary_usd, 0.5) AS median_salary_usd
        FROM {table}
        WHERE role_name = %s AND posting_date >= %s AND posting_date < %s {ind_clause}
        """,
        (role, start, end, *ind_params),
    )[0]

    return {
        "job_postings": row["job_postings"] or 0,
        "open_roles": row["open_roles"] or 0,
        "median_salary_usd": row["median_salary_usd"],
    }


def _delta_pct(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None or prior == 0:
        return None
    return round(100.0 * (current - prior) / prior, 1)


def _build_metrics(role: str, industry: str, window: Window) -> dict:
    current = _metrics_over_window(role, industry, window.window_start, window.window_end)

    if window.prior_window_start is None:
        return {
            "job_postings": {"value": current["job_postings"], "delta_pct": None},
            "open_roles": {"value": current["open_roles"], "delta_pct": None},
            "median_salary_usd": {"value": current["median_salary_usd"], "delta_pct": None},
        }

    prior = _metrics_over_window(role, industry, window.prior_window_start, window.prior_window_end)
    return {
        "job_postings": {
            "value": current["job_postings"],
            "delta_pct": _delta_pct(current["job_postings"], prior["job_postings"]),
        },
        "open_roles": {
            "value": current["open_roles"],
            "delta_pct": _delta_pct(current["open_roles"], prior["open_roles"]),
        },
        "median_salary_usd": {
            "value": current["median_salary_usd"],
            "delta_pct": _delta_pct(current["median_salary_usd"], prior["median_salary_usd"]),
        },
    }


# ------------------------------------------------------------- 2. skills

def get_skills_analysis(role: str, industry: str, sort: str, limit: int, window: Window) -> dict:
    table = entity_table("job_posting")  # job_posting_lines
    ind_clause, ind_params = _industry_clause(industry)

    # CTE structure:
    #   filtered   -- postings matching role/industry/window, kept once (not per skill)
    #   totals     -- distinct job_id counts (denominators), full window + each half
    #   exploded   -- one row per (job_id, skill_name) via LATERAL VIEW explode
    #   skill_agg  -- per-skill distinct job_id counts, full window + each half
    # final SELECT cross-joins skill_agg with the single totals row.
    rows = run_query(
        f"""
        WITH filtered AS (
            SELECT job_id, posting_date, skill_names
            FROM {table}
            WHERE role_name = %s AND posting_date >= %s AND posting_date < %s {ind_clause}
        ),
        totals AS (
            SELECT
                COUNT(DISTINCT job_id) AS total_full,
                COUNT(DISTINCT CASE WHEN posting_date < %s THEN job_id END) AS total_prior_half,
                COUNT(DISTINCT CASE WHEN posting_date >= %s THEN job_id END) AS total_recent_half
            FROM filtered
        ),
        exploded AS (
            SELECT job_id, posting_date, skill_name
            FROM filtered
            LATERAL VIEW explode(skill_names) AS skill_name
        ),
        skill_agg AS (
            SELECT
                skill_name,
                COUNT(DISTINCT job_id) AS appear_full,
                COUNT(DISTINCT CASE WHEN posting_date < %s THEN job_id END) AS appear_prior_half,
                COUNT(DISTINCT CASE WHEN posting_date >= %s THEN job_id END) AS appear_recent_half
            FROM exploded
            GROUP BY skill_name
        )
        SELECT skill_agg.*, totals.total_full, totals.total_prior_half, totals.total_recent_half
        FROM skill_agg CROSS JOIN totals
        """,
        (
            role, window.window_start, window.window_end, *ind_params,
            window.window_midpoint, window.window_midpoint,
            window.window_midpoint, window.window_midpoint,
        ),
    )

    skills = []
    for r in rows:
        occurrence_pct = 100.0 * r["appear_full"] / r["total_full"] if r["total_full"] else 0.0
        prior_pct = 100.0 * r["appear_prior_half"] / r["total_prior_half"] if r["total_prior_half"] else 0.0
        recent_pct = 100.0 * r["appear_recent_half"] / r["total_recent_half"] if r["total_recent_half"] else 0.0
        trend_delta_pp = round(recent_pct - prior_pct, 1)
        skills.append({
            "name": r["skill_name"],
            "occurrence_pct": round(occurrence_pct, 1),
            "trend": classify_trend(trend_delta_pp),
            "trend_delta_pp": trend_delta_pp,
        })

    sort_key = (lambda s: s["trend_delta_pp"]) if sort == "trending" else (lambda s: s["occurrence_pct"])
    skills.sort(key=sort_key, reverse=True)
    skills = skills[:limit]
    for i, s in enumerate(skills, start=1):
        s["rank"] = i

    return {
        "metrics": _build_metrics(role, industry, window),
        "skills": skills,
    }


# ------------------------------------------------------ 3. certifications

_BADGE_THRESHOLDS = (15.0, 5.0)  # >=15% High, >=5% Medium, else Low


def _demand_badge(occurrence_pct: float) -> str:
    high, medium = _BADGE_THRESHOLDS
    if occurrence_pct >= high:
        return "High"
    if occurrence_pct >= medium:
        return "Medium"
    return "Low"


def get_certifications_analysis(role: str, industry: str, limit: int, window: Window) -> list[dict]:
    table = entity_table("job_posting")  # job_posting_lines
    ind_clause, ind_params = _industry_clause(industry)

    rows = run_query(
        f"""
        WITH filtered AS (
            SELECT job_id, cert_names
            FROM {table}
            WHERE role_name = %s AND posting_date >= %s AND posting_date < %s {ind_clause}
        ),
        totals AS (
            SELECT COUNT(DISTINCT job_id) AS total FROM filtered
        ),
        exploded AS (
            SELECT job_id, cert_name
            FROM filtered
            LATERAL VIEW explode(cert_names) AS cert_name
        ),
        cert_agg AS (
            SELECT cert_name, COUNT(DISTINCT job_id) AS appear
            FROM exploded
            GROUP BY cert_name
        )
        SELECT cert_agg.cert_name, cert_agg.appear, totals.total
        FROM cert_agg CROSS JOIN totals
        ORDER BY cert_agg.appear DESC
        LIMIT %s
        """,
        (role, window.window_start, window.window_end, *ind_params, limit),
    )

    # job_posting_lines only carries cert NAMES per posting (no provider
    # per-posting) -- look up provider from silver.certifications in one
    # follow-up query, for just the cert names being returned.
    cert_names = [r["cert_name"] for r in rows]
    providers = {}
    if cert_names:
        placeholders = ", ".join(["%s"] * len(cert_names))
        cert_dim_table = entity_table("job_posting").split(".gold.")[0] + ".silver.certifications"
        provider_rows = run_query(
            f"SELECT name, provider FROM {cert_dim_table} WHERE name IN ({placeholders})",
            tuple(cert_names),
        )
        providers = {r["name"]: r["provider"] for r in provider_rows}

    certs = []
    for r in rows:
        occurrence_pct = round(100.0 * r["appear"] / r["total"], 1) if r["total"] else 0.0
        certs.append({
            "name": r["cert_name"],
            "provider": providers.get(r["cert_name"]),
            "occurrence_pct": occurrence_pct,
            "demand_badge": _demand_badge(occurrence_pct),
        })

    for i, c in enumerate(certs, start=1):
        c["rank"] = i
    return certs


# -------------------------------------------------------------- 4. jobs

def get_jobs(
    role: str, skill: str | None, certificate: str | None, industry: str,
    window: Window, page: int, page_size: int,
) -> tuple[list[dict], int]:
    table = entity_table("job_posting")
    where = ["role_name = %s", "posting_date >= %s", "posting_date < %s"]
    params: list = [role, window.window_start, window.window_end]

    ind_clause, ind_params = _industry_clause(industry)
    if ind_clause:
        where.append(ind_clause.replace("AND ", ""))
        params.extend(ind_params)

    if skill:
        where.append("array_contains(skill_names, %s)")
        params.append(skill)
    if certificate:
        where.append("array_contains(cert_names, %s)")
        params.append(certificate)

    where_clause = " AND ".join(where)

    total_row = run_query(f"SELECT COUNT(*) AS total FROM {table} WHERE {where_clause}", tuple(params))
    total = total_row[0]["total"]

    offset = (page - 1) * page_size
    jobs = run_query(
        f"""
        SELECT job_id, title, company, company_domain, seniority, remote,
               description, hybrid, min_salary_usd, max_salary_usd, date_posted, url
        FROM {table}
        WHERE {where_clause}
        ORDER BY date_posted DESC
        LIMIT %s OFFSET %s
        """,
        (*params, page_size, offset),
    )
    return jobs, total
