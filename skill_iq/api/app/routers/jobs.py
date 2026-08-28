import math
from fastapi import APIRouter, HTTPException, Query

from app.core.date_ranges import resolve_window
from app.core.semantic_model import role_values, valid_date_ranges
from app.db.queries import get_jobs
from app.models.schemas import JobsResponse

router = APIRouter(tags=["jobs"])


@router.get("/jobs", response_model=JobsResponse)
def read_jobs(
    role: str | None = Query(None, description="Required. One of the canonical role values."),
    skill: str | None = None,
    certificate: str | None = None,
    date_range: str = Query("90d"),
    industry: str = Query("all"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1),
):
    """Paginated job postings for a role -- the drilldown behind any skill/cert stat."""
    if not role:
        raise HTTPException(status_code=400, detail="Missing required parameter 'role'.")
    if role not in role_values():
        raise HTTPException(status_code=400, detail=f"Invalid role '{role}'. Must be one of {role_values()}.")
    if date_range not in valid_date_ranges():
        raise HTTPException(status_code=400, detail=f"Invalid date_range '{date_range}'. Must be one of {valid_date_ranges()}.")
    if page_size > 50:
        raise HTTPException(status_code=400, detail="page_size must not exceed 50.")
    if skill and certificate:
        raise HTTPException(status_code=400, detail="Provide either 'skill' or 'certificate', not both.")

    window = resolve_window(date_range)
    jobs, total = get_jobs(role, skill, certificate, industry, window, page, page_size)

    applied_slug = skill or certificate
    return {
        "role": role,
        "skill": {"slug": applied_slug} if applied_slug else None,
        "filters": {"date_range": date_range, "industry": industry},
        "pagination": {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, math.ceil(total / page_size)),
        },
        "jobs": jobs,
    }
