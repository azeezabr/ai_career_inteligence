from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException, Query

from app.core.date_ranges import resolve_window
from app.core.semantic_model import role_values, valid_date_ranges
from app.db.queries import get_skills_analysis
from app.models.schemas import SkillsAnalysisResponse

router = APIRouter(tags=["skills"])

VALID_SORTS = ["occurrence", "trending"]


@router.get("/skills", response_model=SkillsAnalysisResponse)
def read_skills(
    role: str | None = Query(None, description="Required. One of the canonical role values."),
    date_range: str = Query("90d"),
    industry: str = Query("all"),
    sort: str = Query("occurrence"),
    limit: int = Query(20, ge=1, le=50),
):
    """Top skills for a role, ranked by occurrence or trend, with role-level KPI metrics."""
    if not role:
        raise HTTPException(status_code=400, detail="Missing required parameter 'role'.")
    if role not in role_values():
        raise HTTPException(status_code=400, detail=f"Invalid role '{role}'. Must be one of {role_values()}.")
    if date_range not in valid_date_ranges():
        raise HTTPException(status_code=400, detail=f"Invalid date_range '{date_range}'. Must be one of {valid_date_ranges()}.")
    if sort not in VALID_SORTS:
        raise HTTPException(status_code=400, detail=f"Invalid sort '{sort}'. Must be one of {VALID_SORTS}.")

    window = resolve_window(date_range)
    result = get_skills_analysis(role, industry, sort, limit, window)

    return {
        "role": role,
        "filters": {"date_range": date_range, "industry": industry},
        "metrics": result["metrics"],
        "skills": result["skills"],
        "meta": {
            "generated_at": datetime.now(timezone.utc),
            "window_start": window.window_start,
            "window_end": window.window_end,
            "window_midpoint": window.window_midpoint,
        },
    }
