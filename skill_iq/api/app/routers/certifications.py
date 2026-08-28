from fastapi import APIRouter, HTTPException, Query

from app.core.date_ranges import resolve_window
from app.core.semantic_model import role_values, valid_date_ranges
from app.db.queries import get_certifications_analysis
from app.models.schemas import CertificationsAnalysisResponse

router = APIRouter(tags=["certifications"])


@router.get("/certifications", response_model=CertificationsAnalysisResponse)
def read_certifications(
    role: str | None = Query(None, description="Required. One of the canonical role values."),
    date_range: str = Query("90d"),
    industry: str = Query("all"),
    limit: int = Query(5, ge=1, le=50),
):
    """Top certifications for a role, ranked by occurrence, with a High/Medium/Low demand badge."""
    if not role:
        raise HTTPException(status_code=400, detail="Missing required parameter 'role'.")
    if role not in role_values():
        raise HTTPException(status_code=400, detail=f"Invalid role '{role}'. Must be one of {role_values()}.")
    if date_range not in valid_date_ranges():
        raise HTTPException(status_code=400, detail=f"Invalid date_range '{date_range}'. Must be one of {valid_date_ranges()}.")

    window = resolve_window(date_range)
    certifications = get_certifications_analysis(role, industry, limit, window)

    return {
        "role": role,
        "filters": {"date_range": date_range, "industry": industry},
        "certifications": certifications,
    }
