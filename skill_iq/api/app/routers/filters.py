from fastapi import APIRouter
from app.db.queries import get_filters
from app.models.schemas import FiltersResponse

router = APIRouter(tags=["filters"])


@router.get("/filters", response_model=FiltersResponse)
def read_filters(role: str | None = None, industry: str | None = None):
    """
    Cascading filter values for the role/industry dropdowns.
    - No params: all roles + all industries.
    - role given: all roles (unchanged) + industries scoped to that role's postings.
    - industry given: roles scoped to that industry's postings + all industries (unchanged).
    """
    return get_filters(role, industry)
