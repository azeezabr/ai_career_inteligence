from datetime import date, datetime
from pydantic import BaseModel


# ---------------------------------------------------------- 1. GET /api/filters

class RoleOption(BaseModel):
    value: str
    label: str


class IndustryOption(BaseModel):
    value: str
    label: str


class FiltersResponse(BaseModel):
    roles: list[RoleOption]
    industries: list[IndustryOption]


# ------------------------------------------------------------ 2. GET /api/skills

class AppliedFilters(BaseModel):
    date_range: str
    industry: str


class MetricValue(BaseModel):
    value: float
    delta_pct: float | None = None  # None when no prior-period comparison is meaningful (date_range=all)


class Metrics(BaseModel):
    job_postings: MetricValue
    open_roles: MetricValue
    median_salary_usd: MetricValue


class SkillRank(BaseModel):
    rank: int
    name: str
    occurrence_pct: float
    trend: str  # "trending" | "stable" | "declining"
    trend_delta_pp: float


class WindowMeta(BaseModel):
    generated_at: datetime
    window_start: date
    window_end: date
    window_midpoint: date


class SkillsAnalysisResponse(BaseModel):
    role: str
    filters: AppliedFilters
    metrics: Metrics
    skills: list[SkillRank]
    meta: WindowMeta


# ------------------------------------------------------ 3. GET /api/certifications

class CertRank(BaseModel):
    rank: int
    name: str
    provider: str | None = None
    occurrence_pct: float
    demand_badge: str  # "High" | "Medium" | "Low"


class CertificationsAnalysisResponse(BaseModel):
    role: str
    filters: AppliedFilters
    certifications: list[CertRank]


# ------------------------------------------------------------- 4. GET /api/jobs

class AppliedSlug(BaseModel):
    slug: str


class Pagination(BaseModel):
    total: int
    page: int
    page_size: int
    total_pages: int


class JobItem(BaseModel):
    job_id: int
    title: str
    company: str | None = None
    company_domain: str | None = None
    seniority: str | None = None
    remote: bool | None = None
    description: str | None = None
    hybrid: bool | None = None
    min_salary_usd: float | None = None
    max_salary_usd: float | None = None
    date_posted: date
    url: str | None = None


class JobsResponse(BaseModel):
    role: str
    skill: AppliedSlug | None = None  # present iff a skill= or certificate= filter was applied
    filters: AppliedFilters
    pagination: Pagination
    jobs: list[JobItem]
