"""
Turns the `date_range` query param (30d|60d|90d|6m|1y|2y|all) into the
concrete windows the query builder needs:

  window_start / window_end     -- the selected range itself
  window_midpoint                -- splits the range in half for skill/cert
                                     trend classification (recent half vs
                                     prior half within the window)
  prior_window_start/end         -- the immediately preceding period of the
                                     SAME length, for the "+12% vs previous
                                     90 days" style metric deltas

`window_end` is "today" (current_date) -- the Gold tables are refreshed
weekly, so in practice this resolves to the most recent complete week.
"""

from dataclasses import dataclass
from datetime import date, timedelta
from fastapi import HTTPException

_RANGE_DAYS = {
    "30d": 30,
    "60d": 60,
    "90d": 90,
    "6m": 182,
    "1y": 365,
    "2y": 730,
    # "all" handled separately -- no fixed length, so no meaningful "prior period"
}

VALID_DATE_RANGES = list(_RANGE_DAYS) + ["all"]


@dataclass
class Window:
    window_start: date
    window_end: date
    window_midpoint: date
    prior_window_start: date | None
    prior_window_end: date | None


def resolve_window(date_range: str, today: date | None = None) -> Window:
    today = today or date.today()

    if date_range not in VALID_DATE_RANGES:
        raise HTTPException(status_code=400, detail=f"Invalid date_range '{date_range}'. Must be one of {VALID_DATE_RANGES}.")

    if date_range == "all":
        # arbitrarily deep lookback; no prior-period comparison is meaningful for "all"
        window_start = today - timedelta(days=3650)
        window_end = today
        midpoint = window_start + (window_end - window_start) / 2
        return Window(window_start, window_end, midpoint, None, None)

    days = _RANGE_DAYS[date_range]
    window_end = today
    window_start = today - timedelta(days=days)
    midpoint = window_start + timedelta(days=days // 2)
    prior_window_end = window_start
    prior_window_start = window_start - timedelta(days=days)

    return Window(window_start, window_end, midpoint, prior_window_start, prior_window_end)
