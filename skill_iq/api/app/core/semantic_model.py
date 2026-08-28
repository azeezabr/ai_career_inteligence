"""
Loads the shared semantic_model.yml (same file the Databricks Gold layer
is documented by) so the API's role enum, filters, and trend thresholds
never live in more than one place.
"""

from functools import lru_cache
from pathlib import Path
import yaml

# semantic_model.yml lives one level up from the api/ package at repo root
_MODEL_PATH = Path(__file__).resolve().parents[3] / "semantic_model" / "semantic_model.yml"


@lru_cache
def load_semantic_model() -> dict:
    with open(_MODEL_PATH) as f:
        return yaml.safe_load(f)


def entity_table(entity_name: str) -> str:
    model = load_semantic_model()
    entity = model["entities"][entity_name]
    return f'{model["catalog"]}.{model["schema"]}.{entity["table"]}'


def canonical_roles() -> list[dict]:
    """[{'value': 'data_engineer', 'label': 'Data Engineer'}, ...] -- the fixed roles enum."""
    return load_semantic_model()["roles"]


def role_values() -> list[str]:
    return [r["value"] for r in canonical_roles()]


def valid_date_ranges() -> list[str]:
    return load_semantic_model()["date_ranges"]


def trend_thresholds() -> tuple[float, float]:
    t = load_semantic_model()["trend"]
    return t["trending_threshold_pp"], t["declining_threshold_pp"]


def classify_trend(delta_pp: float) -> str:
    trending_th, declining_th = trend_thresholds()
    if delta_pp >= trending_th:
        return "trending"
    if delta_pp <= declining_th:
        return "declining"
    return "stable"
