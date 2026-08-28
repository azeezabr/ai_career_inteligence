"""
Central config, populated from environment variables. On Azure Container
Apps these are set via `az containerapp update --set-env-vars` or, for
secrets (token), via a Container Apps secret reference.
"""

from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "SKILL IQ API"
    api_prefix: str = "/api"

    # Databricks SQL Warehouse connection (Unity Catalog gold schema)
    databricks_server_hostname: str
    databricks_http_path: str
    databricks_token: str
    databricks_catalog: str = "skill_iq_catalog"
    databricks_gold_schema: str = "gold"

    # connection pool
    db_pool_size: int = 5

    cors_allowed_origins: list[str] = ["*"]

    class Config:
        env_prefix = "SKILLIQ_"


@lru_cache
def get_settings() -> Settings:
    return Settings()
