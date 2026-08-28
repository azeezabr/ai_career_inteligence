"""
Thin connection layer over `databricks-sql-connector`, pointed at a
Databricks SQL Warehouse (serverless recommended) so the Azure Container
App never talks to a job cluster directly.
"""

from contextlib import contextmanager
from typing import Iterator

from databricks import sql as databricks_sql

from app.core.config import get_settings


@contextmanager
def get_connection() -> Iterator["databricks_sql.client.Connection"]:
    settings = get_settings()
    conn = databricks_sql.connect(
        server_hostname=settings.databricks_server_hostname,
        http_path=settings.databricks_http_path,
        access_token=settings.databricks_token,
    )
    try:
        yield conn
    finally:
        conn.close()


def run_query(query: str, params: tuple = ()) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            columns = [c[0] for c in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
