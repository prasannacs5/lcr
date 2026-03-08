import logging
import os
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, StatementState
from fastapi import HTTPException

logger = logging.getLogger(__name__)

# Default SQL warehouse: host e2-demo-field-eng.cloud.databricks.com, path /sql/1.0/warehouses/4b9b953939869799
DEFAULT_WAREHOUSE_ID = "4b9b953939869799"


def get_warehouse_id() -> str:
    """Return the SQL warehouse ID (from env or default)."""
    return os.environ.get("DATABRICKS_WAREHOUSE_ID", DEFAULT_WAREHOUSE_ID)


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise HTTPException(status_code=500, detail=f"Missing env var: {name}")
    return value


def get_workspace_client() -> WorkspaceClient:
    host = _require_env("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if token:
        os.environ.pop("DATABRICKS_CLIENT_ID", None)
        os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
        logger.info("Using PAT auth via DATABRICKS_TOKEN for WorkspaceClient.")
        return WorkspaceClient(host=host, token=token)

    client_id = _require_env("DATABRICKS_CLIENT_ID")
    client_secret = _require_env("DATABRICKS_CLIENT_SECRET")

    logger.info("Using OAuth M2M via service principal for WorkspaceClient.")
    return WorkspaceClient(
        host=host,
        client_id=client_id,
        client_secret=client_secret,
        auth_type="oauth-m2m",
    )


def execute_sql(
    statement: str,
    catalog: str = "cfo",
    schema: str = "aura_bank",
    warehouse_id: str | None = None,
    parameters: list[dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """
    Execute a SQL statement on the configured SQL warehouse and return rows as a list of dicts.
    Uses the same workspace auth (PAT or OAuth M2M).
    parameters: optional list of {"name": "...", "value": "...", "type": "DATE"|...} for :name in statement.
    """
    from databricks.sdk.service.sql import StatementParameterListItem

    w = get_workspace_client()
    wid = warehouse_id or get_warehouse_id()
    kwargs = {
        "statement": statement,
        "warehouse_id": wid,
        "catalog": catalog,
        "schema": schema,
        "disposition": Disposition.INLINE,
        "wait_timeout": "30s",
    }
    if parameters:
        kwargs["parameters"] = [
            StatementParameterListItem(
                name=p["name"],
                value=p.get("value"),
                type=p.get("type"),
            )
            for p in parameters
        ]
    response = w.statement_execution.execute_statement(**kwargs)
    if not response.status or response.status.state != StatementState.SUCCEEDED:
        err = getattr(response.status, "error", None) or response.status
        raise HTTPException(
            status_code=502,
            detail=f"SQL execution failed: {err}",
        )
    manifest = response.manifest
    result = response.result
    if not result or not result.data_array:
        return []
    columns = []
    if manifest and manifest.schema and manifest.schema.columns:
        columns = [c.name or f"col_{i}" for i, c in enumerate(manifest.schema.columns)]
    else:
        columns = [f"col_{i}" for i in range(len(result.data_array[0]))]
    rows: list[dict[str, Any]] = []
    for arr in result.data_array:
        rows.append(dict(zip(columns, arr)))
    return rows
