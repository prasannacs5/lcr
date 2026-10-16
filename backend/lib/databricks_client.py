import logging
import os
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, StatementState
from fastapi import HTTPException

logger = logging.getLogger(__name__)


def get_warehouse_id() -> str:
    """Return the SQL warehouse ID from DATABRICKS_WAREHOUSE_ID env var."""
    wid = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not wid:
        raise HTTPException(
            status_code=500,
            detail="Missing required env var: DATABRICKS_WAREHOUSE_ID",
        )
    return wid


def get_workspace_client() -> WorkspaceClient:
    """Create a WorkspaceClient using automatic auth detection.

    On Databricks Apps the platform injects DATABRICKS_HOST, CLIENT_ID,
    and CLIENT_SECRET for the app's service principal automatically.
    WorkspaceClient() picks these up via unified auth.
    For local dev, set DATABRICKS_HOST + DATABRICKS_TOKEN.
    """
    return WorkspaceClient()


def get_host() -> str:
    """Return the workspace host URL (with https:// scheme) from WorkspaceClient or env."""
    host = os.environ.get("DATABRICKS_HOST")
    if not host:
        host = get_workspace_client().config.host
    host = host.rstrip("/")
    if not host.startswith("https://") and not host.startswith("http://"):
        host = f"https://{host}"
    return host


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
