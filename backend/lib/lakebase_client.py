"""Lakebase (managed Postgres) operational read path for the LCR app.

This is the application-facing, low-latency serving layer. When LCR_USE_LAKEBASE is
enabled, LCR result reads are served from a Lakebase Postgres instance that mirrors the
governed gold table, instead of issuing a SQL-warehouse query on every request.

The flag defaults OFF, so importing or deploying this module changes nothing about the
running app until it is explicitly enabled. See notebook/lcr_lakebase_serving.py for how
the instance and synced table are provisioned.
"""

import logging
import os
import time
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)


def use_lakebase() -> bool:
    """True only when LCR_USE_LAKEBASE is explicitly turned on."""
    return os.environ.get("LCR_USE_LAKEBASE", "false").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _cfg(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing env var for Lakebase: {name}")
    return value


def _connect():
    """Open a psycopg connection using a short-lived Databricks-issued credential."""
    import psycopg  # imported lazily so the dependency is only needed when enabled

    from backend.lib.databricks_client import get_workspace_client

    w = get_workspace_client()
    instance = _cfg("LCR_LAKEBASE_INSTANCE", "lcr-lakebase")
    host = _cfg("LCR_LAKEBASE_HOST")
    database = _cfg("LCR_LAKEBASE_DATABASE", "databricks_postgres")

    cred = w.database.generate_database_credential(
        request_id=str(time.time()), instance_names=[instance]
    )
    user = os.environ.get("LCR_LAKEBASE_USER") or w.current_user.me().user_name
    return psycopg.connect(
        host=host,
        port=int(os.environ.get("LCR_LAKEBASE_PORT", "5432")),
        dbname=database,
        user=user,
        password=cred.token,
        sslmode="require",
    )


def query_lcr_results(as_of_date: date | None = None) -> list[dict[str, Any]]:
    """Read LCR results from the Lakebase operational store as a list of dicts.

    Mirrors the shape returned by databricks_client.execute_sql so callers are agnostic
    to the backing store.
    """
    table = _cfg("LCR_LAKEBASE_TABLE", "public.lcr_results_latest")
    conn = _connect()
    try:
        with conn.cursor() as cur:
            if as_of_date is not None:
                cur.execute(
                    f"SELECT * FROM {table} WHERE as_of_date = %s", (as_of_date.isoformat(),)
                )
            else:
                cur.execute(f"SELECT * FROM {table}")
            columns = [c.name for c in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()
