"""Service layer for LCR data from hive_metastore.aura_bank catalog."""

import os
from datetime import date
from typing import Any

from backend.lib.databricks_client import execute_sql

CATALOG = os.environ.get("LCR_CATALOG", "hive_metastore")
SCHEMA = os.environ.get("LCR_SCHEMA", "aura_bank")
LCR_RESULTS_TABLE = "lcr_results"
HQLA_BREAKDOWN_TABLE = "hqla_breakdown"
CASH_OUTFLOWS_BREAKDOWN_TABLE = "cash_outflows_breakdown"
RECO_TABLE = "reco_table"


def _gl_catalog() -> str:
    return os.environ.get("LCR_GL_CATALOG", CATALOG)


def _gl_schema() -> str:
    return os.environ.get("LCR_GL_SCHEMA", SCHEMA)


def _gl_table() -> str:
    return os.environ.get("LCR_GL_TABLE", "general_ledger")


def _gl_timestamp_column() -> str:
    """Column in general_ledger used for date filtering (default: timestamp)."""
    return os.environ.get("LCR_GL_TIMESTAMP_COLUMN", "timestamp")


def get_lcr_results(as_of_date: date | None = None) -> list[dict[str, Any]]:
    """
    Fetch LCR result rows from hive_metastore.aura_bank.lcr_results.
    If as_of_date is provided, filter by as_of_date column; otherwise return all rows.
    """
    if as_of_date is not None:
        statement = (
            "SELECT * FROM hive_metastore.aura_bank.lcr_results WHERE as_of_date = :as_of_date"
        )
        return execute_sql(
            statement,
            catalog=CATALOG,
            schema=SCHEMA,
            parameters=[
                {"name": "as_of_date", "value": as_of_date.isoformat(), "type": "DATE"}
            ],
        )
    statement = "SELECT * FROM hive_metastore.aura_bank.lcr_results"
    return execute_sql(statement, catalog=CATALOG, schema=SCHEMA)


def get_hqla_breakdown(
    snapshot_id: str | None = None,
    as_of_date: date | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch rows from hive_metastore.aura_bank.hqla_breakdown.
    Filter by snapshot_id if provided, else by as_of_date if provided, else return all.
    """
    if snapshot_id:
        statement = (
            "SELECT * FROM hive_metastore.aura_bank.hqla_breakdown WHERE snapshot_id = :snapshot_id"
        )
        return execute_sql(
            statement,
            catalog=CATALOG,
            schema=SCHEMA,
            parameters=[
                {"name": "snapshot_id", "value": snapshot_id, "type": "STRING"}
            ],
        )
    if as_of_date is not None:
        statement = (
            "SELECT * FROM hive_metastore.aura_bank.hqla_breakdown WHERE as_of_date = :as_of_date"
        )
        return execute_sql(
            statement,
            catalog=CATALOG,
            schema=SCHEMA,
            parameters=[
                {"name": "as_of_date", "value": as_of_date.isoformat(), "type": "DATE"}
            ],
        )
    return execute_sql(
        "SELECT * FROM hive_metastore.aura_bank.hqla_breakdown",
        catalog=CATALOG,
        schema=SCHEMA,
    )


def get_cash_outflows_breakdown(
    snapshot_id: str | None = None,
    as_of_date: date | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch rows from hive_metastore.aura_bank.cash_outflows_breakdown.
    Filter by snapshot_id if provided, else by as_of_date if provided, else return all.
    """
    if snapshot_id:
        statement = (
            "SELECT * FROM hive_metastore.aura_bank.cash_outflows_breakdown WHERE snapshot_id = :snapshot_id"
        )
        return execute_sql(
            statement,
            catalog=CATALOG,
            schema=SCHEMA,
            parameters=[
                {"name": "snapshot_id", "value": snapshot_id, "type": "STRING"}
            ],
        )
    if as_of_date is not None:
        statement = (
            "SELECT * FROM hive_metastore.aura_bank.cash_outflows_breakdown WHERE as_of_date = :as_of_date"
        )
        return execute_sql(
            statement,
            catalog=CATALOG,
            schema=SCHEMA,
            parameters=[
                {"name": "as_of_date", "value": as_of_date.isoformat(), "type": "DATE"}
            ],
        )
    return execute_sql(
        "SELECT * FROM hive_metastore.aura_bank.cash_outflows_breakdown",
        catalog=CATALOG,
        schema=SCHEMA,
    )


def _reco_table() -> str:
    return os.environ.get("LCR_RECO_TABLE", RECO_TABLE)


def _reco_date_column() -> str:
    """Column in reco_table used for ordering recent records (default: timestamp)."""
    return os.environ.get("LCR_RECO_DATE_COLUMN", "timestamp")


def _reco_tag_column() -> str:
    """Column in reco_table used to group records into sections (default: tag)."""
    return os.environ.get("LCR_RECO_TAG_COLUMN", "tag")


def get_reco_records(limit: int = 500) -> list[dict[str, Any]]:
    """
    Fetch recent records from hive_metastore.aura_bank.reco_table, ordered by date descending.
    Table and date column are configurable via LCR_RECO_TABLE, LCR_RECO_DATE_COLUMN.
    Used to drive Executive Summary sections grouped by tag (LCR_RECO_TAG_COLUMN).
    """
    tbl = _reco_table()
    date_col = _reco_date_column()
    full_table = f"{CATALOG}.{SCHEMA}.{tbl}"
    # Order by date descending to get recent records first; limit to avoid huge payloads
    statement = f"SELECT * FROM {full_table} ORDER BY `{date_col}` DESC LIMIT {limit}"
    return execute_sql(statement, catalog=CATALOG, schema=SCHEMA)


def get_gl_data(as_of_date: date | None = None) -> list[dict[str, Any]]:
    """
    Fetch GL (General Ledger) rows from general_ledger (configurable via
    LCR_GL_TABLE, LCR_GL_CATALOG, LCR_GL_SCHEMA). Filter by the timestamp
    column (LCR_GL_TIMESTAMP_COLUMN, default "timestamp") matching as_of_date.
    """
    cat, sch, tbl = _gl_catalog(), _gl_schema(), _gl_table()
    ts_col = _gl_timestamp_column()
    full_table = f"{cat}.{sch}.{tbl}"
    if as_of_date is None:
        return execute_sql(
            f"SELECT * FROM {full_table}",
            catalog=cat,
            schema=sch,
        )
    # Filter by date part of timestamp column (e.g. DATE(timestamp) = :as_of_date)
    statement = f"SELECT * FROM {full_table} WHERE DATE(`{ts_col}`) = :as_of_date"
    return execute_sql(
        statement,
        catalog=cat,
        schema=sch,
        parameters=[
            {"name": "as_of_date", "value": as_of_date.isoformat(), "type": "DATE"}
        ],
    )


REPO_TABLE_CATALOG = os.environ.get("LCR_CATALOG", "hive_metastore")
REPO_TABLE_SCHEMA = os.environ.get("LCR_SCHEMA", "aura_bank")
REPO_TABLE_NAME = "repo_table"


def _repo_date_column() -> str:
    """Column in repo_table used for date filter (default: as_of_date)."""
    return os.environ.get("LCR_REPO_DATE_COLUMN", "as_of_date")


def get_repo_notes(as_of_date: date) -> list[dict[str, Any]]:
    """
    Fetch notes from banking_cfo_treasury.aura.repo_table for the given as_of_date.
    Used by FR 2052a management report. Date column configurable via LCR_REPO_DATE_COLUMN.
    """
    date_col = _repo_date_column()
    full_table = f"{REPO_TABLE_CATALOG}.{REPO_TABLE_SCHEMA}.{REPO_TABLE_NAME}"
    statement = f"SELECT * FROM {full_table} WHERE DATE(`{date_col}`) = :as_of_date"
    return execute_sql(
        statement,
        catalog=REPO_TABLE_CATALOG,
        schema=REPO_TABLE_SCHEMA,
        parameters=[
            {"name": "as_of_date", "value": as_of_date.isoformat(), "type": "DATE"}
        ],
    )
