"""In-process LCR agent using LLM tool-calling loop + Genie REST API.

Same pattern as the CFO Command Center agent: the LLM decides when to
query data via Genie, polls for results, and synthesises a final answer.
All configuration (Genie space ID, LLM endpoint) is read from env vars
so the app can be deployed to multiple workspaces without code changes.
"""

import json
import logging
import os
import time

import requests as http_requests
from databricks.sdk import WorkspaceClient

from backend.lib.databricks_client import execute_sql

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — all overridable via env vars in app.yaml
# ---------------------------------------------------------------------------
LLM_ENDPOINT_NAME = os.environ.get("LCR_LLM_ENDPOINT_NAME", "databricks-claude-sonnet-4")
GENIE_SPACE_ID = os.environ.get("LCR_GENIE_SPACE_ID", "01f123d006e91b97aad469ecbc444560")

SYSTEM_PROMPT = """You are a financial data analyst assistant helping a bank's Treasury team
with Liquidity Coverage Ratio (LCR) reporting and analysis.

You have access to a data query tool for looking up financial metrics including:

- LCR ratios, historical trends, and regulatory status
- High-Quality Liquid Asset (HQLA) portfolio composition
- Cash flow projections by category and product line
- General ledger account balances and transactions
- FR 2052a regulatory filing data
- Scenario analysis and sensitivity modeling
- Period-over-period variance and attribution

Use the query_lcr_data tool to retrieve data whenever the user asks about specific figures,
trends, or portfolio breakdowns. Present results clearly using tables and formatting
when appropriate. If the data doesn't fully answer the question, explain what you found
and suggest follow-up queries."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_lcr_data",
            "description": "Query LCR ratios, HQLA portfolio, cash flow data, general ledger balances, and financial reporting metrics. Use for any data lookup, trend analysis, or portfolio breakdown.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about LCR and liquidity data",
                    }
                },
                "required": ["question"],
            },
        },
    }
]

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------
workspace_client = WorkspaceClient()
host = workspace_client.config.host

_cached_token = None
_token_expiry = 0


def _get_token() -> str:
    global _cached_token, _token_expiry
    if _cached_token and time.time() < _token_expiry:
        return _cached_token

    # Method 1: Direct token from SDK config (works in Databricks Apps)
    if workspace_client.config.token:
        _cached_token = workspace_client.config.token
        _token_expiry = time.time() + 2700
        return _cached_token

    # Method 2: authenticate() callable
    auth = workspace_client.config.authenticate()
    headers = {}
    if callable(auth):
        auth(headers)
    elif isinstance(auth, dict):
        headers = auth
    bearer = headers.get("Authorization", "")
    _cached_token = bearer[7:] if bearer.startswith("Bearer ") else bearer
    _token_expiry = time.time() + 2700
    return _cached_token


# ---------------------------------------------------------------------------
# LLM via REST
# ---------------------------------------------------------------------------
def _call_llm(messages: list, tools: list = None) -> dict:
    """Call the Databricks Foundation Model API via REST."""
    url = f"{host}/serving-endpoints/{LLM_ENDPOINT_NAME}/invocations"
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }
    payload = {"messages": messages}
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    resp = http_requests.post(url, json=payload, headers=headers, timeout=120)
    if not resp.ok:
        raise RuntimeError(f"LLM error ({resp.status_code}): {resp.text[:500]}")
    return resp.json()


# ---------------------------------------------------------------------------
# Genie Query via REST API
# ---------------------------------------------------------------------------
def _query_genie(space_id: str, question: str) -> str:
    """Query a Genie space and return the result as text."""
    token = _get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    start_url = f"{host}/api/2.0/genie/spaces/{space_id}/start-conversation"
    payload = {"content": question}

    try:
        resp = http_requests.post(start_url, json=payload, headers=headers, timeout=120)
        if not resp.ok:
            return f"Error querying Genie: {resp.status_code} - {resp.text[:500]}"

        data = resp.json()
        conversation_id = data.get("conversation_id", "")
        message_id = data.get("message_id", "")

        if not conversation_id or not message_id:
            return _extract_genie_response(data)

        result_url = f"{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
        for _ in range(60):
            time.sleep(2)
            poll_resp = http_requests.get(result_url, headers=headers, timeout=30)
            if not poll_resp.ok:
                continue

            msg_data = poll_resp.json()
            status = msg_data.get("status", "")

            if status == "COMPLETED":
                return _extract_genie_response(msg_data)
            elif status in ("FAILED", "CANCELLED"):
                error = msg_data.get("error", {}).get("message", "Query failed")
                return f"Genie query failed: {error}"

        return "Genie query timed out after 2 minutes."

    except Exception as e:
        logger.exception("Genie query error")
        return f"Error: {str(e)}"


def _query_sql_direct(question: str) -> str:
    """Fallback: use the LLM to generate SQL and execute it directly against hive_metastore."""
    try:
        catalog = os.environ.get("LCR_CATALOG", "hive_metastore")
        schema = os.environ.get("LCR_SCHEMA", "aura_bank")
        sql_prompt = [
            {"role": "system", "content": f"""You are a SQL expert. Generate a single SQL query to answer the user's question.
The data is in catalog '{catalog}' schema '{schema}' with these tables:
- lcr_results: snapshot_id, calculation_timestamp, as_of_date, lcr_ratio (decimal, 1.0=100%), hqla_total (millions), net_cash_outflows (millions), regulatory_status, created_by
- hqla_breakdown: snapshot_id, as_of_date, hqla_type, account_type, num_accounts, gross_balance_m, haircut_rate, net_hqla_value_m
- cash_outflows_breakdown: snapshot_id, as_of_date, account_type, num_accounts, gross_balance_m, runoff_rate, cash_outflow_m
- general_ledger: gl_account, account_name, category (Asset/Liability/Equity), account_type, hqla_type, runoff, timestamp, net_balance_m
- haircuts: asset_category, hqla_level, fed_haircut (string with %), bcbs_haircut
- runoff_rates: liability_category, specific_component, fed_runoff_rate (string with %)
- reco_table: timestamp, tag (exec_summary/risk_assessment/forecast_analysis), text

Return ONLY the SQL query, nothing else. Use fully qualified table names ({catalog}.{schema}.table_name)."""},
            {"role": "user", "content": question}
        ]
        sql_result = _call_llm(sql_prompt)
        sql = sql_result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        sql = sql.replace("```sql", "").replace("```", "").strip()

        rows = execute_sql(sql, catalog=catalog, schema=schema)
        if not rows:
            return "Query returned no results."
        # Format as readable text
        result_lines = []
        if rows:
            result_lines.append(" | ".join(rows[0].keys()))
            result_lines.append("-" * 60)
            for row in rows[:50]:
                result_lines.append(" | ".join(str(v) for v in row.values()))
        return f"SQL: {sql}\n\nResults ({len(rows)} rows):\n" + "\n".join(result_lines)
    except Exception as e:
        logger.exception("Direct SQL query error")
        return f"Error running direct SQL: {str(e)}"


def _extract_genie_response(data: dict) -> str:
    """Extract readable text from Genie response."""
    parts = []
    token = _get_token()

    attachments = data.get("attachments", [])
    for att in attachments:
        # Query attachment — has SQL and statement_id
        query_info = att.get("query")
        if query_info:
            description = query_info.get("description", "")
            sql = query_info.get("query", "")
            statement_id = query_info.get("statement_id", "")
            if description:
                parts.append(f"**Analysis:** {description}")
            if sql:
                parts.append(f"```sql\n{sql}\n```")
            # Fetch actual query results via SQL Statements API
            if statement_id:
                table = _fetch_statement_result(statement_id, token)
                if table:
                    parts.append(table)

        # Text attachment — Genie's natural language summary
        text_info = att.get("text")
        if text_info:
            content = text_info.get("content", "")
            if content:
                parts.append(content)

    # Fallback to top-level content
    content = data.get("content", "")
    if content and not parts:
        parts.append(content)

    return "\n\n".join(parts) if parts else "No results returned from Genie."


def _fetch_statement_result(statement_id: str, token: str) -> str:
    """Fetch query results from the SQL Statements API and format as markdown table."""
    try:
        url = f"{host}/api/2.0/sql/statements/{statement_id}"
        headers = {"Authorization": f"Bearer {token}"}
        resp = http_requests.get(url, headers=headers, timeout=30)
        if not resp.ok:
            return ""
        data = resp.json()
        columns = data.get("manifest", {}).get("schema", {}).get("columns", [])
        rows = data.get("result", {}).get("data_array", [])
        if not columns or not rows:
            return ""
        col_names = [c["name"] for c in columns]
        lines = []
        lines.append("| " + " | ".join(col_names) + " |")
        lines.append("| " + " | ".join(["---"] * len(col_names)) + " |")
        for row in rows[:50]:
            vals = [str(v) if v is not None else "" for v in row]
            lines.append("| " + " | ".join(vals) + " |")
        return "\n".join(lines)
    except Exception as e:
        logger.warning(f"Failed to fetch statement result: {e}")
        return ""


# ---------------------------------------------------------------------------
# Agent Logic (tool-calling loop)
# ---------------------------------------------------------------------------
def run_agent_sync(user_message: str) -> str:
    """Run the agent with tool calling and return full response."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]

    for turn in range(5):
        try:
            result = _call_llm(messages, tools=TOOLS)
        except Exception as e:
            return f"Error calling LLM: {str(e)}"

        choice = result.get("choices", [{}])[0]
        message = choice.get("message", {})

        tool_calls = message.get("tool_calls")
        if not tool_calls:
            return message.get("content", "No response generated.")

        messages.append(message)

        for tc in tool_calls:
            fn_name = tc["function"]["name"]
            try:
                args = json.loads(tc["function"]["arguments"])
            except json.JSONDecodeError:
                args = {"question": user_message}

            question = args.get("question", user_message)

            if fn_name == "query_lcr_data":
                tool_result = _query_sql_direct(question)
                if "Error" in tool_result:
                    logger.info("Direct SQL failed, trying Genie")
                    tool_result = _query_genie(GENIE_SPACE_ID, question)
            else:
                tool_result = f"Unknown tool: {fn_name}"

            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": tool_result,
            })

    return "Agent reached maximum turns without a final answer."


def stream_agent(user_message: str):
    """Generator yielding SSE-compatible events for the agent."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]

    for turn in range(5):
        try:
            result = _call_llm(messages, tools=TOOLS)
        except Exception as e:
            yield {"type": "error", "message": f"LLM error: {str(e)}"}
            return

        choice = result.get("choices", [{}])[0]
        message = choice.get("message", {})

        tool_calls = message.get("tool_calls")
        if not tool_calls:
            content = message.get("content", "No response generated.")
            yield {"type": "token", "content": content}
            yield {"type": "done"}
            return

        messages.append(message)

        for tc in tool_calls:
            fn_name = tc["function"]["name"]
            try:
                args = json.loads(tc["function"]["arguments"])
            except json.JSONDecodeError:
                args = {"question": user_message}

            question = args.get("question", user_message)
            yield {"type": "status", "message": "Querying LCR data..."}

            if fn_name == "query_lcr_data":
                tool_result = _query_sql_direct(question)
                if "Error" in tool_result:
                    logger.info("Direct SQL failed, trying Genie")
                    yield {"type": "status", "message": "Trying Genie space..."}
                    tool_result = _query_genie(GENIE_SPACE_ID, question)
            else:
                tool_result = f"Unknown tool: {fn_name}"

            messages.append({
                "role": "tool",
                "tool_call_id": tc["id"],
                "content": tool_result,
            })

    yield {"type": "token", "content": "Agent reached maximum turns."}
    yield {"type": "done"}
