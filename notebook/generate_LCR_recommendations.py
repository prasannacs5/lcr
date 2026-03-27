# Databricks notebook source
# DBTITLE 1,Setup and create table
from datetime import datetime
import os
import time
import json

CATALOG = os.environ.get("LCR_CATALOG", "banking_cfo_treasury")
SCHEMA = os.environ.get("LCR_SCHEMA", "aura_bank")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.reco_table (
    timestamp TIMESTAMP,
    tag STRING,
    text STRING
  )
  USING DELTA
""")

print(f"Table {CATALOG}.{SCHEMA}.reco_table ready")

# COMMAND ----------

# DBTITLE 1,Fetch context data (once)
MAX_SAMPLE_ROWS = 15
RELEVANT_TABLES = {"lcr_results", "hqla_breakdown", "cash_outflows_breakdown", "general_ledger"}

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()

context_parts = [f"# LCR Data Context from {CATALOG}.{SCHEMA}\n"]
for table_row in tables:
    table_name = table_row['tableName']
    if table_name not in RELEVANT_TABLES:
        continue
    full_name = f"{CATALOG}.{SCHEMA}.{table_name}"
    try:
        desc = spark.sql(f"DESCRIBE TABLE {full_name}").collect()
        cols = [r['col_name'] for r in desc if r['col_name'] and not r['col_name'].startswith('#')]
        df = spark.sql(f"SELECT * FROM {full_name} ORDER BY 1 DESC LIMIT {MAX_SAMPLE_ROWS}")
        pdf = df.toPandas()
        for col in pdf.columns:
            if pdf[col].dtype == 'datetime64[ns]':
                pdf[col] = pdf[col].astype(str)
        context_parts.append(f"\n## {table_name}\nColumns: {', '.join(cols)}\nData ({len(pdf)} rows):")
        context_parts.append(json.dumps(pdf.to_dict(orient='records'), indent=2))
    except Exception as e:
        print(f"Warning: skipping {table_name}: {e}")

CACHED_CONTEXT = "\n".join(context_parts)
print(f"Context built: {len(CACHED_CONTEXT)} chars, ~{len(CACHED_CONTEXT)//4} tokens (approx)")

# COMMAND ----------

# DBTITLE 1,Define model calling function
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

MIN_RESPONSE_LENGTH = 2000
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30

w = WorkspaceClient()
MODEL_NAME = os.environ.get("DATABRICKS_RECO_MODEL", "databricks-gemini-2-5-flash")
print(f"Using model: {MODEL_NAME}")


def call_model(prompt, analysis_type):
    """Call Foundation Model API using SDK (handles auth properly). Retries on truncated responses."""
    full_prompt = f"{CACHED_CONTEXT}\n\n# Task: {analysis_type}\n\n{prompt}"

    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content="You are a financial analyst specializing in liquidity risk management and regulatory compliance. Analyze the provided LCR data and provide detailed, actionable insights. Always provide a complete, thorough response."),
        ChatMessage(role=ChatMessageRole.USER, content=full_prompt),
    ]

    best_text = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  [{analysis_type}] Attempt {attempt}/{MAX_RETRIES}...")
            response = w.serving_endpoints.query(
                name=MODEL_NAME,
                messages=messages,
                max_tokens=16384,
                temperature=0.7,
            )

            # Log diagnostics
            choices = getattr(response, "choices", None) or []
            usage = getattr(response, "usage", None)
            if usage:
                print(f"    usage: prompt_tok={getattr(usage,'prompt_tokens',None)} completion_tok={getattr(usage,'completion_tokens',None)} total_tok={getattr(usage,'total_tokens',None)}")
                reasoning_tok = getattr(usage, 'reasoning_tokens', None)
                if reasoning_tok:
                    print(f"    reasoning_tokens={reasoning_tok}")

            if not choices:
                print(f"    WARNING: No choices returned")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                return best_text or "Error: Model returned no choices"

            first = choices[0]
            finish_reason = getattr(first, "finish_reason", "unknown")
            print(f"    finish_reason={finish_reason}")

            # Extract text
            msg = getattr(first, "message", None)
            text = ""
            if msg and getattr(msg, "content", None):
                content = msg.content
                if isinstance(content, str):
                    text = content.strip()
                elif isinstance(content, list):
                    parts = []
                    for block in content:
                        if isinstance(block, dict) and "text" in block:
                            parts.append(block["text"] or "")
                        elif isinstance(block, str):
                            parts.append(block)
                    text = " ".join(parts).strip()

            print(f"    text_length={len(text)} chars")

            # Keep the longest response we've seen
            if len(text) > len(best_text):
                best_text = text

            if finish_reason == "stop" and len(text) >= MIN_RESPONSE_LENGTH:
                return text

            if finish_reason != "stop":
                print(f"    WARNING: finish_reason={finish_reason} (not 'stop') — response likely truncated")

            if len(text) < MIN_RESPONSE_LENGTH:
                print(f"    WARNING: Response too short ({len(text)} < {MIN_RESPONSE_LENGTH})")

            if attempt < MAX_RETRIES:
                print(f"    Waiting {RETRY_DELAY_SECONDS}s before retry...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue

        except Exception as e:
            print(f"    ERROR (attempt {attempt}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
                continue
            return best_text or f"Error: {e}"

    # Return best effort
    if best_text:
        print(f"  Returning best-effort response: {len(best_text)} chars")
    return best_text or "Error: All retry attempts failed"


def insert_reco(tag, text):
    """Insert recommendation if it's not an error."""
    if text.startswith("Error"):
        print(f"SKIPPING INSERT for {tag} — error: {text[:200]}")
        return
    ts = datetime.now()
    spark.createDataFrame(
        [(ts, tag, text)],
        ["timestamp", "tag", "text"]
    ).write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.reco_table")
    print(f"Inserted {tag}: {len(text)} chars at {ts}")


print("Functions ready")

# COMMAND ----------

# DBTITLE 1,Generate Executive Summary
print("=" * 60)
print("GENERATING: Executive Summary")
print("=" * 60)

exec_summary = call_model("""
Provide an executive summary for liquidity analysis focusing on the Liquidity Coverage Ratio (LCR).

Based on the data provided, include:
- Overall liquidity position and current LCR ratio
- High-quality liquid assets (HQLA) composition and status
- Net cash outflow trends and key drivers
- Regulatory compliance status (minimum LCR requirement is 100%)
- Top 3 key recommendations for senior management

Keep the summary concise, data-driven, and actionable for C-level executives.
Format the output with clear sections and bullet points.
""", "Executive Summary")

insert_reco("exec_summary", exec_summary)

print(f"\nWaiting 30s before next call...")
time.sleep(30)

# COMMAND ----------

# DBTITLE 1,Generate Risk Assessment
print("=" * 60)
print("GENERATING: Risk Assessment")
print("=" * 60)

risk_assessment = call_model("""
Provide a comprehensive liquidity risk assessment based on the LCR data.

Analyze and identify:
- Top 3 liquidity risks with severity ratings (High/Medium/Low)
- Concentration risks in funding sources or asset composition
- Potential stress scenarios and their impact on LCR
- Early warning indicators to monitor
- Specific mitigation strategies for each identified risk

Provide quantitative evidence from the data to support your risk ratings.
Format the output with clear risk categories and actionable recommendations.
""", "Risk Assessment")

insert_reco("risk_assessment", risk_assessment)

print(f"\nWaiting 30s before next call...")
time.sleep(30)

# COMMAND ----------

# DBTITLE 1,Generate Forecast Analysis
print("=" * 60)
print("GENERATING: Forecast Analysis")
print("=" * 60)

forecast_analysis = call_model("""
Provide a forward-looking forecast analysis for the Liquidity Coverage Ratio.

Based on the historical data and current trends, analyze:
- Projected LCR trajectory over the next 30-90 days
- Expected changes in HQLA composition
- Anticipated cash flow patterns and seasonal factors
- Potential regulatory or market events that could impact liquidity
- Recommended actions to maintain optimal LCR levels
- Scenario analysis (base case, stress case, optimistic case)

Provide specific forecasts with supporting data points and assumptions.
Format the output with clear sections for different time horizons and scenarios.
""", "Forecast Analysis")

insert_reco("forecast_analysis", forecast_analysis)

# COMMAND ----------

# DBTITLE 1,Results Summary
print("=" * 80)
print("LCR RECOMMENDATIONS GENERATION COMPLETE")
print("=" * 80)

latest_recs = spark.sql(f"""
  SELECT timestamp, tag, LENGTH(text) as text_length,
         LEFT(text, 150) as text_preview
  FROM {CATALOG}.{SCHEMA}.reco_table
  WHERE DATE(timestamp) = CURRENT_DATE()
  ORDER BY timestamp DESC
""")

display(latest_recs)

# COMMAND ----------

