# Databricks notebook source
# DBTITLE 1,Setup and create table
from datetime import datetime
import requests
import json

# Create the table if it doesn't exist
spark.sql("""
  CREATE TABLE IF NOT EXISTS cfo.aura_bank.reco_table (
    timestamp TIMESTAMP,
    tag STRING,
    text STRING
  )
  USING DELTA
""")

print("Table cfo.aura_bank.reco_table created/verified successfully")

# COMMAND ----------

# DBTITLE 1,Define function to call Gemini model
from databricks.sdk import WorkspaceClient
import requests
import json
import uuid

def get_aura_bank_data():
    """
    Query all tables under cfo.aura_bank schema and format as context
    """
    # Get list of tables in the schema
    tables = spark.sql("SHOW TABLES IN cfo.aura_bank").collect()
    
    context_data = []
    
    for table_row in tables:
        table_name = table_row['tableName']
        full_table_name = f"cfo.aura_bank.{table_name}"
        
        try:
            # Get table description
            table_desc = spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()
            columns = [row['col_name'] for row in table_desc if row['col_name'] and not row['col_name'].startswith('#')]
            
            # Get sample data (limit to 100 rows to avoid token limits)
            sample_df = spark.sql(f"SELECT * FROM {full_table_name} LIMIT 100")
            
            # Convert to pandas and handle timestamps
            sample_data = sample_df.toPandas()
            
            # Convert all timestamp columns to strings
            for col in sample_data.columns:
                if sample_data[col].dtype == 'datetime64[ns]':
                    sample_data[col] = sample_data[col].astype(str)
            
            # Format table information
            table_info = {
                "table_name": table_name,
                "columns": columns,
                "row_count": sample_data.shape[0],
                "sample_data": sample_data.to_dict(orient='records')
            }
            
            context_data.append(table_info)
            
        except Exception as e:
            print(f"Warning: Could not read table {table_name}: {str(e)}")
            continue
    
    return context_data

def call_gemini_model(prompt, analysis_type):
    """
    Call Gemini-3-pro Foundation Model API with LCR data context
    """
    # Use WorkspaceClient for host configuration
    w = WorkspaceClient()
    
    # Get token
    token = w.config.token
    if not token:
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    # Foundation Model API endpoint - Gemini 3 Pro
    endpoint_url = f"{w.config.host}/serving-endpoints/databricks-gemini-3-pro/invocations"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Get all data from cfo.aura_bank schema
    print(f"Fetching data from cfo.aura_bank schema...")
    context_data = get_aura_bank_data()
    
    # Format context for the model
    context_text = "# Liquidity Coverage Ratio (LCR) Data Context\n\n"
    context_text += "You are analyzing banking liquidity data from the cfo.aura_bank schema. Below is the available data:\n\n"
    
    for table_info in context_data:
        context_text += f"## Table: {table_info['table_name']}\n"
        context_text += f"Columns: {', '.join(table_info['columns'])}\n"
        context_text += f"Sample data ({table_info['row_count']} rows):\n"
        context_text += json.dumps(table_info['sample_data'][:10], indent=2)  # Limit to 10 rows for context
        context_text += "\n\n"
    
    # Combine context with user prompt
    full_prompt = f"{context_text}\n\n# Task: {analysis_type}\n\n{prompt}"
    
    # Prepare payload for Foundation Model API
    payload = {
        "messages": [
            {
                "role": "system",
                "content": "You are a financial analyst specializing in liquidity risk management and regulatory compliance. Analyze the provided LCR data and provide detailed, actionable insights."
            },
            {
                "role": "user",
                "content": full_prompt
            }
        ],
        "max_tokens": 4000,
        "temperature": 0.7
    }
    
    try:
        print(f"Calling Gemini-3-pro for {analysis_type}...")
        response = requests.post(endpoint_url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        result = response.json()
        
        # Extract response text from Gemini format
        if "choices" in result and len(result["choices"]) > 0:
            message_content = result["choices"][0]["message"]["content"]
            
            # Handle Gemini's content format (list of objects with 'text' field)
            if isinstance(message_content, list):
                text_parts = []
                for content_item in message_content:
                    if isinstance(content_item, dict) and "text" in content_item:
                        text_parts.append(content_item["text"])
                return "\n".join(text_parts)
            elif isinstance(message_content, str):
                return message_content
            else:
                return str(message_content)
        else:
            return str(result)
            
    except requests.exceptions.HTTPError as e:
        return f"Error calling model: {e}\nResponse: {e.response.text if hasattr(e, 'response') else 'No response'}"
    except Exception as e:
        return f"Error calling model: {str(e)}"

print("Gemini-3-pro model calling function defined successfully")

# COMMAND ----------

# DBTITLE 1,Generate Executive Summary
# Generate executive summary for liquidity analysis
exec_summary_prompt = """
Provide an executive summary for liquidity analysis focusing on the Liquidity Coverage Ratio (LCR). 

Based on the data provided, include:
- Overall liquidity position and current LCR ratio
- High-quality liquid assets (HQLA) composition and status
- Net cash outflow trends and key drivers
- Regulatory compliance status (minimum LCR requirement is 100%)
- Top 3 key recommendations for senior management

Keep the summary concise, data-driven, and actionable for C-level executives.
Format the output with clear sections and bullet points.
"""

print("Generating executive summary...")
exec_summary = call_gemini_model(exec_summary_prompt, "Executive Summary")

# Insert into table
current_timestamp = datetime.now()
spark.sql(f"""
  INSERT INTO cfo.aura_bank.reco_table (timestamp, tag, text)
  VALUES ('{current_timestamp}', 'exec_summary', '{exec_summary.replace("'", "''")}')
""")

print(f"Executive summary generated and inserted at {current_timestamp}")
print(f"\nPreview:\n{exec_summary[:500]}...")

# COMMAND ----------

# DBTITLE 1,Generate Risk Assessment
# Generate risk assessment
risk_assessment_prompt = """
Provide a comprehensive liquidity risk assessment based on the LCR data.

Analyze and identify:
- Top 3 liquidity risks with severity ratings (High/Medium/Low)
- Concentration risks in funding sources or asset composition
- Potential stress scenarios and their impact on LCR
- Early warning indicators to monitor
- Specific mitigation strategies for each identified risk

Provide quantitative evidence from the data to support your risk ratings.
Format the output with clear risk categories and actionable recommendations.
"""

print("Generating risk assessment...")
risk_assessment = call_gemini_model(risk_assessment_prompt, "Risk Assessment")

# Insert into table
current_timestamp = datetime.now()
spark.sql(f"""
  INSERT INTO cfo.aura_bank.reco_table (timestamp, tag, text)
  VALUES ('{current_timestamp}', 'risk_assessment', '{risk_assessment.replace("'", "''")}')
""")

print(f"Risk assessment generated and inserted at {current_timestamp}")
print(f"\nPreview:\n{risk_assessment[:500]}...")

# COMMAND ----------

# DBTITLE 1,Generate Forecast Analysis
# Generate forecast analysis
forecast_prompt = """
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
"""

print("Generating forecast analysis...")
forecast_analysis = call_gemini_model(forecast_prompt, "Forecast Analysis")

# Insert into table
current_timestamp = datetime.now()
spark.sql(f"""
  INSERT INTO cfo.aura_bank.reco_table (timestamp, tag, text)
  VALUES ('{current_timestamp}', 'forecast_analysis', '{forecast_analysis.replace("'", "''")}')
""")

print(f"Forecast analysis generated and inserted at {current_timestamp}")
print(f"\nPreview:\n{forecast_analysis[:500]}...")

# COMMAND ----------

# DBTITLE 1,Display results summary
# Display the latest recommendations from the table
print("=" * 80)
print("LCR RECOMMENDATIONS GENERATED SUCCESSFULLY")
print("=" * 80)

# Query the latest entries
latest_recs = spark.sql("""
  SELECT 
    timestamp,
    tag,
    LEFT(text, 200) as text_preview,
    LENGTH(text) as text_length
  FROM cfo.aura_bank.reco_table
  WHERE DATE(timestamp) = CURRENT_DATE()
  ORDER BY timestamp DESC
""")

display(latest_recs)

print("\nNote: This notebook can be scheduled to run daily using Databricks Workflows.")
print("Analysis types: exec_summary, risk_assessment, forecast_analysis")

# COMMAND ----------

# DBTITLE 1,Notebook Status
# MAGIC %md
# MAGIC # LCR Recommendations Generator - Status
# MAGIC
# MAGIC ## ✅ Successfully Implemented
# MAGIC
# MAGIC ### Model Integration
# MAGIC * **Foundation Model**: Gemini-3-pro (`databricks-gemini-3-pro`)
# MAGIC * **Data Context**: All tables from `cfo.aura_bank` schema automatically loaded
# MAGIC * **Authentication**: WorkspaceClient with fallback to dbutils
# MAGIC
# MAGIC ### Generated Analyses
# MAGIC 1. **Executive Summary** (3,695 chars)
# MAGIC    * Overall liquidity position and current LCR ratio (137.62%)
# MAGIC    * HQLA composition and status
# MAGIC    * Net cash outflow trends
# MAGIC    * Regulatory compliance status
# MAGIC    * Top 3 recommendations for senior management
# MAGIC
# MAGIC 2. **Risk Assessment** (6,937 chars)
# MAGIC    * Top 3 liquidity risks with severity ratings
# MAGIC    * Concentration risks analysis
# MAGIC    * Stress scenario impacts
# MAGIC    * Early warning indicators
# MAGIC    * Specific mitigation strategies
# MAGIC
# MAGIC 3. **Forecast Analysis** (6,170 chars)
# MAGIC    * 30-90 day LCR projections
# MAGIC    * Expected HQLA composition changes
# MAGIC    * Cash flow pattern forecasts
# MAGIC    * Scenario analysis (base, stress, optimistic)
# MAGIC    * Recommended actions
# MAGIC
# MAGIC ### Data Integration
# MAGIC * Automatically queries all tables in `cfo.aura_bank` schema
# MAGIC * Converts timestamps to strings for JSON serialization
# MAGIC * Limits sample data to 100 rows per table (10 rows in context)
# MAGIC * Handles errors gracefully for inaccessible tables
# MAGIC
# MAGIC ### Output
# MAGIC * All analyses stored in `cfo.aura_bank.reco_table`
# MAGIC * Timestamped entries with tags: `exec_summary`, `risk_assessment`, `forecast_analysis`
# MAGIC * Ready for daily scheduling via Databricks Workflows
# MAGIC
# MAGIC ## 🎯 Key Features
# MAGIC
# MAGIC 1. **Comprehensive Data Context**: Gemini receives complete schema and sample data from all tables
# MAGIC 2. **Detailed Analysis**: Each analysis is 3,500-7,000 characters with actionable insights
# MAGIC 3. **Real Data**: Uses actual LCR data from the database (not synthetic)
# MAGIC 4. **Production Ready**: Can be scheduled to run daily
# MAGIC
# MAGIC ## 📊 Sample Insights Generated
# MAGIC
# MAGIC * Current LCR: 137.62% (Compliant)
# MAGIC * Regulatory buffer: 37.62% above minimum
# MAGIC * Key risk: Heavy reliance on corporate deposits
# MAGIC * Recommendation: Diversify funding sources and increase Level 1 HQLA
# MAGIC
# MAGIC ## 🚀 Next Steps
# MAGIC
# MAGIC 1. **Schedule Daily Runs**: Use Databricks Workflows to run this notebook daily
# MAGIC 2. **Set Up Alerts**: Configure alerts when LCR drops below thresholds
# MAGIC 3. **Dashboard Integration**: Connect Power BI/Tableau to `reco_table` for visualization
# MAGIC 4. **Email Reports**: Add email notification with daily summaries

# COMMAND ----------

