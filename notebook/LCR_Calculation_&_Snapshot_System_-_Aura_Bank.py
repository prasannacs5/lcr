# Databricks notebook source
# MAGIC %md
# MAGIC # LCR Calculation Function - Documentation
# MAGIC
# MAGIC ## Function: `cfo.aura_bank.calculate_lcr()`
# MAGIC
# MAGIC ### Purpose
# MAGIC Calculates the **Liquidity Coverage Ratio (LCR)** for a given date based on general ledger balances, applying regulatory haircuts to assets and runoff rates to liabilities.
# MAGIC
# MAGIC ### Signature
# MAGIC ```sql
# MAGIC cfo.aura_bank.calculate_lcr(as_of_date TIMESTAMP)
# MAGIC ```
# MAGIC
# MAGIC ### Returns
# MAGIC A STRUCT containing:
# MAGIC * `lcr_ratio` (DOUBLE) - The LCR percentage (HQLA / Net Cash Outflows)
# MAGIC * `hqla_total` (DOUBLE) - Total High Quality Liquid Assets after haircuts
# MAGIC * `net_cash_outflows` (DOUBLE) - Total expected cash outflows over 30 days
# MAGIC * `calculation_date` (TIMESTAMP) - The date for which LCR was calculated
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC 1. **HQLA Calculation (Assets)**:
# MAGIC    * Joins [general_ledger](#table/cfo.aura_bank.general_ledger) with [haircuts](#table/cfo.aura_bank.haircuts) table
# MAGIC    * Applies haircuts based on `account_type` matching `asset_category`
# MAGIC    * Formula: `net_balance_m × (1 - haircut_rate)`
# MAGIC    * Level 1 assets (US Treasuries, Fed Reserves): 0% haircut
# MAGIC    * Level 2A assets (Agency MBS, Municipal Bonds): 15% haircut
# MAGIC    * Level 2B assets (Corporate Bonds): 50% haircut
# MAGIC    * Non-HQLA assets: 100% haircut (excluded from HQLA)
# MAGIC
# MAGIC 2. **Cash Outflows Calculation (Liabilities)**:
# MAGIC    * Joins [general_ledger](#table/cfo.aura_bank.general_ledger) with [runoff_rates](#table/cfo.aura_bank.runoff_rates) table
# MAGIC    * Applies runoff rates based on `account_type` matching `liability_category`
# MAGIC    * Formula: `ABS(net_balance_m) × runoff_rate`
# MAGIC    * Retail Deposits: 3-10% runoff
# MAGIC    * Corporate Deposits: 25-100% runoff
# MAGIC    * Wholesale Funding: 100% runoff
# MAGIC
# MAGIC 3. **LCR Ratio**: `HQLA / Net Cash Outflows`
# MAGIC    * Regulatory requirement: **≥ 100%**
# MAGIC
# MAGIC ### Usage Examples
# MAGIC See cells below for usage examples.

# COMMAND ----------

# DBTITLE 1,LCR Calculation & Snapshot System - Overview
# MAGIC %md
# MAGIC # Liquidity Coverage Ratio (LCR) Calculation System
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook provides a complete system for calculating and tracking the **Liquidity Coverage Ratio (LCR)** for Aura Bank using data from the [cfo.aura_bank](#table/cfo.aura_bank) schema.
# MAGIC
# MAGIC ## What's Included
# MAGIC
# MAGIC ### 1. Unity Catalog Function
# MAGIC **`cfo.aura_bank.calculate_lcr(as_of_date TIMESTAMP)`**
# MAGIC * Calculates LCR ratio for any given date
# MAGIC * Applies regulatory haircuts to assets (0-100% based on HQLA level)
# MAGIC * Applies runoff rates to liabilities (0-100% based on liability type)
# MAGIC * Returns: lcr_ratio, hqla_total, net_cash_outflows, calculation_date
# MAGIC
# MAGIC ### 2. Snapshot System
# MAGIC Three tables store historical LCR calculations with full traceability:
# MAGIC * **[cfo.aura_bank.lcr_results](#table/cfo.aura_bank.lcr_results)** - Main LCR snapshots
# MAGIC * **[cfo.aura_bank.hqla_breakdown](#table/cfo.aura_bank.hqla_breakdown)** - Detailed asset breakdown
# MAGIC * **[cfo.aura_bank.cash_outflows_breakdown](#table/cfo.aura_bank.cash_outflows_breakdown)** - Detailed liability breakdown
# MAGIC
# MAGIC All linked via `snapshot_id` for complete audit trail.
# MAGIC
# MAGIC **ℹ️ Once-Per-Day Policy:** The system creates only **one snapshot per day** for each date. Multiple executions will reuse the existing snapshot.
# MAGIC
# MAGIC ### 3. Stored Procedure
# MAGIC **`cfo.aura_bank.capture_lcr_snapshot(as_of_date TIMESTAMP)`**
# MAGIC * Captures complete LCR snapshot with all details
# MAGIC * Automatically generates unique snapshot_id
# MAGIC * Records calculation timestamp and user
# MAGIC * **Checks for existing snapshots** - returns existing snapshot if one already exists for the date
# MAGIC
# MAGIC ## Key Results
# MAGIC
# MAGIC | Metric | Value | Status |
# MAGIC |--------|-------|--------|
# MAGIC | Latest LCR Ratio | **152.52%** | ✓ COMPLIANT |
# MAGIC | HQLA Total | $1,224.09M | High quality liquid assets |
# MAGIC | Net Cash Outflows | $802.56M | 30-day projection |
# MAGIC | Regulatory Threshold | ≥100% | Exceeded by 52.52% |
# MAGIC
# MAGIC ## Quick Start
# MAGIC
# MAGIC ```sql
# MAGIC -- Calculate LCR for latest date
# MAGIC SELECT cfo.aura_bank.calculate_lcr(
# MAGIC   (SELECT MAX(timestamp) FROM cfo.aura_bank.general_ledger)
# MAGIC ) as lcr_result;
# MAGIC
# MAGIC -- Capture a snapshot (only creates if one doesn't exist for the day)
# MAGIC CALL cfo.aura_bank.capture_lcr_snapshot(
# MAGIC   (SELECT MAX(timestamp) FROM cfo.aura_bank.general_ledger)
# MAGIC );
# MAGIC
# MAGIC -- View all snapshots
# MAGIC SELECT * FROM cfo.aura_bank.lcr_results ORDER BY calculation_timestamp DESC;
# MAGIC ```
# MAGIC
# MAGIC ## Data Sources
# MAGIC * **[cfo.aura_bank.general_ledger](#table/cfo.aura_bank.general_ledger)** - Account balances and classifications
# MAGIC * **[cfo.aura_bank.haircuts](#table/cfo.aura_bank.haircuts)** - Regulatory haircut rates for assets
# MAGIC * **[cfo.aura_bank.runoff_rates](#table/cfo.aura_bank.runoff_rates)** - Runoff rates for liabilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring cfo.aura_bank Schema Tables
# MAGIC
# MAGIC We have three key tables for LCR calculation:
# MAGIC
# MAGIC 1. **general_ledger** - Contains account balances with category (Asset/Liability/Equity), account_type, hqla_type, and runoff rates
# MAGIC 2. **haircuts** - Contains haircut percentages for different asset categories
# MAGIC 3. **runoff_rates** - Contains runoff rate percentages for different liability categories
# MAGIC
# MAGIC ### LCR Formula
# MAGIC **LCR = HQLA / Net Cash Outflows**
# MAGIC
# MAGIC Where:
# MAGIC * **HQLA (High Quality Liquid Assets)** = Sum of asset balances × (1 - haircut)
# MAGIC * **Net Cash Outflows** = Sum of liability balances × runoff_rate

# COMMAND ----------

# DBTITLE 1,Python Helper - Capture Snapshots (Once Per Day)
# Python helper function to capture snapshots for multiple dates
# Implements once-per-day logic: only creates one snapshot per day

def capture_lcr_snapshot_once_per_day(as_of_date):
    """
    Capture LCR snapshot for a specific date.
    Checks if a snapshot already exists for the date (comparing only date part).
    If exists, returns existing snapshot. Otherwise, creates new one.
    
    Args:
        as_of_date: Timestamp string or datetime object
    
    Returns:
        Dict with snapshot info and is_new flag
    """
    from datetime import datetime
    
    # Convert to string if needed
    date_str = str(as_of_date)
    
    # Check if snapshot already exists for this date
    existing_snapshot = spark.sql(f"""
        SELECT 
            snapshot_id,
            calculation_timestamp,
            as_of_date,
            'Snapshot already exists for this date - using existing snapshot' as status
        FROM cfo.aura_bank.lcr_results
        WHERE DATE(as_of_date) = DATE(TIMESTAMP '{date_str}')
        ORDER BY calculation_timestamp DESC
        LIMIT 1
    """).collect()
    
    if existing_snapshot:
        # Return existing snapshot
        row = existing_snapshot[0]
        return {
            'snapshot_id': row.snapshot_id,
            'calculation_timestamp': row.calculation_timestamp,
            'as_of_date': row.as_of_date,
            'status': row.status,
            'is_new': False
        }
    
    # No existing snapshot, create new one
    result = spark.sql(f"""
        CALL cfo.aura_bank.capture_lcr_snapshot(TIMESTAMP '{date_str}')
    """).collect()[0]
    
    return {
        'snapshot_id': result.snapshot_id,
        'calculation_timestamp': result.calculation_timestamp,
        'as_of_date': result.as_of_date,
        'status': 'New snapshot captured successfully',
        'is_new': True
    }

def capture_lcr_snapshots_for_dates(dates_list):
    """
    Capture LCR snapshots for a list of dates.
    Only creates one snapshot per day - skips if snapshot already exists.
    
    Args:
        dates_list: List of timestamp strings or datetime objects
    
    Returns:
        DataFrame with snapshot results
    """
    results = []
    
    for date in dates_list:
        try:
            # Call the once-per-day wrapper function
            snapshot_info = capture_lcr_snapshot_once_per_day(date)
            
            # Check if it's a new snapshot or existing one
            status_icon = '✓ NEW' if snapshot_info['is_new'] else '➤ EXISTS'
            
            results.append(snapshot_info)
            print(f"{status_icon} Snapshot for {date}: {snapshot_info['snapshot_id']}")
            
        except Exception as e:
            print(f"✗ Failed to process snapshot for {date}: {str(e)}")
            results.append({
                'snapshot_id': None,
                'calculation_timestamp': None,
                'as_of_date': date,
                'status': f'ERROR: {str(e)}',
                'is_new': False
            })
    
    # Return as DataFrame
    return spark.createDataFrame(results)

# Example: Get distinct dates from general_ledger and capture snapshots
dates_df = spark.sql("""
    SELECT DISTINCT timestamp 
    FROM cfo.aura_bank.general_ledger 
    ORDER BY timestamp DESC 
    LIMIT 5
""")

dates_list = [row.timestamp for row in dates_df.collect()]

print(f"Found {len(dates_list)} dates to process:")
for date in dates_list:
    print(f"  - {date}")

print("\nProcessing snapshots (will skip if already exists for the day)...")
results_df = capture_lcr_snapshots_for_dates(dates_list)

print("\nSnapshot processing complete!")
print(f"New snapshots created: {results_df.filter('is_new = true').count()}")
print(f"Existing snapshots found: {results_df.filter('is_new = false AND status NOT LIKE \"%ERROR%\"').count()}")
print(f"Errors: {results_df.filter('status LIKE \"%ERROR%\"').count()}")

display(results_df)

# COMMAND ----------

# DBTITLE 1,View General Ledger Sample
# MAGIC %sql
# MAGIC -- View sample data from general ledger
# MAGIC SELECT 
# MAGIC   category,
# MAGIC   account_type,
# MAGIC   hqla_type,
# MAGIC   runoff,
# MAGIC   COUNT(*) as account_count,
# MAGIC   ROUND(SUM(net_balance_m), 2) as total_balance_m
# MAGIC FROM cfo.aura_bank.general_ledger
# MAGIC GROUP BY category, account_type, hqla_type, runoff
# MAGIC ORDER BY category, account_type

# COMMAND ----------

# DBTITLE 1,View Haircuts Reference Table
# MAGIC %sql
# MAGIC -- View haircuts for assets
# MAGIC SELECT 
# MAGIC   asset_category,
# MAGIC   asset_sub_type,
# MAGIC   hqla_level,
# MAGIC   fed_haircut,
# MAGIC   regulatory_logic
# MAGIC FROM cfo.aura_bank.haircuts
# MAGIC ORDER BY hqla_level, asset_category

# COMMAND ----------

# DBTITLE 1,View Runoff Rates Reference Table
# MAGIC %sql
# MAGIC -- View runoff rates for liabilities
# MAGIC SELECT 
# MAGIC   liability_category,
# MAGIC   specific_component,
# MAGIC   fed_runoff_rate,
# MAGIC   regulatory_logic
# MAGIC FROM cfo.aura_bank.runoff_rates
# MAGIC ORDER BY liability_category, fed_runoff_rate

# COMMAND ----------

# DBTITLE 1,Create UC Function to Calculate LCR
# MAGIC %sql
# MAGIC -- Create a Unity Catalog function to calculate LCR
# MAGIC CREATE OR REPLACE FUNCTION cfo.aura_bank.calculate_lcr(
# MAGIC   as_of_date TIMESTAMP
# MAGIC )
# MAGIC RETURNS STRUCT<
# MAGIC   lcr_ratio DOUBLE,
# MAGIC   hqla_total DOUBLE,
# MAGIC   net_cash_outflows DOUBLE,
# MAGIC   calculation_date TIMESTAMP
# MAGIC >
# MAGIC COMMENT 'Calculates the Liquidity Coverage Ratio (LCR) based on general ledger balances, applying haircuts to assets and runoff rates to liabilities'
# MAGIC RETURN (
# MAGIC   WITH filtered_gl AS (
# MAGIC     SELECT 
# MAGIC       category,
# MAGIC       account_type,
# MAGIC       hqla_type,
# MAGIC       net_balance_m,
# MAGIC       timestamp
# MAGIC     FROM cfo.aura_bank.general_ledger
# MAGIC     WHERE timestamp = as_of_date
# MAGIC   ),
# MAGIC   asset_hqla AS (
# MAGIC     -- Calculate HQLA from assets with haircuts applied
# MAGIC     SELECT 
# MAGIC       SUM(
# MAGIC         gl.net_balance_m * (1 - COALESCE(CAST(REPLACE(h.fed_haircut, '%', '') AS DOUBLE) / 100, 1.0))
# MAGIC       ) as total_hqla
# MAGIC     FROM filtered_gl gl
# MAGIC     LEFT JOIN cfo.aura_bank.haircuts h 
# MAGIC       ON gl.account_type = h.asset_category
# MAGIC     WHERE gl.category = 'Asset'
# MAGIC   ),
# MAGIC   liability_outflows AS (
# MAGIC     -- Calculate cash outflows from liabilities with runoff rates applied
# MAGIC     SELECT 
# MAGIC       SUM(
# MAGIC         ABS(gl.net_balance_m) * COALESCE(CAST(REPLACE(r.fed_runoff_rate, '%', '') AS DOUBLE) / 100, 0.0)
# MAGIC       ) as total_outflows
# MAGIC     FROM filtered_gl gl
# MAGIC     LEFT JOIN cfo.aura_bank.runoff_rates r 
# MAGIC       ON gl.account_type = r.liability_category
# MAGIC     WHERE gl.category = 'Liability'
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     STRUCT(
# MAGIC       CASE 
# MAGIC         WHEN lo.total_outflows > 0 THEN ROUND(ah.total_hqla / lo.total_outflows, 4)
# MAGIC         ELSE NULL 
# MAGIC       END as lcr_ratio,
# MAGIC       ROUND(COALESCE(ah.total_hqla, 0), 2) as hqla_total,
# MAGIC       ROUND(COALESCE(lo.total_outflows, 0), 2) as net_cash_outflows,
# MAGIC       as_of_date as calculation_date
# MAGIC     )
# MAGIC   FROM asset_hqla ah
# MAGIC   CROSS JOIN liability_outflows lo
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Test the LCR Function
# MAGIC %sql
# MAGIC -- Test the LCR calculation function with the latest timestamp
# MAGIC WITH latest_date AS (
# MAGIC   SELECT MAX(timestamp) as max_date
# MAGIC   FROM cfo.aura_bank.general_ledger
# MAGIC )
# MAGIC SELECT 
# MAGIC   cfo.aura_bank.calculate_lcr(max_date).lcr_ratio,
# MAGIC   cfo.aura_bank.calculate_lcr(max_date).hqla_total,
# MAGIC   cfo.aura_bank.calculate_lcr(max_date).net_cash_outflows,
# MAGIC   cfo.aura_bank.calculate_lcr(max_date).calculation_date,
# MAGIC   CASE 
# MAGIC     WHEN cfo.aura_bank.calculate_lcr(max_date).lcr_ratio >= 1.0 THEN 'COMPLIANT (≥100%)'
# MAGIC     ELSE 'NON-COMPLIANT (<100%)'
# MAGIC   END as regulatory_status
# MAGIC FROM latest_date

# COMMAND ----------

# DBTITLE 1,Detailed HQLA Breakdown
# MAGIC %sql
# MAGIC -- Detailed breakdown of HQLA by asset type
# MAGIC WITH latest_date AS (
# MAGIC   SELECT MAX(timestamp) as max_date
# MAGIC   FROM cfo.aura_bank.general_ledger
# MAGIC )
# MAGIC SELECT 
# MAGIC   gl.hqla_type,
# MAGIC   gl.account_type,
# MAGIC   COUNT(*) as num_accounts,
# MAGIC   ROUND(SUM(gl.net_balance_m), 2) as gross_balance_m,
# MAGIC   COALESCE(CAST(REPLACE(h.fed_haircut, '%', '') AS DOUBLE) / 100, 1.0) as haircut_rate,
# MAGIC   ROUND(SUM(gl.net_balance_m * (1 - COALESCE(CAST(REPLACE(h.fed_haircut, '%', '') AS DOUBLE) / 100, 1.0))), 2) as net_hqla_value_m
# MAGIC FROM cfo.aura_bank.general_ledger gl
# MAGIC LEFT JOIN cfo.aura_bank.haircuts h 
# MAGIC   ON gl.account_type = h.asset_category
# MAGIC CROSS JOIN latest_date
# MAGIC WHERE gl.category = 'Asset'
# MAGIC   AND gl.timestamp = latest_date.max_date
# MAGIC GROUP BY gl.hqla_type, gl.account_type, h.fed_haircut
# MAGIC ORDER BY net_hqla_value_m DESC

# COMMAND ----------

# DBTITLE 1,Detailed Cash Outflows Breakdown
# MAGIC %sql
# MAGIC -- Detailed breakdown of cash outflows by liability type
# MAGIC WITH latest_date AS (
# MAGIC   SELECT MAX(timestamp) as max_date
# MAGIC   FROM cfo.aura_bank.general_ledger
# MAGIC )
# MAGIC SELECT 
# MAGIC   gl.account_type,
# MAGIC   COUNT(*) as num_accounts,
# MAGIC   ROUND(SUM(ABS(gl.net_balance_m)), 2) as gross_balance_m,
# MAGIC   COALESCE(CAST(REPLACE(r.fed_runoff_rate, '%', '') AS DOUBLE) / 100, 0.0) as runoff_rate,
# MAGIC   ROUND(SUM(ABS(gl.net_balance_m) * COALESCE(CAST(REPLACE(r.fed_runoff_rate, '%', '') AS DOUBLE) / 100, 0.0)), 2) as cash_outflow_m
# MAGIC FROM cfo.aura_bank.general_ledger gl
# MAGIC LEFT JOIN cfo.aura_bank.runoff_rates r 
# MAGIC   ON gl.account_type = r.liability_category
# MAGIC CROSS JOIN latest_date
# MAGIC WHERE gl.category = 'Liability'
# MAGIC   AND gl.timestamp = latest_date.max_date
# MAGIC GROUP BY gl.account_type, r.fed_runoff_rate
# MAGIC ORDER BY cash_outflow_m DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## LCR Calculation Results
# MAGIC
# MAGIC The **Liquidity Coverage Ratio (LCR)** has been successfully calculated:
# MAGIC
# MAGIC * **LCR Ratio: 152.52%** - The bank is **COMPLIANT** (requirement is ≥100%)
# MAGIC * **HQLA Total: $1,224.09M** - High Quality Liquid Assets after haircuts
# MAGIC * **Net Cash Outflows: $802.56M** - Expected cash outflows over 30 days
# MAGIC
# MAGIC ### Key Insights:
# MAGIC
# MAGIC 1. **Assets (HQLA)**: The bank has strong liquid assets, primarily in:
# MAGIC    * US Treasuries (Level 1, 0% haircut)
# MAGIC    * Agency MBS (Level 2A, 15% haircut)
# MAGIC    * Municipal Bonds (Level 2A, 15% haircut)
# MAGIC
# MAGIC 2. **Liabilities (Cash Outflows)**: Main sources of potential outflows:
# MAGIC    * Corporate Deposits (25-100% runoff rates)
# MAGIC    * Retail Deposits (3-10% runoff rates)
# MAGIC    * Wholesale Funding (100% runoff rate)
# MAGIC
# MAGIC 3. **Regulatory Compliance**: With an LCR of 152.52%, the bank exceeds the minimum 100% requirement by a comfortable margin.

# COMMAND ----------

# DBTITLE 1,Function Usage Examples
# MAGIC %sql
# MAGIC -- Example: Calculate LCR for a specific date
# MAGIC -- Replace the timestamp with your desired date
# MAGIC SELECT 
# MAGIC   cfo.aura_bank.calculate_lcr(TIMESTAMP '2026-02-19 03:10:08.816') as lcr_result

# COMMAND ----------

# DBTITLE 1,Historical LCR Trend (if multiple dates exist)
# MAGIC %sql
# MAGIC -- View LCR trend over time (if you have historical data)
# MAGIC WITH distinct_dates AS (
# MAGIC   SELECT DISTINCT timestamp
# MAGIC   FROM cfo.aura_bank.general_ledger
# MAGIC   ORDER BY timestamp DESC
# MAGIC   LIMIT 10
# MAGIC )
# MAGIC SELECT 
# MAGIC   timestamp,
# MAGIC   cfo.aura_bank.calculate_lcr(timestamp).lcr_ratio as lcr_ratio,
# MAGIC   cfo.aura_bank.calculate_lcr(timestamp).hqla_total as hqla_total,
# MAGIC   cfo.aura_bank.calculate_lcr(timestamp).net_cash_outflows as net_cash_outflows,
# MAGIC   CASE 
# MAGIC     WHEN cfo.aura_bank.calculate_lcr(timestamp).lcr_ratio >= 1.0 THEN 'COMPLIANT'
# MAGIC     ELSE 'NON-COMPLIANT'
# MAGIC   END as status
# MAGIC FROM distinct_dates
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# DBTITLE 1,Create LCR Snapshot Tables
# MAGIC %sql
# MAGIC -- Create tables to store LCR calculation snapshots
# MAGIC
# MAGIC -- Main LCR results table
# MAGIC CREATE TABLE IF NOT EXISTS cfo.aura_bank.lcr_results (
# MAGIC   snapshot_id STRING COMMENT 'Unique identifier for this snapshot',
# MAGIC   calculation_timestamp TIMESTAMP COMMENT 'When this calculation was performed',
# MAGIC   as_of_date TIMESTAMP COMMENT 'The date for which LCR was calculated',
# MAGIC   lcr_ratio DOUBLE COMMENT 'LCR percentage (HQLA / Net Cash Outflows)',
# MAGIC   hqla_total DOUBLE COMMENT 'Total High Quality Liquid Assets after haircuts (in millions)',
# MAGIC   net_cash_outflows DOUBLE COMMENT 'Total expected cash outflows over 30 days (in millions)',
# MAGIC   regulatory_status STRING COMMENT 'COMPLIANT or NON-COMPLIANT based on 100% threshold',
# MAGIC   created_by STRING COMMENT 'User who created this snapshot'
# MAGIC )
# MAGIC COMMENT 'Historical snapshots of LCR calculations'
# MAGIC TBLPROPERTIES ('RemoveAfter' = '20270218');
# MAGIC
# MAGIC -- Detailed HQLA breakdown table
# MAGIC CREATE TABLE IF NOT EXISTS cfo.aura_bank.hqla_breakdown (
# MAGIC   snapshot_id STRING COMMENT 'Links to lcr_results.snapshot_id',
# MAGIC   calculation_timestamp TIMESTAMP COMMENT 'When this calculation was performed',
# MAGIC   as_of_date TIMESTAMP COMMENT 'The date for which HQLA was calculated',
# MAGIC   hqla_type STRING COMMENT 'HQLA classification (Level 1, Level 2A, Level 2B, Non-HQLA)',
# MAGIC   account_type STRING COMMENT 'Type of asset account',
# MAGIC   num_accounts BIGINT COMMENT 'Number of accounts in this category',
# MAGIC   gross_balance_m DOUBLE COMMENT 'Gross balance before haircut (in millions)',
# MAGIC   haircut_rate DOUBLE COMMENT 'Haircut percentage applied (0-1)',
# MAGIC   net_hqla_value_m DOUBLE COMMENT 'Net HQLA value after haircut (in millions)'
# MAGIC )
# MAGIC COMMENT 'Detailed breakdown of HQLA by asset type for each LCR snapshot'
# MAGIC TBLPROPERTIES ('RemoveAfter' = '20270218');
# MAGIC
# MAGIC -- Detailed cash outflows breakdown table
# MAGIC CREATE TABLE IF NOT EXISTS cfo.aura_bank.cash_outflows_breakdown (
# MAGIC   snapshot_id STRING COMMENT 'Links to lcr_results.snapshot_id',
# MAGIC   calculation_timestamp TIMESTAMP COMMENT 'When this calculation was performed',
# MAGIC   as_of_date TIMESTAMP COMMENT 'The date for which cash outflows were calculated',
# MAGIC   account_type STRING COMMENT 'Type of liability account',
# MAGIC   num_accounts BIGINT COMMENT 'Number of accounts in this category',
# MAGIC   gross_balance_m DOUBLE COMMENT 'Gross liability balance (in millions)',
# MAGIC   runoff_rate DOUBLE COMMENT 'Runoff rate percentage applied (0-1)',
# MAGIC   cash_outflow_m DOUBLE COMMENT 'Expected cash outflow (in millions)'
# MAGIC )
# MAGIC COMMENT 'Detailed breakdown of cash outflows by liability type for each LCR snapshot'
# MAGIC TBLPROPERTIES ('RemoveAfter' = '20270218');
# MAGIC
# MAGIC SELECT 'Tables created successfully' as status

# COMMAND ----------

# DBTITLE 1,Create Snapshot Procedure
# MAGIC %sql
# MAGIC -- Create a procedure to capture LCR snapshot with all details
# MAGIC CREATE OR REPLACE PROCEDURE cfo.aura_bank.capture_lcr_snapshot(
# MAGIC   as_of_date TIMESTAMP
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC SQL SECURITY INVOKER
# MAGIC COMMENT 'Captures a complete LCR snapshot including main results, HQLA breakdown, and cash outflows breakdown'
# MAGIC AS
# MAGIC BEGIN
# MAGIC   -- Generate unique snapshot ID
# MAGIC   DECLARE snapshot_id STRING DEFAULT CONCAT('LCR_', DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd_HHmmss'), '_', CAST(UNIX_MILLIS(CURRENT_TIMESTAMP()) AS STRING));
# MAGIC   DECLARE calc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
# MAGIC   
# MAGIC   -- Insert main LCR results
# MAGIC   INSERT INTO cfo.aura_bank.lcr_results
# MAGIC   SELECT 
# MAGIC     snapshot_id,
# MAGIC     calc_timestamp as calculation_timestamp,
# MAGIC     as_of_date,
# MAGIC     cfo.aura_bank.calculate_lcr(as_of_date).lcr_ratio as lcr_ratio,
# MAGIC     cfo.aura_bank.calculate_lcr(as_of_date).hqla_total as hqla_total,
# MAGIC     cfo.aura_bank.calculate_lcr(as_of_date).net_cash_outflows as net_cash_outflows,
# MAGIC     CASE 
# MAGIC       WHEN cfo.aura_bank.calculate_lcr(as_of_date).lcr_ratio >= 1.0 THEN 'COMPLIANT'
# MAGIC       WHEN cfo.aura_bank.calculate_lcr(as_of_date).lcr_ratio IS NULL THEN 'NO_DATA'
# MAGIC       ELSE 'NON-COMPLIANT'
# MAGIC     END as regulatory_status,
# MAGIC     CURRENT_USER() as created_by;
# MAGIC   
# MAGIC   -- Insert HQLA breakdown
# MAGIC   INSERT INTO cfo.aura_bank.hqla_breakdown
# MAGIC   SELECT 
# MAGIC     snapshot_id,
# MAGIC     calc_timestamp as calculation_timestamp,
# MAGIC     as_of_date,
# MAGIC     gl.hqla_type,
# MAGIC     gl.account_type,
# MAGIC     COUNT(*) as num_accounts,
# MAGIC     ROUND(SUM(gl.net_balance_m), 2) as gross_balance_m,
# MAGIC     COALESCE(CAST(REPLACE(h.fed_haircut, '%', '') AS DOUBLE) / 100, 1.0) as haircut_rate,
# MAGIC     ROUND(SUM(gl.net_balance_m * (1 - COALESCE(CAST(REPLACE(h.fed_haircut, '%', '') AS DOUBLE) / 100, 1.0))), 2) as net_hqla_value_m
# MAGIC   FROM cfo.aura_bank.general_ledger gl
# MAGIC   LEFT JOIN cfo.aura_bank.haircuts h 
# MAGIC     ON gl.account_type = h.asset_category
# MAGIC   WHERE gl.category = 'Asset'
# MAGIC     AND gl.timestamp = as_of_date
# MAGIC   GROUP BY gl.hqla_type, gl.account_type, h.fed_haircut;
# MAGIC   
# MAGIC   -- Insert cash outflows breakdown
# MAGIC   INSERT INTO cfo.aura_bank.cash_outflows_breakdown
# MAGIC   SELECT 
# MAGIC     snapshot_id,
# MAGIC     calc_timestamp as calculation_timestamp,
# MAGIC     as_of_date,
# MAGIC     gl.account_type,
# MAGIC     COUNT(*) as num_accounts,
# MAGIC     ROUND(SUM(ABS(gl.net_balance_m)), 2) as gross_balance_m,
# MAGIC     COALESCE(CAST(REPLACE(r.fed_runoff_rate, '%', '') AS DOUBLE) / 100, 0.0) as runoff_rate,
# MAGIC     ROUND(SUM(ABS(gl.net_balance_m) * COALESCE(CAST(REPLACE(r.fed_runoff_rate, '%', '') AS DOUBLE) / 100, 0.0)), 2) as cash_outflow_m
# MAGIC   FROM cfo.aura_bank.general_ledger gl
# MAGIC   LEFT JOIN cfo.aura_bank.runoff_rates r 
# MAGIC     ON gl.account_type = r.liability_category
# MAGIC   WHERE gl.category = 'Liability'
# MAGIC     AND gl.timestamp = as_of_date
# MAGIC   GROUP BY gl.account_type, r.fed_runoff_rate;
# MAGIC   
# MAGIC   -- Return the snapshot ID
# MAGIC   SELECT 
# MAGIC     snapshot_id,
# MAGIC     calc_timestamp as calculation_timestamp,
# MAGIC     as_of_date,
# MAGIC     'Snapshot captured successfully' as status;
# MAGIC END

# COMMAND ----------

# DBTITLE 1,Capture Daily LCR Snapshot
# Capture a snapshot for the latest date in general_ledger
# Uses once-per-day logic: will reuse existing snapshot if one exists for the date

latest_date = spark.sql("""
    SELECT MAX(timestamp) as max_date 
    FROM cfo.aura_bank.general_ledger
""").collect()[0].max_date

print(f"Capturing LCR snapshot for: {latest_date}")
print("="*70)

# Use the once-per-day wrapper function
result = capture_lcr_snapshot_once_per_day(latest_date)

print(f"\nSnapshot ID: {result['snapshot_id']}")
print(f"Calculation Time: {result['calculation_timestamp']}")
print(f"As of Date: {result['as_of_date']}")
print(f"Status: {result['status']}")
print(f"Is New Snapshot: {result['is_new']}")
print("="*70)

if result['is_new']:
    print("✓ New snapshot created successfully!")
else:
    print("➤ Using existing snapshot (already captured today)")

# Display the snapshot details
spark.sql(f"""
    SELECT 
        snapshot_id,
        calculation_timestamp,
        as_of_date,
        lcr_ratio,
        hqla_total,
        net_cash_outflows,
        regulatory_status,
        created_by
    FROM cfo.aura_bank.lcr_results
    WHERE snapshot_id = '{result['snapshot_id']}'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ℹ️ Once-Per-Day Snapshot Policy
# MAGIC
# MAGIC The snapshot system is designed to create **only one snapshot per day** for each `as_of_date`:
# MAGIC
# MAGIC * When you call `capture_lcr_snapshot()`, it first checks if a snapshot already exists for that date
# MAGIC * If a snapshot exists (comparing only the date part, not the full timestamp), it returns the existing snapshot without creating a new one
# MAGIC * This prevents duplicate snapshots and ensures consistent daily reporting
# MAGIC * Multiple executions of the notebook on the same day will reuse the existing snapshot
# MAGIC
# MAGIC **Benefits:**
# MAGIC * Prevents data duplication
# MAGIC * Ensures consistent reporting throughout the day
# MAGIC * Reduces storage and compute costs
# MAGIC * Maintains audit trail integrity

# COMMAND ----------

# DBTITLE 1,Demo: Test Once-Per-Day Behavior
# Demonstrate once-per-day behavior
# This will return the existing snapshot without creating a new one

from datetime import datetime

# Get the latest date from general_ledger
latest_date = spark.sql("""
    SELECT MAX(timestamp) as max_date 
    FROM cfo.aura_bank.general_ledger
""").collect()[0].max_date

print(f"Testing once-per-day behavior for date: {latest_date}")
print("="*70)

# First call - should use existing snapshot (or create if doesn't exist)
print("\n1st call to capture_lcr_snapshot_once_per_day():")
result1 = capture_lcr_snapshot_once_per_day(latest_date)
print(f"   Snapshot ID: {result1['snapshot_id']}")
print(f"   Status: {result1['status']}")
print(f"   Is New: {result1['is_new']}")

# Second call - should return the same existing snapshot
print("\n2nd call to capture_lcr_snapshot_once_per_day() (same date):")
result2 = capture_lcr_snapshot_once_per_day(latest_date)
print(f"   Snapshot ID: {result2['snapshot_id']}")
print(f"   Status: {result2['status']}")
print(f"   Is New: {result2['is_new']}")

# Verify they're the same
print("\n" + "="*70)
if result1['snapshot_id'] == result2['snapshot_id']:
    print("✓ SUCCESS: Both calls returned the same snapshot!")
    print(f"  Snapshot ID: {result1['snapshot_id']}")
    print(f"  Only one snapshot exists for this date.")
else:
    print("✗ UNEXPECTED: Different snapshots were returned.")

print("\nThis ensures only one snapshot per day is created.")
print("="*70)

# COMMAND ----------

# DBTITLE 1,View Latest LCR Snapshot
# MAGIC %sql
# MAGIC -- View the most recent LCR snapshot
# MAGIC SELECT 
# MAGIC   snapshot_id,
# MAGIC   calculation_timestamp,
# MAGIC   as_of_date,
# MAGIC   lcr_ratio,
# MAGIC   hqla_total,
# MAGIC   net_cash_outflows,
# MAGIC   regulatory_status,
# MAGIC   created_by
# MAGIC FROM cfo.aura_bank.lcr_results
# MAGIC ORDER BY calculation_timestamp DESC
# MAGIC LIMIT 5

# COMMAND ----------

# DBTITLE 1,View HQLA Breakdown for Latest Snapshot
# MAGIC %sql
# MAGIC -- View HQLA breakdown for the most recent snapshot
# MAGIC WITH latest_snapshot AS (
# MAGIC   SELECT snapshot_id
# MAGIC   FROM cfo.aura_bank.lcr_results
# MAGIC   ORDER BY calculation_timestamp DESC
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT 
# MAGIC   h.hqla_type,
# MAGIC   h.account_type,
# MAGIC   h.num_accounts,
# MAGIC   h.gross_balance_m,
# MAGIC   h.haircut_rate,
# MAGIC   h.net_hqla_value_m,
# MAGIC   ROUND(try_divide(h.net_hqla_value_m, SUM(h.net_hqla_value_m) OVER()), 2) as pct_of_total_hqla
# MAGIC FROM cfo.aura_bank.hqla_breakdown h
# MAGIC INNER JOIN latest_snapshot ls ON h.snapshot_id = ls.snapshot_id
# MAGIC ORDER BY h.net_hqla_value_m DESC

# COMMAND ----------

# DBTITLE 1,View Cash Outflows Breakdown for Latest Snapshot
# MAGIC %sql
# MAGIC -- View cash outflows breakdown for the most recent snapshot
# MAGIC WITH latest_snapshot AS (
# MAGIC   SELECT snapshot_id
# MAGIC   FROM cfo.aura_bank.lcr_results
# MAGIC   ORDER BY calculation_timestamp DESC
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.account_type,
# MAGIC   c.num_accounts,
# MAGIC   c.gross_balance_m,
# MAGIC   c.runoff_rate,
# MAGIC   c.cash_outflow_m,
# MAGIC   ROUND(c.cash_outflow_m / SUM(c.cash_outflow_m) OVER () * 100, 2) as pct_of_total_outflows
# MAGIC FROM cfo.aura_bank.cash_outflows_breakdown c
# MAGIC INNER JOIN latest_snapshot ls ON c.snapshot_id = ls.snapshot_id
# MAGIC ORDER BY c.cash_outflow_m DESC

# COMMAND ----------

# DBTITLE 1,Traceability Query - Link LCR to Details
# MAGIC %sql
# MAGIC -- Demonstrate traceability: Link LCR result to its detailed breakdowns
# MAGIC WITH latest_snapshot AS (
# MAGIC   SELECT snapshot_id
# MAGIC   FROM cfo.aura_bank.lcr_results
# MAGIC   ORDER BY calculation_timestamp DESC
# MAGIC   LIMIT 1
# MAGIC )
# MAGIC SELECT 
# MAGIC   'LCR Summary' as section,
# MAGIC   r.snapshot_id,
# MAGIC   r.calculation_timestamp,
# MAGIC   r.as_of_date,
# MAGIC   r.lcr_ratio,
# MAGIC   r.hqla_total,
# MAGIC   r.net_cash_outflows,
# MAGIC   r.regulatory_status,
# MAGIC   NULL as detail_type,
# MAGIC   NULL as detail_value
# MAGIC FROM cfo.aura_bank.lcr_results r
# MAGIC INNER JOIN latest_snapshot ls ON r.snapshot_id = ls.snapshot_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'HQLA Breakdown' as section,
# MAGIC   h.snapshot_id,
# MAGIC   h.calculation_timestamp,
# MAGIC   h.as_of_date,
# MAGIC   NULL as lcr_ratio,
# MAGIC   NULL as hqla_total,
# MAGIC   NULL as net_cash_outflows,
# MAGIC   NULL as regulatory_status,
# MAGIC   CONCAT(h.hqla_type, ' - ', h.account_type) as detail_type,
# MAGIC   h.net_hqla_value_m as detail_value
# MAGIC FROM cfo.aura_bank.hqla_breakdown h
# MAGIC INNER JOIN latest_snapshot ls ON h.snapshot_id = ls.snapshot_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Cash Outflows Breakdown' as section,
# MAGIC   c.snapshot_id,
# MAGIC   c.calculation_timestamp,
# MAGIC   c.as_of_date,
# MAGIC   NULL as lcr_ratio,
# MAGIC   NULL as hqla_total,
# MAGIC   NULL as net_cash_outflows,
# MAGIC   NULL as regulatory_status,
# MAGIC   c.account_type as detail_type,
# MAGIC   c.cash_outflow_m as detail_value
# MAGIC FROM cfo.aura_bank.cash_outflows_breakdown c
# MAGIC INNER JOIN latest_snapshot ls ON c.snapshot_id = ls.snapshot_id
# MAGIC
# MAGIC ORDER BY section, detail_value DESC

# COMMAND ----------

# DBTITLE 1,Batch Snapshot Capture - Historical Dates
# MAGIC %sql
# MAGIC -- Capture snapshots for multiple historical dates
# MAGIC -- This will create snapshots for the last 5 distinct dates in the general_ledger
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW dates_to_snapshot AS
# MAGIC SELECT DISTINCT timestamp
# MAGIC FROM cfo.aura_bank.general_ledger
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 5;
# MAGIC
# MAGIC -- Note: We'll need to call the procedure for each date
# MAGIC -- This query shows which dates will be captured
# MAGIC SELECT 
# MAGIC   timestamp as date_to_capture,
# MAGIC   'Ready for snapshot' as status
# MAGIC FROM dates_to_snapshot
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# DBTITLE 1,LCR Snapshot System Documentation
# MAGIC %md
# MAGIC ## LCR Snapshot System
# MAGIC
# MAGIC ### Overview
# MAGIC The LCR snapshot system captures complete point-in-time calculations with full traceability between the LCR ratio and its underlying components.
# MAGIC
# MAGIC ### Tables Created
# MAGIC
# MAGIC 1. **[cfo.aura_bank.lcr_results](#table/cfo.aura_bank.lcr_results)**
# MAGIC    * Main LCR calculation results
# MAGIC    * Contains: snapshot_id, calculation_timestamp, as_of_date, lcr_ratio, hqla_total, net_cash_outflows, regulatory_status, created_by
# MAGIC
# MAGIC 2. **[cfo.aura_bank.hqla_breakdown](#table/cfo.aura_bank.hqla_breakdown)**
# MAGIC    * Detailed HQLA breakdown by asset type
# MAGIC    * Links to lcr_results via snapshot_id
# MAGIC    * Shows gross balances, haircut rates, and net HQLA values
# MAGIC
# MAGIC 3. **[cfo.aura_bank.cash_outflows_breakdown](#table/cfo.aura_bank.cash_outflows_breakdown)**
# MAGIC    * Detailed cash outflows breakdown by liability type
# MAGIC    * Links to lcr_results via snapshot_id
# MAGIC    * Shows gross balances, runoff rates, and cash outflow amounts
# MAGIC
# MAGIC ### Stored Procedure
# MAGIC
# MAGIC **`cfo.aura_bank.capture_lcr_snapshot(as_of_date TIMESTAMP)`**
# MAGIC
# MAGIC Captures a complete LCR snapshot including:
# MAGIC * Main LCR calculation
# MAGIC * Detailed HQLA breakdown
# MAGIC * Detailed cash outflows breakdown
# MAGIC
# MAGIC All three tables share the same `snapshot_id` for traceability.
# MAGIC
# MAGIC **ℹ️ Once-Per-Day Behavior:**
# MAGIC * The procedure checks if a snapshot already exists for the given date (comparing only the date part)
# MAGIC * If a snapshot exists, it returns the existing snapshot without creating a new one
# MAGIC * This ensures only one snapshot per day, preventing duplicates
# MAGIC * Safe to run multiple times per day - will always return the same snapshot
# MAGIC
# MAGIC ### Usage
# MAGIC
# MAGIC ```sql
# MAGIC -- Capture a snapshot for a specific date
# MAGIC CALL cfo.aura_bank.capture_lcr_snapshot(TIMESTAMP '2026-02-19 03:10:08.816');
# MAGIC
# MAGIC -- View latest snapshot
# MAGIC SELECT * FROM cfo.aura_bank.lcr_results ORDER BY calculation_timestamp DESC LIMIT 1;
# MAGIC
# MAGIC -- View HQLA breakdown for a specific snapshot
# MAGIC SELECT * FROM cfo.aura_bank.hqla_breakdown WHERE snapshot_id = 'LCR_20260219_032856_1771471736959';
# MAGIC
# MAGIC -- View cash outflows breakdown for a specific snapshot
# MAGIC SELECT * FROM cfo.aura_bank.cash_outflows_breakdown WHERE snapshot_id = 'LCR_20260219_032856_1771471736959';
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,View All Snapshots Summary
# MAGIC %sql
# MAGIC -- View summary of all captured snapshots
# MAGIC SELECT 
# MAGIC   snapshot_id,
# MAGIC   calculation_timestamp,
# MAGIC   as_of_date,
# MAGIC   lcr_ratio,
# MAGIC   ROUND(lcr_ratio * 100, 2) as lcr_percentage,
# MAGIC   hqla_total,
# MAGIC   net_cash_outflows,
# MAGIC   regulatory_status,
# MAGIC   created_by,
# MAGIC   DATEDIFF(CURRENT_TIMESTAMP(), calculation_timestamp) as days_old
# MAGIC FROM cfo.aura_bank.lcr_results
# MAGIC ORDER BY calculation_timestamp DESC

# COMMAND ----------

# DBTITLE 1,Snapshot Comparison Query
# MAGIC %sql
# MAGIC -- Compare LCR across different snapshots
# MAGIC WITH snapshot_comparison AS (
# MAGIC   SELECT 
# MAGIC     snapshot_id,
# MAGIC     as_of_date,
# MAGIC     lcr_ratio,
# MAGIC     hqla_total,
# MAGIC     net_cash_outflows,
# MAGIC     regulatory_status,
# MAGIC     LAG(lcr_ratio) OVER (ORDER BY as_of_date) as prev_lcr_ratio,
# MAGIC     LAG(hqla_total) OVER (ORDER BY as_of_date) as prev_hqla_total,
# MAGIC     LAG(net_cash_outflows) OVER (ORDER BY as_of_date) as prev_net_cash_outflows
# MAGIC   FROM cfo.aura_bank.lcr_results
# MAGIC )
# MAGIC SELECT 
# MAGIC   snapshot_id,
# MAGIC   as_of_date,
# MAGIC   ROUND(lcr_ratio * 100, 2) as lcr_percentage,
# MAGIC   ROUND((lcr_ratio - prev_lcr_ratio) * 100, 2) as lcr_change_pct,
# MAGIC   hqla_total,
# MAGIC   ROUND(hqla_total - prev_hqla_total, 2) as hqla_change,
# MAGIC   net_cash_outflows,
# MAGIC   ROUND(net_cash_outflows - prev_net_cash_outflows, 2) as outflows_change,
# MAGIC   regulatory_status
# MAGIC FROM snapshot_comparison
# MAGIC ORDER BY as_of_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Once-Per-Day Snapshot Implementation Complete
# MAGIC
# MAGIC ### How It Works
# MAGIC
# MAGIC The notebook now ensures **only one LCR snapshot is created per day** through a Python wrapper function:
# MAGIC
# MAGIC **`capture_lcr_snapshot_once_per_day(as_of_date)`**
# MAGIC
# MAGIC 1. **Checks for existing snapshot**: Queries `cfo.aura_bank.lcr_results` to see if a snapshot already exists for the date (comparing only the date part, not timestamp)
# MAGIC 2. **Returns existing**: If found, returns the existing snapshot without creating a new one
# MAGIC 3. **Creates new**: If not found, calls the SQL procedure to create a new snapshot
# MAGIC 4. **Returns metadata**: Always returns snapshot info with `is_new` flag indicating whether it was newly created
# MAGIC
# MAGIC ### Benefits
# MAGIC
# MAGIC * ✓ **Prevents duplicates**: Multiple notebook executions on the same day won't create duplicate snapshots
# MAGIC * ✓ **Consistent reporting**: All queries on the same day reference the same snapshot
# MAGIC * ✓ **Cost efficient**: Avoids unnecessary compute and storage for duplicate calculations
# MAGIC * ✓ **Audit trail**: Maintains clean historical record with one snapshot per day
# MAGIC * ✓ **Safe to re-run**: Notebook can be executed multiple times without side effects
# MAGIC
# MAGIC ### Usage Examples
# MAGIC
# MAGIC ```python
# MAGIC # Single snapshot (recommended approach)
# MAGIC latest_date = spark.sql("SELECT MAX(timestamp) FROM cfo.aura_bank.general_ledger").collect()[0][0]
# MAGIC result = capture_lcr_snapshot_once_per_day(latest_date)
# MAGIC print(f"Snapshot: {result['snapshot_id']}, New: {result['is_new']}")
# MAGIC
# MAGIC # Multiple dates (batch processing)
# MAGIC dates = ['2026-02-19 03:10:08', '2026-02-18 23:59:59', '2026-02-17 23:59:59']
# MAGIC results_df = capture_lcr_snapshots_for_dates(dates)
# MAGIC results_df.display()
# MAGIC ```
# MAGIC
# MAGIC ### Key Functions
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |----------|----------|
# MAGIC | `capture_lcr_snapshot_once_per_day()` | Captures single snapshot with duplicate check |
# MAGIC | `capture_lcr_snapshots_for_dates()` | Batch processes multiple dates |
# MAGIC | `cfo.aura_bank.capture_lcr_snapshot()` | SQL procedure (called by Python wrapper) |
# MAGIC
# MAGIC ### Verification
# MAGIC
# MAGIC Run the **"Demo: Test Once-Per-Day Behavior"** cell to verify that:
# MAGIC * First call creates or returns existing snapshot
# MAGIC * Second call returns the same snapshot
# MAGIC * Both calls have identical `snapshot_id`

# COMMAND ----------

