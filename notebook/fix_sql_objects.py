# Databricks notebook source
# DBTITLE 1,Fix calculate_lcr function - use DATE() matching
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION banking_cfo_treasury.aura_bank.calculate_lcr(
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
# MAGIC     FROM banking_cfo_treasury.aura_bank.general_ledger
# MAGIC     WHERE DATE(timestamp) = DATE(as_of_date)
# MAGIC   ),
# MAGIC   asset_hqla AS (
# MAGIC     SELECT
# MAGIC       SUM(
# MAGIC         gl.net_balance_m * (1 - COALESCE(CAST(REPLACE(h.fed_haircut, '%', '') AS DOUBLE) / 100, 1.0))
# MAGIC       ) as total_hqla
# MAGIC     FROM filtered_gl gl
# MAGIC     LEFT JOIN banking_cfo_treasury.aura_bank.haircuts h
# MAGIC       ON gl.account_type = h.asset_category
# MAGIC     WHERE gl.category = 'Asset'
# MAGIC   ),
# MAGIC   liability_outflows AS (
# MAGIC     SELECT
# MAGIC       SUM(
# MAGIC         ABS(gl.net_balance_m) * COALESCE(CAST(REPLACE(r.fed_runoff_rate, '%', '') AS DOUBLE) / 100, 0.0)
# MAGIC       ) as total_outflows
# MAGIC     FROM filtered_gl gl
# MAGIC     LEFT JOIN banking_cfo_treasury.aura_bank.runoff_rates r
# MAGIC       ON gl.account_type = r.liability_category
# MAGIC     WHERE gl.category = 'Liability'
# MAGIC   )
# MAGIC   SELECT
# MAGIC     STRUCT(
# MAGIC       CAST(CASE
# MAGIC         WHEN lo.total_outflows > 0 THEN ROUND(ah.total_hqla / lo.total_outflows, 4)
# MAGIC         ELSE CAST(NULL AS DOUBLE)
# MAGIC       END AS DOUBLE) as lcr_ratio,
# MAGIC       CAST(ROUND(COALESCE(ah.total_hqla, CAST(0 AS DOUBLE)), 2) AS DOUBLE) as hqla_total,
# MAGIC       CAST(ROUND(COALESCE(lo.total_outflows, CAST(0 AS DOUBLE)), 2) AS DOUBLE) as net_cash_outflows,
# MAGIC       CAST(as_of_date AS TIMESTAMP) as calculation_date
# MAGIC     )
# MAGIC   FROM asset_hqla ah
# MAGIC   CROSS JOIN liability_outflows lo
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Fix capture_lcr_snapshot procedure - use DATE() matching
# MAGIC %sql
# MAGIC CREATE OR REPLACE PROCEDURE banking_cfo_treasury.aura_bank.capture_lcr_snapshot(
# MAGIC   as_of_date TIMESTAMP
# MAGIC )
# MAGIC LANGUAGE SQL
# MAGIC SQL SECURITY INVOKER
# MAGIC COMMENT 'Captures a complete LCR snapshot including main results, HQLA breakdown, and cash outflows breakdown'
# MAGIC AS
# MAGIC BEGIN
# MAGIC   DECLARE snapshot_id STRING DEFAULT CONCAT('LCR_', DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd_HHmmss'), '_', CAST(UNIX_MILLIS(CURRENT_TIMESTAMP()) AS STRING));
# MAGIC   DECLARE calc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
# MAGIC
# MAGIC   INSERT INTO banking_cfo_treasury.aura_bank.lcr_results
# MAGIC   SELECT
# MAGIC     snapshot_id,
# MAGIC     calc_timestamp as calculation_timestamp,
# MAGIC     as_of_date,
# MAGIC     banking_cfo_treasury.aura_bank.calculate_lcr(as_of_date).lcr_ratio as lcr_ratio,
# MAGIC     banking_cfo_treasury.aura_bank.calculate_lcr(as_of_date).hqla_total as hqla_total,
# MAGIC     banking_cfo_treasury.aura_bank.calculate_lcr(as_of_date).net_cash_outflows as net_cash_outflows,
# MAGIC     CASE
# MAGIC       WHEN banking_cfo_treasury.aura_bank.calculate_lcr(as_of_date).lcr_ratio >= 1.0 THEN 'COMPLIANT'
# MAGIC       WHEN banking_cfo_treasury.aura_bank.calculate_lcr(as_of_date).lcr_ratio IS NULL THEN 'NO_DATA'
# MAGIC       ELSE 'NON-COMPLIANT'
# MAGIC     END as regulatory_status,
# MAGIC     CURRENT_USER() as created_by;
# MAGIC
# MAGIC   INSERT INTO banking_cfo_treasury.aura_bank.hqla_breakdown
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
# MAGIC   FROM banking_cfo_treasury.aura_bank.general_ledger gl
# MAGIC   LEFT JOIN banking_cfo_treasury.aura_bank.haircuts h
# MAGIC     ON gl.account_type = h.asset_category
# MAGIC   WHERE gl.category = 'Asset'
# MAGIC     AND DATE(gl.timestamp) = DATE(as_of_date)
# MAGIC   GROUP BY gl.hqla_type, gl.account_type, h.fed_haircut;
# MAGIC
# MAGIC   INSERT INTO banking_cfo_treasury.aura_bank.cash_outflows_breakdown
# MAGIC   SELECT
# MAGIC     snapshot_id,
# MAGIC     calc_timestamp as calculation_timestamp,
# MAGIC     as_of_date,
# MAGIC     gl.account_type,
# MAGIC     COUNT(*) as num_accounts,
# MAGIC     ROUND(SUM(ABS(gl.net_balance_m)), 2) as gross_balance_m,
# MAGIC     COALESCE(CAST(REPLACE(r.fed_runoff_rate, '%', '') AS DOUBLE) / 100, 0.0) as runoff_rate,
# MAGIC     ROUND(SUM(ABS(gl.net_balance_m) * COALESCE(CAST(REPLACE(r.fed_runoff_rate, '%', '') AS DOUBLE) / 100, 0.0)), 2) as cash_outflow_m
# MAGIC   FROM banking_cfo_treasury.aura_bank.general_ledger gl
# MAGIC   LEFT JOIN banking_cfo_treasury.aura_bank.runoff_rates r
# MAGIC     ON gl.account_type = r.liability_category
# MAGIC   WHERE gl.category = 'Liability'
# MAGIC     AND DATE(gl.timestamp) = DATE(as_of_date)
# MAGIC   GROUP BY gl.account_type, r.fed_runoff_rate;
# MAGIC
# MAGIC   SELECT
# MAGIC     snapshot_id,
# MAGIC     calc_timestamp as calculation_timestamp,
# MAGIC     as_of_date,
# MAGIC     'Snapshot captured successfully' as status;
# MAGIC END

# COMMAND ----------

# DBTITLE 1,Verify
# MAGIC %sql
# MAGIC -- Test the fixed function
# MAGIC WITH latest_date AS (
# MAGIC   SELECT MAX(timestamp) as max_date
# MAGIC   FROM banking_cfo_treasury.aura_bank.general_ledger
# MAGIC )
# MAGIC SELECT
# MAGIC   banking_cfo_treasury.aura_bank.calculate_lcr(max_date).*
# MAGIC FROM latest_date
