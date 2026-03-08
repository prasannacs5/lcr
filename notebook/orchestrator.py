# Databricks notebook source
# DBTITLE 1,Insert latest assets and liabilities with project info
import uuid
from datetime import datetime

# Generate unique project identifiers
project_id = str(uuid.uuid4())
project_timestamp = datetime.now()
project_name = f"LCR_Analysis_{project_timestamp.strftime('%Y%m%d_%H%M%S')}"

print(f"Project ID: {project_id}")
print(f"Project Name: {project_name}")
print(f"Project Timestamp: {project_timestamp}")

# Insert project into lcr_projects table
insert_project_query = f"""
    INSERT INTO prasanna.lcr.lcr_projects (id, timestamp, project_name)
    VALUES ('{project_id}', '{project_timestamp}', '{project_name}')
"""

spark.sql(insert_project_query)
print(f"\n✓ Inserted project into lcr_projects")

# Get latest timestamp from assets table
latest_assets_df = spark.sql("""
    SELECT MAX(timestamp) as max_timestamp
    FROM prasanna.lcr.assets
""")
latest_assets_timestamp = latest_assets_df.collect()[0]['max_timestamp']
print(f"\nLatest Assets Timestamp: {latest_assets_timestamp}")

# Get latest timestamp from liabilities table
latest_liabilities_df = spark.sql("""
    SELECT MAX(timestamp) as max_timestamp
    FROM prasanna.lcr.liabilities
""")
latest_liabilities_timestamp = latest_liabilities_df.collect()[0]['max_timestamp']
print(f"Latest Liabilities Timestamp: {latest_liabilities_timestamp}")

# Insert assets with latest timestamp
insert_assets_query = f"""
    INSERT INTO prasanna.lcr.lcr_assets
    SELECT 
        timestamp,
        '{project_name}' as project_name,
        '{project_id}' as project_id,
        asset_type as asset_name,
        amount_millions,
        haircut_id
    FROM prasanna.lcr.assets
    WHERE timestamp = '{latest_assets_timestamp}'
"""

spark.sql(insert_assets_query)
print(f"\n✓ Inserted assets records with timestamp {latest_assets_timestamp}")

# Insert liabilities with latest timestamp
insert_liabilities_query = f"""
    INSERT INTO prasanna.lcr.lcr_liabilities
    SELECT 
        timestamp,
        '{project_name}' as project_name,
        '{project_id}' as project_id,
        liability_type as liability_name,
        amount_millions,
        runoff_rates_id
    FROM prasanna.lcr.liabilities
    WHERE timestamp = '{latest_liabilities_timestamp}'
"""

spark.sql(insert_liabilities_query)
print(f"✓ Inserted liabilities records with timestamp {latest_liabilities_timestamp}")

# Verify insertions
assets_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM prasanna.lcr.lcr_assets 
    WHERE project_id = '{project_id}'
""").collect()[0]['count']

liabilities_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM prasanna.lcr.lcr_liabilities 
    WHERE project_id = '{project_id}'
""").collect()[0]['count']

print(f"\n=== Insertion Summary ===")
print(f"Assets inserted: {assets_count}")
print(f"Liabilities inserted: {liabilities_count}")

# COMMAND ----------

# DBTITLE 1,Insert HQLA calculations
# Insert into lcr_hqla by joining with haircuts table
insert_hqla_query = f"""
    INSERT INTO prasanna.lcr.lcr_hqla
    SELECT 
        a.timestamp,
        a.project_name,
        a.project_id,
        a.asset_name,
        a.amount_millions,
        CAST(REPLACE(h.fed_haircut, '%', '') AS DOUBLE) / 100 as haircut_pct,
        a.amount_millions * (1 - CAST(REPLACE(h.fed_haircut, '%', '') AS DOUBLE) / 100) as hqla_value_millions,
        CASE WHEN h.hqla_level = 'Non-HQLA' THEN true ELSE false END as not_hqla
    FROM prasanna.lcr.lcr_assets a
    INNER JOIN prasanna.lcr.haircuts h ON a.haircut_id = h.id
    WHERE a.project_id = '{project_id}'
"""

spark.sql(insert_hqla_query)
print(f"✓ Inserted HQLA calculations for project {project_id}")

# Verify HQLA insertion
hqla_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM prasanna.lcr.lcr_hqla 
    WHERE project_id = '{project_id}'
""").collect()[0]['count']

print(f"HQLA records inserted: {hqla_count}")

# COMMAND ----------

# DBTITLE 1,Insert runoff rate calculations
import re

def parse_runoff_rate(rate_str):
    # Remove % and whitespace
    rate_str = rate_str.replace('%', '').strip()
    # Check for range pattern
    match = re.match(r'^(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)$', rate_str)
    if match:
        low = float(match.group(1))
        high = float(match.group(2))
        return (low + high) / 2
    else:
        try:
            return float(rate_str)
        except ValueError:
            return None

# Register UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

parse_runoff_rate_udf = udf(parse_runoff_rate, DoubleType())

# Prepare liabilities and runoff_rates DataFrames
liabilities_df = spark.table("prasanna.lcr.lcr_liabilities").filter(f"project_id = '{project_id}'")
runoff_rates_df = spark.table("prasanna.lcr.runoff_rates")

# Join and apply UDF
joined_df = liabilities_df.join(
    runoff_rates_df,
    liabilities_df.runoff_rates_id == runoff_rates_df.id,
    "inner"
).withColumn(
    "runoff_percent",
    parse_runoff_rate_udf(runoff_rates_df.fed_runoff_rate) / 100
).withColumn(
    "cash_outflow_millions",
    liabilities_df.amount_millions * (parse_runoff_rate_udf(runoff_rates_df.fed_runoff_rate) / 100)
).select(
    liabilities_df.liability_name.alias("liability"),
    liabilities_df.amount_millions,
    "runoff_percent",
    "cash_outflow_millions",
    liabilities_df.project_id,
    liabilities_df.project_name,
    liabilities_df.timestamp
)

# Insert into lcr_runoff_rates
joined_df.createOrReplaceTempView("tmp_lcr_runoff_rates")
spark.sql(f"""
    INSERT INTO prasanna.lcr.lcr_runoff_rates
    SELECT * FROM tmp_lcr_runoff_rates
""")
print(f"✓ Inserted runoff rate calculations for project {project_id}")

# Verify runoff rates insertion
runoff_count = spark.sql(f"""
    SELECT COUNT(*) as count 
    FROM prasanna.lcr.lcr_runoff_rates 
    WHERE project_id = '{project_id}'
""").collect()[0]['count']

print(f"Runoff rate records inserted: {runoff_count}")

# COMMAND ----------

# DBTITLE 1,Calculate and insert LCR, LDR, and NSFR ratios
# Calculate total HQLA and total cash outflows for LCR
total_hqla_result = spark.sql(f"""
    SELECT SUM(hqla_value_millions) as total_hqla
    FROM prasanna.lcr.lcr_hqla
    WHERE project_id = '{project_id}'
""").collect()[0]

total_outflow_result = spark.sql(f"""
    SELECT SUM(cash_outflow_millions) as total_outflow
    FROM prasanna.lcr.lcr_runoff_rates
    WHERE project_id = '{project_id}'
""").collect()[0]

total_hqla = total_hqla_result['total_hqla']
total_outflow = total_outflow_result['total_outflow']

# Calculate LDR (Loan-to-Deposit Ratio)
# Loans from assets
total_loans_result = spark.sql(f"""
    SELECT SUM(amount_millions) as total_loans
    FROM prasanna.lcr.lcr_assets
    WHERE project_id = '{project_id}'
    AND asset_name LIKE '%Loan%'
""").collect()[0]

# Deposits from liabilities
total_deposits_result = spark.sql(f"""
    SELECT SUM(amount_millions) as total_deposits
    FROM prasanna.lcr.lcr_liabilities
    WHERE project_id = '{project_id}'
    AND liability_name LIKE '%deposit%'
""").collect()[0]

total_loans = total_loans_result['total_loans'] or 0
total_deposits = total_deposits_result['total_deposits'] or 0

# Calculate NSFR (Net Stable Funding Ratio)
# Available Stable Funding: Long-term debt + Equity + stable deposits
available_stable_funding_result = spark.sql(f"""
    SELECT SUM(amount_millions) as asf
    FROM prasanna.lcr.lcr_liabilities
    WHERE project_id = '{project_id}'
    AND (liability_name LIKE '%Long-term%' 
         OR liability_name LIKE '%Equity%'
         OR liability_name LIKE '%Retail deposits (insured)%')
""").collect()[0]

# Required Stable Funding: Loans and other illiquid assets
required_stable_funding_result = spark.sql(f"""
    SELECT SUM(amount_millions) as rsf
    FROM prasanna.lcr.lcr_assets
    WHERE project_id = '{project_id}'
    AND (asset_name LIKE '%Loan%' OR asset_name LIKE '%Corporate bonds%')
""").collect()[0]

available_stable_funding = available_stable_funding_result['asf'] or 0
required_stable_funding = required_stable_funding_result['rsf'] or 0

# Calculate ratios
lcr_value = total_hqla / total_outflow if total_outflow and total_outflow > 0 else None
lcr_percent = lcr_value * 100 if lcr_value else None

ldr_value = total_loans / total_deposits if total_deposits and total_deposits > 0 else None
ldr_percent = ldr_value * 100 if ldr_value else None

nsfr_value = available_stable_funding / required_stable_funding if required_stable_funding and required_stable_funding > 0 else None
nsfr_percent = nsfr_value * 100 if nsfr_value else None

# Insert into lcr_ratios table
if lcr_value is not None:
    insert_ratios_query = f"""
        INSERT INTO prasanna.lcr.lcr_ratios (
            timestamp,
            scenario_name,
            project_name,
            project_id,
            lcr_value,
            lcr_percent,
            ldr_value,
            ldr_percent,
            nsfr_value,
            nsfr_percent,
            hqla_value_millions,
            net_outflows_millions
        )
        VALUES (
            '{project_timestamp}',
            'Base Scenario',
            '{project_name}',
            '{project_id}',
            {lcr_value},
            {lcr_percent},
            {ldr_value if ldr_value else 'NULL'},
            {ldr_percent if ldr_percent else 'NULL'},
            {nsfr_value if nsfr_value else 'NULL'},
            {nsfr_percent if nsfr_percent else 'NULL'},
            {total_hqla},
            {total_outflow}
        )
    """
    
    spark.sql(insert_ratios_query)
    print(f"✓ Inserted all ratios into lcr_ratios table\n")
    
    print(f"=== LCR (Liquidity Coverage Ratio) ===")
    print(f"  HQLA: ${total_hqla:.2f}M")
    print(f"  Net Outflows: ${total_outflow:.2f}M")
    print(f"  LCR Value: {lcr_value:.4f}")
    print(f"  LCR Percent: {lcr_percent:.2f}%")
    print(f"  Status: {'✓ COMPLIANT' if lcr_percent >= 100 else '✗ NON-COMPLIANT'} (Minimum: 100%)\n")
    
    if ldr_value:
        print(f"=== LDR (Loan-to-Deposit Ratio) ===")
        print(f"  Total Loans: ${total_loans:.2f}M")
        print(f"  Total Deposits: ${total_deposits:.2f}M")
        print(f"  LDR Value: {ldr_value:.4f}")
        print(f"  LDR Percent: {ldr_percent:.2f}%")
        print(f"  Status: {'✓ HEALTHY' if ldr_percent <= 90 else '⚠ HIGH'} (Target: <90%)\n")
    
    if nsfr_value:
        print(f"=== NSFR (Net Stable Funding Ratio) ===")
        print(f"  Available Stable Funding: ${available_stable_funding:.2f}M")
        print(f"  Required Stable Funding: ${required_stable_funding:.2f}M")
        print(f"  NSFR Value: {nsfr_value:.4f}")
        print(f"  NSFR Percent: {nsfr_percent:.2f}%")
        print(f"  Status: {'✓ COMPLIANT' if nsfr_percent >= 100 else '✗ NON-COMPLIANT'} (Minimum: 100%)")
else:
    print("⚠ Cannot calculate ratios: insufficient data")

# COMMAND ----------

# DBTITLE 1,Display summary results
# Display summary of HQLA calculations
print("\n=== HQLA Summary ===")
hqla_summary = spark.sql(f"""
    SELECT 
        asset_name,
        amount_millions,
        haircut_pct,
        hqla_value_millions,
        not_hqla
    FROM prasanna.lcr.lcr_hqla
    WHERE project_id = '{project_id}'
    ORDER BY hqla_value_millions DESC
""")

display(hqla_summary)

# Display summary of runoff rate calculations
print("\n=== Runoff Rates Summary ===")
runoff_summary = spark.sql(f"""
    SELECT 
        liability,
        amount_millions,
        runoff_percent,
        cash_outflow_millions
    FROM prasanna.lcr.lcr_runoff_rates
    WHERE project_id = '{project_id}'
    ORDER BY cash_outflow_millions DESC
""")

display(runoff_summary)

# Calculate totals
total_hqla = spark.sql(f"""
    SELECT SUM(hqla_value_millions) as total_hqla
    FROM prasanna.lcr.lcr_hqla
    WHERE project_id = '{project_id}'
""").collect()[0]['total_hqla']

total_outflow = spark.sql(f"""
    SELECT SUM(cash_outflow_millions) as total_outflow
    FROM prasanna.lcr.lcr_runoff_rates
    WHERE project_id = '{project_id}'
""").collect()[0]['total_outflow']

print(f"\n=== LCR Calculation ===")
print(f"Total HQLA: ${total_hqla:.2f}M")
print(f"Total Cash Outflow: ${total_outflow:.2f}M")
if total_outflow and total_outflow > 0:
    lcr_ratio = (total_hqla / total_outflow) * 100
    print(f"LCR Ratio: {lcr_ratio:.2f}%")
    print(f"Regulatory Minimum: 100%")
    print(f"Status: {'✓ COMPLIANT' if lcr_ratio >= 100 else '✗ NON-COMPLIANT'}")