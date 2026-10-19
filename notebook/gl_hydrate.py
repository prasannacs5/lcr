# Databricks notebook source
# DBTITLE 1,Create CFO Aura Bank Tables with Synthetic Data
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sum as spark_sum, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from datetime import datetime, timedelta
import random

# Create catalog and schema if they don't exist
spark.sql("CREATE CATALOG IF NOT EXISTS cfo")
spark.sql("CREATE SCHEMA IF NOT EXISTS cfo.aura_bank")

# ============================================
# 1. CREATE HAIRCUTS TABLE
# ============================================
print("Setting up haircuts table...")

spark.sql("""
CREATE TABLE IF NOT EXISTS cfo.aura_bank.haircuts (
    id BIGINT,
    asset_category STRING,
    asset_sub_type STRING,
    hqla_level STRING,
    fed_haircut STRING,
    bcbs_haircut STRING,
    regulatory_logic STRING
)
""")

# Check if haircuts table is empty
haircuts_count = spark.table("cfo.aura_bank.haircuts").count()

if haircuts_count == 0:
    haircuts_data = [
        (1, "Cash + Fed reserves", "Central Bank Reserves", "Level 1", "0%", "0%", "§ 50.20(a)(1)"),
        (2, "Cash + Fed reserves", "Coins & Banknotes", "Level 1", "0%", "0%", "§ 50.20(a)(1)"),
        (3, "US Treasuries", "Bill, Note, Bond", "Level 1", "0%", "0%", "§ 50.20(a)(2)"),
        (4, "Agency MBS (AAA)", "GNMA (Gov Guaranteed)", "Level 1", "0%", "0%", "§ 50.20(a)(4)"),
        (5, "Agency MBS (AAA)", "FNMA / FHLMC (GSE)", "Level 2A", "15%", "15%", "§ 50.20(b)(1)"),
        (6, "Corporate bonds", "Rated AA- or Higher", "Level 2A", "15%", "15%", "§ 50.20(b)(2)"),
        (7, "Corporate bonds", "Rated A+ to BBB-", "Level 2B", "50%", "50%", "§ 50.20(c)(1)"),
        (8, "Municipal Bonds", "General Obligation (GO)", "Level 2A", "15%", "N/A", "US Specific Rule"),
        (9, "S&P 500 Equity", "Common Stock", "Level 2B", "50%", "50%", "§ 50.20(c)(2)"),
        (10, "Loans (Mortgages)", "Performing < 30d maturity", "Non-HQLA", "100%", "100%", "Treated as Inflow"),
        (11, "Loans (Business)", "Performing < 30d maturity", "Non-HQLA", "100%", "100%", "Treated as Inflow")
    ]
    
    haircuts_schema = StructType([
        StructField("id", LongType(), True),
        StructField("asset_category", StringType(), True),
        StructField("asset_sub_type", StringType(), True),
        StructField("hqla_level", StringType(), True),
        StructField("fed_haircut", StringType(), True),
        StructField("bcbs_haircut", StringType(), True),
        StructField("regulatory_logic", StringType(), True)
    ])
    
    haircuts_df = spark.createDataFrame(haircuts_data, haircuts_schema)
    haircuts_df.write.mode("append").saveAsTable("cfo.aura_bank.haircuts")
    print("✓ Haircuts table created with 11 entries")
else:
    print(f"✓ Haircuts table already exists with {haircuts_count} entries")

# ============================================
# 2. CREATE RUNOFF_RATES TABLE
# ============================================
print("Setting up runoff_rates table...")

spark.sql("""
CREATE TABLE IF NOT EXISTS cfo.aura_bank.runoff_rates (
    id BIGINT,
    liability_category STRING,
    specific_component STRING,
    fed_runoff_rate STRING,
    regulatory_logic STRING
)
""")

# Check if runoff_rates table is empty
runoff_count = spark.table("cfo.aura_bank.runoff_rates").count()

if runoff_count == 0:
    runoff_data = [
        (1, "Retail Deposits", "Stable (Insured + Relationship)", "3%", "Standard for 'sticky' core deposits."),
        (2, "Retail Deposits", "Less Stable (Uninsured / Digital)", "10%", "Base rate for uninsured retail."),
        (3, "Retail Deposits", "High Yield (Uninsured)", "10%", "Standard Reg WW rate for uninsured retail."),
        (4, "Corporate Deposits", "Operational (Payroll)", "25%", "Cash used for daily business operations."),
        (5, "Corporate Deposits", "Non-Operational (Financial)", "100%", "Assumes financial firms pull 100% in stress."),
        (6, "Corporate Deposits", "Non-Operational (Non-Financial)", "40%", "Higher risk than operational."),
        (7, "Wholesale Funding", "Unsecured / Commercial Paper", "100%", "Short-term market funding that freezes."),
        (8, "Short-term Debt", "Maturity ≤ 30 Days", "100%", "Any debt due within the LCR window."),
        (9, "Long-term Debt", "Maturity > 30 Days", "0%", "No cash leaves the bank within 30 days."),
        (10, "Equity", "Shareholders' Equity", "0%", "Equity is permanent capital; it never runs off.")
    ]
    
    runoff_schema = StructType([
        StructField("id", LongType(), True),
        StructField("liability_category", StringType(), True),
        StructField("specific_component", StringType(), True),
        StructField("fed_runoff_rate", StringType(), True),
        StructField("regulatory_logic", StringType(), True)
    ])
    
    runoff_df = spark.createDataFrame(runoff_data, runoff_schema)
    runoff_df.write.mode("append").saveAsTable("cfo.aura_bank.runoff_rates")
    print("✓ Runoff rates table created with 10 entries")
else:
    print(f"✓ Runoff rates table already exists with {runoff_count} entries")

# ============================================
# 3. CREATE GENERAL_LEDGER TABLE
# ============================================

# Check if table exists and has data
try:
    existing_gl = spark.table("cfo.aura_bank.general_ledger")
    table_exists = True
    existing_count = existing_gl.count()
    print(f"Found existing GL table with {existing_count} records")
except:
    table_exists = False
    existing_count = 0
    print("Creating new GL table...")

# Define GL schema
gl_schema = StructType([
    StructField("gl_account", StringType(), True),
    StructField("account_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("hqla_type", StringType(), True),
    StructField("runoff", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("net_balance_m", DoubleType(), True)
])

if not table_exists or existing_count == 0:
    # Create table structure
    spark.sql("""
    CREATE TABLE IF NOT EXISTS cfo.aura_bank.general_ledger (
        gl_account STRING,
        account_name STRING,
        category STRING,
        account_type STRING,
        hqla_type STRING,
        runoff STRING,
        timestamp TIMESTAMP,
        net_balance_m DOUBLE
    )
    """)
    
    # Initial data load - create base GL entries
    # Assets = Liabilities + Equity
    # Let's set: Assets = 31,791.5M, Liabilities = 28,791.5M, Equity = 3,000M
    gl_data = [
        # ASSETS (positive values) - Total: 31,791.5M
        ("101001", "Cash & Vault", "Asset", "Cash + Fed reserves", "Level 1", "0%", datetime(2026, 2, 18, 17, 59, 59), 500.0),
        ("101005", "Fed Reserve Account", "Asset", "Cash + Fed reserves", "Level 1", "0%", datetime(2026, 2, 18, 17, 59, 59), 4000.0),
        ("102010", "US Treasury Bills", "Asset", "US Treasuries", "Level 1", "0%", datetime(2026, 2, 18, 17, 0, 5), 3500.0),
        ("103050", "Agency MBS - FNMA", "Asset", "Agency MBS (AAA)", "Level 2A", "0%", datetime(2026, 2, 18, 17, 0, 5), 2500.0),
        ("105001", "Corp Bonds (IG)", "Asset", "Corporate bonds", "Level 2B", "0%", datetime(2026, 2, 18, 17, 0, 5), 1000.0),
        ("110000", "Residential Mortgages", "Asset", "Loans (Mortgages)", "Non-HQLA", "0%", datetime(2026, 2, 18, 17, 30, 10), 18000.0),
        ("199999", "Other Assets (Investments)", "Asset", "Loans (Business)", "Non-HQLA", "0%", datetime(2026, 2, 18, 17, 59, 59), 2291.5),
        # LIABILITIES (negative values) - Total: -28,791.5M
        ("201001", "Retail Checking (Ins)", "Liability", "Retail Deposits", "N/A", "3%", datetime(2026, 2, 18, 23, 59, 59), -15000.0),
        ("201002", "Retail Savings (Ins)", "Liability", "Retail Deposits", "N/A", "3%", datetime(2026, 2, 18, 23, 59, 59), -7300.0),
        ("202001", "Retail HY (Unins)", "Liability", "Retail Deposits", "N/A", "10%", datetime(2026, 2, 18, 23, 59, 59), -3200.0),
        ("203010", "Corp Payroll Accts", "Liability", "Corporate Deposits", "N/A", "25%", datetime(2026, 2, 18, 23, 59, 59), -2000.0),
        ("203020", "Corp Excess Cash (Fin)", "Liability", "Corporate Deposits", "N/A", "100%", datetime(2026, 2, 18, 23, 59, 59), -800.0),
        ("205001", "Commercial Paper", "Liability", "Wholesale Funding", "N/A", "100%", datetime(2026, 2, 18, 23, 59, 59), -250.0),
        ("208000", "Short-term Debt", "Liability", "Short-term Debt", "N/A", "100%", datetime(2026, 2, 18, 23, 59, 59), -241.5),
        # EQUITY (negative value) - Total: -3,000M
        ("301000", "Common Equity", "Equity", "Equity", "N/A", "0%", datetime(2026, 2, 18, 18, 0, 0), -3000.0)
    ]
    
    gl_df = spark.createDataFrame(gl_data, gl_schema)
    gl_df.write.mode("append").saveAsTable("cfo.aura_bank.general_ledger")
    print(f"✓ General Ledger table created with {len(gl_data)} initial entries")
    
else:
    # Add new entries using balanced double-entry construction.
    # Each funding event books an asset and an equal, opposite funding source (a liability
    # or equity credit) of the same magnitude. Because every event nets to zero, the
    # accounting identity Assets = Liabilities + Equity is preserved by construction, so
    # there is no after-the-fact plug entry. The Lakeflow pipeline
    # (notebook/lcr_lakeflow_pipeline) additionally enforces this identity as a declarative
    # data-quality expectation, quarantining any snapshot that does not balance.
    current_time = datetime.now()

    # Each template is (asset_leg, funding_leg); the funding leg's amount mirrors the asset.
    funding_events = [
        (
            (f"11{random.randint(1000, 9999)}", "Commercial Loans", "Asset", "Loans (Business)", "Non-HQLA", "0%"),
            (f"20{random.randint(1000, 9999)}", "Retail CDs", "Liability", "Retail Deposits", "N/A", "10%"),
            round(random.uniform(50, 200), 2),
        ),
        (
            (f"10{random.randint(1000, 9999)}", "Municipal Bonds", "Asset", "Municipal Bonds", "Level 2A", "0%"),
            (f"20{random.randint(1000, 9999)}", "Corp Operating Deposits", "Liability", "Corporate Deposits", "N/A", "25%"),
            round(random.uniform(100, 300), 2),
        ),
        (
            (f"10{random.randint(1000, 9999)}", "Treasury Notes", "Asset", "US Treasuries", "Level 1", "0%"),
            (f"20{random.randint(1000, 9999)}", "Money Market Deposits", "Liability", "Retail Deposits", "N/A", "3%"),
            round(random.uniform(200, 500), 2),
        ),
        (
            (f"10{random.randint(1000, 9999)}", "Agency Securities", "Asset", "Agency MBS (AAA)", "Level 2A", "0%"),
            (f"20{random.randint(1000, 9999)}", "Wholesale Borrowings", "Liability", "Wholesale Funding", "N/A", "100%"),
            round(random.uniform(150, 400), 2),
        ),
        (
            (f"11{random.randint(1000, 9999)}", "Auto Loans", "Asset", "Loans (Business)", "Non-HQLA", "0%"),
            (f"30{random.randint(1000, 9999)}", "Retained Earnings", "Equity", "Equity", "N/A", "0%"),
            round(random.uniform(80, 250), 2),
        ),
    ]

    new_transactions = []
    for asset_leg, funding_leg, amount in funding_events:
        new_transactions.append((*asset_leg, current_time, amount))
        new_transactions.append((*funding_leg, current_time, -amount))

    new_assets = sum(t[7] for t in new_transactions if t[2] == "Asset")
    new_funding = sum(t[7] for t in new_transactions if t[2] in ("Liability", "Equity"))

    # Create DataFrame for new transactions
    new_gl_df = spark.createDataFrame(new_transactions, gl_schema)

    # Append to existing table
    new_gl_df.write.mode("append").saveAsTable("cfo.aura_bank.general_ledger")

    print(f"✓ Added {len(new_transactions)} balanced entries to General Ledger")
    print(f"  New Assets: ${new_assets:,.2f}M")
    print(f"  New Funding (Liabilities + Equity): ${abs(new_funding):,.2f}M")
    print(f"  Balance Check: ${new_assets + new_funding:,.2f}M (balanced by construction, should be 0.00)")

# ============================================
# VERIFY ACCOUNTING EQUATION
# ============================================
gl_final = spark.table("cfo.aura_bank.general_ledger")

# Calculate totals by category
totals = gl_final.groupBy("category").agg(spark_sum("net_balance_m").alias("total")).collect()

print("\n" + "="*60)
print("BALANCE SHEET VERIFICATION")
print("="*60)

assets_total = 0
liabilities_total = 0
equity_total = 0

for row in totals:
    if row.category == "Asset":
        assets_total = row.total
        print(f"Total Assets:      ${assets_total:>15,.2f}M")
    elif row.category == "Liability":
        liabilities_total = row.total
        print(f"Total Liabilities: ${abs(liabilities_total):>15,.2f}M")
    elif row.category == "Equity":
        equity_total = row.total
        print(f"Total Equity:      ${abs(equity_total):>15,.2f}M")

balance = assets_total + liabilities_total + equity_total
print("="*60)
print(f"Net Balance:       ${balance:>15,.2f}M")
print(f"Status: {'✓ BALANCED' if abs(balance) < 0.01 else '✗ UNBALANCED'}")
print("="*60)

print(f"\nTotal GL Entries: {gl_final.count()}")

# COMMAND ----------

# DBTITLE 1,View Haircuts Table
# Display haircuts table
print("HAIRCUTS TABLE")
print("="*80)
display(spark.table("cfo.aura_bank.haircuts"))

# COMMAND ----------

# DBTITLE 1,View Runoff Rates Table
# Display runoff_rates table
print("RUNOFF RATES TABLE")
print("="*80)
display(spark.table("cfo.aura_bank.runoff_rates"))

# COMMAND ----------

# DBTITLE 1,View General Ledger Table
# Display general_ledger table
print("GENERAL LEDGER TABLE")
print("="*80)
gl_df = spark.table("cfo.aura_bank.general_ledger").orderBy("category", "gl_account")
display(gl_df)

# Show summary by category
print("\nSUMMARY BY CATEGORY")
print("="*80)
summary = spark.sql("""
    SELECT 
        category,
        COUNT(*) as num_accounts,
        SUM(net_balance_m) as total_balance_m
    FROM cfo.aura_bank.general_ledger
    GROUP BY category
    ORDER BY category
""")
display(summary)

# COMMAND ----------

# DBTITLE 1,Important Note About Balance
# MAGIC %md
# MAGIC ## Balanced by construction
# MAGIC
# MAGIC New general ledger entries are booked as **double-entry funding events**: every asset
# MAGIC leg is paired with an equal and opposite funding leg (a liability or an equity credit)
# MAGIC of the same magnitude. Because each event nets to zero, the accounting identity
# MAGIC **Assets = Liabilities + Equity** holds for every batch without any after-the-fact plug
# MAGIC or adjustment entry.
# MAGIC
# MAGIC The governed ingestion path (`notebook/lcr_lakeflow_pipeline`) enforces the same
# MAGIC identity declaratively: it computes the net balance per GL snapshot and quarantines any
# MAGIC snapshot that does not balance, rather than silencing an imbalance. So data integrity is
# MAGIC guaranteed both where the data is produced and where it is governed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding more transactions
# MAGIC
# MAGIC Re-running Cell 1 detects the existing table and appends a batch of balanced
# MAGIC double-entry funding events (an asset leg plus a matching liability or equity leg for
# MAGIC each event). Every batch keeps **Assets = Liabilities + Equity** satisfied, and the
# MAGIC verification cell above prints the net balance (which stays at 0.00 by construction).

# COMMAND ----------

# DBTITLE 1,View Current GL Statistics
# Show current statistics
gl_stats = spark.sql("""
    SELECT 
        COUNT(*) as total_entries,
        COUNT(DISTINCT category) as num_categories,
        MIN(timestamp) as earliest_entry,
        MAX(timestamp) as latest_entry
    FROM cfo.aura_bank.general_ledger
""")

print("GENERAL LEDGER STATISTICS")
print("="*60)
display(gl_stats)

# Show breakdown by HQLA type for assets
print("\nASSET BREAKDOWN BY HQLA TYPE")
print("="*60)
asset_breakdown = spark.sql("""
    SELECT 
        hqla_type,
        COUNT(*) as num_accounts,
        SUM(net_balance_m) as total_balance_m
    FROM cfo.aura_bank.general_ledger
    WHERE category = 'Asset'
    GROUP BY hqla_type
    ORDER BY total_balance_m DESC
""")
display(asset_breakdown)

# Show breakdown by runoff rate for liabilities
print("\nLIABILITY BREAKDOWN BY RUNOFF RATE")
print("="*60)
liability_breakdown = spark.sql("""
    SELECT 
        runoff,
        COUNT(*) as num_accounts,
        SUM(net_balance_m) as total_balance_m
    FROM cfo.aura_bank.general_ledger
    WHERE category = 'Liability'
    GROUP BY runoff
    ORDER BY runoff
""")
display(liability_breakdown)

# COMMAND ----------

# DBTITLE 1,Fix: Delete and Recreate GL Data
