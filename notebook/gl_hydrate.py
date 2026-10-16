# Databricks notebook source
# DBTITLE 1,GL Hydration - Daily Balance Evolution
"""
General Ledger Hydration for Aura Bank LCR Demo.

Runs daily. Reads the latest GL snapshot, applies small mean-reverting
perturbations to each account, and writes a new day's GL entries.
Maintains the accounting equation: Assets = Liabilities + Equity.

Target LCR band: 130-160% (realistic for a well-capitalised US bank).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as spark_sum,
    max as spark_max, when, date_format, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from datetime import datetime, timedelta
import random

# ============================================
# CONFIG
# ============================================
CATALOG = "banking_cfo_treasury"
SCHEMA = "aura_bank"
GL_TABLE = f"{CATALOG}.{SCHEMA}.general_ledger"
HAIRCUTS_TABLE = f"{CATALOG}.{SCHEMA}.haircuts"
RUNOFF_TABLE = f"{CATALOG}.{SCHEMA}.runoff_rates"

TARGET_LCR_LOW = 1.35   # 135%
TARGET_LCR_HIGH = 1.55  # 155%
TARGET_LCR_MID = 1.45   # 145%

# Daily perturbation ranges by account_type: (min_pct, max_pct)
# These define the *magnitude* of change; direction is randomised with
# mean-reversion bias applied on top.
VOLATILITY = {
    # Assets
    "Cash + Fed reserves":  (0.002, 0.008),
    "US Treasuries":        (0.001, 0.005),
    "Agency MBS (AAA)":     (0.002, 0.007),
    "Corporate bonds":      (0.003, 0.010),
    "Loans (Mortgages)":    (0.0005, 0.002),
    "Loans (Business)":     (0.001, 0.005),
    "Municipal Bonds":      (0.002, 0.006),
    # Liabilities
    "Retail Deposits":      (0.0005, 0.003),
    "Corporate Deposits":   (0.003, 0.010),
    "Wholesale Funding":    (0.005, 0.020),
    "Short-term Debt":      (0.003, 0.015),
    "Long-term Debt":       (0.0, 0.001),
    # Equity — computed as residual, not perturbed
    "Equity":               (0.0, 0.0),
}

# GL schema
gl_schema = StructType([
    StructField("gl_account", StringType(), True),
    StructField("account_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("hqla_type", StringType(), True),
    StructField("runoff", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("net_balance_m", DoubleType(), True),
])

# COMMAND ----------

# DBTITLE 1,Ensure Tables Exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GL_TABLE} (
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

# Ensure haircuts & runoff_rates exist (idempotent)
haircuts_count = spark.table(HAIRCUTS_TABLE).count()
if haircuts_count == 0:
    from pyspark.sql.types import LongType
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
        (11, "Loans (Business)", "Performing < 30d maturity", "Non-HQLA", "100%", "100%", "Treated as Inflow"),
    ]
    haircuts_schema = StructType([
        StructField("id", LongType(), True),
        StructField("asset_category", StringType(), True),
        StructField("asset_sub_type", StringType(), True),
        StructField("hqla_level", StringType(), True),
        StructField("fed_haircut", StringType(), True),
        StructField("bcbs_haircut", StringType(), True),
        StructField("regulatory_logic", StringType(), True),
    ])
    spark.createDataFrame(haircuts_data, haircuts_schema) \
        .write.mode("append").saveAsTable(HAIRCUTS_TABLE)
    print("✓ Seeded haircuts table")

runoff_count = spark.table(RUNOFF_TABLE).count()
if runoff_count == 0:
    from pyspark.sql.types import LongType
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
        (10, "Equity", "Shareholders' Equity", "0%", "Equity is permanent capital; it never runs off."),
    ]
    runoff_schema = StructType([
        StructField("id", LongType(), True),
        StructField("liability_category", StringType(), True),
        StructField("specific_component", StringType(), True),
        StructField("fed_runoff_rate", StringType(), True),
        StructField("regulatory_logic", StringType(), True),
    ])
    spark.createDataFrame(runoff_data, runoff_schema) \
        .write.mode("append").saveAsTable(RUNOFF_TABLE)
    print("✓ Seeded runoff_rates table")

# COMMAND ----------

# DBTITLE 1,Read Latest GL Snapshot
gl_df = spark.table(GL_TABLE)
row_count = gl_df.count()

if row_count == 0:
    # First run ever — seed with initial data
    print("No existing GL data. Seeding initial balance sheet...")
    seed_ts = datetime(2026, 2, 26, 17, 0, 0)
    seed_data = [
        # ASSETS
        ("101001", "Cash & Vault", "Asset", "Cash + Fed reserves", "Level 1", "0%", seed_ts, 500.0),
        ("101005", "Fed Reserve Account", "Asset", "Cash + Fed reserves", "Level 1", "0%", seed_ts, 4000.0),
        ("102010", "US Treasury Bills", "Asset", "US Treasuries", "Level 1", "0%", seed_ts, 3500.0),
        ("103050", "Agency MBS - FNMA", "Asset", "Agency MBS (AAA)", "Level 2A", "0%", seed_ts, 2500.0),
        ("105001", "Corp Bonds (IG)", "Asset", "Corporate bonds", "Level 2B", "0%", seed_ts, 1000.0),
        ("110000", "Residential Mortgages", "Asset", "Loans (Mortgages)", "Non-HQLA", "0%", seed_ts, 18000.0),
        ("199999", "Other Assets (Investments)", "Asset", "Loans (Business)", "Non-HQLA", "0%", seed_ts, 2291.5),
        # LIABILITIES
        ("201001", "Retail Checking (Ins)", "Liability", "Retail Deposits", "N/A", "3%", seed_ts, -15000.0),
        ("201002", "Retail Savings (Ins)", "Liability", "Retail Deposits", "N/A", "3%", seed_ts, -7300.0),
        ("202001", "Retail HY (Unins)", "Liability", "Retail Deposits", "N/A", "10%", seed_ts, -3200.0),
        ("203010", "Corp Payroll Accts", "Liability", "Corporate Deposits", "N/A", "25%", seed_ts, -2000.0),
        ("203020", "Corp Excess Cash (Fin)", "Liability", "Corporate Deposits", "N/A", "100%", seed_ts, -800.0),
        ("205001", "Commercial Paper", "Liability", "Wholesale Funding", "N/A", "100%", seed_ts, -250.0),
        ("208000", "Short-term Debt", "Liability", "Short-term Debt", "N/A", "100%", seed_ts, -241.5),
        # EQUITY
        ("301000", "Common Equity", "Equity", "Equity", "N/A", "0%", seed_ts, -3000.0),
    ]
    seed_df = spark.createDataFrame(seed_data, gl_schema)
    seed_df.write.mode("append").saveAsTable(GL_TABLE)
    print(f"✓ Seeded GL with {len(seed_data)} rows at {seed_ts}")
    # Re-read
    gl_df = spark.table(GL_TABLE)

# Get latest date in GL
latest_ts = gl_df.agg(spark_max("timestamp")).collect()[0][0]
latest_date = latest_ts.date() if latest_ts else None
print(f"Latest GL date: {latest_date} ({latest_ts})")

# Check if today's data already exists
today = datetime.now().date()
if latest_date == today:
    print(f"✓ GL data for {today} already exists. Nothing to do.")
    dbutils.notebook.exit(f"Already hydrated for {today}")

# Get the latest day's rows (the snapshot we'll evolve)
latest_rows = gl_df.filter(
    col("timestamp").cast("date") == lit(str(latest_date))
).toPandas()

print(f"Found {len(latest_rows)} GL rows for {latest_date}")
print(f"Will generate data for {today}")

# COMMAND ----------

# DBTITLE 1,Estimate Current LCR for Mean-Reversion Bias

# Quick LCR estimate from the latest snapshot
# Use hqla_type for haircuts (matches SQL function) and GL runoff column for outflows
HQLA_HAIRCUTS = {
    "Level 1": 0.0,
    "Level 2A": 0.15,
    "Level 2B": 0.50,
    "Non-HQLA": 1.0,
}

# Calculate HQLA and outflows from latest rows
hqla_total = 0.0
outflow_total = 0.0
for _, row in latest_rows.iterrows():
    balance = float(row["net_balance_m"])
    category = str(row["category"])

    if category == "Asset":
        hqla_type = str(row["hqla_type"])
        haircut = HQLA_HAIRCUTS.get(hqla_type, 1.0)
        hqla_total += balance * (1.0 - haircut)
    elif category == "Liability":
        runoff_str = str(row["runoff"]).replace("%", "")
        try:
            runoff_rate = float(runoff_str) / 100.0
        except ValueError:
            runoff_rate = 0.0
        outflow_total += abs(balance) * runoff_rate

current_lcr = hqla_total / outflow_total if outflow_total > 0 else 999.0
print(f"Estimated current LCR: {current_lcr:.4f} ({current_lcr*100:.1f}%)")
print(f"  HQLA: ${hqla_total:,.1f}M | Outflows: ${outflow_total:,.1f}M")

# Determine mean-reversion bias
# If LCR is below target band, bias HQLA up and outflows down
# If LCR is above target band, bias HQLA down and outflows up
if current_lcr < TARGET_LCR_LOW:
    lcr_bias = 1.0   # strong push up
    print(f"  Bias: STRONG UP (below {TARGET_LCR_LOW*100:.0f}%)")
elif current_lcr < TARGET_LCR_MID:
    lcr_bias = 0.3    # gentle push up
    print(f"  Bias: gentle up (below midpoint)")
elif current_lcr > TARGET_LCR_HIGH:
    lcr_bias = -1.0   # strong push down
    print(f"  Bias: STRONG DOWN (above {TARGET_LCR_HIGH*100:.0f}%)")
elif current_lcr > TARGET_LCR_MID:
    lcr_bias = -0.3   # gentle push down
    print(f"  Bias: gentle down (above midpoint)")
else:
    lcr_bias = 0.0
    print(f"  Bias: neutral (near midpoint)")

# COMMAND ----------

# DBTITLE 1,Generate New Day's GL Entries

new_ts = datetime.combine(today, datetime.min.time().replace(hour=17))
new_rows = []

# Separate equity rows — we'll recompute them as residual
equity_rows = latest_rows[latest_rows["category"] == "Equity"]
non_equity_rows = latest_rows[latest_rows["category"] != "Equity"]

for _, row in non_equity_rows.iterrows():
    acct_type = row["account_type"]
    category = row["category"]
    balance = row["net_balance_m"]

    vol_min, vol_max = VOLATILITY.get(acct_type, (0.001, 0.005))

    # Random magnitude
    magnitude = random.uniform(vol_min, vol_max)

    # Determine direction with mean-reversion bias
    # For assets (positive balance): lcr_bias > 0 means we want HQLA up → assets up
    # For liabilities (negative balance): lcr_bias > 0 means we want outflows down → liabilities down (less negative)
    base_direction = random.choice([-1, 1])

    if category == "Asset":
        # Positive lcr_bias → favour positive direction (increase assets)
        bias_weight = lcr_bias * 0.3  # scale down so bias doesn't dominate
        direction = 1 if (random.random() < 0.5 + bias_weight) else -1
    elif category == "Liability":
        # Positive lcr_bias → favour making liabilities less negative (reducing outflows)
        # Less negative = direction +1 on a negative balance → smaller absolute value
        bias_weight = lcr_bias * 0.2
        direction = 1 if (random.random() < 0.5 + bias_weight) else -1
    else:
        direction = base_direction

    change = balance * magnitude * direction
    new_balance = round(balance + change, 2)

    new_rows.append((
        row["gl_account"],
        row["account_name"],
        category,
        acct_type,
        row["hqla_type"],
        row["runoff"],
        new_ts,
        new_balance,
    ))

# Compute equity as residual to maintain accounting equation
total_assets = sum(r[7] for r in new_rows if r[2] == "Asset")
total_liabilities = sum(r[7] for r in new_rows if r[2] == "Liability")
required_equity = -(total_assets + total_liabilities)

# Distribute across equity accounts proportionally
equity_original_total = equity_rows["net_balance_m"].sum()
for _, row in equity_rows.iterrows():
    if equity_original_total != 0:
        proportion = row["net_balance_m"] / equity_original_total
    else:
        proportion = 1.0 / len(equity_rows)
    new_balance = round(required_equity * proportion, 2)
    new_rows.append((
        row["gl_account"],
        row["account_name"],
        "Equity",
        row["account_type"],
        row["hqla_type"],
        row["runoff"],
        new_ts,
        new_balance,
    ))

print(f"Generated {len(new_rows)} GL entries for {today}")

# Verify accounting equation
total_all = sum(r[7] for r in new_rows)
print(f"  Assets:      ${sum(r[7] for r in new_rows if r[2] == 'Asset'):>12,.2f}M")
print(f"  Liabilities: ${sum(abs(r[7]) for r in new_rows if r[2] == 'Liability'):>12,.2f}M")
print(f"  Equity:      ${sum(abs(r[7]) for r in new_rows if r[2] == 'Equity'):>12,.2f}M")
print(f"  Net balance: ${total_all:>12,.4f}M {'✓' if abs(total_all) < 0.1 else '✗'}")

# COMMAND ----------

# DBTITLE 1,Write New GL Entries
new_gl_df = spark.createDataFrame(new_rows, gl_schema)
new_gl_df.write.mode("append").saveAsTable(GL_TABLE)
print(f"✓ Wrote {len(new_rows)} GL entries for {today}")

# COMMAND ----------

# DBTITLE 1,Verify & Summary
# Show the new LCR estimate
new_hqla = 0.0
new_outflow = 0.0
for r in new_rows:
    gl_acct, acct_name, category, acct_type, hqla_type, runoff_str, ts, balance = r
    if category == "Asset":
        haircut = HQLA_HAIRCUTS.get(hqla_type, 1.0)
        new_hqla += balance * (1.0 - haircut)
    elif category == "Liability":
        try:
            ro = float(str(runoff_str).replace("%", "")) / 100.0
        except ValueError:
            ro = 0.0
        new_outflow += abs(balance) * ro

new_lcr = new_hqla / new_outflow if new_outflow > 0 else 999.0
lcr_change = (new_lcr - current_lcr) * 100

print(f"\n{'='*60}")
print(f"GL HYDRATION SUMMARY — {today}")
print(f"{'='*60}")
print(f"Previous LCR: {current_lcr*100:.2f}%")
print(f"New LCR est:  {new_lcr*100:.2f}%")
print(f"Change:       {lcr_change:+.2f} pp")
print(f"Target band:  {TARGET_LCR_LOW*100:.0f}% – {TARGET_LCR_HIGH*100:.0f}%")
print(f"{'='*60}")
