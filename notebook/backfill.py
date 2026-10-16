# Databricks notebook source
# DBTITLE 1,Backfill: Clean Bad Data & Regenerate Mar 20-23
"""
One-time backfill script.
1. Deletes bad GL data for dates >= Mar 20
2. Deletes corresponding LCR snapshot data
3. Regenerates GL data day by day from Mar 19's snapshot
4. Captures LCR snapshots for each regenerated date
"""

from datetime import datetime, date, timedelta
import random

CATALOG = "banking_cfo_treasury"
SCHEMA = "aura_bank"
GL_TABLE = f"{CATALOG}.{SCHEMA}.general_ledger"

TARGET_LCR_LOW = 1.35
TARGET_LCR_HIGH = 1.55
TARGET_LCR_MID = 1.45

VOLATILITY = {
    "Cash + Fed reserves":  (0.002, 0.008),
    "US Treasuries":        (0.001, 0.005),
    "Agency MBS (AAA)":     (0.002, 0.007),
    "Corporate bonds":      (0.003, 0.010),
    "Loans (Mortgages)":    (0.0005, 0.002),
    "Loans (Business)":     (0.001, 0.005),
    "Municipal Bonds":      (0.002, 0.006),
    "Retail Deposits":      (0.0005, 0.003),
    "Corporate Deposits":   (0.003, 0.010),
    "Wholesale Funding":    (0.005, 0.020),
    "Short-term Debt":      (0.003, 0.015),
    "Long-term Debt":       (0.0, 0.001),
    "Equity":               (0.0, 0.0),
}

# COMMAND ----------

# DBTITLE 1,Step 1: Delete bad data from Mar 20 onward
print("Deleting bad GL data for dates >= 2026-03-20...")
spark.sql(f"""
    DELETE FROM {GL_TABLE}
    WHERE DATE(timestamp) >= '2026-03-20'
""")
print("✓ Deleted bad GL rows")

print("Deleting bad LCR results for dates >= 2026-03-20...")
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.lcr_results
    WHERE DATE(as_of_date) >= '2026-03-20'
""")
print("✓ Deleted bad lcr_results rows")

print("Deleting bad HQLA breakdown for dates >= 2026-03-20...")
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.hqla_breakdown
    WHERE DATE(as_of_date) >= '2026-03-20'
""")
print("✓ Deleted bad hqla_breakdown rows")

print("Deleting bad cash outflows breakdown for dates >= 2026-03-20...")
spark.sql(f"""
    DELETE FROM {CATALOG}.{SCHEMA}.cash_outflows_breakdown
    WHERE DATE(as_of_date) >= '2026-03-20'
""")
print("✓ Deleted bad cash_outflows_breakdown rows")

# COMMAND ----------

# DBTITLE 1,Step 2: Load haircuts and runoff maps
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit, max as spark_max

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

haircuts_map = {}
for row in spark.table(f"{CATALOG}.{SCHEMA}.haircuts").collect():
    cat = row.asset_category
    rate_str = row.fed_haircut.replace("%", "")
    try:
        haircuts_map[cat] = float(rate_str) / 100.0
    except ValueError:
        haircuts_map[cat] = 1.0

runoff_map = {}
for row in spark.table(f"{CATALOG}.{SCHEMA}.runoff_rates").collect():
    cat = row.liability_category
    rate_str = row.fed_runoff_rate.replace("%", "")
    try:
        runoff_map[cat] = float(rate_str) / 100.0
    except ValueError:
        runoff_map[cat] = 0.0

print(f"Loaded {len(haircuts_map)} haircut categories, {len(runoff_map)} runoff categories")

# COMMAND ----------

# DBTITLE 1,Step 3: Read Mar 19 snapshot as base

gl_df = spark.table(GL_TABLE)
latest_ts = gl_df.agg(spark_max("timestamp")).collect()[0][0]
latest_date = latest_ts.date()
print(f"Latest GL date after cleanup: {latest_date}")

base_rows = gl_df.filter(
    col("timestamp").cast("date") == lit(str(latest_date))
).toPandas()
print(f"Base snapshot has {len(base_rows)} rows")


def estimate_lcr(rows_list):
    """Quick LCR estimate from a list of row tuples."""
    hqla = 0.0
    outflow = 0.0
    for r in rows_list:
        balance = r[7]
        acct_type = r[3]
        category = r[2]
        if category == "Asset":
            haircut = haircuts_map.get(acct_type, 1.0)
            hqla += balance * (1.0 - haircut)
        elif category == "Liability":
            ro = runoff_map.get(acct_type, 0.0)
            outflow += abs(balance) * ro
    return hqla / outflow if outflow > 0 else 999.0


def evolve_one_day(prev_rows_pd, target_date, current_lcr):
    """Generate one day's GL from previous day with mean-reverting perturbations."""
    new_ts = datetime.combine(target_date, datetime.min.time().replace(hour=17))

    # Mean-reversion bias
    if current_lcr < TARGET_LCR_LOW:
        lcr_bias = 1.0
    elif current_lcr < TARGET_LCR_MID:
        lcr_bias = 0.3
    elif current_lcr > TARGET_LCR_HIGH:
        lcr_bias = -1.0
    elif current_lcr > TARGET_LCR_MID:
        lcr_bias = -0.3
    else:
        lcr_bias = 0.0

    equity_rows = prev_rows_pd[prev_rows_pd["category"] == "Equity"]
    non_equity_rows = prev_rows_pd[prev_rows_pd["category"] != "Equity"]

    new_rows = []
    for _, row in non_equity_rows.iterrows():
        acct_type = str(row["account_type"])
        category = str(row["category"])
        balance = float(row["net_balance_m"])

        vol_min, vol_max = VOLATILITY.get(acct_type, (0.001, 0.005))
        magnitude = random.uniform(vol_min, vol_max)

        if category == "Asset":
            bias_weight = lcr_bias * 0.3
            direction = 1 if (random.random() < 0.5 + bias_weight) else -1
        elif category == "Liability":
            bias_weight = lcr_bias * 0.2
            direction = 1 if (random.random() < 0.5 + bias_weight) else -1
        else:
            direction = random.choice([-1, 1])

        new_balance = round(float(balance) + float(balance) * magnitude * direction, 2)
        new_rows.append((
            str(row["gl_account"]), str(row["account_name"]), str(category), str(acct_type),
            str(row["hqla_type"]), str(row["runoff"]), new_ts, new_balance,
        ))

    # Equity as residual
    total_assets = sum(r[7] for r in new_rows if r[2] == "Asset")
    total_liabilities = sum(r[7] for r in new_rows if r[2] == "Liability")
    required_equity = -(total_assets + total_liabilities)

    equity_original_total = float(equity_rows["net_balance_m"].sum())
    for _, row in equity_rows.iterrows():
        if equity_original_total != 0:
            proportion = float(row["net_balance_m"]) / equity_original_total
        else:
            proportion = 1.0 / len(equity_rows)
        new_rows.append((
            str(row["gl_account"]), str(row["account_name"]), "Equity", str(row["account_type"]),
            str(row["hqla_type"]), str(row["runoff"]), new_ts, round(required_equity * proportion, 2),
        ))

    return new_rows

# COMMAND ----------

# DBTITLE 1,Step 4: Regenerate GL data day by day

import pandas as pd

# Dates to backfill
start_date = latest_date + timedelta(days=1)
end_date = date.today()

current_rows_pd = base_rows
current_lcr = estimate_lcr([
    (str(r["gl_account"]), str(r["account_name"]), str(r["category"]), str(r["account_type"]),
     str(r["hqla_type"]), str(r["runoff"]), r["timestamp"], float(r["net_balance_m"]))
    for _, r in current_rows_pd.iterrows()
])

print(f"Base LCR on {latest_date}: {current_lcr*100:.2f}%")
print(f"Backfilling {start_date} to {end_date}...")
print()

d = start_date
while d <= end_date:
    new_rows = evolve_one_day(current_rows_pd, d, current_lcr)
    new_lcr = estimate_lcr(new_rows)

    # Write to table
    new_gl_df = spark.createDataFrame(new_rows, gl_schema)
    new_gl_df.write.mode("append").saveAsTable(GL_TABLE)

    lcr_change = (new_lcr - current_lcr) * 100
    print(f"  {d}: LCR={new_lcr*100:.2f}% (Δ{lcr_change:+.2f}pp) | {len(new_rows)} rows")

    # Convert to pandas for next iteration
    current_rows_pd = new_gl_df.toPandas()
    current_lcr = new_lcr
    d += timedelta(days=1)

print(f"\n✓ Backfill complete: {start_date} to {end_date}")

# COMMAND ----------

# DBTITLE 1,Step 5: Capture LCR snapshots for backfilled dates
print("Capturing LCR snapshots for backfilled dates...")

d = start_date
while d <= end_date:
    ts_str = f"{d} 17:00:00"
    try:
        spark.sql(f"""
            CALL {CATALOG}.{SCHEMA}.capture_lcr_snapshot(TIMESTAMP '{ts_str}')
        """)
        print(f"  ✓ Snapshot captured for {d}")
    except Exception as e:
        print(f"  ✗ Snapshot failed for {d}: {e}")
    d += timedelta(days=1)

print("\n✓ All snapshots captured")

# COMMAND ----------

# DBTITLE 1,Step 6: Verify results
print("VERIFICATION")
print("=" * 60)

result = spark.sql(f"""
    SELECT
        DATE(as_of_date) as dt,
        ROUND(lcr_ratio * 100, 2) as lcr_pct,
        ROUND(hqla_total, 2) as hqla_m,
        ROUND(net_cash_outflows, 2) as outflows_m
    FROM {CATALOG}.{SCHEMA}.lcr_results
    ORDER BY as_of_date
""")
display(result)

# Check day-over-day changes
print("\nDay-over-day LCR changes:")
rows = result.collect()
for i in range(1, len(rows)):
    prev = rows[i-1].lcr_pct
    curr = rows[i].lcr_pct
    change = curr - prev
    flag = " ⚠️" if abs(change) > 5 else ""
    print(f"  {rows[i].dt}: {curr}% (Δ{change:+.2f}pp){flag}")
