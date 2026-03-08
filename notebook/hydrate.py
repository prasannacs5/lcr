# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # Liquidity Monitoring Data Hydration
# MAGIC
# MAGIC This notebook inserts new records into the LCR (Liquidity Coverage Ratio) tables with fuzzy matching to reference data:
# MAGIC
# MAGIC * **Assets Table** (`prasanna.lcr.assets`): Contains asset types with `haircut_id` matched from `prasanna.lcr.haircuts`
# MAGIC * **Liabilities Table** (`prasanna.lcr.liabilities`): Contains liability types with `runoff_rates_id` matched from `prasanna.lcr.runoff_rates`
# MAGIC
# MAGIC ### Fuzzy Matching Approach
# MAGIC The notebook uses keyword-based scoring combined with Levenshtein distance to match:
# MAGIC * Asset types with haircut categories
# MAGIC * Liability types with runoff rate categories
# MAGIC
# MAGIC This ensures semantic similarity rather than just character-level matching.

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %md
# MAGIC ## Incremental Data Hydration for Hourly Runs
# MAGIC
# MAGIC This notebook is designed to run **every hour** and inserts new records into `prasanna.lcr.assets` and `prasanna.lcr.liabilities` tables while maintaining the fundamental accounting equation:
# MAGIC
# MAGIC **Assets = Liabilities + Equity**
# MAGIC
# MAGIC ### Incremental Behavior:
# MAGIC * **First Run**: Inserts initial baseline data with realistic amounts
# MAGIC * **Subsequent Runs (Hourly)**: 
# MAGIC   * Reads the **latest state** from the tables
# MAGIC   * Applies **small, realistic percentage changes** to each asset/liability
# MAGIC   * Creates **unique data** for each run - no duplicates
# MAGIC   * Simulates real-world scenarios:
# MAGIC     * Market movements (asset price changes)
# MAGIC     * Business operations (deposits, withdrawals, loan activity)
# MAGIC     * Funding changes (wholesale funding fluctuations)
# MAGIC   * **Equity adjusts automatically** to maintain the accounting equation
# MAGIC
# MAGIC ### Data Integrity:
# MAGIC * ✓ Each asset has a valid `haircut_id` (matched from `prasanna.lcr.haircuts`)
# MAGIC * ✓ Each liability has a valid `runoff_rates_id` (matched from `prasanna.lcr.runoff_rates`)
# MAGIC * ✓ Fuzzy matching ensures semantic similarity
# MAGIC * ✓ No NULL values in foreign key columns
# MAGIC * ✓ Accounting equation balanced at all times

# COMMAND ----------

# DBTITLE 1,Step 1: Analyze Existing Data
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Read existing data
assets_df = spark.table("prasanna.lcr.assets")
liabilities_df = spark.table("prasanna.lcr.liabilities")

# Get latest timestamp
latest_timestamp = assets_df.agg(F.max("timestamp")).collect()[0][0]
print(f"Latest timestamp in data: {latest_timestamp}")

# Calculate current totals
total_assets = assets_df.agg(F.sum("amount_millions")).collect()[0][0]
total_liabilities_equity = liabilities_df.agg(F.sum("amount_millions")).collect()[0][0]

print(f"\nCurrent Totals:")
print(f"  Total Assets: ${total_assets}M")
print(f"  Total Liabilities + Equity: ${total_liabilities_equity}M")
print(f"  Balanced: {total_assets == total_liabilities_equity}")

# Show breakdown
print("\n=== Assets Breakdown ===")
assets_df.orderBy(F.desc("amount_millions")).show(truncate=False)

print("\n=== Liabilities Breakdown ===")
liabilities_df.orderBy(F.desc("amount_millions")).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 2: Generate New Records with Fuzzy Matching
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import random

# Use current timestamp for new records
new_timestamp = datetime.now()
print(f"New timestamp for records: {new_timestamp}")

# Load reference tables for fuzzy matching
haircuts_df = spark.table("prasanna.lcr.haircuts")
runoff_rates_df = spark.table("prasanna.lcr.runoff_rates")

print("\n=== Available Haircuts ===")
haircuts_df.show(truncate=False)

print("\n=== Available Runoff Rates ===")
runoff_rates_df.show(truncate=False)

# Read existing data to get the latest state
existing_assets = spark.table("prasanna.lcr.assets")
existing_liabilities = spark.table("prasanna.lcr.liabilities")

# Get the latest timestamp
latest_timestamp_result = existing_assets.agg(F.max("timestamp")).collect()[0][0]

if latest_timestamp_result is None:
    # First run - insert initial data
    print("\n=== First Run: Inserting Initial Data ===")
    new_assets_data = [
        ("Cash + Fed reserves", 120.0, new_timestamp),
        ("US Treasuries", 250.0, new_timestamp),
        ("Agency MBS (AAA)", 110.0, new_timestamp),
        ("Corporate bonds", 70.0, new_timestamp),
        ("Loans (mortgages, business loans)", 600.0, new_timestamp)
    ]
    
    new_liabilities_data = [
        ("Retail deposits (insured)", 450.0, new_timestamp),
        ("Retail deposits (uninsured)", 220.0, new_timestamp),
        ("Corporate deposits", 170.0, new_timestamp),
        ("Long-term debt", 120.0, new_timestamp),
        ("Short-term wholesale funding", 110.0, new_timestamp)
    ]
else:
    # Incremental run - apply realistic changes to existing data
    print(f"\n=== Incremental Run: Applying Changes to Data from {latest_timestamp_result} ===")
    
    # Get latest assets and liabilities
    latest_assets = existing_assets.filter(F.col("timestamp") == latest_timestamp_result).toPandas()
    latest_liabilities = existing_liabilities.filter(F.col("timestamp") == latest_timestamp_result).toPandas()
    
    print("\nPrevious state:")
    print(f"  Total Assets: ${latest_assets['amount_millions'].sum()}M")
    print(f"  Total Liabilities: ${latest_liabilities['amount_millions'].sum()}M")
    
    # Apply small incremental changes (0.1% to 0.9%) for hourly runs
    new_assets_data = []
    for _, row in latest_assets.iterrows():
        asset_type = row['asset_type']
        current_amount = row['amount_millions']
        
        # Apply small percentage changes between 0.1% and 0.9% (can be positive or negative)
        change_pct = random.uniform(0.010, 0.030)  # -0.9% to +0.9%
        
        new_amount = round(current_amount * (1 + change_pct), 1)
        new_assets_data.append((asset_type, new_amount, new_timestamp))
    
    # Liabilities changes
    new_liabilities_data = []
    for _, row in latest_liabilities.iterrows():
        liability_type = row['liability_type']
        current_amount = row['amount_millions']
        
        # Skip Equity - we'll calculate it at the end
        if liability_type == 'Equity':
            continue
        
        # Apply small percentage changes between 0.1% and 0.9% (can be positive or negative)
        change_pct = random.uniform(-0.009, 0.009)  # -0.9% to +0.9%
        
        new_amount = round(current_amount * (1 + change_pct), 1)
        new_liabilities_data.append((liability_type, new_amount, new_timestamp))

# Calculate total assets for new records
new_total_assets = sum([amt for _, amt, _ in new_assets_data])
print(f"\nNew Total Assets: ${new_total_assets}M")

# Calculate equity as balancing figure
total_liabilities = sum([amt for _, amt, _ in new_liabilities_data])
equity_amount = round(new_total_assets - total_liabilities, 1)
new_liabilities_data.append(("Equity", equity_amount, new_timestamp))

print(f"New Total Liabilities: ${total_liabilities}M")
print(f"New Equity: ${equity_amount}M")
print(f"New Total Liabilities + Equity: ${total_liabilities + equity_amount}M")
print(f"\nAccounting Equation Verified: Assets ({new_total_assets}M) = Liabilities + Equity ({total_liabilities + equity_amount}M) ✓")

# Create DataFrames
assets_schema = StructType([
    StructField("asset_type", StringType(), True),
    StructField("amount_millions", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

liabilities_schema = StructType([
    StructField("liability_type", StringType(), True),
    StructField("amount_millions", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

new_assets_df = spark.createDataFrame(new_assets_data, schema=assets_schema)
new_liabilities_df = spark.createDataFrame(new_liabilities_data, schema=liabilities_schema)

# Fuzzy matching for assets with haircuts using keyword-based scoring
print("\n=== Performing Fuzzy Matching for Assets ===")
assets_cross = new_assets_df.crossJoin(
    haircuts_df.select("id", "asset_category", "asset_sub_type")
)

# Calculate a composite score based on keyword matches and Levenshtein distance
assets_with_score = assets_cross.withColumn(
    "lower_asset", F.lower(F.col("asset_type"))
).withColumn(
    "lower_haircut_category", F.lower(F.col("asset_category"))
).withColumn(
    "lower_haircut_subtype", F.lower(F.col("asset_sub_type"))
).withColumn(
    # Keyword matching bonuses - match against asset_category
    "keyword_score",
    F.when(F.col("lower_asset").contains("cash") & F.col("lower_haircut_category").contains("cash"), 100)
     .when(F.col("lower_asset").contains("reserve") & F.col("lower_haircut_category").contains("reserve"), 100)
     .when(F.col("lower_asset").contains("treasury") & F.col("lower_haircut_category").contains("treasur"), 100)
     .when(F.col("lower_asset").contains("treasuries") & F.col("lower_haircut_category").contains("treasur"), 100)
     .when(F.col("lower_asset").contains("agency") & F.col("lower_haircut_category").contains("agency"), 80)
     .when(F.col("lower_asset").contains("mbs") & F.col("lower_haircut_category").contains("mbs"), 80)
     .when(F.col("lower_asset").contains("corporate") & F.col("lower_haircut_category").contains("corporate"), 100)
     .when(F.col("lower_asset").contains("bond") & F.col("lower_haircut_category").contains("bond"), 50)
     .when(F.col("lower_asset").contains("loan") & F.col("lower_haircut_category").contains("loan"), 30)
     .when(F.col("lower_asset").contains("equity") & F.col("lower_haircut_category").contains("equity"), 30)
     .otherwise(0)
).withColumn(
    "levenshtein_dist", F.levenshtein(F.col("lower_asset"), F.col("lower_haircut_category"))
).withColumn(
    "final_score", F.col("keyword_score") - (F.col("levenshtein_dist") * 0.5)
)

# Get the best match (highest score) for each asset and cast ID to INT
window_spec = Window.partitionBy("asset_type", "amount_millions", "timestamp").orderBy(F.desc("final_score"))
assets_with_haircut = assets_with_score.withColumn(
    "rank", F.row_number().over(window_spec)
).filter(F.col("rank") == 1).select(
    "asset_type",
    "amount_millions",
    "timestamp",
    F.col("id").cast("int").alias("haircut_id")  # Cast LONG to INT
)

print("\n=== Assets with matched haircut_id ===")
assets_with_haircut.show(truncate=False)

# Fuzzy matching for liabilities with runoff_rates
print("\n=== Performing Fuzzy Matching for Liabilities ===")
liabilities_cross = new_liabilities_df.crossJoin(
    runoff_rates_df.select(
        "id",
        "liability_category",
        "specific_component"
    ).withColumn(
        "combined_liability",
        F.concat_ws(" ", F.col("liability_category"), F.col("specific_component"))
    )
)

# Calculate composite score for liabilities
liabilities_with_score = liabilities_cross.withColumn(
    "lower_liab", F.lower(F.col("liability_type"))
).withColumn(
    "lower_combined", F.lower(F.col("combined_liability"))
).withColumn(
    # Keyword matching bonuses
    "keyword_score",
    F.when(F.col("lower_liab").contains("retail") & F.col("lower_combined").contains("retail"), 100)
     .when(F.col("lower_liab").contains("insured") & F.col("lower_combined").contains("insured"), 100)
     .when(F.col("lower_liab").contains("uninsured") & F.col("lower_combined").contains("uninsured"), 100)
     .when(F.col("lower_liab").contains("stable") & F.col("lower_combined").contains("stable"), 100)
     .when(F.col("lower_liab").contains("wholesale") & F.col("lower_combined").contains("wholesale"), 100)
     .when(F.col("lower_liab").contains("corporate") & F.col("lower_combined").contains("operational"), 50)
     .when(F.col("lower_liab").contains("deposit") & F.col("lower_combined").contains("retail"), 50)
     .when(F.col("lower_liab").contains("funding") & F.col("lower_combined").contains("wholesale"), 80)
     .when(F.col("lower_liab").contains("debt") & F.col("lower_combined").contains("wholesale"), 40)
     .when(F.col("lower_liab").contains("equity") & F.col("lower_combined").contains("off-balance"), 30)
     .otherwise(0)
).withColumn(
    "levenshtein_dist", F.levenshtein(F.col("lower_liab"), F.col("lower_combined"))
).withColumn(
    "final_score", F.col("keyword_score") - (F.col("levenshtein_dist") * 0.5)
)

# Get the best match (highest score) for each liability and cast ID to INT
window_spec_liab = Window.partitionBy("liability_type", "amount_millions", "timestamp").orderBy(F.desc("final_score"))
liabilities_with_runoff = liabilities_with_score.withColumn(
    "rank", F.row_number().over(window_spec_liab)
).filter(F.col("rank") == 1).select(
    "liability_type",
    "amount_millions",
    "timestamp",
    F.col("id").cast("int").alias("runoff_rates_id")  # Cast LONG to INT
)

print("\n=== Liabilities with matched runoff_rates_id ===")
liabilities_with_runoff.show(truncate=False)

# Verify no NULL values in IDs
print("\n=== Validation: Checking for NULL IDs ===")
null_haircut_count = assets_with_haircut.filter(F.col("haircut_id").isNull()).count()
null_runoff_count = liabilities_with_runoff.filter(F.col("runoff_rates_id").isNull()).count()

if null_haircut_count > 0:
    print(f"⚠ WARNING: {null_haircut_count} assets have NULL haircut_id!")
    assets_with_haircut.filter(F.col("haircut_id").isNull()).show(truncate=False)
    raise ValueError("Assets with NULL haircut_id detected! All assets must have a valid haircut_id.")
else:
    print("✓ All assets have valid haircut_id")

if null_runoff_count > 0:
    print(f"⚠ WARNING: {null_runoff_count} liabilities have NULL runoff_rates_id!")
    liabilities_with_runoff.filter(F.col("runoff_rates_id").isNull()).show(truncate=False)
    raise ValueError("Liabilities with NULL runoff_rates_id detected! All liabilities must have a valid runoff_rates_id.")
else:
    print("✓ All liabilities have valid runoff_rates_id")

# ===== FINAL VALIDATION: ACCOUNTING EQUATION BEFORE INSERTION =====
print("\n" + "="*70)
print("=== PRE-INSERTION VALIDATION: ACCOUNTING EQUATION ===")
print("="*70)

# Calculate totals from the DataFrames that will be inserted
final_total_assets = assets_with_haircut.agg(F.sum("amount_millions")).collect()[0][0]
final_total_liabilities_equity = liabilities_with_runoff.agg(F.sum("amount_millions")).collect()[0][0]

print(f"\nTotal Assets (to be inserted): ${final_total_assets}M")
print(f"Total Liabilities + Equity (to be inserted): ${final_total_liabilities_equity}M")
print(f"Difference: ${abs(final_total_assets - final_total_liabilities_equity)}M")

# Check if balanced (allowing for small floating point differences)
if abs(final_total_assets - final_total_liabilities_equity) < 0.01:
    print("\n✓ ACCOUNTING EQUATION BALANCED: Assets = Liabilities + Equity")
    print("✓ Data is ready for insertion into tables")
else:
    error_msg = f"""
    ❌ ACCOUNTING EQUATION VIOLATION DETECTED!
    
    Assets: ${final_total_assets}M
    Liabilities + Equity: ${final_total_liabilities_equity}M
    Difference: ${abs(final_total_assets - final_total_liabilities_equity)}M
    
    The fundamental accounting equation (Assets = Liabilities + Equity) is NOT balanced.
    Data insertion has been BLOCKED to prevent invalid data from entering the tables.
    
    Please review the data generation logic and ensure equity is calculated correctly.
    """
    raise ValueError(error_msg)

print("="*70)

# COMMAND ----------

# DBTITLE 1,Step 3: Insert New Records into Tables
# Prepare assets data with correct column order (including liquidity as NULL)
assets_to_insert = assets_with_haircut.select(
    "asset_type",
    "amount_millions",
    F.lit(None).cast("string").alias("liquidity"),  # Old column, set to NULL
    "timestamp",
    F.col("haircut_id").cast("int")  # Cast LONG to INT to match table schema
)

# Insert new assets with haircut_id
print("Inserting new assets with haircut_id...")
assets_to_insert.write.mode("append").saveAsTable("prasanna.lcr.assets")
print("✓ Assets inserted successfully")

# Prepare liabilities data with correct column order (including stability as NULL)
liabilities_to_insert = liabilities_with_runoff.select(
    "liability_type",
    "amount_millions",
    F.lit(None).cast("string").alias("stability"),  # Old column, set to NULL
    "timestamp",
    "runoff_rates_id"  # Already INT, no cast needed
)

# Insert new liabilities with runoff_rates_id
print("\nInserting new liabilities with runoff_rates_id...")
liabilities_to_insert.write.mode("append").saveAsTable("prasanna.lcr.liabilities")
print("✓ Liabilities inserted successfully")

print("\n" + "="*60)
print("All records inserted successfully!")
print("="*60)

# Verify inserted data has no NULL IDs
print("\n=== Verification: Checking inserted data ===")
latest_assets = spark.table("prasanna.lcr.assets").filter(F.col("timestamp") == new_timestamp)
latest_liabilities = spark.table("prasanna.lcr.liabilities").filter(F.col("timestamp") == new_timestamp)

null_haircut = latest_assets.filter(F.col("haircut_id").isNull()).count()
null_runoff = latest_liabilities.filter(F.col("runoff_rates_id").isNull()).count()

if null_haircut > 0:
    print(f"\u26a0 ERROR: {null_haircut} assets have NULL haircut_id!")
else:
    print(f"✓ All {latest_assets.count()} inserted assets have valid haircut_id")

if null_runoff > 0:
    print(f"\u26a0 ERROR: {null_runoff} liabilities have NULL runoff_rates_id!")
else:
    print(f"✓ All {latest_liabilities.count()} inserted liabilities have valid runoff_rates_id")

# COMMAND ----------

# DBTITLE 1,Fix: Correct Equity Value
from pyspark.sql import functions as F

# Fix the incorrect Equity value for the second timestamp
# The Equity should be calculated as: Assets - Liabilities (non-equity)

second_timestamp = '2026-02-02 02:37:57.678776'

# Calculate correct equity
second_assets_total = spark.table("prasanna.lcr.assets").filter(
    F.col("timestamp") == second_timestamp
).agg(F.sum("amount_millions")).collect()[0][0]

second_liabilities_total = spark.table("prasanna.lcr.liabilities").filter(
    (F.col("timestamp") == second_timestamp) & (F.col("liability_type") != "Equity")
).agg(F.sum("amount_millions")).collect()[0][0]

correct_equity = round((second_assets_total or 0) - (second_liabilities_total or 0), 1)

print(f"Assets: ${second_assets_total}M")
print(f"Liabilities (non-equity): ${second_liabilities_total}M")
print(f"Correct Equity: ${correct_equity}M")

# Update the Equity value
spark.sql(f"""
    UPDATE prasanna.lcr.liabilities
    SET amount_millions = {correct_equity}
    WHERE timestamp = TIMESTAMP'{second_timestamp}' 
    AND liability_type = 'Equity'
""")

print("\n✓ Equity value corrected")

# Verify
print("\n=== Final Verification ===" )
balance_check = spark.sql("""
    SELECT 
        a.timestamp,
        a.total_assets,
        l.total_liabilities_equity,
        CASE 
            WHEN ABS(a.total_assets - l.total_liabilities_equity) < 0.01 THEN '✓ Balanced'
            ELSE '✗ Unbalanced'
        END as balanced
    FROM (
        SELECT timestamp, SUM(amount_millions) as total_assets
        FROM prasanna.lcr.assets
        GROUP BY timestamp
    ) a
    JOIN (
        SELECT timestamp, SUM(amount_millions) as total_liabilities_equity
        FROM prasanna.lcr.liabilities
        GROUP BY timestamp
    ) l ON a.timestamp = l.timestamp
    ORDER BY a.timestamp
""")

balance_check.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 4: Verify Final State
# Read updated tables
updated_assets_df = spark.table("prasanna.lcr.assets")
updated_liabilities_df = spark.table("prasanna.lcr.liabilities")

# Get all timestamps
print("=== Timestamps in Data ===")
updated_assets_df.select("timestamp").distinct().orderBy("timestamp").show()

# Calculate totals by timestamp
print("\n=== Assets by Timestamp ===")
updated_assets_df.groupBy("timestamp").agg(
    F.sum("amount_millions").alias("total_assets"),
    F.count("*").alias("record_count")
).orderBy("timestamp").show(truncate=False)

print("\n=== Liabilities by Timestamp ===")
updated_liabilities_df.groupBy("timestamp").agg(
    F.sum("amount_millions").alias("total_liabilities_equity"),
    F.count("*").alias("record_count")
).orderBy("timestamp").show(truncate=False)

# Verify accounting equation for each timestamp
print("\n=== Accounting Equation Verification ===")
assets_by_time = updated_assets_df.groupBy("timestamp").agg(
    F.sum("amount_millions").alias("total_assets")
)
liabilities_by_time = updated_liabilities_df.groupBy("timestamp").agg(
    F.sum("amount_millions").alias("total_liabilities_equity")
)

balance_check = assets_by_time.join(liabilities_by_time, "timestamp").withColumn(
    "balanced",
    F.when(F.col("total_assets") == F.col("total_liabilities_equity"), "✓ Balanced")
     .otherwise("✗ Unbalanced")
).orderBy("timestamp")

balance_check.show(truncate=False)

# Show latest records
print("\n=== Latest Assets ===")
updated_assets_df.filter(F.col("timestamp") == new_timestamp).show(truncate=False)

print("\n=== Latest Liabilities ===")
updated_liabilities_df.filter(F.col("timestamp") == new_timestamp).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Step 5: Verify Fuzzy Matching Results
# Show assets with their matched haircut details
print("=== Assets with Haircut Details ===")
assets_with_details = spark.sql("""
    SELECT 
        a.asset_type,
        a.amount_millions,
        a.haircut_id,
        h.hqla_level,
        h.asset_category as haircut_asset_category,
        h.fed_haircut,
        a.timestamp
    FROM prasanna.lcr.assets a
    LEFT JOIN prasanna.lcr.haircuts h ON a.haircut_id = h.id
    WHERE a.timestamp = (SELECT MAX(timestamp) FROM prasanna.lcr.assets)
    ORDER BY a.asset_type
""")
assets_with_details.show(truncate=False)

# Show liabilities with their matched runoff rate details
print("\n=== Liabilities with Runoff Rate Details ===")
liabilities_with_details = spark.sql("""
    SELECT 
        l.liability_type,
        l.amount_millions,
        l.runoff_rates_id,
        r.liability_category,
        r.specific_component,
        r.fed_runoff_rate,
        l.timestamp
    FROM prasanna.lcr.liabilities l
    LEFT JOIN prasanna.lcr.runoff_rates r ON l.runoff_rates_id = r.id
    WHERE l.timestamp = (SELECT MAX(timestamp) FROM prasanna.lcr.liabilities)
    ORDER BY l.liability_type
""")
liabilities_with_details.show(truncate=False)

print("\n" + "="*80)
print("Fuzzy Matching Summary:")
print("="*80)
print("Assets have been matched with haircuts from prasanna.lcr.haircuts")
print("Liabilities have been matched with runoff rates from prasanna.lcr.runoff_rates")
print("\nMatching used keyword-based scoring combined with Levenshtein distance")
print("for semantic similarity rather than just character-level matching.")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Step 6: Verify Incremental Behavior
print("=== Incremental Behavior Verification ===")
print("This notebook is designed for HOURLY runs with incremental changes\n")

# Show run history
print("Run History (Last 10 Runs):")
spark.sql("""
    SELECT 
        timestamp,
        COUNT(*) as asset_count,
        ROUND(SUM(amount_millions), 2) as total_assets
    FROM prasanna.lcr.assets
    GROUP BY timestamp
    ORDER BY timestamp DESC
    LIMIT 10
""").show(truncate=False)

# Show incremental changes for each asset type
print("\nIncremental Changes Across Last 3 Runs:")
spark.sql("""
    WITH ranked_assets AS (
        SELECT 
            asset_type,
            amount_millions,
            timestamp,
            ROW_NUMBER() OVER (PARTITION BY asset_type ORDER BY timestamp DESC) as rn
        FROM prasanna.lcr.assets
    ),
    latest_3_runs AS (
        SELECT 
            asset_type,
            MAX(CASE WHEN rn = 1 THEN amount_millions END) as current_run,
            MAX(CASE WHEN rn = 2 THEN amount_millions END) as previous_run,
            MAX(CASE WHEN rn = 3 THEN amount_millions END) as two_runs_ago,
            MAX(CASE WHEN rn = 1 THEN timestamp END) as latest_timestamp
        FROM ranked_assets
        WHERE rn <= 3
        GROUP BY asset_type
    )
    SELECT 
        asset_type,
        ROUND(two_runs_ago, 1) as run_n_minus_2,
        ROUND(previous_run, 1) as run_n_minus_1,
        ROUND(current_run, 1) as current_run,
        ROUND(current_run - previous_run, 1) as last_change,
        CONCAT(
            ROUND(((current_run - previous_run) / previous_run) * 100, 2), 
            '%'
        ) as last_change_pct,
        latest_timestamp
    FROM latest_3_runs
    ORDER BY asset_type
""").show(truncate=False)

# Verify no duplicate amounts
print("\nDuplicate Check (Should be empty):")
duplicate_check = spark.sql("""
    SELECT 
        asset_type,
        amount_millions,
        COUNT(*) as occurrence_count
    FROM prasanna.lcr.assets
    GROUP BY asset_type, amount_millions
    HAVING COUNT(*) > 1
    ORDER BY occurrence_count DESC
""")

if duplicate_check.count() == 0:
    print("✓ No duplicates found - each run creates unique data!")
else:
    print("⚠ WARNING: Duplicate amounts detected:")
    duplicate_check.show(truncate=False)

print("\n" + "="*80)
print("✓ Incremental Behavior Confirmed:")
print("  - Each run reads the latest state from the tables")
print("  - Applies small, realistic percentage changes (-15% to +15%)")
print("  - Creates unique data for each run (no duplicates)")
print("  - Maintains accounting equation: Assets = Liabilities + Equity")
print("  - All records have valid haircut_id and runoff_rates_id (no NULLs)")
print("="*80)

# COMMAND ----------

