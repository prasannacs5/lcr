# Databricks notebook source
# DBTITLE 1,Import libraries and create assets data
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime

# Create assets data with timestamp
assets_data = [
    ("Cash + Fed reserves", 100.0, "Instant", datetime(2026, 1, 21, 9, 0, 0)),
    ("US Treasuries", 200.0, "Very high", datetime(2026, 1, 21, 9, 0, 0)),
    ("Agency MBS (AAA)", 100.0, "High (with haircut)", datetime(2026, 1, 21, 9, 0, 0)),
    ("Corporate bonds", 50.0, "Medium", datetime(2026, 1, 21, 9, 0, 0)),
    ("Loans (mortgages, business loans)", 550.0, "Low", datetime(2026, 1, 21, 9, 0, 0))
]

# Define schema
assets_schema = StructType([
    StructField("asset_type", StringType(), False),
    StructField("amount_millions", DoubleType(), False),
    StructField("liquidity", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])

# Create DataFrame
df_assets = spark.createDataFrame(assets_data, schema=assets_schema)

# Display the assets data
display(df_assets)

# COMMAND ----------

# DBTITLE 1,Save assets table to prasanna.lcr.assets
# Create catalog and schema if they don't exist
spark.sql("CREATE CATALOG IF NOT EXISTS prasanna")
spark.sql("CREATE SCHEMA IF NOT EXISTS prasanna.lcr")

# Check if table exists
try:
    spark.sql("DESCRIBE TABLE prasanna.lcr.assets")
    print("⚠ Table prasanna.lcr.assets already exists. Skipping creation.")
    print("To update, please drop the table first or use a different approach.")
except:
    # Create the assets table (table doesn't exist)
    df_assets.write.saveAsTable("prasanna.lcr.assets")
    print("✓ Assets table created successfully at prasanna.lcr.assets")
    print(f"Total assets: ${df_assets.agg(F.sum('amount_millions')).collect()[0][0]} million")

# COMMAND ----------

# DBTITLE 1,Create liabilities data
# Create liabilities data with timestamp
liabilities_data = [
    ("Retail deposits (insured)", 400.0, "Stable", datetime(2026, 1, 21, 9, 0, 0)),
    ("Retail deposits (uninsured)", 200.0, "Less stable", datetime(2026, 1, 21, 9, 0, 0)),
    ("Corporate deposits", 150.0, "Volatile", datetime(2026, 1, 21, 9, 0, 0)),
    ("Short-term wholesale funding", 100.0, "Very volatile", datetime(2026, 1, 21, 9, 0, 0)),
    ("Long-term debt", 100.0, "Stable", datetime(2026, 1, 21, 9, 0, 0)),
    ("Equity", 50.0, "Permanent", datetime(2026, 1, 21, 9, 0, 0))
]

# Define schema
liabilities_schema = StructType([
    StructField("liability_type", StringType(), False),
    StructField("amount_millions", DoubleType(), False),
    StructField("stability", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])

# Create DataFrame
df_liabilities = spark.createDataFrame(liabilities_data, schema=liabilities_schema)

# Display the liabilities data
display(df_liabilities)

# COMMAND ----------

# DBTITLE 1,Save liabilities table to prasanna.lcr.liabilities
# Check if table exists
try:
    spark.sql("DESCRIBE TABLE prasanna.lcr.liabilities")
    print("⚠ Table prasanna.lcr.liabilities already exists. Skipping creation.")
    print("To update, please drop the table first or use a different approach.")
except:
    # Create the liabilities table (table doesn't exist)
    df_liabilities.write.saveAsTable("prasanna.lcr.liabilities")
    print("✓ Liabilities table created successfully at prasanna.lcr.liabilities")
    print(f"Total liabilities + equity: ${df_liabilities.agg(F.sum('amount_millions')).collect()[0][0]} million")

# COMMAND ----------

# DBTITLE 1,Verify both tables in prasanna.lcr schema
# Show tables in the schema
print("Tables in prasanna.lcr schema:")
spark.sql("SHOW TABLES IN prasanna.lcr").show()

print("\n" + "="*80)
print("ASSETS TABLE")
print("="*80)
spark.sql("SELECT * FROM prasanna.lcr.assets").show(truncate=False)

print("\n" + "="*80)
print("LIABILITIES TABLE")
print("="*80)
spark.sql("SELECT * FROM prasanna.lcr.liabilities").show(truncate=False)

print("\n" + "="*80)
print("BALANCE SHEET SUMMARY")
print("="*80)
total_assets = spark.sql("SELECT SUM(amount_millions) as total FROM prasanna.lcr.assets").collect()[0][0]
total_liabilities = spark.sql("SELECT SUM(amount_millions) as total FROM prasanna.lcr.liabilities").collect()[0][0]

print(f"Total Assets: ${total_assets} million")
print(f"Total Liabilities + Equity: ${total_liabilities} million")
print(f"Balance Sheet Balanced: {total_assets == total_liabilities}")

# COMMAND ----------

# DBTITLE 1,30-Day Basel Stress Scenario - Cash Outflows
# Load liabilities data from table
df_liabilities_loaded = spark.sql("SELECT * FROM prasanna.lcr.liabilities")

# Define stress factors for 30-day scenario (Basel LCR assumptions)
stress_factors = {
    "Retail deposits (insured)": 0.05,      # 5% outflow
    "Retail deposits (uninsured)": 0.15,    # 15% outflow
    "Corporate deposits": 0.25,             # 25% outflow
    "Short-term wholesale funding": 1.00,   # 100% outflow
    "Long-term debt": 0.00,                 # Stable, no outflow
    "Equity": 0.00                          # Permanent, no outflow
}

# Calculate stressed outflows for each liability type
outflows_data = []
for row in df_liabilities_loaded.collect():
    liability_type = row['liability_type']
    amount = row['amount_millions']
    stress_factor = stress_factors.get(liability_type, 0.0)
    outflow = amount * stress_factor
    outflows_data.append((liability_type, amount, stress_factor, outflow))

# Create outflows DataFrame
outflows_schema = StructType([
    StructField("liability_type", StringType(), False),
    StructField("amount_millions", DoubleType(), False),
    StructField("stress_factor", DoubleType(), False),
    StructField("expected_outflow", DoubleType(), False)
])

df_outflows = spark.createDataFrame(outflows_data, schema=outflows_schema)

# Add credit line drawdowns (additional stress assumption)
credit_line_drawdowns = 30.0  # $30M as per Basel scenario

# Calculate total stressed outflows
total_liability_outflows = df_outflows.agg(F.sum('expected_outflow')).collect()[0][0]
total_stressed_outflows = total_liability_outflows + credit_line_drawdowns

print("="*80)
print("30-DAY BASEL STRESS SCENARIO - EXPECTED CASH OUTFLOWS")
print("="*80)
display(df_outflows)
print(f"\nTotal liability outflows: ${total_liability_outflows} million")
print(f"Credit line drawdowns: ${credit_line_drawdowns} million")
print(f"Total stressed outflows: ${total_stressed_outflows} million")

# COMMAND ----------

# DBTITLE 1,30-Day Basel Stress Scenario - Cash Inflows
# Define expected cash inflows (30-day period)
loan_repayments = 80.0  # Loan repayments due in 30 days
bond_coupons_maturities = 40.0  # Bond coupons & maturities

total_inflows = loan_repayments + bond_coupons_maturities

# Apply regulatory cap: max inflows = 75% of outflows
regulatory_cap_factor = 0.75
max_inflows_allowed = regulatory_cap_factor * total_stressed_outflows

# Determine actual inflows (capped)
actual_inflows = min(total_inflows, max_inflows_allowed)

print("="*80)
print("30-DAY BASEL STRESS SCENARIO - EXPECTED CASH INFLOWS")
print("="*80)
print(f"Loan repayments (30 days): ${loan_repayments} million")
print(f"Bond coupons & maturities: ${bond_coupons_maturities} million")
print(f"Total expected inflows: ${total_inflows} million")
print(f"\nRegulatory cap (75% of outflows): ${max_inflows_allowed} million")
print(f"Actual inflows (after cap): ${actual_inflows} million")

if total_inflows <= max_inflows_allowed:
    print(f"\n✓ All inflows can be counted (below regulatory cap)")
else:
    print(f"\n⚠ Inflows capped by regulation")

# COMMAND ----------

# DBTITLE 1,30-Day Basel Stress Scenario - Net Cash Outflows
# Calculate net cash outflows
net_cash_outflows = total_stressed_outflows - actual_inflows

print("="*80)
print("30-DAY BASEL STRESS SCENARIO - NET CASH OUTFLOWS")
print("="*80)
print(f"Total stressed outflows: ${total_stressed_outflows} million")
print(f"Less: Actual inflows: ${actual_inflows} million")
print(f"Net cash outflows (30 days): ${net_cash_outflows} million")

# Create summary DataFrame for stress scenario
stress_summary_data = [
    ("Expected Cash Outflows", "", ""),
    ("  Retail deposits (insured) - 5%", 20.0, ""),
    ("  Retail deposits (uninsured) - 15%", 30.0, ""),
    ("  Corporate deposits - 25%", 37.5, ""),
    ("  Short-term wholesale funding - 100%", 100.0, ""),
    ("  Credit line drawdowns", 30.0, ""),
    ("Total Outflows", 217.5, "A"),
    ("", "", ""),
    ("Expected Cash Inflows", "", ""),
    ("  Loan repayments", 80.0, ""),
    ("  Bond coupons & maturities", 40.0, ""),
    ("Total Inflows (before cap)", 120.0, ""),
    ("  Regulatory cap (75% of outflows)", 163.125, ""),
    ("Actual Inflows (after cap)", 120.0, "B"),
    ("", "", ""),
    ("Net Cash Outflows (A - B)", 97.5, "Result")
]

stress_summary_schema = StructType([
    StructField("description", StringType(), True),
    StructField("amount_millions", StringType(), True),
    StructField("reference", StringType(), True)
])

# Convert numeric values to strings for display
stress_summary_display = [(desc, str(amt) if amt != "" else "", ref) 
                          for desc, amt, ref in stress_summary_data]

df_stress_summary = spark.createDataFrame(stress_summary_display, schema=stress_summary_schema)

print("\n" + "="*80)
print("STRESS SCENARIO SUMMARY")
print("="*80)
display(df_stress_summary)

# COMMAND ----------

# DBTITLE 1,Save stress scenario results to table
# Create stress scenario results table
stress_results_data = [(
    datetime(2026, 1, 21, 9, 0, 0),  # timestamp
    "30-Day Basel Stress",            # scenario_name
    total_stressed_outflows,          # total_outflows
    actual_inflows,                   # total_inflows
    net_cash_outflows,                # net_outflows
    max_inflows_allowed,              # regulatory_cap
    credit_line_drawdowns             # credit_line_drawdowns
)]

stress_results_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("scenario_name", StringType(), False),
    StructField("total_outflows_millions", DoubleType(), False),
    StructField("total_inflows_millions", DoubleType(), False),
    StructField("net_outflows_millions", DoubleType(), False),
    StructField("regulatory_cap_millions", DoubleType(), False),
    StructField("credit_line_drawdowns_millions", DoubleType(), False)
])

df_stress_results = spark.createDataFrame(stress_results_data, schema=stress_results_schema)

# Check if table exists
try:
    spark.sql("DESCRIBE TABLE prasanna.lcr.stress_scenarios")
    print("⚠ Table prasanna.lcr.stress_scenarios already exists. Skipping creation.")
except:
    # Create the stress scenarios table
    df_stress_results.write.saveAsTable("prasanna.lcr.stress_scenarios")
    print("✓ Stress scenarios table created successfully at prasanna.lcr.stress_scenarios")
    print(f"\nNet cash outflows (30-day stress): ${net_cash_outflows} million")
    print("This value will be used for LCR calculation.")

display(df_stress_results)

# COMMAND ----------

# DBTITLE 1,Calculate High Quality Liquid Assets (HQLA)
# Load assets data from table
df_assets_loaded = spark.sql("SELECT * FROM prasanna.lcr.assets")

# Define HQLA haircuts (regulatory requirements)
hqla_haircuts = {
    "Cash + Fed reserves": 0.00,           # Level 1 HQLA - no haircut
    "US Treasuries": 0.00,                 # Level 1 HQLA - no haircut
    "Agency MBS (AAA)": 0.15,              # Level 2A HQLA - 15% haircut
    "Corporate bonds": 1.00,               # Not HQLA - 100% haircut (excluded)
    "Loans (mortgages, business loans)": 1.00  # Not HQLA - 100% haircut (excluded)
}

# Calculate HQLA value for each asset
hqla_data = []
for row in df_assets_loaded.collect():
    asset_type = row['asset_type']
    amount = row['amount_millions']
    haircut = hqla_haircuts.get(asset_type, 1.00)
    hqla_value = amount * (1 - haircut)
    is_hqla = "Yes" if haircut < 1.00 else "No"
    
    # Determine HQLA level
    if haircut == 0.00 and is_hqla == "Yes":
        hqla_level = "Level 1"
    elif haircut == 0.15:
        hqla_level = "Level 2A"
    else:
        hqla_level = "Not HQLA"
    
    hqla_data.append((asset_type, amount, haircut, hqla_value, hqla_level, is_hqla))

# Create HQLA DataFrame
hqla_schema = StructType([
    StructField("asset_type", StringType(), False),
    StructField("amount_millions", DoubleType(), False),
    StructField("haircut", DoubleType(), False),
    StructField("hqla_value", DoubleType(), False),
    StructField("hqla_level", StringType(), False),
    StructField("is_hqla", StringType(), False)
])

df_hqla = spark.createDataFrame(hqla_data, schema=hqla_schema)

# Calculate total HQLA
total_hqla = df_hqla.agg(F.sum('hqla_value')).collect()[0][0]

print("="*80)
print("HIGH QUALITY LIQUID ASSETS (HQLA) CALCULATION")
print("="*80)
display(df_hqla)
print(f"\nTotal HQLA: ${total_hqla} million")
print(f"\nBreakdown:")
print(f"  Level 1 HQLA (0% haircut): ${100 + 200} million")
print(f"  Level 2A HQLA (15% haircut): ${85} million")
print(f"  Non-HQLA assets: ${50 + 550} million (excluded from HQLA)")

# COMMAND ----------

# DBTITLE 1,Calculate Liquidity Coverage Ratio (LCR)
# Calculate LCR
lcr_ratio = (total_hqla / net_cash_outflows) * 100
regulatory_minimum = 100.0  # 100% minimum requirement

# Determine compliance status
compliance_status = "COMPLIANT" if lcr_ratio >= regulatory_minimum else "NON-COMPLIANT"
excess_coverage = lcr_ratio - regulatory_minimum

# Calculate how many times the bank can survive the stress
survival_multiple = total_hqla / net_cash_outflows

print("="*80)
print("LIQUIDITY COVERAGE RATIO (LCR) CALCULATION")
print("="*80)
print(f"\nHigh Quality Liquid Assets (HQLA): ${total_hqla} million")
print(f"Net Cash Outflows (30-day stress): ${net_cash_outflows} million")
print(f"\nLCR = HQLA / Net Cash Outflows")
print(f"LCR = ${total_hqla} / ${net_cash_outflows}")
print(f"LCR = {lcr_ratio:.2f}%")
print(f"\n" + "="*80)
print(f"REGULATORY ASSESSMENT")
print("="*80)
print(f"Regulatory Minimum: {regulatory_minimum}%")
print(f"Bank's LCR: {lcr_ratio:.2f}%")
print(f"Status: {compliance_status}")
print(f"Excess Coverage: {excess_coverage:.2f}%")
print(f"\nInterpretation:")
print(f"  ✓ The bank can survive {survival_multiple:.2f}x the required 30-day stress")
print(f"  ✓ The bank has {excess_coverage:.0f}% more liquidity than required")
print(f"  ✓ On paper: Very liquid position")

# COMMAND ----------

# DBTITLE 1,Save LCR results to table
# Create LCR results table
lcr_results_data = [(
    datetime(2026, 1, 21, 9, 0, 0),  # timestamp
    total_hqla,                       # hqla
    net_cash_outflows,                # net_outflows
    lcr_ratio,                        # lcr_ratio
    compliance_status,                # compliance_status
    regulatory_minimum                # regulatory_minimum
)]

lcr_results_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("hqla_millions", DoubleType(), False),
    StructField("net_outflows_millions", DoubleType(), False),
    StructField("lcr_ratio_percent", DoubleType(), False),
    StructField("compliance_status", StringType(), False),
    StructField("regulatory_minimum_percent", DoubleType(), False)
])

df_lcr_results = spark.createDataFrame(lcr_results_data, schema=lcr_results_schema)

# Check if table exists
try:
    spark.sql("DESCRIBE TABLE prasanna.lcr.lcr_results")
    print("⚠ Table prasanna.lcr.lcr_results already exists. Skipping creation.")
except:
    # Create the LCR results table
    df_lcr_results.write.saveAsTable("prasanna.lcr.lcr_results")
    print("✓ LCR results table created successfully at prasanna.lcr.lcr_results")

display(df_lcr_results)

# COMMAND ----------

# DBTITLE 1,Bank Run Scenario - Immediate Outflows
# Define bank run stress factors (more severe than Basel)
bank_run_factors = {
    "Retail deposits (insured)": 0.00,      # Insured deposits stay (FDIC protection)
    "Retail deposits (uninsured)": 0.40,    # 40% run
    "Corporate deposits": 0.50,             # 50% run
    "Short-term wholesale funding": 1.00,   # 100% gone
    "Long-term debt": 0.00,                 # Stable
    "Equity": 0.00                          # Permanent
}

# Calculate bank run outflows
bank_run_data = []
for row in df_liabilities_loaded.collect():
    liability_type = row['liability_type']
    amount = row['amount_millions']
    run_factor = bank_run_factors.get(liability_type, 0.0)
    outflow = amount * run_factor
    bank_run_data.append((liability_type, amount, run_factor, outflow))

# Create bank run DataFrame
bank_run_schema = StructType([
    StructField("liability_type", StringType(), False),
    StructField("amount_millions", DoubleType(), False),
    StructField("run_factor", DoubleType(), False),
    StructField("immediate_outflow", DoubleType(), False)
])

df_bank_run = spark.createDataFrame(bank_run_data, schema=bank_run_schema)

# Calculate total immediate outflows
total_immediate_outflows = df_bank_run.agg(F.sum('immediate_outflow')).collect()[0][0]

print("="*80)
print("BANK RUN SCENARIO - IMMEDIATE OUTFLOWS")
print("="*80)
print("Panic scenario: Severe deposit run")
print()
display(df_bank_run)
print(f"\nTotal immediate outflows: ${total_immediate_outflows} million")

# COMMAND ----------

# DBTITLE 1,Bank Run Scenario - Available Liquidity
# Calculate immediately available liquidity
# Assets that can be quickly converted to cash
immediate_liquidity = {
    "Cash + Fed reserves": 100.0,    # Instant
    "US Treasuries": 200.0,          # Can sell or repo immediately
    "Agency MBS (AAA)": 85.0         # After 15% haircut
}

total_immediate_liquidity = sum(immediate_liquidity.values())

# Calculate liquidity surplus/deficit
liquidity_surplus = total_immediate_liquidity - total_immediate_outflows
survival_status = "SURVIVES" if liquidity_surplus >= 0 else "FAILS"

print("="*80)
print("BANK RUN SCENARIO - AVAILABLE LIQUIDITY")
print("="*80)
print("\nImmediately Available Liquidity:")
for asset, value in immediate_liquidity.items():
    print(f"  {asset}: ${value} million")
print(f"\nTotal usable liquidity: ${total_immediate_liquidity} million")
print(f"Total immediate outflows: ${total_immediate_outflows} million")
print(f"\nLiquidity surplus/(deficit): ${liquidity_surplus} million")
print(f"\n" + "="*80)
print(f"BANK RUN OUTCOME: {survival_status}")
print("="*80)

if survival_status == "SURVIVES":
    print(f"\n✓ Bank survives without selling loans")
    print(f"  Remaining liquidity buffer: ${liquidity_surplus} million")
    coverage_ratio = (total_immediate_liquidity / total_immediate_outflows) * 100
    print(f"  Coverage ratio: {coverage_ratio:.1f}%")
else:
    print(f"\n✗ Bank would need to sell illiquid assets (loans)")
    print(f"  Liquidity shortfall: ${abs(liquidity_surplus)} million")

# COMMAND ----------

# DBTITLE 1,Save bank run scenario results
# Create bank run results table
bank_run_results_data = [(
    datetime(2026, 1, 21, 9, 0, 0),   # timestamp
    "Bank Run Scenario",               # scenario_name
    total_immediate_outflows,          # immediate_outflows
    total_immediate_liquidity,         # available_liquidity
    liquidity_surplus,                 # liquidity_surplus
    survival_status                    # survival_status
)]

bank_run_results_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("scenario_name", StringType(), False),
    StructField("immediate_outflows_millions", DoubleType(), False),
    StructField("available_liquidity_millions", DoubleType(), False),
    StructField("liquidity_surplus_millions", DoubleType(), False),
    StructField("survival_status", StringType(), False)
])

df_bank_run_results = spark.createDataFrame(bank_run_results_data, schema=bank_run_results_schema)

# Check if table exists
try:
    spark.sql("DESCRIBE TABLE prasanna.lcr.bank_run_scenarios")
    print("⚠ Table prasanna.lcr.bank_run_scenarios already exists. Skipping creation.")
except:
    # Create the bank run scenarios table
    df_bank_run_results.write.saveAsTable("prasanna.lcr.bank_run_scenarios")
    print("✓ Bank run scenarios table created successfully at prasanna.lcr.bank_run_scenarios")

display(df_bank_run_results)

# COMMAND ----------

# DBTITLE 1,Calculate Loan-to-Deposit Ratio
# Calculate total loans and total deposits
total_loans = 550.0  # From assets table

# Calculate total deposits (retail + corporate)
retail_insured = 400.0
retail_uninsured = 200.0
corporate_deposits = 150.0
total_deposits = retail_insured + retail_uninsured + corporate_deposits

# Calculate Loan-to-Deposit ratio
ldr = (total_loans / total_deposits) * 100

print("="*80)
print("LOAN-TO-DEPOSIT RATIO (LDR)")
print("="*80)
print(f"\nTotal Loans: ${total_loans} million")
print(f"Total Deposits: ${total_deposits} million")
print(f"\nLoan-to-Deposit Ratio = Loans / Deposits")
print(f"LDR = ${total_loans} / ${total_deposits}")
print(f"LDR = {ldr:.2f}%")
print(f"\nInterpretation:")
if ldr < 80:
    print(f"  ✓ Very conservative - Strong liquidity position")
    print(f"  ✓ Bank has significant deposit funding not tied up in loans")
    print(f"  ✓ Low liquidity risk from loan portfolio")
elif ldr < 90:
    print(f"  ✓ Conservative - Good liquidity position")
else:
    print(f"  ⚠ Aggressive - Monitor liquidity closely")

print(f"\nContext: Most banks target LDR between 80-90%")

# COMMAND ----------

# DBTITLE 1,Mark-to-Market Scenario for Securities
# Scenario: Interest rates jumped, causing mark-to-market losses
# Treasuries book value: $200M, market value after rate increase: $170M

treasuries_book_value = 200.0
treasuries_market_value = 170.0  # After interest rate increase
treasuries_unrealized_loss = treasuries_book_value - treasuries_market_value

# Recalculate HQLA with mark-to-market values
cash_reserves = 100.0
treasuries_mtm = 170.0  # Market value
agency_mbs = 85.0  # Already haircut

total_hqla_mtm = cash_reserves + treasuries_mtm + agency_mbs

# Recalculate LCR with mark-to-market
lcr_mtm = (total_hqla_mtm / net_cash_outflows) * 100

# Impact on bank run scenario
immediate_liquidity_mtm = cash_reserves + treasuries_mtm + agency_mbs
liquidity_surplus_mtm = immediate_liquidity_mtm - total_immediate_outflows

print("="*80)
print("MARK-TO-MARKET SCENARIO (Interest Rate Shock)")
print("="*80)
print(f"\nScenario: Interest rates increased, causing unrealized losses on securities")
print(f"\nUS Treasuries:")
print(f"  Book value: ${treasuries_book_value} million")
print(f"  Market value: ${treasuries_market_value} million")
print(f"  Unrealized loss: ${treasuries_unrealized_loss} million ({(treasuries_unrealized_loss/treasuries_book_value)*100:.1f}%)")

print(f"\nRecalculated HQLA (mark-to-market):")
print(f"  Cash + reserves: ${cash_reserves} million")
print(f"  Treasuries (market value): ${treasuries_mtm} million")
print(f"  Agency MBS (haircut): ${agency_mbs} million")
print(f"  Total HQLA (MTM): ${total_hqla_mtm} million")

print(f"\nImpact on LCR:")
print(f"  Original LCR: {lcr_ratio:.2f}%")
print(f"  LCR with MTM: {lcr_mtm:.2f}%")
print(f"  Change: {lcr_mtm - lcr_ratio:.2f}%")
print(f"  Status: {'STILL COMPLIANT' if lcr_mtm >= 100 else 'NON-COMPLIANT'}")

print(f"\nImpact on Bank Run Scenario:")
print(f"  Original liquidity surplus: ${liquidity_surplus} million")
print(f"  MTM liquidity surplus: ${liquidity_surplus_mtm} million")
print(f"  Change: ${liquidity_surplus_mtm - liquidity_surplus} million")
print(f"  Status: {'STILL SURVIVES' if liquidity_surplus_mtm >= 0 else 'WOULD FAIL'}")

print(f"\n" + "="*80)
print(f"KEY INSIGHT (SVB-type risk)")
print("="*80)
print(f"  ✓ Bank still survives with less margin")
print(f"  ⚠ Unrealized losses + concentrated deposits = liquidity crunch risk")
print(f"  ⚠ This is where SVB-type problems emerge")

# COMMAND ----------

# DBTITLE 1,Save supplementary metrics to table
# Create supplementary metrics table
supplementary_metrics_data = [(
    datetime(2026, 1, 21, 9, 0, 0),  # timestamp
    total_loans,                      # total_loans
    total_deposits,                   # total_deposits
    ldr,                              # loan_to_deposit_ratio
    treasuries_book_value,            # treasuries_book_value
    treasuries_market_value,          # treasuries_market_value
    treasuries_unrealized_loss,       # unrealized_loss
    total_hqla_mtm,                   # hqla_mtm
    lcr_mtm                           # lcr_mtm
)]

supplementary_metrics_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("total_loans_millions", DoubleType(), False),
    StructField("total_deposits_millions", DoubleType(), False),
    StructField("loan_to_deposit_ratio_percent", DoubleType(), False),
    StructField("treasuries_book_value_millions", DoubleType(), False),
    StructField("treasuries_market_value_millions", DoubleType(), False),
    StructField("unrealized_loss_millions", DoubleType(), False),
    StructField("hqla_mtm_millions", DoubleType(), False),
    StructField("lcr_mtm_percent", DoubleType(), False)
])

df_supplementary_metrics = spark.createDataFrame(supplementary_metrics_data, schema=supplementary_metrics_schema)

# Check if table exists
try:
    spark.sql("DESCRIBE TABLE prasanna.lcr.supplementary_metrics")
    print("⚠ Table prasanna.lcr.supplementary_metrics already exists. Skipping creation.")
except:
    # Create the supplementary metrics table
    df_supplementary_metrics.write.saveAsTable("prasanna.lcr.supplementary_metrics")
    print("✓ Supplementary metrics table created successfully at prasanna.lcr.supplementary_metrics")

display(df_supplementary_metrics)

# COMMAND ----------

# DBTITLE 1,Liquidity Monitoring - Executive Summary
print("="*80)
print("BANK LIQUIDITY MONITORING - EXECUTIVE SUMMARY")
print("="*80)
print(f"Analysis Date: {datetime(2026, 1, 21, 9, 0, 0).strftime('%Y-%m-%d %H:%M')}")
print(f"\n" + "="*80)
print("BALANCE SHEET OVERVIEW")
print("="*80)
print(f"Total Assets: $1,000 million")
print(f"Total Liabilities + Equity: $1,000 million")
print(f"Balance Sheet: Balanced ✓")

print(f"\n" + "="*80)
print("KEY LIQUIDITY METRICS")
print("="*80)
print(f"\n1. LIQUIDITY COVERAGE RATIO (LCR)")
print(f"   LCR: {lcr_ratio:.2f}%")
print(f"   Regulatory Minimum: 100%")
print(f"   Status: {compliance_status}")
print(f"   Excess Coverage: {excess_coverage:.0f}%")
print(f"   → Bank can survive {survival_multiple:.2f}x the required 30-day stress")

print(f"\n2. LOAN-TO-DEPOSIT RATIO (LDR)")
print(f"   LDR: {ldr:.2f}%")
print(f"   Industry Target: 80-90%")
print(f"   Assessment: Very Conservative")
print(f"   → Strong liquidity position with low loan concentration")

print(f"\n3. HIGH QUALITY LIQUID ASSETS (HQLA)")
print(f"   Total HQLA: ${total_hqla} million")
print(f"   Level 1 HQLA: $300 million (Cash + Treasuries)")
print(f"   Level 2A HQLA: $85 million (Agency MBS after haircut)")

print(f"\n" + "="*80)
print("STRESS TEST RESULTS")
print("="*80)
print(f"\n1. 30-DAY BASEL STRESS SCENARIO")
print(f"   Expected Outflows: ${total_stressed_outflows} million")
print(f"   Expected Inflows: ${actual_inflows} million")
print(f"   Net Cash Outflows: ${net_cash_outflows} million")
print(f"   Result: PASS ✓")

print(f"\n2. BANK RUN SCENARIO (Severe Stress)")
print(f"   Immediate Outflows: ${total_immediate_outflows} million")
print(f"   Available Liquidity: ${total_immediate_liquidity} million")
print(f"   Liquidity Surplus: ${liquidity_surplus} million")
print(f"   Result: {survival_status} ✓")

print(f"\n3. MARK-TO-MARKET SCENARIO (Interest Rate Shock)")
print(f"   Unrealized Loss on Treasuries: ${treasuries_unrealized_loss} million")
print(f"   LCR after MTM: {lcr_mtm:.2f}%")
print(f"   Liquidity Surplus after MTM: ${liquidity_surplus_mtm} million")
print(f"   Result: STILL SURVIVES (with reduced margin) ⚠")

print(f"\n" + "="*80)
print("RISK ASSESSMENT")
print("="*80)
print(f"\n✓ STRENGTHS:")
print(f"  • Very strong LCR at 395% (well above 100% minimum)")
print(f"  • Conservative loan-to-deposit ratio at 73%")
print(f"  • Survives severe bank run scenario")
print(f"  • High quality liquid assets portfolio")

print(f"\n⚠ RISKS TO MONITOR:")
print(f"  • Interest rate risk: $30M unrealized loss scenario")
print(f"  • Deposit concentration: 60% in retail deposits")
print(f"  • Mark-to-market losses reduce liquidity buffer")
print(f"  • SVB-type risk: unrealized losses + deposit flight")

print(f"\n" + "="*80)
print("TABLES CREATED IN prasanna.lcr SCHEMA")
print("="*80)
print(f"  1. assets - Balance sheet assets with liquidity classification")
print(f"  2. liabilities - Balance sheet liabilities with stability classification")
print(f"  3. stress_scenarios - 30-day Basel stress test results")
print(f"  4. lcr_results - Liquidity Coverage Ratio calculations")
print(f"  5. bank_run_scenarios - Severe stress scenario results")
print(f"  6. supplementary_metrics - LDR and mark-to-market analysis")

print(f"\n" + "="*80)
print("CONCLUSION")
print("="*80)
print(f"The bank demonstrates STRONG liquidity position across all scenarios.")
print(f"Continue monitoring interest rate risk and deposit concentration.")
print("="*80)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

haircuts_data = [
    (1, "Cash + Fed reserves", "Central Bank Reserves", "Level 1", "0%", "0%", "§ 50.20(a)(1)"),
    (2, "Cash + Fed reserves", "Coins & Banknotes", "Level 1", "0%", "0%", "§ 50.20(a)(1)"),
    (3, "US Treasuries", "Bill, Note, Bond", "Level 1", "0%", "0%", "§ 50.20(a)(2)"),
    (4, "Agency MBS (AAA)", "GNMA (Gov Guaranteed)", "Level 1", "0%", "0%", "§ 50.20(a)(4)"),
    (5, "Agency MBS (AAA)", "FNMA / FHLMC (GSE)", "Level 2A", "15%", "15%", "§ 50.20(b)(1)"),
    (6, "Corporate bonds", "Rated AA- or Higher", "Level 2A", "15%", "15%", "§ 50.20(b)(2)"),
    (7, "Corporate bonds", "Rated A+ to BBB-", "Level 2B", "50%", "50%", "§ 50.20(c)(1)"),
    (8, "Municipal Bonds", "General Obligation (GO)", "Level 2A", "15%", "N/A", "US Specific Rule"),
    (9, "S&P 500 Equity", "Common Stock", "Level 2B", "50%", "50%", "§ 50.20(c)(2)")
]

haircuts_schema = StructType([
    StructField("id", LongType(), False),
    StructField("asset_category", StringType(), False),
    StructField("asset_sub_type", StringType(), False),
    StructField("hqla_level", StringType(), False),
    StructField("fed_haircut", StringType(), False),
    StructField("bcbs_haircut", StringType(), False),
    StructField("regulatory_logic", StringType(), False)
])

df_haircuts = spark.createDataFrame(haircuts_data, schema=haircuts_schema)

# Create the table
try:
    spark.sql("DESCRIBE TABLE prasanna.lcr.haircuts")
    print("⚠ Table prasanna.lcr.haircuts already exists. Skipping creation.")
except:
    df_haircuts.write.saveAsTable("prasanna.lcr.haircuts")
    print("✓ Table prasanna.lcr.haircuts created successfully.")

display(df_haircuts)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

runoff_rates_data = [
    (1, "Retail deposits (insured)", "Transactional / Stable", "3%", "5%", "+2.5%", "§ 50.32(a)(1)"),
    (2, "Retail deposits (uninsured)", "Less Stable / Wealth", "10%", "10%", "+2.5%", "§ 50.32(a)(2)"),
    (3, "Retail deposits (brokered)", "Swept / Third-party", "10% - 40%", "10% - 40%", "N/A", "§ 50.32(g)"),
    (4, "Corporate deposits", "Operational (Payroll)", "25%", "25%", "N/A", "§ 50.32(h)(1)"),
    (5, "Corporate deposits", "Non-Operational (Financial)", "100%", "100%", "N/A", "§ 50.32(h)(2)"),
    (6, "Corporate deposits", "Non-Operational (Non-Fin)", "40%", "40%", "N/A", "§ 50.32(h)(5)"),
    (7, "Wholesale funding", "Unsecured / Commercial Paper", "100%", "100%", "N/A", "§ 50.32(h)(2)"),
    (8, "Long-term debt", "Maturity > 30 Days", "0%", "0%", "N/A", "Excluded"),
    (9, "Short-term debt", "Maturity ≤ 30 Days", "100%", "100%", "N/A", "Full Outflow")
]

runoff_rates_schema = StructType([
    StructField("id", LongType(), False),
    StructField("liability_category", StringType(), False),
    StructField("customer_sub_type", StringType(), False),
    StructField("fed_runoff_rate", StringType(), False),
    StructField("bcbs_runoff_rate", StringType(), False),
    StructField("digital_surcharge_2026", StringType(), False),
    StructField("regulatory_logic", StringType(), False)
])

df_runoff_rates = spark.createDataFrame(runoff_rates_data, schema=runoff_rates_schema)

try:
    spark.sql("DESCRIBE TABLE prasanna.lcr.runoff_rates")
    print("⚠ Table prasanna.lcr.runoff_rates already exists. Skipping creation.")
except:
    df_runoff_rates.write.saveAsTable("prasanna.lcr.runoff_rates")
    print("✓ Table prasanna.lcr.runoff_rates created successfully.")

display(df_runoff_rates)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from prasanna.lcr.assets where 1=1

# COMMAND ----------

spark.sql("""
INSERT INTO prasanna.lcr.haircuts (id, asset_category, asset_sub_type, hqla_level, fed_haircut, bcbs_haircut, regulatory_logic) VALUES
(10, 'Loans (Mortgages)', 'Performing < 30d maturity', 'Non-HQLA', '100%', '100%', 'Treated as Inflow'),
(11, 'Loans (Business)', 'Performing < 30d maturity', 'Non-HQLA', '100%', '100%', 'Treated as Inflow')
""")

# COMMAND ----------

# DBTITLE 1,Fix: Drop column with correct spelling
# MAGIC %sql
# MAGIC ALTER TABLE prasanna.lcr.lcr_assets SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
# MAGIC
# MAGIC ALTER TABLE prasanna.lcr.lcr_assets
# MAGIC DROP COLUMN haircut_id;
# MAGIC
# MAGIC DESCRIBE prasanna.lcr.lcr_assets

# COMMAND ----------

# Create the runoff_rates table
spark.sql("""
CREATE TABLE IF NOT EXISTS prasanna.lcr.runoff_rates (
    id BIGINT,
    liability_category STRING,
    specific_component STRING,
    fed_runoff_rate STRING,
    regulatory_logic STRING
)
""")

# Insert rows into the runoff_rates table
spark.sql("""
INSERT INTO prasanna.lcr.runoff_rates (id, liability_category, specific_component, fed_runoff_rate, regulatory_logic) VALUES
(1, 'Retail Deposits', 'Stable (Insured + Relationship)', '3%', 'Standard for "sticky" core deposits.'),
(2, 'Retail Deposits', 'Less Stable (Uninsured / Digital)', '13%', 'Base 10% + 2.5% Digital Surcharge for 2026.'),
(3, 'Retail Deposits', 'Fully Insured (GL: $2231.1M)', '3%', 'Matches your GL item for Insured Retail.'),
(4, 'Retail Deposits', 'Uninsured (GL: $1033.6M)', '10%', 'Standard Reg WW rate for uninsured retail.'),
(5, 'Corporate Deposits', 'Operational (Payroll / GL: $71.1M)', '25%', 'Cash used for daily business operations.'),
(6, 'Corporate Deposits', 'Non-Operational (Financial / GL: $109.3M)', '100%', 'Assumes financial firms pull 100% in stress.'),
(7, 'Corporate Deposits', 'Non-Operational (Non-Fin / GL: $22.4M)', '40%', 'Higher risk than operational but not "hot" money.'),
(8, 'Wholesale Funding', 'Unsecured / Comm. Paper (GL: $85.4M)', '100%', 'Short-term market funding that "freezes."'),
(9, 'Short-term Debt', 'Maturity ≤ 30 Days (GL: $92.1M)', '100%', 'Any debt due within the LCR window.'),
(10, 'Long-term Debt', 'Maturity > 30 Days (GL: $165.8M)', '0%', 'No cash leaves the bank within 30 days.'),
(11, 'Equity', "Shareholders' Equity", '0%', 'Equity is permanent capital; it never "runs off."'),
(12, 'Off-Balance Sheet', 'Committed Liquidity Facilities', '30% - 100%', '30% for Non-Fin Corps; 100% for Banks.')
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from prasanna.lcr.runoff_rates where 1=1

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from datetime import datetime

runoff_rates_data = [
#    ("Corporate deposits Non-Operational (Financial)", 108.9, "25%", 27.2, 101, "Liquidity Stress Test", datetime(2026, 2, 1, 9, 0, 0)),
#    ("Long-term debt Maturity > 30 Days", 168.6, "0%", 0.0, 101, "Liquidity Stress Test", datetime(2026, 2, 1, 9, 0, 0))
]

runoff_rates_schema = StructType([
    StructField("liability", StringType(), False),
    StructField("amount_millions", DoubleType(), False),
    StructField("runoff_percent", DoubleType(), False),
    StructField("cash_outflow_millions", DoubleType(), False),
    StructField("project_id", StringType(), False),
    StructField("project_name", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])

df_runoff_rates = spark.createDataFrame(runoff_rates_data, schema=runoff_rates_schema)

df_runoff_rates.write.saveAsTable("prasanna.lcr.lcr_runoff_rates")

display(df_runoff_rates)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from prasanna.lcr.assets where 1=1;
# MAGIC delete from prasanna.lcr.liabilities where 1=1;

# COMMAND ----------

