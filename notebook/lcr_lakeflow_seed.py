# Databricks notebook source
# MAGIC %md
# MAGIC # LCR Lakeflow landing seed (one time)
# MAGIC
# MAGIC Writes the raw source feeds (general ledger, HQLA haircuts, runoff rates) out to a
# MAGIC Unity Catalog Volume so the Lakeflow Declarative Pipeline (`lcr_lakeflow_pipeline`)
# MAGIC can ingest them with Auto Loader, exactly as it would ingest files landed by an
# MAGIC upstream system or Lakeflow Connect.
# MAGIC
# MAGIC This is a helper, not part of the running app or the live `cfo.aura_bank` flow. It
# MAGIC only **reads** the existing governed tables and **writes raw files** into a new,
# MAGIC isolated landing volume. Nothing the app queries is modified.

# COMMAND ----------

# DBTITLE 1,Config
# Source (read only) and the isolated landing zone we write raw files to.
SOURCE_CATALOG = "cfo"
SOURCE_SCHEMA = "aura_bank"
LANDING_VOLUME = "lcr_raw_landing"  # cfo.aura_bank.lcr_raw_landing

LANDING_ROOT = f"/Volumes/{SOURCE_CATALOG}/{SOURCE_SCHEMA}/{LANDING_VOLUME}"

# Raw feed name -> source table. These are the raw inputs a bank would land from its
# core banking / treasury systems before any governance is applied.
RAW_FEEDS = {
    "general_ledger": "general_ledger",
    "haircuts": "haircuts",
    "runoff_rates": "runoff_rates",
}

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {SOURCE_CATALOG}.{SOURCE_SCHEMA}.{LANDING_VOLUME}"
)
print(f"Landing volume ready: {LANDING_ROOT}")

# COMMAND ----------

# DBTITLE 1,Export each source table to raw CSV in its own landing folder
from datetime import datetime

batch = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

for feed, table in RAW_FEEDS.items():
    src = f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.{table}"
    dest_dir = f"{LANDING_ROOT}/{feed}"
    # One CSV batch per feed. Auto Loader picks up new files on each pipeline run.
    (
        spark.table(src)
        .coalesce(1)
        .write.mode("append")
        .option("header", "true")
        .csv(f"{dest_dir}/_staging_{batch}")
    )
    # Flatten the Spark part file into a single stable file name per batch.
    staged = f"{dest_dir}/_staging_{batch}"
    part = [f.path for f in dbutils.fs.ls(staged) if f.path.endswith(".csv")][0]
    dbutils.fs.mv(part, f"{dest_dir}/{feed}_{batch}.csv")
    dbutils.fs.rm(staged, recurse=True)
    print(f"Landed {src} -> {dest_dir}/{feed}_{batch}.csv")

print("\nRaw landing seed complete. Run the lcr_lakeflow_pipeline pipeline next.")
