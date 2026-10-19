# Databricks notebook source
# MAGIC %md
# MAGIC # LCR Lakeflow Declarative Pipeline (raw to governed gold)
# MAGIC
# MAGIC This is the ingestion and transformation leg of the LCR solution, built as a
# MAGIC Lakeflow Declarative Pipeline. It carries the raw general ledger, HQLA haircut, and
# MAGIC runoff-rate feeds from the landing volume through a bronze / silver / gold medallion
# MAGIC into a governed Unity Catalog schema, applying Basel III haircut and runoff mechanics
# MAGIC in the gold layer.
# MAGIC
# MAGIC **Isolation:** the pipeline target is the parallel schema `cfo.aura_bank_lakeflow`
# MAGIC (set in the pipeline settings, see `resources/lcr_lakeflow_pipeline.json`). It reads
# MAGIC raw files landed by `lcr_lakeflow_seed`; it does not touch the live `cfo.aura_bank`
# MAGIC tables the app serves.
# MAGIC
# MAGIC **Data quality, correct by construction:** the accounting identity
# MAGIC `Assets = Liabilities + Equity` is enforced here as a declarative expectation. GL
# MAGIC snapshots that do not balance are quarantined for review instead of being silenced
# MAGIC with a manually inserted plug entry.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    LongType,
)

LANDING_ROOT = spark.conf.get(
    "lcr.landing_root", "/Volumes/cfo/aura_bank/lcr_raw_landing"
)

# Explicit schemas keep Auto Loader deterministic for these well known feeds.
GL_SCHEMA = StructType(
    [
        StructField("gl_account", StringType(), True),
        StructField("account_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("hqla_type", StringType(), True),
        StructField("runoff", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("net_balance_m", DoubleType(), True),
    ]
)
HAIRCUTS_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("asset_category", StringType(), True),
        StructField("asset_sub_type", StringType(), True),
        StructField("hqla_level", StringType(), True),
        StructField("fed_haircut", StringType(), True),
        StructField("bcbs_haircut", StringType(), True),
        StructField("regulatory_logic", StringType(), True),
    ]
)
RUNOFF_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("liability_category", StringType(), True),
        StructField("specific_component", StringType(), True),
        StructField("fed_runoff_rate", StringType(), True),
        StructField("regulatory_logic", StringType(), True),
    ]
)


def _pct_to_frac(col):
    """'15%' -> 0.15 ; 'N/A' / null -> 0.0 . Used for haircut and runoff strings."""
    digits = F.regexp_extract(F.coalesce(col, F.lit("")), r"(\d+(?:\.\d+)?)", 1)
    return F.when(digits == "", F.lit(0.0)).otherwise(digits.cast("double") / 100.0)


def _auto_load(feed: str, schema: StructType):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(schema)
        .load(f"{LANDING_ROOT}/{feed}")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw ingest with Auto Loader

# COMMAND ----------


@dlt.table(comment="Raw general ledger feed, ingested as landed.")
def bronze_general_ledger():
    return _auto_load("general_ledger", GL_SCHEMA).withColumn(
        "_ingested_at", F.current_timestamp()
    )


@dlt.table(comment="Raw HQLA haircut reference feed.")
def bronze_haircuts():
    return _auto_load("haircuts", HAIRCUTS_SCHEMA).withColumn(
        "_ingested_at", F.current_timestamp()
    )


@dlt.table(comment="Raw liability runoff-rate reference feed.")
def bronze_runoff_rates():
    return _auto_load("runoff_rates", RUNOFF_SCHEMA).withColumn(
        "_ingested_at", F.current_timestamp()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: typed, validated, with expectations

# COMMAND ----------


@dlt.table(comment="Cleaned general ledger with a parsed runoff fraction.")
@dlt.expect_or_drop("valid_category", "category IN ('Asset','Liability','Equity')")
@dlt.expect("has_balance", "net_balance_m IS NOT NULL")
@dlt.expect("has_timestamp", "timestamp IS NOT NULL")
def silver_general_ledger():
    return (
        dlt.read("bronze_general_ledger")
        .withColumn("runoff_frac", _pct_to_frac(F.col("runoff")))
        .withColumn("as_of_date", F.to_date("timestamp"))
    )


@dlt.table(comment="Cleaned haircut reference with a parsed haircut fraction.")
@dlt.expect_or_drop(
    "valid_level", "hqla_level IN ('Level 1','Level 2A','Level 2B','Non-HQLA')"
)
def silver_haircuts():
    return dlt.read("bronze_haircuts").withColumn(
        "haircut_frac", _pct_to_frac(F.col("fed_haircut"))
    )


@dlt.table(comment="Cleaned runoff reference with a parsed runoff fraction.")
@dlt.expect("runoff_in_range", "runoff_frac >= 0 AND runoff_frac <= 1")
def silver_runoff_rates():
    return dlt.read("bronze_runoff_rates").withColumn(
        "runoff_frac", _pct_to_frac(F.col("fed_runoff_rate"))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Governance: accounting identity enforced declaratively
# MAGIC
# MAGIC Per GL timestamp, `Assets + Liabilities + Equity` must net to zero (liabilities and
# MAGIC equity are stored as credits / negatives). Balanced snapshots flow to gold; any
# MAGIC imbalanced snapshot is dropped from the gold path and captured in a quarantine table
# MAGIC for review. No plug entry is fabricated to force a balance.

# COMMAND ----------


@dlt.table(comment="Balance status per GL snapshot (identity: assets = liabilities + equity).")
def gold_gl_balance_check():
    return (
        dlt.read("silver_general_ledger")
        .groupBy("timestamp", "as_of_date")
        .agg(F.round(F.sum("net_balance_m"), 2).alias("net_balance_m"))
        .withColumn("is_balanced", F.abs(F.col("net_balance_m")) < F.lit(0.01))
    )


@dlt.table(comment="Imbalanced GL snapshots held for review rather than plugged.")
@dlt.expect_all({"flagged_for_review": "is_balanced = false"})
def quarantine_unbalanced_gl():
    return dlt.read("gold_gl_balance_check").where("is_balanced = false")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: Basel III weighted HQLA, net outflows, and LCR

# COMMAND ----------


@dlt.table(comment="Weighted HQLA by asset after Basel III haircuts, balanced snapshots only.")
@dlt.expect("non_negative_hqla", "weighted_hqla_m >= 0")
def gold_hqla_breakdown():
    gl = dlt.read("silver_general_ledger").where("category = 'Asset'")
    balanced = dlt.read("gold_gl_balance_check").where("is_balanced = true").select(
        "timestamp"
    )
    haircuts = dlt.read("silver_haircuts").select(
        F.col("hqla_level"), F.col("haircut_frac")
    )
    return (
        gl.join(balanced, "timestamp")
        .join(haircuts, gl["hqla_type"] == haircuts["hqla_level"], "left")
        .withColumn(
            "weighted_hqla_m",
            F.col("net_balance_m") * (F.lit(1.0) - F.coalesce(F.col("haircut_frac"), F.lit(1.0))),
        )
        .select(
            "as_of_date",
            "timestamp",
            "gl_account",
            "account_name",
            F.col("hqla_type").alias("hqla_level"),
            "net_balance_m",
            "haircut_frac",
            "weighted_hqla_m",
        )
    )


@dlt.table(comment="Weighted cash outflows by liability after runoff rates, balanced snapshots only.")
@dlt.expect("non_negative_outflow", "weighted_outflow_m >= 0")
def gold_cash_outflows_breakdown():
    gl = dlt.read("silver_general_ledger").where("category = 'Liability'")
    balanced = dlt.read("gold_gl_balance_check").where("is_balanced = true").select(
        "timestamp"
    )
    return (
        gl.join(balanced, "timestamp")
        .withColumn("weighted_outflow_m", F.abs(F.col("net_balance_m")) * F.col("runoff_frac"))
        .select(
            "as_of_date",
            "timestamp",
            "gl_account",
            "account_name",
            F.col("runoff").alias("runoff_rate"),
            "runoff_frac",
            "net_balance_m",
            "weighted_outflow_m",
        )
    )


@dlt.table(comment="Governed LCR result per snapshot: total HQLA / net cash outflows.")
@dlt.expect_or_drop("positive_outflow", "net_outflow_m > 0")
@dlt.expect("lcr_sane", "lcr_ratio_pct > 0")
def gold_lcr_results():
    hqla = (
        dlt.read("gold_hqla_breakdown")
        .groupBy("as_of_date", "timestamp")
        .agg(F.round(F.sum("weighted_hqla_m"), 2).alias("total_hqla_m"))
    )
    outflow = (
        dlt.read("gold_cash_outflows_breakdown")
        .groupBy("as_of_date", "timestamp")
        .agg(F.round(F.sum("weighted_outflow_m"), 2).alias("net_outflow_m"))
    )
    return (
        hqla.join(outflow, ["as_of_date", "timestamp"])
        .withColumn(
            "lcr_ratio_pct",
            F.round(F.col("total_hqla_m") / F.col("net_outflow_m") * 100.0, 2),
        )
        .withColumn("is_compliant", F.col("lcr_ratio_pct") >= F.lit(100.0))
        .withColumn("_computed_at", F.current_timestamp())
    )
