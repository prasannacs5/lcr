# Databricks notebook source
# MAGIC %md
# MAGIC # LCR Lakebase operational serving layer
# MAGIC
# MAGIC Stands up a Lakebase (managed Postgres) instance and syncs the governed gold LCR
# MAGIC results into it, giving the application an operational, low-latency store to read
# MAGIC from instead of issuing a SQL-warehouse query on every request.
# MAGIC
# MAGIC **Isolation:** this creates a new instance (`lcr-lakebase`) and a synced Postgres
# MAGIC table. It reads the governed gold table read-only. The running app continues to use
# MAGIC the SQL warehouse unless `LCR_USE_LAKEBASE=true` is set on the app (default off), so
# MAGIC nothing changes for the live deployment until you opt in.
# MAGIC
# MAGIC The app-side read path lives in `backend/lib/lakebase_client.py`.

# COMMAND ----------

# DBTITLE 1,Config
INSTANCE_NAME = "lcr-lakebase"
CAPACITY = "CU_1"
PG_DATABASE = "databricks_postgres"  # default logical database on a Lakebase instance

# Source gold table to serve. Point at the live gold table, or the Lakeflow gold
# (cfo.aura_bank_lakeflow.gold_lcr_results) once that pipeline has run.
SOURCE_TABLE = "cfo.aura_bank.lcr_results"
SYNCED_TABLE = "cfo.aura_bank.lcr_results_synced"  # UC-registered synced table
PG_SCHEMA = "public"
PG_TABLE = "lcr_results_latest"

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Create the Lakebase instance (idempotent)
from databricks.sdk.service.database import DatabaseInstance

existing = {i.name for i in w.database.list_database_instances()}
if INSTANCE_NAME in existing:
    print(f"Instance {INSTANCE_NAME} already exists")
else:
    w.database.create_database_instance(
        DatabaseInstance(name=INSTANCE_NAME, capacity=CAPACITY)
    )
    print(f"Creating instance {INSTANCE_NAME} ...")

# Wait until AVAILABLE
import time

for _ in range(60):
    inst = w.database.get_database_instance(name=INSTANCE_NAME)
    if inst.state and str(inst.state).endswith("AVAILABLE"):
        break
    time.sleep(10)

inst = w.database.get_database_instance(name=INSTANCE_NAME)
print(f"State: {inst.state}")
print(f"Read/write DNS: {inst.read_write_dns}")

# COMMAND ----------

# DBTITLE 1,Sync the governed gold LCR results into Lakebase
# A synced table keeps the Postgres copy current from the Delta gold table. SNAPSHOT
# refreshes on demand; switch to CONTINUOUS for always-fresh intraday serving.
from databricks.sdk.service.database import (
    SyncedDatabaseTable,
    SyncedTableSpec,
    NewPipelineSpec,
    SyncedTableSchedulingPolicy,
)

try:
    w.database.create_synced_database_table(
        SyncedDatabaseTable(
            name=SYNCED_TABLE,
            database_instance_name=INSTANCE_NAME,
            logical_database_name=PG_DATABASE,
            spec=SyncedTableSpec(
                source_table_full_name=SOURCE_TABLE,
                primary_key_columns=["snapshot_id"],
                scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog="cfo", storage_schema="aura_bank_lakeflow"
                ),
            ),
        )
    )
    print(f"Synced table {SYNCED_TABLE} created from {SOURCE_TABLE}")
except Exception as e:
    # Re-run safe: a second create raises ALREADY_EXISTS.
    print(f"Synced table create skipped: {e}")

# COMMAND ----------

# DBTITLE 1,Low-latency read: the application-facing access pattern
# This is exactly what the app does when LCR_USE_LAKEBASE=true. We generate a short-lived
# Postgres credential via the SDK and run a point query over the operational store.
import psycopg
import time

cred = w.database.generate_database_credential(
    request_id=str(time.time()), instance_names=[INSTANCE_NAME]
)
conn = psycopg.connect(
    host=inst.read_write_dns,
    port=5432,
    dbname=PG_DATABASE,
    user=w.current_user.me().user_name,
    password=cred.token,
    sslmode="require",
)

t0 = time.time()
with conn.cursor() as cur:
    cur.execute(
        f"SELECT * FROM {PG_SCHEMA}.{PG_TABLE} ORDER BY as_of_date DESC LIMIT 5"
    )
    rows = cur.fetchall()
elapsed_ms = (time.time() - t0) * 1000
conn.close()

print(f"Lakebase point query returned {len(rows)} rows in {elapsed_ms:.1f} ms")
for r in rows:
    print(r)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wiring the app to Lakebase
# MAGIC
# MAGIC Set these on the app (in `app.yaml` env), then flip the flag when ready:
# MAGIC
# MAGIC ```
# MAGIC LCR_USE_LAKEBASE=true
# MAGIC LCR_LAKEBASE_INSTANCE=lcr-lakebase
# MAGIC LCR_LAKEBASE_HOST=<read_write_dns from above>
# MAGIC LCR_LAKEBASE_DATABASE=databricks_postgres
# MAGIC LCR_LAKEBASE_TABLE=public.lcr_results_latest
# MAGIC ```
# MAGIC
# MAGIC With the flag unset or false, the app reads the SQL warehouse exactly as before.
