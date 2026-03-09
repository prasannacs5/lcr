# Databricks notebook source
# DBTITLE 1,Configuration
# Set these widget values when running the notebook, or accept the defaults.
dbutils.widgets.text("genie_room_id", "", "Genie Room ID (required)")
dbutils.widgets.text("secret_scope", "lcr-agent", "Secret scope name")
dbutils.widgets.text("llm_endpoint_name", "databricks-claude-3-7-sonnet", "LLM endpoint name")
dbutils.widgets.text("endpoint_name", "lcr-agent-endpoint-v4", "Serving endpoint name")
dbutils.widgets.text("catalog", "cfo", "Unity Catalog name")
dbutils.widgets.text("schema", "aura_bank", "Schema name")
dbutils.widgets.text("model_name", "lcr_model", "Model name in UC")

GENIE_ROOM_ID = dbutils.widgets.get("genie_room_id")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
LLM_ENDPOINT_NAME = dbutils.widgets.get("llm_endpoint_name")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODEL_NAME = dbutils.widgets.get("model_name")
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{MODEL_NAME}"

assert GENIE_ROOM_ID, "genie_room_id widget is required"

print(f"Config: model={UC_MODEL_NAME}, endpoint={ENDPOINT_NAME}, genie_room={GENIE_ROOM_ID}")

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install -U -q databricks-openai databricks-agents uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Log Model to Unity Catalog
import mlflow
import shutil
import sys
import os

# Re-read widgets after Python restart
GENIE_ROOM_ID = dbutils.widgets.get("genie_room_id")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
LLM_ENDPOINT_NAME = dbutils.widgets.get("llm_endpoint_name")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODEL_NAME = dbutils.widgets.get("model_name")
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{MODEL_NAME}"

# Find agent.py relative to this notebook
notebook_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
agent_workspace_path = f"/Workspace{notebook_dir}/agent.py"
shutil.copy(agent_workspace_path, '/tmp/agent.py')

if '/tmp' not in sys.path:
    sys.path.insert(0, '/tmp')

# Set env vars so agent.py can initialize without real MCP connections at import time
os.environ["LCR_GENIE_ROOM_ID"] = GENIE_ROOM_ID
os.environ["LCR_LLM_ENDPOINT_NAME"] = LLM_ENDPOINT_NAME

# Force fresh import
if 'agent' in sys.modules:
    del sys.modules['agent']

from agent import AGENT

# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

print(f"Logging model to Unity Catalog: {UC_MODEL_NAME}")

with mlflow.start_run(run_name="lcr_agent_model") as run:
    model_info = mlflow.pyfunc.log_model(
        python_model="/tmp/agent.py",
        artifact_path="agent",
        registered_model_name=UC_MODEL_NAME,
    )
    mlflow.set_tag("model_type", "mcp_agent")
    mlflow.set_tag("use_case", "liquidity_coverage_ratio")

    print(f"Model logged successfully!")
    print(f"   Model URI: {model_info.model_uri}")
    print(f"   Run ID: {run.info.run_id}")
    print(f"   Version: {model_info.registered_model_version}")

# COMMAND ----------

# DBTITLE 1,Deploy Model to Serving Endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

print(f"Deploying model to endpoint: {ENDPOINT_NAME}")
print(f"Model: {UC_MODEL_NAME} v{model_info.registered_model_version}")

# Environment vars for the serving endpoint — all config is externalized
env_vars = {
    "LCR_GENIE_ROOM_ID": GENIE_ROOM_ID,
    "LCR_LLM_ENDPOINT_NAME": LLM_ENDPOINT_NAME,
}

# If a secret scope exists with SP credentials, reference them
try:
    dbutils.secrets.get(SECRET_SCOPE, "databricks_host")
    env_vars["DATABRICKS_HOST"] = "{{secrets/" + SECRET_SCOPE + "/databricks_host}}"
    env_vars["DATABRICKS_CLIENT_ID"] = "{{secrets/" + SECRET_SCOPE + "/client_id}}"
    env_vars["DATABRICKS_CLIENT_SECRET"] = "{{secrets/" + SECRET_SCOPE + "/client_secret}}"
    print(f"Using credentials from secret scope: {SECRET_SCOPE}")
except Exception:
    print(f"Secret scope '{SECRET_SCOPE}' not found or empty — endpoint will use default auth")

served_entity = ServedEntityInput(
    entity_name=UC_MODEL_NAME,
    entity_version=str(model_info.registered_model_version),
    scale_to_zero_enabled=True,
    workload_size="Small",
    environment_vars=env_vars,
)

try:
    endpoint = w.serving_endpoints.create_and_wait(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            name=ENDPOINT_NAME,
            served_entities=[served_entity],
        ),
    )
    print(f"\nModel deployed successfully!")
    print(f"   Endpoint: {ENDPOINT_NAME}")

except Exception as e:
    if "already exists" in str(e):
        print(f"\nEndpoint '{ENDPOINT_NAME}' already exists. Updating...")
        endpoint = w.serving_endpoints.update_config_and_wait(
            name=ENDPOINT_NAME,
            served_entities=[served_entity],
        )
        print(f"\nEndpoint updated successfully!")
    else:
        print(f"\nDeployment failed: {str(e)}")
        raise e
