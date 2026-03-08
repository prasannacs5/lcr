# Databricks notebook source
# DBTITLE 1,Install Dependencies
# MAGIC %pip install -U -q databricks-openai databricks-agents uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,LCR Agent Implementation
# MAGIC %%writefile agent.py
# MAGIC import asyncio
# MAGIC import mlflow
# MAGIC import nest_asyncio
# MAGIC import os
# MAGIC import json
# MAGIC import copy
# MAGIC
# MAGIC from agents import Agent, set_default_openai_api, set_default_openai_client, Runner
# MAGIC from agents.mcp import MCPServerStreamableHttpParams
# MAGIC from agents.tracing import set_trace_processors
# MAGIC from agents.result import StreamEvent
# MAGIC from databricks_openai.agents.mcp_server import McpServer
# MAGIC from databricks_openai import AsyncDatabricksOpenAI
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from mlflow.pyfunc import ResponsesAgent
# MAGIC from mlflow.types.responses import (
# MAGIC     ResponsesAgentRequest,
# MAGIC     ResponsesAgentResponse,
# MAGIC     ResponsesAgentStreamEvent,
# MAGIC )
# MAGIC from typing import Generator, AsyncIterator, AsyncGenerator, List, Dict, Any
# MAGIC from uuid import uuid4
# MAGIC
# MAGIC nest_asyncio.apply()
# MAGIC
# MAGIC # ---------------------------------------------------------------------------
# MAGIC # Configuration
# MAGIC # ---------------------------------------------------------------------------
# MAGIC set_default_openai_client(AsyncDatabricksOpenAI())
# MAGIC set_default_openai_api("chat_completions")
# MAGIC set_trace_processors([])
# MAGIC
# MAGIC LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"
# MAGIC SYSTEM_PROMPT = "You are a helpful financial assistant specialized in Liquidity Coverage Ratio (LCR) analysis and regulatory compliance. You can analyze liquidity positions, calculate LCR metrics, and provide insights on regulatory requirements."
# MAGIC
# MAGIC # Auth Setup - Module level initialization with proper fallback
# MAGIC try:
# MAGIC     workspace_client = WorkspaceClient()
# MAGIC     host = workspace_client.config.host
# MAGIC except Exception:
# MAGIC     # In serving environment, WorkspaceClient() may fail
# MAGIC     # We'll initialize it later with explicit credentials
# MAGIC     workspace_client = None
# MAGIC     host = os.getenv("DATABRICKS_HOST", "https://e2-demo-field-eng.cloud.databricks.com")
# MAGIC
# MAGIC if os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET"):
# MAGIC     oauth_workspace_client = WorkspaceClient(
# MAGIC         host=host,
# MAGIC         client_id=os.getenv("DATABRICKS_CLIENT_ID"),
# MAGIC         client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
# MAGIC         auth_type="oauth-m2m",
# MAGIC     )
# MAGIC else:
# MAGIC     # Fallback for dev/testing
# MAGIC     oauth_workspace_client = WorkspaceClient(
# MAGIC         host=host,
# MAGIC         client_id='7b50dc4d-f302-4afb-8539-59085ae60f74',
# MAGIC         client_secret='<YOUR_CLIENT_SECRET>',
# MAGIC         auth_type="oauth-m2m",
# MAGIC     )
# MAGIC
# MAGIC class MCPToolCallingAgent(ResponsesAgent):
# MAGIC     
# MAGIC     def _sanitize_history(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
# MAGIC         """
# MAGIC         Sanitizes message history to ensure proper Claude message format:
# MAGIC         - Removes duplicate tool call IDs
# MAGIC         - Merges consecutive assistant messages (ONLY if they don't have tool_calls AND no tool execution between them)
# MAGIC         - Ensures tool calls are followed by tool results
# MAGIC         """
# MAGIC         if not messages:
# MAGIC             return messages
# MAGIC             
# MAGIC         clean_history = []
# MAGIC         seen_ids = set()
# MAGIC         last_was_tool_execution = False
# MAGIC         
# MAGIC         i = 0
# MAGIC         while i < len(messages):
# MAGIC             msg = copy.deepcopy(messages[i])
# MAGIC             msg_type = msg.get("type", "")
# MAGIC             msg_role = msg.get("role", "")
# MAGIC             
# MAGIC             # Skip messages with fake IDs at the message level
# MAGIC             if msg.get("id") == "__fake_id__":
# MAGIC                 i += 1
# MAGIC                 continue
# MAGIC             
# MAGIC             # Track tool execution messages
# MAGIC             if msg_type in ["function_call", "function_call_output"]:
# MAGIC                 last_was_tool_execution = True
# MAGIC                 i += 1
# MAGIC                 continue
# MAGIC             
# MAGIC             # Handle tool_calls in the message
# MAGIC             if "tool_calls" in msg and isinstance(msg["tool_calls"], list):
# MAGIC                 valid_tools = []
# MAGIC                 for tc in msg["tool_calls"]:
# MAGIC                     tid = tc.get("id")
# MAGIC                     if tid and tid != "__fake_id__" and tid not in seen_ids:
# MAGIC                         seen_ids.add(tid)
# MAGIC                         valid_tools.append(tc)
# MAGIC                 msg["tool_calls"] = valid_tools
# MAGIC                 
# MAGIC                 if not msg["tool_calls"]:
# MAGIC                     msg.pop("tool_calls", None)
# MAGIC                 else:
# MAGIC                     last_was_tool_execution = True
# MAGIC             
# MAGIC             # Handle content blocks
# MAGIC             content = msg.get("content")
# MAGIC             if isinstance(content, list):
# MAGIC                 valid_content = []
# MAGIC                 for block in content:
# MAGIC                     if isinstance(block, dict):
# MAGIC                         bid = block.get("id")
# MAGIC                         if bid:
# MAGIC                             if bid == "__fake_id__" or bid in seen_ids:
# MAGIC                                 continue
# MAGIC                             seen_ids.add(bid)
# MAGIC                     valid_content.append(block)
# MAGIC                 msg["content"] = valid_content
# MAGIC                 
# MAGIC                 if not msg["content"]:
# MAGIC                     i += 1
# MAGIC                     continue
# MAGIC             
# MAGIC             # Handle tool results
# MAGIC             call_id = msg.get("call_id") or msg.get("tool_call_id")
# MAGIC             if call_id:
# MAGIC                 if call_id == "__fake_id__" or call_id in seen_ids:
# MAGIC                     i += 1
# MAGIC                     continue
# MAGIC                 seen_ids.add(call_id)
# MAGIC             
# MAGIC             # Merge consecutive assistant messages
# MAGIC             if (clean_history and 
# MAGIC                 msg_role == "assistant" and 
# MAGIC                 clean_history[-1].get("role") == "assistant" and
# MAGIC                 not msg.get("tool_calls") and
# MAGIC                 not clean_history[-1].get("tool_calls") and
# MAGIC                 not last_was_tool_execution):
# MAGIC                 
# MAGIC                 last_msg = clean_history[-1]
# MAGIC                 
# MAGIC                 if isinstance(msg.get("content"), str) and isinstance(last_msg.get("content"), str):
# MAGIC                     if msg["content"] != last_msg["content"]:
# MAGIC                         last_msg["content"] = last_msg["content"] + "\n" + msg["content"]
# MAGIC                 elif isinstance(msg.get("content"), list):
# MAGIC                     if not isinstance(last_msg.get("content"), list):
# MAGIC                         last_msg["content"] = [last_msg["content"]] if last_msg.get("content") else []
# MAGIC                     last_msg["content"].extend(msg["content"])
# MAGIC             else:
# MAGIC                 clean_history.append(msg)
# MAGIC                 if msg_role == "assistant":
# MAGIC                     last_was_tool_execution = False
# MAGIC             
# MAGIC             i += 1
# MAGIC         
# MAGIC         return clean_history
# MAGIC
# MAGIC     async def process_agent_stream_events(
# MAGIC         self, async_stream: AsyncIterator[StreamEvent]
# MAGIC     ) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
# MAGIC         curr_item_id = str(uuid4())
# MAGIC         
# MAGIC         async for event in async_stream:
# MAGIC             # Handle Standard OpenAI Events
# MAGIC             if event.type == "raw_response_event":
# MAGIC                 event_data = event.data.model_dump()
# MAGIC                 if (
# MAGIC                     event_data["type"] == "response.output_item.done"
# MAGIC                     and event_data.get("item") is not None
# MAGIC                     and event_data["item"].get("id") is not None
# MAGIC                 ):
# MAGIC                     if event_data["item"].get("status") is None:
# MAGIC                         event_data["item"].pop("status", None)
# MAGIC                     
# MAGIC                     if isinstance(event_data["item"].get("output"), list):
# MAGIC                         output_list = event_data["item"]["output"]
# MAGIC                         if output_list and isinstance(output_list[0], dict) and "text" in output_list[0]:
# MAGIC                             event_data["item"]["output"] = output_list[0]["text"]
# MAGIC                         else:
# MAGIC                             event_data["item"]["output"] = str(output_list)
# MAGIC                     
# MAGIC                     event_data["item"]["id"] = curr_item_id
# MAGIC                     event_data.pop("output_index", None)
# MAGIC                     event_data.pop("sequence_number", None)
# MAGIC                     event_data.pop("id", None)
# MAGIC                     yield ResponsesAgentStreamEvent(**event_data)
# MAGIC                 
# MAGIC                 elif (
# MAGIC                     event_data["type"] == "response.output_text.delta"
# MAGIC                     and event_data.get("item_id") is not None
# MAGIC                 ):
# MAGIC                     event_data["item_id"] = curr_item_id
# MAGIC                     yield ResponsesAgentStreamEvent(**event_data)
# MAGIC
# MAGIC             # Handle Runner Events (Tool Calls AND Results)
# MAGIC             elif event.type == "run_item_stream_event":
# MAGIC                 # Handle tool call initiation
# MAGIC                 if event.item.type == "tool_call_item":
# MAGIC                     # Extract tool information
# MAGIC                     tool_name = getattr(event.item, 'name', 'unknown_tool')
# MAGIC                     tool_args = getattr(event.item, 'arguments', {})
# MAGIC                     
# MAGIC                     # Format tool call message with name and details
# MAGIC                     if isinstance(tool_args, str):
# MAGIC                         try:
# MAGIC                             tool_args = json.loads(tool_args)
# MAGIC                         except:
# MAGIC                             pass
# MAGIC                     
# MAGIC                     # Create a descriptive message
# MAGIC                     tool_message = f"🔧 Calling tool: {tool_name}"
# MAGIC                     if tool_args and isinstance(tool_args, dict):
# MAGIC                         # Include key parameters (limit to avoid too much detail)
# MAGIC                         param_summary = ", ".join([f"{k}={v}" for k, v in list(tool_args.items())[:3]])
# MAGIC                         if len(tool_args) > 3:
# MAGIC                             param_summary += "..."
# MAGIC                         tool_message += f" ({param_summary})"
# MAGIC                     
# MAGIC                     # Yield tool call message
# MAGIC                     yield ResponsesAgentStreamEvent(
# MAGIC                         type="response.output_text.delta",
# MAGIC                         item_id=curr_item_id,
# MAGIC                         delta=tool_message,
# MAGIC                     )
# MAGIC                     
# MAGIC                     # Immediately yield "Tool executing" message
# MAGIC                     yield ResponsesAgentStreamEvent(
# MAGIC                         type="response.output_text.delta",
# MAGIC                         item_id=curr_item_id,
# MAGIC                         delta=" ⏳ Tool executing...",
# MAGIC                     )
# MAGIC                 
# MAGIC                 # Handle tool results
# MAGIC                 elif event.item.type == "tool_call_output_item":
# MAGIC                     item_dict = event.item.to_input_item()
# MAGIC                     
# MAGIC                     if isinstance(item_dict.get("output"), list):
# MAGIC                         output_list = item_dict["output"]
# MAGIC                         if output_list and isinstance(output_list[0], dict) and "text" in output_list[0]:
# MAGIC                             item_dict["output"] = output_list[0]["text"]
# MAGIC                         else:
# MAGIC                             item_dict["output"] = str(output_list)
# MAGIC                     
# MAGIC                     # Send a completion message
# MAGIC                     yield ResponsesAgentStreamEvent(
# MAGIC                         type="response.output_text.delta",
# MAGIC                         item_id=curr_item_id,
# MAGIC                         delta=" ✅ Tool execution completed",
# MAGIC                     )
# MAGIC                     
# MAGIC                     # Then yield the actual result
# MAGIC                     yield ResponsesAgentStreamEvent(
# MAGIC                         type="response.output_item.done",
# MAGIC                         item=item_dict,
# MAGIC                     )
# MAGIC
# MAGIC     async def _predict_stream_async(
# MAGIC         self, request: ResponsesAgentRequest
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         
# MAGIC         async with (
# MAGIC             McpServer(
# MAGIC                 name="liquidity_coverage_ratio",
# MAGIC                 params=MCPServerStreamableHttpParams(url=f"{host}/api/2.0/mcp/genie/01f10d4664e216f2936c2d8b8c2e1bdb"),
# MAGIC                 workspace_client=oauth_workspace_client,
# MAGIC             ) as lcr_genie,
# MAGIC         ):
# MAGIC             agent = Agent(
# MAGIC                 name="lcr_agent",
# MAGIC                 instructions=SYSTEM_PROMPT,
# MAGIC                 model=LLM_ENDPOINT_NAME,
# MAGIC                 mcp_servers=[
# MAGIC                     lcr_genie,
# MAGIC                 ],
# MAGIC             )
# MAGIC             
# MAGIC             # Get raw input
# MAGIC             raw_messages = [i.model_dump() for i in request.input]
# MAGIC             
# MAGIC             # Sanitize history
# MAGIC             clean_messages = self._sanitize_history(raw_messages)
# MAGIC
# MAGIC             # Run with clean messages
# MAGIC             result = Runner.run_streamed(agent, input=clean_messages)
# MAGIC             
# MAGIC             async for event in self.process_agent_stream_events(result.stream_events()):
# MAGIC                 yield event
# MAGIC
# MAGIC     def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
# MAGIC         outputs = [
# MAGIC             event.item
# MAGIC             for event in self.predict_stream(request)
# MAGIC             if event.type == "response.output_item.done"
# MAGIC         ]
# MAGIC         return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)
# MAGIC
# MAGIC     def predict_stream(
# MAGIC         self, request: ResponsesAgentRequest
# MAGIC     ) -> Generator[ResponsesAgentStreamEvent, None, None]:
# MAGIC         agen = self._predict_stream_async(request)
# MAGIC
# MAGIC         try:
# MAGIC             loop = asyncio.get_event_loop()
# MAGIC         except RuntimeError:
# MAGIC             loop = asyncio.new_event_loop()
# MAGIC             asyncio.set_event_loop(loop)
# MAGIC
# MAGIC         ait = agen.__aiter__()
# MAGIC
# MAGIC         while True:
# MAGIC             try:
# MAGIC                 item = loop.run_until_complete(ait.__anext__())
# MAGIC             except StopAsyncIteration:
# MAGIC                 break
# MAGIC             else:
# MAGIC                 yield item
# MAGIC
# MAGIC mlflow.openai.autolog()
# MAGIC AGENT = MCPToolCallingAgent()
# MAGIC mlflow.models.set_model(AGENT)

# COMMAND ----------

# DBTITLE 1,Test LCR Agent
dbutils.library.restartPython()

from agent import AGENT

# Test query for LCR analysis
result = AGENT.predict({"input": [{"role": "user", "content": "What is the latest liquidity coverage ratio for Aura Bank? "}]})
print(result.model_dump(exclude_none=True))

# COMMAND ----------

# DBTITLE 1,List MCP Server Tools
import asyncio
import nest_asyncio
from databricks.sdk import WorkspaceClient
from databricks_openai.agents.mcp_server import McpServer
from agents.mcp import MCPServerStreamableHttpParams

nest_asyncio.apply()

workspace_client = WorkspaceClient()
host = workspace_client.config.host

# OAuth workspace client
import os
oauth_workspace_client = WorkspaceClient(
    host=host,
    client_id='7b50dc4d-f302-4afb-8539-59085ae60f74',
    client_secret='<YOUR_CLIENT_SECRET>',
    auth_type="oauth-m2m",
)

async def list_mcp_tools():
    """List all tools available from each MCP server"""
    
    # Liquidity Coverage Ratio Genie
    print("=" * 60)
    print("LIQUIDITY COVERAGE RATIO GENIE TOOLS")
    print("=" * 60)
    async with McpServer(
        name="liquidity_coverage_ratio",
        params=MCPServerStreamableHttpParams(
            url=f"{host}/api/2.0/mcp/genie/01f10d4664e216f2936c2d8b8c2e1bdb",
        ),
        workspace_client=oauth_workspace_client,
    ) as lcr_genie:
        tools = await lcr_genie.list_tools()
        for tool in tools:
            print(f"\n📦 {tool.name}")
            print(f"   Description: {tool.description}")
            if hasattr(tool, 'inputSchema') and tool.inputSchema:
                print(f"   Input Schema: {tool.inputSchema}")
    
    # Calculate LCR Function
    print("\n" + "=" * 60)
    print("CALCULATE LCR FUNCTION TOOLS")
    print("=" * 60)
    async with McpServer(
        name="calculate_lcr",
        params=MCPServerStreamableHttpParams(
            url=f"{host}/api/2.0/mcp/functions/cfo/aura_bank/calculate_lcr",
        ),
        workspace_client=oauth_workspace_client,
    ) as calc_lcr:
        tools = await calc_lcr.list_tools()
        for tool in tools:
            print(f"\n📦 {tool.name}")
            print(f"   Description: {tool.description}")
            if hasattr(tool, 'inputSchema') and tool.inputSchema:
                print(f"   Input Schema: {tool.inputSchema}")
    
    # Calculate LCR Snapshot Function
    print("\n" + "=" * 60)
    print("CALCULATE LCR SNAPSHOT FUNCTION TOOLS")
    print("=" * 60)
    async with McpServer(
        name="capture_lcr_snapshot",
        params=MCPServerStreamableHttpParams(
            url=f"{host}/api/2.0/mcp/functions/cfo/aura_bank/capture_lcr_snapshot",
        ),
        workspace_client=oauth_workspace_client,
    ) as calc_lcr_snapshot:
        tools = await calc_lcr_snapshot.list_tools()
        for tool in tools:
            print(f"\n📦 {tool.name}")
            print(f"   Description: {tool.description}")
            if hasattr(tool, 'inputSchema') and tool.inputSchema:
                print(f"   Input Schema: {tool.inputSchema}")

# Run the async function
await list_mcp_tools()

# COMMAND ----------

# DBTITLE 1,Log Model to Unity Catalog
import mlflow
from mlflow.models import infer_signature
from agent import AGENT

# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Define the model path in Unity Catalog
catalog = "cfo"
schema = "aura_bank"
model_name = "lcr_model"
uc_model_name = f"{catalog}.{schema}.{model_name}"

print(f"Logging model to Unity Catalog: {uc_model_name}")

# Log the model with MLflow
with mlflow.start_run(run_name="lcr_agent_model") as run:
    # Log the agent model - use relative path to the local file
    model_info = mlflow.pyfunc.log_model(
        python_model="agent.py",  # Use relative path instead of full Workspace path
        artifact_path="agent",
        registered_model_name=uc_model_name,
    )
    
    # Log additional metadata
    mlflow.set_tag("model_type", "mcp_agent")
    mlflow.set_tag("use_case", "liquidity_coverage_ratio")
    mlflow.set_tag("mcp_servers", "liquidity_coverage_ratio,calculate_lcr,capture_lcr_snapshot")
    
    print(f"✅ Model logged successfully!")
    print(f"   Model URI: {model_info.model_uri}")
    print(f"   Run ID: {run.info.run_id}")

# COMMAND ----------

# DBTITLE 1,Deploy Model to Serving Endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

# Initialize workspace client
w = WorkspaceClient()

# Define endpoint configuration (using v4 with all required env vars)
endpoint_name = "lcr-agent-endpoint-v4"

print(f"Deploying model to endpoint: {endpoint_name}")
print(f"Model: {uc_model_name}")
print(f"Version: {model_info.registered_model_version}")

try:
    # Create or update the serving endpoint
    endpoint = w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            name=endpoint_name,
            served_entities=[
                ServedEntityInput(
                    entity_name=uc_model_name,
                    entity_version=str(model_info.registered_model_version),
                    scale_to_zero_enabled=True,
                    workload_size="Small",
                    environment_vars={
                        "DATABRICKS_HOST": "https://e2-demo-field-eng.cloud.databricks.com",
                        "DATABRICKS_CLIENT_ID": "7b50dc4d-f302-4afb-8539-59085ae60f74",
                        "DATABRICKS_CLIENT_SECRET": "<YOUR_CLIENT_SECRET>"
                    }
                )
            ]
        ),
    )
    
    print(f"\n✅ Model deployed successfully!")
    print(f"   Endpoint name: {endpoint_name}")
    print(f"   Model version: {model_info.registered_model_version}")
    print(f"   Endpoint URL: https://e2-demo-field-eng.cloud.databricks.com/ml/endpoints/{endpoint_name}")
    
except Exception as e:
    if "already exists" in str(e):
        print(f"\n⚠️  Endpoint '{endpoint_name}' already exists. Updating...")
        
        # Update existing endpoint
        endpoint = w.serving_endpoints.update_config_and_wait(
            name=endpoint_name,
            served_entities=[
                ServedEntityInput(
                    entity_name=uc_model_name,
                    entity_version=str(model_info.registered_model_version),
                    scale_to_zero_enabled=True,
                    workload_size="Small",
                    environment_vars={
                        "DATABRICKS_HOST": "https://e2-demo-field-eng.cloud.databricks.com",
                        "DATABRICKS_CLIENT_ID": "7b50dc4d-f302-4afb-8539-59085ae60f74",
                        "DATABRICKS_CLIENT_SECRET": "<YOUR_CLIENT_SECRET>"
                    }
                )
            ],
        )
        
        print(f"\n✅ Endpoint updated successfully!")
        print(f"   Model version: {model_info.registered_model_version}")
    else:
        print(f"\n❌ Deployment failed: {str(e)}")
        raise e

# COMMAND ----------

# DBTITLE 1,Test Deployed Endpoint
import requests
import json

# Get the workspace URL and token
workspace_url = w.config.host
token = w.config.token

# Update endpoint name to match deployment
endpoint_name = "lcr-agent-endpoint-v4"

# Prepare test request (ResponsesAgent format)
test_query = {
    "input": [
        {
            "role": "user",
            "content": "What is the latest liquidity coverage ratio for Aura Bank?"
        }
    ]
}

print(f"Testing endpoint: {endpoint_name}")
print(f"Query: {test_query['input'][0]['content']}\n")

# Call the endpoint
response = requests.post(
    f"{workspace_url}/serving-endpoints/{endpoint_name}/invocations",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    },
    json=test_query,
    timeout=120
)

if response.status_code == 200:
    result = response.json()
    print("✅ Endpoint test successful!")
    print(f"\nResponse:\n{json.dumps(result, indent=2)}")
else:
    print(f"❌ Endpoint test failed with status code: {response.status_code}")
    print(f"Error: {response.text}")

# COMMAND ----------

# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC # Liquidity Coverage Ratio (LCR) MCP Model
# MAGIC
# MAGIC This notebook demonstrates an AI agent configured with Model Context Protocol (MCP) tools for **Liquidity Coverage Ratio (LCR)** analysis and regulatory compliance.
# MAGIC
# MAGIC ## MCP Tools Configured
# MAGIC
# MAGIC The agent has access to three specialized MCP tools:
# MAGIC
# MAGIC 1. **Liquidity Coverage Ratio Genie** - Natural language interface for LCR queries and analysis
# MAGIC    * Endpoint: `/api/2.0/mcp/genie/01f10d4664e216f2936c2d8b8c2e1bdb`
# MAGIC
# MAGIC 2. **Calculate LCR Function** - Programmatic LCR calculation
# MAGIC    * Endpoint: `/api/2.0/mcp/functions/cfo/aura_bank/calculate_lcr`
# MAGIC
# MAGIC 3. **Calculate LCR Snapshot Function** - Point-in-time LCR snapshot generation
# MAGIC    * Endpoint: `/api/2.0/mcp/functions/cfo/aura_bank/calculate_lcr_snapshot`
# MAGIC
# MAGIC ## Notebook Structure
# MAGIC
# MAGIC * **Cell 2**: Install required packages (`databricks-openai`, `databricks-agents`, `uv`)
# MAGIC * **Cell 3**: Define the `MCPToolCallingAgent` class with LCR-specific configuration
# MAGIC * **Cell 4**: Test the agent with sample LCR queries
# MAGIC * **Cell 5**: List all available tools from the MCP servers
# MAGIC
# MAGIC ## Use Cases
# MAGIC
# MAGIC * Calculate current LCR for regulatory reporting
# MAGIC * Analyze liquidity positions and trends
# MAGIC * Generate point-in-time snapshots for compliance
# MAGIC * Query historical LCR data and metrics
# MAGIC * Understand regulatory requirements and thresholds