import asyncio
import mlflow
import nest_asyncio
import os
import json
import copy

from agents import Agent, set_default_openai_api, set_default_openai_client, Runner
from agents.mcp import MCPServerStreamableHttpParams
from agents.tracing import set_trace_processors
from agents.result import StreamEvent
from databricks_openai.agents.mcp_server import McpServer
from databricks_openai import AsyncDatabricksOpenAI
from databricks.sdk import WorkspaceClient
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from typing import Generator, AsyncIterator, AsyncGenerator, List, Dict, Any
from uuid import uuid4

nest_asyncio.apply()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
set_default_openai_client(AsyncDatabricksOpenAI())
set_default_openai_api("chat_completions")
set_trace_processors([])

# ---------------------------------------------------------------------------
# All environment-specific config is read from env vars.
# Set these on the model serving endpoint's environment_vars:
#   LCR_LLM_ENDPOINT_NAME  - chat model (default: databricks-claude-3-7-sonnet)
#   LCR_GENIE_ROOM_ID      - Genie AI/BI room ID (required)
#   DATABRICKS_HOST         - workspace URL (injected by platform or set manually)
#   DATABRICKS_CLIENT_ID    - SP client ID for MCP auth (optional; uses unified auth if unset)
#   DATABRICKS_CLIENT_SECRET- SP client secret for MCP auth (optional)
# ---------------------------------------------------------------------------
LLM_ENDPOINT_NAME = os.getenv("LCR_LLM_ENDPOINT_NAME", "databricks-claude-3-7-sonnet")
GENIE_ROOM_ID = os.getenv("LCR_GENIE_ROOM_ID", "")
SYSTEM_PROMPT = "You are a helpful financial assistant specialized in Liquidity Coverage Ratio (LCR) analysis and regulatory compliance. You can analyze liquidity positions, calculate LCR metrics, and provide insights on regulatory requirements."

if not GENIE_ROOM_ID:
    raise RuntimeError("LCR_GENIE_ROOM_ID env var is required. Set it to the Genie AI/BI room ID.")

# Auth Setup - uses automatic credential detection
# In serving: picks up DATABRICKS_HOST + DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET env vars
# In notebook: picks up notebook context credentials
workspace_client = WorkspaceClient()
host = workspace_client.config.host

# For MCP server auth, use OAuth M2M if env vars are set, otherwise reuse workspace client
if os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET"):
    oauth_workspace_client = WorkspaceClient(
        host=host,
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
        auth_type="oauth-m2m",
    )
else:
    oauth_workspace_client = workspace_client

class MCPToolCallingAgent(ResponsesAgent):
    
    def _sanitize_history(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sanitizes message history to ensure proper Claude message format:
        - Removes duplicate tool call IDs
        - Merges consecutive assistant messages (ONLY if they don't have tool_calls AND no tool execution between them)
        - Ensures tool calls are followed by tool results
        """
        if not messages:
            return messages
            
        clean_history = []
        seen_ids = set()
        last_was_tool_execution = False
        
        i = 0
        while i < len(messages):
            msg = copy.deepcopy(messages[i])
            msg_type = msg.get("type", "")
            msg_role = msg.get("role", "")
            
            # Skip messages with fake IDs at the message level
            if msg.get("id") == "__fake_id__":
                i += 1
                continue
            
            # Track tool execution messages
            if msg_type in ["function_call", "function_call_output"]:
                last_was_tool_execution = True
                i += 1
                continue
            
            # Handle tool_calls in the message
            if "tool_calls" in msg and isinstance(msg["tool_calls"], list):
                valid_tools = []
                for tc in msg["tool_calls"]:
                    tid = tc.get("id")
                    if tid and tid != "__fake_id__" and tid not in seen_ids:
                        seen_ids.add(tid)
                        valid_tools.append(tc)
                msg["tool_calls"] = valid_tools
                
                if not msg["tool_calls"]:
                    msg.pop("tool_calls", None)
                else:
                    last_was_tool_execution = True
            
            # Handle content blocks
            content = msg.get("content")
            if isinstance(content, list):
                valid_content = []
                for block in content:
                    if isinstance(block, dict):
                        bid = block.get("id")
                        if bid:
                            if bid == "__fake_id__" or bid in seen_ids:
                                continue
                            seen_ids.add(bid)
                    valid_content.append(block)
                msg["content"] = valid_content
                
                if not msg["content"]:
                    i += 1
                    continue
            
            # Handle tool results
            call_id = msg.get("call_id") or msg.get("tool_call_id")
            if call_id:
                if call_id == "__fake_id__" or call_id in seen_ids:
                    i += 1
                    continue
                seen_ids.add(call_id)
            
            # Merge consecutive assistant messages
            if (clean_history and 
                msg_role == "assistant" and 
                clean_history[-1].get("role") == "assistant" and
                not msg.get("tool_calls") and
                not clean_history[-1].get("tool_calls") and
                not last_was_tool_execution):
                
                last_msg = clean_history[-1]
                
                if isinstance(msg.get("content"), str) and isinstance(last_msg.get("content"), str):
                    if msg["content"] != last_msg["content"]:
                        last_msg["content"] = last_msg["content"] + "\n" + msg["content"]
                elif isinstance(msg.get("content"), list):
                    if not isinstance(last_msg.get("content"), list):
                        last_msg["content"] = [last_msg["content"]] if last_msg.get("content") else []
                    last_msg["content"].extend(msg["content"])
            else:
                clean_history.append(msg)
                if msg_role == "assistant":
                    last_was_tool_execution = False
            
            i += 1
        
        return clean_history

    async def process_agent_stream_events(
        self, async_stream: AsyncIterator[StreamEvent]
    ) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
        curr_item_id = str(uuid4())
        
        async for event in async_stream:
            # Handle Standard OpenAI Events
            if event.type == "raw_response_event":
                event_data = event.data.model_dump()
                if (
                    event_data["type"] == "response.output_item.done"
                    and event_data.get("item") is not None
                    and event_data["item"].get("id") is not None
                ):
                    if event_data["item"].get("status") is None:
                        event_data["item"].pop("status", None)
                    
                    if isinstance(event_data["item"].get("output"), list):
                        output_list = event_data["item"]["output"]
                        if output_list and isinstance(output_list[0], dict) and "text" in output_list[0]:
                            event_data["item"]["output"] = output_list[0]["text"]
                        else:
                            event_data["item"]["output"] = str(output_list)
                    
                    event_data["item"]["id"] = curr_item_id
                    event_data.pop("output_index", None)
                    event_data.pop("sequence_number", None)
                    event_data.pop("id", None)
                    yield ResponsesAgentStreamEvent(**event_data)
                
                elif (
                    event_data["type"] == "response.output_text.delta"
                    and event_data.get("item_id") is not None
                ):
                    event_data["item_id"] = curr_item_id
                    yield ResponsesAgentStreamEvent(**event_data)

            # Handle Runner Events (Tool Calls AND Results)
            elif event.type == "run_item_stream_event":
                # Handle tool call initiation
                if event.item.type == "tool_call_item":
                    # Extract tool information
                    tool_name = getattr(event.item, 'name', 'unknown_tool')
                    tool_args = getattr(event.item, 'arguments', {})
                    
                    # Format tool call message with name and details
                    if isinstance(tool_args, str):
                        try:
                            tool_args = json.loads(tool_args)
                        except:
                            pass
                    
                    # Create a descriptive message
                    tool_message = f"🔧 Calling tool: {tool_name}"
                    if tool_args and isinstance(tool_args, dict):
                        # Include key parameters (limit to avoid too much detail)
                        param_summary = ", ".join([f"{k}={v}" for k, v in list(tool_args.items())[:3]])
                        if len(tool_args) > 3:
                            param_summary += "..."
                        tool_message += f" ({param_summary})"
                    
                    # Yield tool call message
                    yield ResponsesAgentStreamEvent(
                        type="response.output_text.delta",
                        item_id=curr_item_id,
                        delta=tool_message,
                    )
                    
                    # Immediately yield "Tool executing" message
                    yield ResponsesAgentStreamEvent(
                        type="response.output_text.delta",
                        item_id=curr_item_id,
                        delta=" ⏳ Tool executing...",
                    )
                
                # Handle tool results
                elif event.item.type == "tool_call_output_item":
                    item_dict = event.item.to_input_item()
                    
                    if isinstance(item_dict.get("output"), list):
                        output_list = item_dict["output"]
                        if output_list and isinstance(output_list[0], dict) and "text" in output_list[0]:
                            item_dict["output"] = output_list[0]["text"]
                        else:
                            item_dict["output"] = str(output_list)
                    
                    # Send a completion message
                    yield ResponsesAgentStreamEvent(
                        type="response.output_text.delta",
                        item_id=curr_item_id,
                        delta=" ✅ Tool execution completed",
                    )
                    
                    # Then yield the actual result
                    yield ResponsesAgentStreamEvent(
                        type="response.output_item.done",
                        item=item_dict,
                    )

    async def _predict_stream_async(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        
        async with (
            McpServer(
                name="liquidity_coverage_ratio",
                params=MCPServerStreamableHttpParams(url=f"{host}/api/2.0/mcp/genie/{GENIE_ROOM_ID}"),
                workspace_client=oauth_workspace_client,
            ) as lcr_genie,
        ):
            agent = Agent(
                name="lcr_agent",
                instructions=SYSTEM_PROMPT,
                model=LLM_ENDPOINT_NAME,
                mcp_servers=[
                    lcr_genie,
                ],
            )
            
            # Get raw input
            raw_messages = [i.model_dump() for i in request.input]
            
            # Sanitize history
            clean_messages = self._sanitize_history(raw_messages)

            # Run with clean messages
            result = Runner.run_streamed(agent, input=clean_messages)
            
            async for event in self.process_agent_stream_events(result.stream_events()):
                yield event

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        agen = self._predict_stream_async(request)

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        ait = agen.__aiter__()

        while True:
            try:
                item = loop.run_until_complete(ait.__anext__())
            except StopAsyncIteration:
                break
            else:
                yield item

mlflow.openai.autolog()
AGENT = MCPToolCallingAgent()
mlflow.models.set_model(AGENT)
