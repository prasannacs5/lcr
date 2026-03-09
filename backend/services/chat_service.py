import json
import logging
import os
import re
import time
from typing import Any
import requests
from fastapi import HTTPException

logger = logging.getLogger(__name__)

DEFAULT_CHAT_ENDPOINT_NAME = "lcr-agent-endpoint-v4"

# Default: no system message to avoid "text content blocks must be non-empty" during agent tool use.
# Set LCR_CHAT_SYSTEM_CONTEXT in env to add system instructions if your endpoint supports it.
LCR_SYSTEM_CONTEXT = ""

# In-memory cache for OAuth token (Databricks Apps / service principal). Refresh 5 min before expiry.
_oauth_token: str | None = None
_oauth_expires_at: float = 0
_OAUTH_REFRESH_BUFFER = 300  # seconds


def _is_databricks_apps() -> bool:
    """True when running as a Databricks App (platform sets DATABRICKS_APP_APPLICATION_ID)."""
    return bool(os.environ.get("DATABRICKS_APP_APPLICATION_ID"))


def _get_oauth_access_token() -> str:
    """Obtain Bearer token via WorkspaceClient's unified auth (auto-detects Apps SP credentials)."""
    global _oauth_token, _oauth_expires_at
    if _oauth_token and (time.time() + _OAUTH_REFRESH_BUFFER) < _oauth_expires_at:
        return _oauth_token
    try:
        from backend.lib.databricks_client import get_workspace_client
        w = get_workspace_client()
        auth = w.config.authenticate()
        # authenticate() may return a dict of headers or a callable that populates headers
        if callable(auth):
            headers = {}
            auth(headers)
        else:
            headers = auth if isinstance(auth, dict) else {}
        bearer = headers.get("Authorization", "")
        if bearer.startswith("Bearer "):
            access_token = bearer[7:]
        else:
            access_token = bearer
        if not access_token:
            raise HTTPException(status_code=502, detail="Could not obtain access token from WorkspaceClient.")
        _oauth_token = access_token
        _oauth_expires_at = time.time() + 3000  # refresh in ~50 min
        return _oauth_token
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("OAuth token request failed")
        raise HTTPException(
            status_code=502,
            detail=f"Token request failed: {e!s}",
        ) from e


def _get_max_turns() -> int:
    """Max agent turns (tool + model steps). Higher allows complex questions. Default 25."""
    try:
        return max(1, int(os.environ.get("LCR_CHAT_MAX_TURNS", "25")))
    except ValueError:
        return 25


def _use_streaming() -> bool:
    """Use streaming invocation (same as playground); may allow more turns. Default True."""
    return os.environ.get("LCR_CHAT_STREAM", "true").lower() in ("1", "true", "yes")

def _get_token() -> str:
    """Use DATABRICKS_TOKEN when set (local). Otherwise use WorkspaceClient unified auth."""
    token = os.environ.get("DATABRICKS_TOKEN")
    if token and token.strip():
        return token.strip()
    return _get_oauth_access_token()

def _get_endpoint_url() -> str:
    url = os.environ.get("DATABRICKS_LCR_CHAT_ENDPOINT")
    if url:
        return url
    from backend.lib.databricks_client import get_host
    host = get_host()
    endpoint_name = os.environ.get("LCR_CHAT_ENDPOINT_NAME", DEFAULT_CHAT_ENDPOINT_NAME)
    return f"{host}/serving-endpoints/{endpoint_name}/invocations"

def _restore_spaces(text: str) -> str:
    """
    Restore word boundaries when tokens were concatenated without spaces (e.g. stream
    chunks "help"+"you" -> "helpyou"). Run on the full text so mixed content is fixed.
    """
    if not text or not text.strip():
        return text
    # Insert space before uppercase that follows lowercase: "findThe" -> "find The"
    out = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    # Period/capital run-together: "period.For" -> "period. For"
    out = re.sub(r"\.([A-Z])", r". \1", out)
    # Common words that often get concatenated; insert space before when mid-run
    common = (
        "the", "and", "to", "for", "you", "with", "this", "from", "that", "have", "will",
        "can", "are", "were", "been", "has", "had", "not", "but", "they", "we", "your",
        "need", "get", "let", "me", "do", "try", "use", "based", "when", "would", "there",
        "information", "about", "date", "following", "details", "according", "however",
        "apologize", "available", "found", "system", "either", "like", "query", "provide",
        "help", "find", "retrieve", "using", "appropriate", "tool", "instead", "more",
        "gathered", "might", "indicate", "specifically", "exact", "closest", "value",
        "sources", "transactions", "recorded", "additional", "breakdown", "contribute",
        # LCR / executive summary and compliance vocabulary
        "executive", "summary", "liquidity", "coverage", "ratio", "analysis", "poll",
        "complete", "response", "here", "key", "metrics", "average", "ranges", "across",
        "most", "recent", "reporting", "days", "high", "quality", "liquid", "assets",
        "totals", "million", "net", "cash", "outflows", "values", "compliance", "status",
        "all", "analyzed", "show", "compliant", "regulatory", "zero", "non", "instances",
        "no", "violations", "detected", "period", "trends", "observations", "consistently",
        "remain", "above", "minimum", "threshold", "typically", "healthy", "indicating",
        "strong", "positioning", "sufficient", "buffer", "maintained", "risk", "assessment",
        "current", "position", "appears", "robust", "immediate", "concerns", "bank",
        "maintains", "adequate", "cover", "potential", "short", "term", "stress", "scenarios",
        "detailed", "specific", "ranges", "queries", "performed", "aspect", "such", "longer",
        "components", "data", "an", "of", "in", "is", "on", "any", "or", "over",
    )
    for word in common:
        # Add space before word when preceded by a letter and (at end or followed by letter/number)
        pattern = r"([a-zA-Z])(" + re.escape(word) + r"(?=[a-zA-Z0-9]|$))"
        out = re.sub(pattern, r"\1 \2", out, flags=re.IGNORECASE)
    # Fix "number,number" -> "number, number" (e.g. "1.31 to1.53" handled by "to"; "21,2026" -> "21, 2026")
    out = re.sub(r"(\d),(\d)", r"\1, \2", out)
    return out.strip()


def _get_timeout() -> tuple[int, int]:
    try:
        read_s = int(os.environ.get("DATABRICKS_CHAT_TIMEOUT", "180"))
    except ValueError:
        read_s = 180
    return (15, max(read_s, 60))


def _is_empty_content_error(err_msg: str) -> bool:
    """True if the API error is the 'text content blocks must be non-empty' validation error."""
    return "text content blocks must be non-empty" in str(err_msg)


def _raise_chat_error(status_code: int, err_msg: str) -> None:
    err_str = str(err_msg)
    if "Max turns" in err_str:
        raise HTTPException(
            status_code=504,
            detail="Complexity limit reached. Try a simpler LCR question.",
        )
    if _is_empty_content_error(err_str):
        raise HTTPException(
            status_code=400,
            detail="The agent produced an invalid response (empty content in one of its steps). Try a simpler question or rephrase.",
        )
    if len(err_str) > 500:
        err_str = err_str[:497] + "..."
    raise HTTPException(status_code=status_code, detail=err_str)

def _parse_reply(data: Any) -> str:
    """
    Bulletproof parser to handle 'expected string, got list' errors.
    Extracts text from various Databricks Agent response shapes.
    """
    if not data:
        return ""

    # 1. Handle if the entire response is a list (common in Agent Traces)
    messages = []
    if isinstance(data, list):
        messages = data
    elif isinstance(data, dict):
        # Check standard Databricks wrapper keys
        messages = data.get("predictions", data.get("messages", data.get("output", [])))
        
        # If 'output' was already a string, return it immediately
        if isinstance(messages, str):
            return messages.strip()

    # 2. If we have a list of messages, find the FINAL assistant thought/text
    final_text_parts = []
    
    if isinstance(messages, list):
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            
            # Look for roles 'assistant' or 'agent'
            role = str(msg.get("role", "")).lower()
            if role in ("assistant", "agent"):
                content = msg.get("content")
                
                # Case A: Content is a list of blocks (e.g., [{'type': 'text', 'text': '...'}] )
                if isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict):
                            # Grab 'text' or 'content' keys
                            t = block.get("text") or block.get("content")
                            if t: final_text_parts.append(str(t))
                
                # Case B: Content is a direct string
                elif isinstance(content, str) and content.strip():
                    final_text_parts.append(content)

    # 3. Join all parts. If the API returned one fragment per word/token, joining with
    # newlines gives no spaces; rejoin with space so output is human-readable.
    if final_text_parts:
        # Drop any parts that are type labels (e.g. response.output_item.done)
        filtered = [p for p in final_text_parts if _is_content_text(str(p))]
        if filtered:
            joined = "\n\n".join(filtered).strip()
            # If result has no spaces but we have multiple parts, likely word-by-word chunks
            if joined and " " not in joined and len(filtered) > 1:
                joined = " ".join(p.strip() for p in filtered if p and str(p).strip()).strip()
            # If still no spaces (single block from API with concatenated words), restore boundaries
            if joined:
                return _restore_spaces(joined)

    # 4. OpenAI-style: choices[0].delta.content or choices[0].message.content
    if isinstance(data, dict):
        choices = data.get("choices")
        if isinstance(choices, list) and choices:
            msg = choices[0].get("message") or choices[0].get("delta") or {}
            c = msg.get("content")
            if isinstance(c, str) and c.strip() and _is_content_text(c):
                return c.strip()
        fallback = data.get("text") or data.get("response") or data.get("output") or ""
        if isinstance(fallback, str) and fallback.strip() and _is_content_text(fallback):
            return fallback.strip()

    # 5. Deep search for any substantial 'text' or 'content' string (nested in streamed shape)
    found = _find_first_text(data, max_depth=8)
    if found and _is_content_text(found):
        return found.strip()

    keys = list(data.keys()) if isinstance(data, dict) else ("list", len(data) if isinstance(data, list) else 0)
    # Stream chunks (type, delta, ...) are normal; only warn for unexpected full payloads
    if isinstance(data, dict) and "type" in data and "delta" in data:
        logger.debug("No text in stream chunk. Keys: %s", keys)
    else:
        logger.warning(
            "Could not extract reply from agent response. Top-level keys: %s",
            keys,
        )
    return "No displayable text returned from the LCR agent."


def _find_first_text(obj: Any, max_depth: int) -> str:
    """Recursively find first non-empty string in 'text' or 'content' key."""
    if max_depth <= 0:
        return ""
    if isinstance(obj, str):
        return obj if obj.strip() else ""
    if isinstance(obj, dict):
        for key in ("text", "content", "delta", "output", "response", "message"):
            if key in obj:
                v = obj[key]
                if isinstance(v, str) and v.strip():
                    return v
                if isinstance(v, (dict, list)):
                    t = _find_first_text(v, max_depth - 1)
                    if t:
                        return t
        for v in obj.values():
            t = _find_first_text(v, max_depth - 1)
            if t:
                return t
    if isinstance(obj, list):
        for item in obj:
            t = _find_first_text(item, max_depth - 1)
            if t:
                return t
    return ""


# Type/event labels that are not actual message content; do not include in reply.
_STREAM_LABEL_BLACKLIST = (
    "response.output_text.delta",
    "response.output_item.delta",
    "response.output_item.done",
    "output_text.delta",
    "output_item.done",
)


def _is_content_text(s: str) -> bool:
    """True if s looks like real content, not a type/event label."""
    if not s or not s.strip():
        return False
    t = s.strip().lower()
    if t in (x.lower() for x in _STREAM_LABEL_BLACKLIST):
        return False
    if "response." in t and ".delta" in t and len(t) < 50:
        return False
    if "response." in t and ".done" in t:
        return False
    return True


def _extract_text_from_chunk(chunk: Any, *, preserve_spaces: bool = False) -> str:
    """
    Pull out a single string from one stream chunk.
    When preserve_spaces=True (for streaming), do not strip so API spacing is kept.
    """
    def _maybe_strip(s: str) -> str:
        return s if preserve_spaces else s.strip()

    if isinstance(chunk, str):
        return _maybe_strip(chunk) if _is_content_text(chunk) else ""
    if not isinstance(chunk, dict):
        return ""

    # Stream chunks with top-level type, delta, content_index, item_id, etc.
    delta = chunk.get("delta")
    if delta is not None:
        if isinstance(delta, str) and _is_content_text(delta):
            return _maybe_strip(delta)
        if isinstance(delta, dict):
            t = delta.get("content") or delta.get("text")
            if isinstance(t, str) and _is_content_text(t):
                return _maybe_strip(t)

    # Databricks agent stream: response.output_text.delta or response.output_text.delta.text
    response = chunk.get("response")
    if isinstance(response, dict):
        output_text = response.get("output_text")
        if isinstance(output_text, dict):
            delta = output_text.get("delta")
            if isinstance(delta, str) and _is_content_text(delta):
                return _maybe_strip(delta)
            if isinstance(delta, dict):
                t = delta.get("text") or delta.get("content")
                if isinstance(t, str) and _is_content_text(t):
                    return _maybe_strip(t)
        output_item = response.get("output_item")
        if isinstance(output_item, dict) and output_item.get("done") is not None:
            t = output_item.get("text") or output_item.get("content") or output_item.get("done")
            if isinstance(t, str) and _is_content_text(t):
                return _maybe_strip(t)

    c = chunk.get("content") or chunk.get("text")
    if isinstance(c, str) and _is_content_text(c):
        return _maybe_strip(c)
    output = chunk.get("output")
    if isinstance(output, str) and _is_content_text(output):
        return _maybe_strip(output)
    if isinstance(output, list):
        for block in output:
            if isinstance(block, dict):
                t = block.get("text") or block.get("content")
                if isinstance(t, str) and t.strip() and _is_content_text(t):
                    return _maybe_strip(t)
    choices = chunk.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("delta") or choices[0].get("message") or {}
        c = msg.get("content")
        if isinstance(c, str) and _is_content_text(c):
            return _maybe_strip(c)
    for key in ("message", "delta", "output_text"):
        val = chunk.get(key)
        if isinstance(val, dict):
            t = val.get("text") or val.get("content")
            if isinstance(t, str) and _is_content_text(t):
                return _maybe_strip(t)
    found = _find_first_text(chunk, max_depth=4)
    if not _is_content_text(found):
        return ""
    return _maybe_strip(found)


def _is_tool_chunk(chunk: Any) -> bool:
    """True if this chunk represents a tool call/output (not displayable text)."""
    if not isinstance(chunk, dict):
        return False
    # Databricks: response.output_item.done or type hint
    r = chunk.get("response") or {}
    if isinstance(r, dict) and r.get("output_item"):
        return True
    if chunk.get("type") in ("tool_use", "tool_result", "item"):
        return True
    return False


def _collect_streamed_reply(resp: requests.Response) -> str:
    """
    Consume a streamed response (SSE or NDJSON) and return the full assistant reply.
    Handles 'data: {...}\n\n' SSE, delta chunks (OpenAI-style), and single JSON body.
    """
    collected: list[Any] = []
    buffer = ""
    for line in resp.iter_lines(decode_unicode=True):
        if line is None:
            continue
        buffer += line + "\n"
        if line.startswith("data: "):
            try:
                data = json.loads(line[6:].strip())
                collected.append(data)
            except json.JSONDecodeError:
                pass
        elif line.strip() and not line.startswith(":"):
            try:
                collected.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    if not collected and buffer.strip():
        try:
            collected.append(json.loads(buffer.strip()))
        except json.JSONDecodeError:
            try:
                for part in buffer.strip().split("\n"):
                    part = part.strip()
                    if part.startswith("data:"):
                        part = part[5:].strip()
                    if part:
                        collected.append(json.loads(part))
            except json.JSONDecodeError:
                pass
    if not collected:
        return ""

    # Try concatenating text from each chunk (OpenAI-style delta stream).
    # Join with no space so token boundaries stay correct (e.g. "liqu"+"idity" -> "liquidity");
    # then _restore_spaces fixes word boundaries.
    parts = [_extract_text_from_chunk(c) for c in collected]
    if any(parts):
        joined = "".join(p for p in parts if p).strip()
        if joined:
            return _restore_spaces(joined)

    # Try parsing as single final payload (last chunk often has full message list)
    if len(collected) == 1 and isinstance(collected[0], dict):
        out = _parse_reply(collected[0])
        if out and not out.startswith("No displayable") and _is_content_text(out):
            return out
    for obj in reversed(collected):
        if isinstance(obj, dict):
            out = _parse_reply(obj)
            if out and not out.startswith("No displayable") and _is_content_text(out):
                return out

    # Pass full list (agent trace format)
    out = _parse_reply(collected)
    if out and not out.startswith("No displayable") and _is_content_text(out):
        return out
    return ""


def _format_reply_for_display(reply: str) -> str:
    """
    Format reply for UI: restore word boundaries, collapse tool-call noise,
    and add paragraph breaks so the frontend can show structured content.
    """
    if not reply or not reply.strip():
        return reply
    if reply.strip().startswith("No displayable"):
        return reply
    # Always restore spaces so concatenated tokens (e.g. "helpyou", "executivesummary") are fixed
    text = _restore_spaces(reply.strip())

    # Collapse repetitive tool blocks to one short line so the actual answer stands out
    # Pattern: "🔧 Calling tool: ... ⏳ Tool executing... ✅ Tool execution completed"
    text = re.sub(
        r"🔧\s*Calling tool:[^\n⏳]*⏳\s*Tool executing\.\.\.[^\n✅]*✅\s*Tool execution completed",
        "🔧 Tool used.\n\n",
        text,
        flags=re.IGNORECASE,
    )
    # Standalone "Tool execution completed" or "Tool used." — ensure paragraph break after
    text = re.sub(r"(✅\s*Tool execution completed)(\s*)", r"\1\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"(🔧\s*Tool used\.)(\s*)", r"\1\n\n", text, flags=re.IGNORECASE)

    # Ensure markdown headers have space after #: "#Executive" -> "# Executive"
    text = re.sub(r"^(#{1,6})([A-Za-z])", r"\1 \2", text, flags=re.MULTILINE)
    # Add paragraph break before common sentence starters so long runs split into paragraphs
    for starter in (
        "Let me ",
        "I'll ",
        "I see ",
        "I have ",
        "The data ",
        "In summary",
        "Therefore",
        "Based on ",
        "Overall",
    ):
        text = re.sub(r"\.\s+" + re.escape(starter), r".\n\n" + starter, text, flags=re.IGNORECASE)

    return text.strip()


def _looks_incomplete(reply: str) -> bool:
    """True if the reply looks like it stopped after tool use without a final outcome."""
    if not reply or len(reply) < 50:
        return False
    r = reply.strip().lower()
    # Ends with tool-completed or similar and no clear conclusion
    if not (r.endswith("tool execution completed") or "tool execution completed" in r[-200:]):
        return False
    # Heuristic: no outcome-like phrasing in the last 400 chars
    tail = r[-400:] if len(r) >= 400 else r
    outcome_markers = (
        "in summary", "in conclusion", "therefore", "we have", "the result",
        "lcr is", "liquidity", "based on the", "the data shows", "answer is",
    )
    return not any(m in tail for m in outcome_markers)


def chat(user_message: str) -> str:
    message_text = (user_message or "").strip()
    if not message_text:
        raise HTTPException(status_code=400, detail="Message cannot be empty.")

    url = _get_endpoint_url()
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }

    # Endpoint requires content as array of input_text blocks with non-empty text.
    # Only add system if non-empty to avoid "text content blocks must be non-empty".
    system_context = (os.environ.get("LCR_CHAT_SYSTEM_CONTEXT") or LCR_SYSTEM_CONTEXT).strip()
    input_messages = []
    if system_context:
        input_messages.append({
            "role": "system",
            "content": [{"type": "input_text", "text": system_context}],
        })
    input_messages.append({
        "role": "user",
        "content": [{"type": "input_text", "text": message_text}],
    })

    use_stream = _use_streaming()
    payload = {
        "input": input_messages,
        "max_turns": _get_max_turns(),
        "stream": use_stream,
    }
    connect_timeout, read_timeout = _get_timeout()

    try:
        if use_stream:
            resp = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
                stream=True,
            )
            if not resp.ok:
                err_body = b"".join(resp.iter_content(chunk_size=8192))
                err_text = err_body.decode("utf-8", errors="replace") if err_body else f"HTTP {resp.status_code}"
                try:
                    err_json = json.loads(err_text)
                    err_msg = err_json.get("message") or err_json.get("error") or err_text
                except Exception:
                    err_msg = err_text
                # Retry once without streaming when we get the empty-content validation error
                if resp.status_code == 400 and _is_empty_content_error(err_msg):
                    payload = {**payload, "stream": False}
                    resp = requests.post(
                        url,
                        json=payload,
                        headers=headers,
                        timeout=(connect_timeout, read_timeout),
                    )
                    if not resp.ok:
                        err_text = resp.text or ""
                        try:
                            err_json = json.loads(err_text) if err_text else {}
                            err_msg = err_json.get("message") or err_json.get("error") or err_text
                        except Exception:
                            err_msg = err_text
                        _raise_chat_error(resp.status_code, err_msg)
                    return _format_reply_for_display(_parse_reply(resp.json()))
                _raise_chat_error(resp.status_code, err_msg)
            reply = _collect_streamed_reply(resp)
            if reply:
                # If streamed reply looks truncated (tool done, no outcome), retry without streaming for full answer
                if _looks_incomplete(reply):
                    payload_full = {**payload, "stream": False}
                    resp_full = requests.post(
                        url,
                        json=payload_full,
                        headers=headers,
                        timeout=(connect_timeout, read_timeout),
                    )
                    if resp_full.ok:
                        full = _parse_reply(resp_full.json())
                        if full and not full.startswith("No displayable"):
                            return _format_reply_for_display(full)
                return _format_reply_for_display(reply)
            use_stream = False
            payload = {**payload, "stream": False}

        resp = requests.post(
            url, json=payload, headers=headers, timeout=(connect_timeout, read_timeout)
        )
        if not resp.ok:
            err_text = resp.text or ""
            try:
                err_json = resp.json()
                err_msg = err_json.get("message") or err_json.get("error") or err_text
            except Exception:
                err_msg = err_text
            _raise_chat_error(resp.status_code, err_msg)

        return _format_reply_for_display(_parse_reply(resp.json()))

    except Exception as e:
        logger.exception("Chat failed")
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=str(e))


def stream_chat(user_message: str):
    """
    Generator that streams the Databricks agent response in real time.
    Yields dicts: {type: "status"|"token"|"tool_call"|"done"|"error", ...}.
    Token content is sent exactly as from the API (preserve_spaces=True) so the UI
    does not squash words.
    """
    message_text = (user_message or "").strip()
    if not message_text:
        yield {"type": "error", "message": "Message cannot be empty."}
        return

    url = _get_endpoint_url()
    headers = {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }
    system_context = (os.environ.get("LCR_CHAT_SYSTEM_CONTEXT") or LCR_SYSTEM_CONTEXT).strip()
    input_messages = []
    if system_context:
        input_messages.append({
            "role": "system",
            "content": [{"type": "input_text", "text": system_context}],
        })
    input_messages.append({
        "role": "user",
        "content": [{"type": "input_text", "text": message_text}],
    })
    payload = {
        "input": input_messages,
        "max_turns": _get_max_turns(),
        "stream": True,
    }
    connect_timeout, read_timeout = _get_timeout()

    try:
        resp = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=(connect_timeout, read_timeout),
            stream=True,
        )
        if not resp.ok:
            err_body = b"".join(resp.iter_content(chunk_size=8192))
            err_text = err_body.decode("utf-8", errors="replace") if err_body else f"HTTP {resp.status_code}"
            try:
                err_json = json.loads(err_text)
                err_msg = err_json.get("message") or err_json.get("error") or err_text
            except Exception:
                err_msg = err_text
            yield {"type": "error", "message": err_msg}
            return

        yield {"type": "status", "message": "Agent is responding…"}
        for line in resp.iter_lines(decode_unicode=True):
            if line is None:
                continue
            s = line.strip()
            if not s or s.startswith(":"):
                continue
            json_str = s[6:].strip() if s.startswith("data:") else s
            if s.startswith("data:") and (json_str == "[DONE]" or json_str == ""):
                continue
            try:
                data = json.loads(json_str)
            except json.JSONDecodeError:
                continue
            if _is_tool_chunk(data):
                yield {"type": "tool_call"}
                continue
            content = _extract_text_from_chunk(data, preserve_spaces=True)
            if content:
                yield {"type": "token", "content": content}
        yield {"type": "done"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Stream chat failed")
        yield {"type": "error", "message": str(e)}