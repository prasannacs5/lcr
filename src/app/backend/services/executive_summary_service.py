"""Summarize executive summary text using a Databricks Foundation Model with WorkspaceClient auth."""

import json
import logging
import os
import re

from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from fastapi import HTTPException

from backend.lib.databricks_client import get_workspace_client

logger = logging.getLogger(__name__)

# Foundation model endpoint for summarization. Override with DATABRICKS_SUMMARY_MODEL env var.
# Must be a chat-capable model available on the target workspace.
DEFAULT_SUMMARY_MODEL = "databricks-gemini-2-5-pro"

EXECUTIVE_SUMMARY_SYSTEM = """You are a financial analyst. Given raw executive summary text from a Liquidity Coverage Ratio (LCR) report, produce a concise formatted summary that clearly highlights:

1. **Top 3 items** – The three most important points or takeaways.
2. **Risks** – Key risks or concerns mentioned or implied.
3. **Forecasts** – Any forward-looking statements, projections, or outlooks.

Format your response in clear sections with these exact headings (use markdown):
- **Top 3 items**
- **Risks**
- **Forecasts**

Use bullet points or short paragraphs under each section. Be concise. If the input does not clearly contain one of these categories, say "Not specified" or briefly note what is available.

IMPORTANT – Highlighting: Wrap specific phrases in the following markers so they can be shown in color:
- [[GREEN]]...[[/GREEN]] – Use for positive or favorable content (e.g. compliant, strong, healthy, adequate, above minimum, sufficient, robust).
- [[ORANGE]]...[[/ORANGE]] – Use for caution or moderate concern (e.g. monitor, watch, moderate risk, near threshold, variance).
- [[RED]]...[[/RED]] – Use for risks, negatives, or concerns (e.g. non-compliant, breach, shortfall, high risk, deficiency, decline).

Apply these markers to the most relevant short phrases or numbers within your text (e.g. "LCR is [[GREEN]]compliant[[/GREEN]]" or "[[RED]]Net outflows increased[[/RED]]"). Do not wrap entire paragraphs. Output only the formatted summary, no preamble."""


def _get_endpoint_name() -> str:
    return os.environ.get("DATABRICKS_SUMMARY_MODEL", DEFAULT_SUMMARY_MODEL)


def summarize_executive_summary(raw_text: str) -> str:
    """
    Call Databricks Foundation Model to summarize and format
    executive summary text into Top 3 items, Risks, and Forecasts.
    Uses WorkspaceClient authentication (same as the rest of the app).
    """
    endpoint_name = _get_endpoint_name()
    user_content = f"Input executive summary text:\n\n{raw_text}"

    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=EXECUTIVE_SUMMARY_SYSTEM),
        ChatMessage(role=ChatMessageRole.USER, content=user_content),
    ]

    try:
        w = get_workspace_client()
        response = w.serving_endpoints.query(
            name=endpoint_name,
            messages=messages,
            max_tokens=2048,
            temperature=0.2,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Databricks Gemini summary request failed")
        raise HTTPException(
            status_code=502,
            detail=f"Executive summary model request failed: {e!s}",
        ) from e

    if not response:
        return "Unable to generate summary. No response from the model."

    # Chat response: choices[0].message.content (may be str or list of content blocks); completions: choices[0].text
    choices = getattr(response, "choices", None) or []
    if not choices:
        return "Unable to generate summary. The model returned no choices."

    first = choices[0]
    msg = getattr(first, "message", None)
    if msg and getattr(msg, "content", None):
        content = msg.content
        if isinstance(content, str):
            text = content.strip()
        elif isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and "text" in block:
                    parts.append(block["text"] or "")
                elif isinstance(block, str):
                    parts.append(block)
            text = " ".join(parts).strip()
        else:
            text = ""
        if text:
            return text
    text = getattr(first, "text", None)
    if text and isinstance(text, str) and text.strip():
        return text.strip()

    return "Unable to generate summary. The model returned no text."


FR2052A_NARRATIVE_SYSTEM = """You are a financial analyst writing narrative for an FR 2052a (Complex Institution Liquidity Monitoring Report) internal management report. You will be given an as-of date, any available LCR (Liquidity Coverage Ratio) data for that date, and any internal notes. Your task is to write 2 to 4 short paragraphs of professional narrative that:
1. Briefly explain the purpose of LCR and this report in the context of liquidity risk and regulatory monitoring.
2. Summarize the data provided (e.g. LCR ratio, HQLA, net cash outflows, regulatory status) when present, or note that no LCR results are available for the date and what that implies.
3. Weave in any internal notes provided, if relevant.
Use plain text only (no markdown, no bullet points). Write in a formal, internal-report style. Keep paragraphs concise. Output only the narrative, no preamble or headings."""


def generate_fr2052a_lcr_narrative(
    as_of_date,
    lcr_rows: list,
    repo_notes: list,
) -> str:
    """
    Call Gemini to generate LCR narrative for the FR 2052a PDF using the given
    as-of date, LCR result rows, and repo notes. Returns plain-text narrative.
    """
    # Build input block for the model
    parts = [f"As-of date: {as_of_date.isoformat()}."]
    if lcr_rows:
        parts.append("LCR data for this date:")
        for i, row in enumerate(lcr_rows[:3]):  # at most 3 rows
            row_str = ", ".join(f"{k}={v}" for k, v in row.items() if v is not None and k not in ("snapshot_id", "calculation_timestamp", "created_by"))
            parts.append(f"  Row {i + 1}: {row_str}")
    else:
        parts.append("No LCR results available for this date.")
    if repo_notes:
        notes_candidates = ("notes", "note", "text", "comment", "description", "content")
        for r in repo_notes:
            for key in notes_candidates:
                if key in r and r[key] is not None and str(r[key]).strip():
                    parts.append(f"Internal note: {str(r[key]).strip()}")
                    break
            else:
                for key, val in r.items():
                    if isinstance(val, str) and val.strip() and key.lower() not in ("id", "as_of_date", "date", "timestamp", "created_at"):
                        parts.append(f"Internal note: {val.strip()}")
                        break
    user_content = "\n".join(parts)

    endpoint_name = _get_endpoint_name()
    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=FR2052A_NARRATIVE_SYSTEM),
        ChatMessage(role=ChatMessageRole.USER, content=user_content),
    ]
    try:
        w = get_workspace_client()
        response = w.serving_endpoints.query(
            name=endpoint_name,
            messages=messages,
            max_tokens=1024,
            temperature=0.2,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Databricks Gemini FR 2052a narrative request failed")
        raise HTTPException(
            status_code=502,
            detail=f"FR 2052a narrative generation failed: {e!s}",
        ) from e

    if not response:
        return ""
    choices = getattr(response, "choices", None) or []
    if not choices:
        return ""
    first = choices[0]
    msg = getattr(first, "message", None)
    if msg and getattr(msg, "content", None):
        content = msg.content
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and "text" in block:
                    parts.append(block.get("text") or "")
                elif isinstance(block, str):
                    parts.append(block)
            return " ".join(parts).strip()
    text = getattr(first, "text", None)
    if text and isinstance(text, str):
        return text.strip()
    return ""


SUGGESTED_QUESTIONS_SYSTEM = """You are helping users of a Liquidity Coverage Ratio (LCR) analytics chat. Given a brief summary of current LCR data, generate 6 to 8 suggested questions that a user might ask. Keep each question SHORT and SIMPLE so the agent can answer with one or two lookups—avoid compound questions or multi-part requests that require many tool calls. Examples of good simple questions: "What is our latest LCR ratio?", "Show me HQLA and net cash outflows for the most recent date.", "Are we LCR compliant?", "How has LCR changed over the last few dates?" Divide into sections like "LCR overview", "Compliance", "Trends", "Summary". Output only valid JSON, no other text. Format: [{"category": "Section name", "text": "Short question"}, ...]"""


def generate_suggested_chat_questions(lcr_rows: list) -> list[dict]:
    """
    Call Gemini to generate suggested chat questions based on LCR data, divided into sections.
    Returns list of {"category": str, "text": str}. Returns empty list on failure.
    """
    if not lcr_rows:
        user_content = "No LCR results are currently available. Suggest 6-8 general questions users might ask an LCR analyst (e.g. about variance, stress testing, intraday liquidity, compliance), divided into 3-4 sections."
    else:
        parts = ["Current LCR data (recent rows):"]
        for i, row in enumerate(lcr_rows[:5]):
            row_str = ", ".join(f"{k}={v}" for k, v in row.items() if v is not None and k not in ("snapshot_id", "calculation_timestamp", "created_by"))
            parts.append(f"  Row {i + 1}: {row_str}")
        user_content = "\n".join(parts)

    endpoint_name = _get_endpoint_name()
    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=SUGGESTED_QUESTIONS_SYSTEM),
        ChatMessage(role=ChatMessageRole.USER, content=user_content),
    ]
    try:
        w = get_workspace_client()
        response = w.serving_endpoints.query(
            name=endpoint_name,
            messages=messages,
            max_tokens=1024,
            temperature=0.3,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Databricks Gemini suggested questions request failed")
        raise HTTPException(
            status_code=502,
            detail=f"Suggested questions generation failed: {e!s}",
        ) from e

    if not response:
        return []
    choices = getattr(response, "choices", None) or []
    if not choices:
        return []
    first = choices[0]
    msg = getattr(first, "message", None)
    raw = None
    if msg and getattr(msg, "content", None):
        content = msg.content
        if isinstance(content, str):
            raw = content.strip()
        elif isinstance(content, list):
            parts = []
            for block in content:
                if isinstance(block, dict) and "text" in block:
                    parts.append(block.get("text") or "")
                elif isinstance(block, str):
                    parts.append(block)
            raw = " ".join(parts).strip()
    if not raw:
        raw = getattr(first, "text", None) or ""
        raw = raw.strip() if isinstance(raw, str) else ""
    if not raw:
        return []

    # Parse JSON (allow markdown code block wrapper)
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```\s*$", "", raw)
    raw = raw.strip()
    try:
        out = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Gemini suggested questions: invalid JSON, returning empty list")
        return []
    if not isinstance(out, list):
        return []
    result = []
    for item in out:
        if isinstance(item, dict) and isinstance(item.get("category"), str) and isinstance(item.get("text"), str):
            result.append({"category": item["category"].strip(), "text": item["text"].strip()})
    return result
