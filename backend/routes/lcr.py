"""LCR API routes."""

import json
from datetime import date

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

from backend.services.chat_service import chat as chat_service_chat
from backend.services.chat_service import stream_chat as chat_service_stream_chat
from backend.services.executive_summary_service import summarize_executive_summary
from backend.services.fr2052a_report_service import build_fr2052a_management_pdf
from backend.services.lcr_service import (
    get_cash_outflows_breakdown,
    get_gl_data,
    get_hqla_breakdown,
    get_lcr_results,
    get_reco_records,
)


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    reply: str

router = APIRouter(prefix="/lcr", tags=["lcr"])


def _sse_event(data: dict) -> str:
    """Format a dict as one SSE event (data line + blank line)."""
    return f"data: {json.dumps(data)}\n\n"


@router.get("/reco")
def reco_table(limit: int = Query(500, ge=1, le=2000, description="Max recent records")) -> list[dict]:
    """
    Get recent records from cfo.aura_bank.reco_table, ordered by date (recent first).
    Used by Executive Summary; frontend groups by tag into 3 sections.
    """
    return get_reco_records(limit=limit)


def _latest_reco_text() -> str:
    """Get concatenated executive summary text for the latest timestamp only."""
    records = get_reco_records(limit=500)
    if not records:
        return ""
    ts_col = "timestamp"
    latest = records[0].get(ts_col)
    for row in records:
        t = row.get(ts_col)
        if t is not None and (latest is None or str(t) > str(latest)):
            latest = t
    if latest is None:
        selected = records
    else:
        selected = [r for r in records if r.get(ts_col) is not None and str(r.get(ts_col)) == str(latest)]
    parts = []
    for row in selected:
        text = row.get("text")
        if text is not None and str(text).strip():
            parts.append(str(text).strip())
    return "\n\n".join(parts)


class ExecutiveSummarySummarizedResponse(BaseModel):
    summary: str


class SuggestedQuestion(BaseModel):
    category: str
    text: str


class SuggestedQuestionsResponse(BaseModel):
    questions: list[SuggestedQuestion]


# Curated pre-generated questions for Chat with liquidity analyst (saves analyst time; no category label per question in UI).
DEFAULT_SUGGESTED_QUESTIONS = [
    # Stress Testing
    SuggestedQuestion(category="Stress Testing", text="What would happen to our LCR if net cash outflows increased by 10%?"),
    SuggestedQuestion(category="Stress Testing", text="Run a simple stress test: how does our LCR look if HQLA drops by 5%?"),
    # Attribution & Variance (The "Why") — 2 per category
    SuggestedQuestion(category="Attribution & Variance", text="LCR dropped from 137% yesterday to 136.4% today. Which specific HQLA asset class or Liability counterparty caused the largest portion of this decline?"),
    SuggestedQuestion(category="Attribution & Variance", text="Is the increase in Net Cash Outflows due to new customer activity (volume) or because the Fed runoff rates for our existing deposits were re-classified (rate/rule change)?"),
    # Composition & Efficiency (Optimization)
    SuggestedQuestion(category="Composition & Efficiency", text="Show me our HQLA breakdown. How close are we to the 40% cap on Level 2 assets? If we hit the cap, how much of our current buffer becomes 'dead weight' for regulatory purposes?"),
    SuggestedQuestion(category="Composition & Efficiency", text="What is the yield drag of our current HQLA portfolio? Suggest a trade to shift $50M from Level 1 Cash to Level 1 Securities that increases yield without lowering the LCR."),
    # Predictive & Forward-Looking (Early Warning)
    SuggestedQuestion(category="Predictive & Forward-Looking", text="Based on our current maturity ladder in the GL, what is our projected LCR for 7 days from now, assuming no new deposits come in?"),
    SuggestedQuestion(category="Predictive & Forward-Looking", text="Do we have a 'Liquidity Cliff' in the next 30 days? Identify any single day where more than 10% of our wholesale funding matures at once."),
    # Behavioral Analysis (AI-Driven Insights)
    SuggestedQuestion(category="Behavioral Analysis", text="The Fed assumes a 3% runoff for stable retail deposits. Based on our actual GL data from the last 6 months, what is our 'implied' runoff rate, and how does it compare to the Fed's 3%?"),
    SuggestedQuestion(category="Behavioral Analysis", text="Is there a correlation between our LCR volatility and specific market indices (e.g., S&P 500 or SOFR spreads) over the last 30 days?"),
]


@router.get("/suggested-questions", response_model=SuggestedQuestionsResponse)
def suggested_questions() -> SuggestedQuestionsResponse:
    """
    Return curated pre-generated questions for the Chat with liquidity analyst section.
    Grouped by category: Stress Testing, Attribution & Variance, Composition & Efficiency, Predictive & Forward-Looking, Behavioral Analysis.
    """
    return SuggestedQuestionsResponse(questions=[q.model_dump() for q in DEFAULT_SUGGESTED_QUESTIONS])


@router.get("/executive-summary/summarized", response_model=ExecutiveSummarySummarizedResponse)
def executive_summary_summarized() -> ExecutiveSummarySummarizedResponse:
    """
    Get executive summary for the latest timestamp, summarized and formatted by Gemini
    to highlight Top 3 items, Risks, and Forecasts.
    """
    raw_text = _latest_reco_text()
    if not raw_text:
        return ExecutiveSummarySummarizedResponse(
            summary="No executive summary text available for the latest timestamp."
        )
    summary = summarize_executive_summary(raw_text)
    return ExecutiveSummarySummarizedResponse(summary=summary)


@router.get("/results")
def lcr_results(
    as_of_date: date | None = Query(
        None, alias="date", description="Filter by as_of_date (e.g. ?date=2025-01-15)"
    ),
) -> list[dict]:
    """
    Get LCR results from cfo.aura_bank.lcr_results.
    Optionally filter by as_of_date (query param ?date=YYYY-MM-DD).
    """
    return get_lcr_results(as_of_date=as_of_date)


@router.get("/hqla-breakdown")
def hqla_breakdown(
    snapshot_id: str | None = Query(None, description="Filter by snapshot_id"),
    as_of_date: date | None = Query(
        None, alias="date", description="Filter by as_of_date (e.g. ?date=2025-01-15)"
    ),
) -> list[dict]:
    """Get HQLA breakdown from cfo.aura_bank.hqla_breakdown."""
    return get_hqla_breakdown(snapshot_id=snapshot_id, as_of_date=as_of_date)


@router.get("/cash-outflows-breakdown")
def cash_outflows_breakdown(
    snapshot_id: str | None = Query(None, description="Filter by snapshot_id"),
    as_of_date: date | None = Query(
        None, alias="date", description="Filter by as_of_date (e.g. ?date=2025-01-15)"
    ),
) -> list[dict]:
    """Get cash outflows breakdown from cfo.aura_bank.cash_outflows_breakdown."""
    return get_cash_outflows_breakdown(
        snapshot_id=snapshot_id, as_of_date=as_of_date
    )


@router.get("/fr2052a-report", response_class=Response)
def fr2052a_report(
    as_of_date: date = Query(..., alias="date", description="As-of date for the report (e.g. ?date=2025-01-15)"),
):
    """
    Generate an FR 2052a-style management report (PDF) for internal use.
    Returns PDF; open in a new window when user clicks COMPLIANT in the metrics table.
    """
    try:
        pdf_bytes = build_fr2052a_management_pdf(as_of_date)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Report generation failed: {e!s}") from e
    filename = f"FR2052a_Management_Report_{as_of_date.isoformat()}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@router.get("/gl-data")
def gl_data(
    as_of_date: date | None = Query(
        None, alias="date", description="Filter by as_of_date (e.g. ?date=2025-01-15)"
    ),
) -> list[dict]:
    """
    Get GL (General Ledger) data for the given date.
    Table is configurable via LCR_GL_TABLE (default gl_data), LCR_GL_CATALOG, LCR_GL_SCHEMA.
    """
    try:
        return get_gl_data(as_of_date=as_of_date)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"GL data unavailable: {e!s}. Check that LCR_GL_TABLE (and LCR_GL_CATALOG/LCR_GL_SCHEMA if set) point to an existing table with as_of_date.",
        )


@router.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    """Send a message to the LCR agent model endpoint and return the reply."""
    reply = chat_service_chat(req.message)
    return ChatResponse(reply=reply)


@router.post("/chat/stream")
def chat_stream(req: ChatRequest):
    """
    Stream the LCR agent response using Server-Sent Events.
    Yields real text deltas from the Databricks API (spaces preserved).
    Events: status, token (content exactly as sent by API), tool_call, done, error.
    """
    def generate():
        try:
            yield _sse_event({"type": "status", "message": "Sending your question to the LCR agent…"})
            for event in chat_service_stream_chat(req.message):
                yield _sse_event(event)
        except Exception as e:
            err_msg = getattr(e, "detail", str(e))
            if isinstance(err_msg, list) and err_msg:
                err_msg = err_msg[0].get("msg", str(err_msg))
            if not isinstance(err_msg, str):
                err_msg = str(e)
            yield _sse_event({"type": "error", "message": err_msg})

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
