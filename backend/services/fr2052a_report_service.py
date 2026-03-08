"""
Generate an FR 2052a-style management report (PDF) for internal use.

FR 2052a (Complex Institution Liquidity Monitoring Report) is a regulatory data
submission to the Federal Reserve. This service produces an internal management
report that maps LCR/liquidity data to the key components described in the
official reporting instructions, for dashboard and control purposes only.
Official submission must use Federal Reserve templates and technical guidance.
"""

import io
import re
from datetime import date
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from backend.services.executive_summary_service import generate_fr2052a_lcr_narrative, summarize_executive_summary
from backend.services.lcr_service import (
    get_cash_outflows_breakdown,
    get_gl_data,
    get_hqla_breakdown,
    get_lcr_results,
    get_reco_records,
    get_repo_notes,
)


def _fmt(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, (int, float)):
        if abs(v) >= 1_000_000:
            return f"{v / 1_000_000:,.2f} M"
        if isinstance(v, float) and 0 < v <= 2:
            return f"{v * 100:.2f}%"
        return f"{v:,.2f}" if isinstance(v, float) else f"{v:,}"
    return str(v)


def _escape_for_pdf(s: str) -> str:
    """Escape so ReportLab Paragraph accepts it (e.g. in XML-style)."""
    if not s:
        return s
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _strip_highlight_markers(text: str) -> str:
    """Remove [[GREEN]]/[[ORANGE]]/[[RED]] markers, keeping inner text."""
    if not text:
        return text
    for tag in ("GREEN", "ORANGE", "RED"):
        text = re.sub(rf"\[\[{tag}\]\]([\s\S]*?)\[\[\/{tag}\]\]", r"\1", text)
    return text


def _get_latest_reco_text() -> str:
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


# Internal columns omitted from breakdown tables in the PDF.
_BREAKDOWN_HIDDEN_KEYS = frozenset({"snapshot_id", "calculation_timestamp", "created_by"})


def _breakdown_rows_to_table(rows: list[dict], preferred_column_order: list[str] | None = None):
    """
    Build (headers, data_rows) for a ReportLab table from breakdown line items.
    Uses all keys present in rows, minus _BREAKDOWN_HIDDEN_KEYS. Column order:
    preferred_column_order (when provided and key exists) then remaining keys sorted.
    """
    if not rows:
        return [], []
    all_keys = set()
    for r in rows:
        all_keys.update(k for k in r.keys() if k not in _BREAKDOWN_HIDDEN_KEYS)
    preferred = list(preferred_column_order or [])
    ordered = [k for k in preferred if k in all_keys]
    ordered += sorted(all_keys - set(ordered))
    headers = [k.replace("_", " ").title() for k in ordered]
    data_rows = []
    for r in rows:
        data_rows.append([_fmt(r.get(k)) for k in ordered])
    return headers, data_rows


def _add_breakdown_table(
    story,
    title: str,
    rows: list[dict],
    heading_style,
    body_style,
    table_cell_style: ParagraphStyle,
    preferred_columns: list[str] | None = None,
    table_header_style: ParagraphStyle | None = None,
) -> None:
    """Append an optional heading and a dynamic table for the given breakdown rows. Uses Paragraph in cells so text wraps."""
    if not rows:
        return
    headers, data_rows = _breakdown_rows_to_table(rows, preferred_columns)
    if not headers:
        return
    if title:
        story.append(Paragraph(title, heading_style))
    n_cols = len(headers)
    header_style = table_header_style or table_cell_style
    header_cells = [Paragraph(_escape_for_pdf(h), header_style) for h in headers]
    data_cells = [
        [Paragraph(_escape_for_pdf(str(cell)), table_cell_style) for cell in row]
        for row in data_rows
    ]
    data = [header_cells] + data_cells
    avail_width = 7.0 * inch
    col_width = max(0.6 * inch, min(1.4 * inch, avail_width / n_cols))
    col_widths = [col_width] * n_cols
    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f2942")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 9),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                ("TOPPADDING", (0, 0), (-1, 0), 6),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 0.15 * inch))


def build_fr2052a_management_pdf(as_of_date: date) -> bytes:
    """
    Build a PDF management report aligned with FR 2052a key components:
    data structure (LCR, HQLA, net cash outflows), reporting scope, and
    internal control context. For internal use only; not the official submission.
    """
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "CustomTitle",
        parent=styles["Heading1"],
        fontSize=16,
        spaceAfter=12,
    )
    heading_style = ParagraphStyle(
        "CustomHeading",
        parent=styles["Heading2"],
        fontSize=12,
        spaceAfter=6,
        spaceBefore=12,
    )
    body_style = styles["Normal"]
    body_style.spaceAfter = 6

    table_cell_style = ParagraphStyle(
        "TableCell",
        parent=styles["Normal"],
        fontSize=8,
        leading=10,
        spaceAfter=0,
        spaceBefore=0,
    )
    table_header_style = ParagraphStyle(
        "TableHeader",
        parent=styles["Normal"],
        fontSize=9,
        leading=10,
        fontName="Helvetica-Bold",
        spaceAfter=0,
        spaceBefore=0,
    )

    story = []

    story.append(
        Paragraph(
            "FR 2052a Management Report (Internal Use)",
            title_style,
        )
    )
    story.append(
        Paragraph(
            "Complex Institution Liquidity Monitoring Report — "
            "Internal control report based on liquidity data. "
            "Official submission uses Federal Reserve Board reporting forms and technical instructions.",
            body_style,
        )
    )
    story.append(Spacer(1, 0.25 * inch))

    # --- Executive Summary (first section) ---
    story.append(Paragraph("Executive Summary", heading_style))
    try:
        raw_reco = _get_latest_reco_text()
        if raw_reco and raw_reco.strip():
            summary_text = summarize_executive_summary(raw_reco)
            summary_plain = _strip_highlight_markers(summary_text or "")
            if summary_plain.strip():
                for block in summary_plain.split("\n\n"):
                    block = block.strip()
                    if block:
                        story.append(Paragraph(_escape_for_pdf(block), body_style))
            else:
                story.append(Paragraph("Executive summary not available.", body_style))
        else:
            story.append(Paragraph("No executive summary text available for the latest timestamp.", body_style))
    except Exception:
        story.append(Paragraph("Executive summary could not be generated.", body_style))
    story.append(Spacer(1, 0.25 * inch))

    story.append(Paragraph("Reporting scope and as-of date", heading_style))
    story.append(
        Paragraph(
            f"<b>As-of date:</b> {as_of_date.isoformat()}. "
            "This report is for the consolidated entity. Material legal entities should be reported separately per supervisory definitions.",
            body_style,
        )
    )
    story.append(Spacer(1, 0.2 * inch))

    # All data for this report date (fetch once)
    try:
        gl_rows = get_gl_data(as_of_date=as_of_date)
    except Exception:
        gl_rows = []
    lcr_rows = get_lcr_results(as_of_date=as_of_date)
    try:
        repo_notes = get_repo_notes(as_of_date)
    except Exception:
        repo_notes = []
    hqla = get_hqla_breakdown(as_of_date=as_of_date)
    outflows = get_cash_outflows_breakdown(as_of_date=as_of_date)

    # Gemini-generated LCR narrative (data + notes)
    try:
        narrative = generate_fr2052a_lcr_narrative(as_of_date, lcr_rows, repo_notes)
    except Exception:
        narrative = ""
    if narrative:
        story.append(Paragraph("LCR summary and context", heading_style))
        for block in narrative.split("\n\n"):
            block = block.strip()
            if block:
                story.append(Paragraph(_escape_for_pdf(block), body_style))
        story.append(Spacer(1, 0.2 * inch))

    # --- 1. General Ledger transactions (as-of date) ---
    story.append(Paragraph("General Ledger transactions", heading_style))
    story.append(
        Paragraph(
            f"GL transactions for as-of date {as_of_date.isoformat()}. Source: configured general ledger table.",
            body_style,
        )
    )
    if gl_rows:
        _add_breakdown_table(
            story,
            "",
            gl_rows,
            heading_style,
            body_style,
            table_cell_style,
            preferred_columns=["timestamp", "account", "amount", "debit", "credit", "description", "account_id"],
            table_header_style=table_header_style,
        )
    else:
        story.append(Paragraph("No GL transactions for this date.", body_style))
        story.append(Spacer(1, 0.15 * inch))

    # --- 2. LCR (key metrics for compliance) ---
    story.append(Paragraph("LCR — key metrics", heading_style))
    story.append(
        Paragraph(
            "Liquidity Coverage Ratio and related metrics for the as-of date. Data supports validation against official FR 2052a reporting requirements.",
            body_style,
        )
    )
    if lcr_rows:
        row = lcr_rows[0]
        col_order = [
            ("as_of_date", "Date"),
            ("lcr", "LCR"),
            ("lcr_ratio", "LCR Ratio"),
            ("lcr_value", "LCR Value"),
            ("lcr_pct", "LCR %"),
            ("lcr_percent", "LCR Percent"),
            ("hqla", "HQLA"),
            ("hqla_amount", "HQLA Amount"),
            ("hqla_value", "HQLA Value"),
            ("hqla_total", "HQLA Total"),
            ("net_cash_outflows", "Net Cash Outflows"),
            ("regulatory_status", "Regulatory Status"),
        ]
        data = [
            [Paragraph(_escape_for_pdf("Data element"), table_cell_style), Paragraph(_escape_for_pdf("Value"), table_cell_style)]
        ]
        added = set()
        for key, label in col_order:
            if key in row and row[key] is not None:
                data.append([
                    Paragraph(_escape_for_pdf(label), table_cell_style),
                    Paragraph(_escape_for_pdf(str(_fmt(row[key]))), table_cell_style),
                ])
                added.add(key)
        for key, val in row.items():
            if key in added or val is None or key in ("snapshot_id", "calculation_timestamp", "created_by"):
                continue
            label = key.replace("_", " ").title()
            data.append([
                Paragraph(_escape_for_pdf(label), table_cell_style),
                Paragraph(_escape_for_pdf(str(_fmt(val))), table_cell_style),
            ])
        t = Table(data, colWidths=[2.5 * inch, 4.0 * inch])
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f2942")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 9),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                    ("TOPPADDING", (0, 0), (-1, 0), 6),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ]
            )
        )
        story.append(t)
        story.append(Spacer(1, 0.2 * inch))
    else:
        story.append(Paragraph("No LCR results for this date.", body_style))
        story.append(Spacer(1, 0.15 * inch))

    # --- 3. HQLA line items ---
    story.append(Paragraph("HQLA line items", heading_style))
    story.append(
        Paragraph(
            "High-Quality Liquid Assets by category/product for the as-of date. Data supports validation against official FR 2052a reporting requirements.",
            body_style,
        )
    )
    if hqla:
        _add_breakdown_table(
            story,
            "",
            hqla,
            heading_style,
            body_style,
            table_cell_style,
            preferred_columns=["as_of_date", "category", "product", "amount", "value", "level", "hqla_type"],
            table_header_style=table_header_style,
        )
    else:
        story.append(Paragraph("No HQLA breakdown for this date.", body_style))
        story.append(Spacer(1, 0.15 * inch))

    # --- 4. Net cash outflows line items ---
    story.append(Paragraph("Net cash outflows line items", heading_style))
    story.append(
        Paragraph(
            "Cash outflows breakdown by product/runoff factor for the as-of date. Align with official data element definitions for submission.",
            body_style,
        )
    )
    if outflows:
        _add_breakdown_table(
            story,
            "",
            outflows,
            heading_style,
            body_style,
            table_cell_style,
            preferred_columns=["as_of_date", "category", "product", "runoff_factor", "amount", "outflow_amount"],
            table_header_style=table_header_style,
        )
    else:
        story.append(Paragraph("No cash outflows breakdown for this date.", body_style))
        story.append(Spacer(1, 0.15 * inch))

    # Notes from cfo.aura.repo_table for this date
    if repo_notes:
        story.append(Paragraph("Notes (cfo.aura.repo_table)", heading_style))
        notes_candidates = ("notes", "note", "text", "comment", "description", "content")
        for i, row in enumerate(repo_notes):
            parts = []
            for key in notes_candidates:
                if key in row and row[key] is not None and str(row[key]).strip():
                    parts.append(str(row[key]).strip())
            if not parts:
                for key, val in row.items():
                    if isinstance(val, str) and val.strip() and key.lower() not in ("id", "as_of_date", "date", "timestamp", "created_at"):
                        parts.append(val.strip())
            if parts:
                text = " ".join(parts).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                story.append(Paragraph(text, body_style))
        story.append(Spacer(1, 0.2 * inch))

    story.append(Paragraph("Technical guidance and disclaimer", heading_style))
    story.append(
        Paragraph(
            "To build or validate the official regulatory submission, use FR 2052a Reporting Forms &amp; Instructions and "
            "Federal Reserve Reporting Central. Submission deadlines are typically 10:00 AM ET, two business days after the as-of date. "
            "Consult your firm's regulatory reporting or legal team to ensure internal interpretations align with current supervisory expectations.",
            body_style,
        )
    )

    doc.build(story)
    buffer.seek(0)
    return buffer.read()
