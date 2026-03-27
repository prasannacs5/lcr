import { useEffect, useMemo, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import rehypeRaw from "rehype-raw";

import "./App.css";
import databankLogo from "./assets/databank-logo.png";

/** Convert Gemini [[GREEN]]/[[ORANGE]]/[[RED]] markers to HTML spans for executive summary highlighting. */
function applySummaryHighlights(text) {
  if (typeof text !== "string") return text;
  return text
    .replace(/\[\[GREEN\]\]([\s\S]*?)\[\[\/GREEN\]\]/g, '<span class="summary-green">$1</span>')
    .replace(/\[\[ORANGE\]\]([\s\S]*?)\[\[\/ORANGE\]\]/g, '<span class="summary-orange">$1</span>')
    .replace(/\[\[RED\]\]([\s\S]*?)\[\[\/RED\]\]/g, '<span class="summary-red">$1</span>');
}

const API_BASE = import.meta.env.VITE_API_BASE ?? "";

const RECO_TAG_COLUMN = "tag"; // column in reco_table used to group into 3 sections
const RECO_COLUMN_ORDER = ["timestamp", "text"]; // reco_table columns (tag used for grouping only) (reco_table: timestamp, tag, text)

function formatRecoCell(value, columnKey) {
  if (value == null || value === "") return "—";
  if (columnKey === "timestamp") {
    try {
      const d = new Date(typeof value === "string" ? value.replace(" ", "T") : value);
      if (!Number.isNaN(d.getTime())) {
        return d.toLocaleString(undefined, {
          dateStyle: "short",
          timeStyle: "short",
        });
      }
    } catch (_) {}
    return String(value);
  }
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

/** Fallback when API suggested questions are not yet loaded or fail. Matches backend curated list. */
const DEFAULT_SUGGESTED_CHAT_QUESTIONS = [
  { category: "Stress Testing", text: "What would happen to our LCR if net cash outflows increased by 10%?" },
  { category: "Stress Testing", text: "Run a simple stress test: how does our LCR look if HQLA drops by 5%?" },
  { category: "Attribution & Variance", text: "LCR dropped from 137% yesterday to 136.4% today. Which specific HQLA asset class or Liability counterparty caused the largest portion of this decline?" },
  { category: "Attribution & Variance", text: "Is the increase in Net Cash Outflows due to new customer activity (volume) or because the Fed runoff rates for our existing deposits were re-classified (rate/rule change)?" },
  { category: "Composition & Efficiency", text: "Show me our HQLA breakdown. How close are we to the 40% cap on Level 2 assets? If we hit the cap, how much of our current buffer becomes 'dead weight' for regulatory purposes?" },
  { category: "Composition & Efficiency", text: "What is the yield drag of our current HQLA portfolio? Suggest a trade to shift $50M from Level 1 Cash to Level 1 Securities that increases yield without lowering the LCR." },
  { category: "Predictive & Forward-Looking", text: "Based on our current maturity ladder in the GL, what is our projected LCR for 7 days from now, assuming no new deposits come in?" },
  { category: "Predictive & Forward-Looking", text: "Do we have a 'Liquidity Cliff' in the next 30 days? Identify any single day where more than 10% of our wholesale funding matures at once." },
  { category: "Behavioral Analysis", text: "The Fed assumes a 3% runoff for stable retail deposits. Based on our actual GL data from the last 6 months, what is our 'implied' runoff rate, and how does it compare to the Fed's 3%?" },
  { category: "Behavioral Analysis", text: "Is there a correlation between our LCR volatility and specific market indices (e.g., S&P 500 or SOFR spreads) over the last 30 days?" },
];

const HIDDEN_COLUMNS = ["snapshot_id", "calculation_timestamp", "created_by"];

const COLUMN_LABELS = {
  as_of_date: "Date",
  lcr: "LCR",
  lcr_ratio: "LCR",
  lcr_value: "LCR",
  lcr_pct: "LCR",
  lcr_percent: "LCR",
  hqla: "HQLA",
  hqla_amount: "HQLA",
  hqla_value: "HQLA",
  hqla_total: "HQLA",
  net_cash_outflows: "Net Cash Outflows",
  regulatory_status: "Regulatory Status",
  data: "Data",
};

const COLUMN_ORDER = [
  "as_of_date",
  "lcr",
  "lcr_ratio",
  "lcr_value",
  "lcr_pct",
  "lcr_percent",
  "hqla",
  "hqla_amount",
  "hqla_value",
  "hqla_total",
  "net_cash_outflows",
  "regulatory_status",
  "data",
];

function groupByDate(rows) {
  const key = "as_of_date";
  const map = new Map();
  for (const row of rows) {
    const raw = row[key];
    const dateStr =
      raw == null
        ? "Unknown"
        : typeof raw === "string"
          ? raw.split("T")[0]
          : String(raw);
    if (!map.has(dateStr)) map.set(dateStr, []);
    map.get(dateStr).push(row);
  }
  return Array.from(map.entries()).sort((a, b) => {
    if (a[0] === "Unknown") return 1;
    if (b[0] === "Unknown") return -1;
    return b[0].localeCompare(a[0]);
  });
}

function getColumns(rows) {
  if (!rows.length) return [];
  const first = rows[0];
  const available = Object.keys(first).filter((k) => !HIDDEN_COLUMNS.includes(k));
  const ordered = [];
  for (const key of COLUMN_ORDER) {
    if (available.includes(key)) ordered.push(key);
  }
  for (const key of available) {
    if (!ordered.includes(key)) ordered.push(key);
  }
  return ordered;
}

/** Single header for Intraday Metrics: Date, LCR, HQLA, Net Cash Outflows, Regulatory Status */
const INTRADAY_HEADERS = [
  { key: "as_of_date", label: "Date", clickable: false },
  { key: "lcr", label: "LCR", clickable: "gl" },
  { key: "hqla", label: "HQLA", clickable: "hqla" },
  { key: "net_cash_outflows", label: "Net Cash Outflows", clickable: "cash_outflows" },
  { key: "regulatory_status", label: "Regulatory Status", clickable: false },
];

function getIntradayColumns(rows) {
  if (!rows.length) return INTRADAY_HEADERS.map((h) => ({ ...h, dataKey: h.key }));
  const first = rows[0];
  const available = Object.keys(first).filter((k) => !HIDDEN_COLUMNS.includes(k));
  return INTRADAY_HEADERS.map((h) => {
    if (available.includes(h.key)) return { ...h, dataKey: h.key };
    const lower = h.key.toLowerCase();
    const found = available.find((k) => k.toLowerCase() === lower || k.toLowerCase().startsWith(lower + "_"));
    if (found) return { ...h, dataKey: found };
    if (h.key === "lcr") {
      const lcrCol = available.find((k) => isLcrColumn(k));
      return { ...h, dataKey: lcrCol || h.key };
    }
    if (h.key === "hqla") {
      const hqlaCol = available.find((k) => k.toLowerCase().includes("hqla"));
      return { ...h, dataKey: hqlaCol || h.key };
    }
    return { ...h, dataKey: h.key };
  });
}

/** Get numeric LCR % from a row (for chart). Prefer lcr_ratio, lcr, lcr_pct, lcr_percent. */
function getLcrPercentFromRow(row) {
  const keys = ["lcr_ratio", "lcr", "lcr_pct", "lcr_percent"];
  for (const k of keys) {
    const v = row[k];
    if (v == null || v === "") continue;
    const num = Number(v);
    if (!Number.isFinite(num)) continue;
    return num > 0 && num <= 2 ? num * 100 : num;
  }
  return null;
}

/** Get chart data: rows sorted by date asc, with date label, LCR %, HQLA, and outflows. */
function getLcrChartData(rows) {
  const sorted = [...rows].sort((a, b) => {
    const aDate = a.as_of_date == null ? "" : String(a.as_of_date).split("T")[0];
    const bDate = b.as_of_date == null ? "" : String(b.as_of_date).split("T")[0];
    return aDate.localeCompare(bDate);
  });
  return sorted.map((row) => {
    const raw = row.as_of_date;
    const dateStr = raw == null ? "" : typeof raw === "string" ? raw.split("T")[0] : String(raw).split("T")[0];
    const label = dateStr ? formatDateLabel(dateStr) : "—";
    let shortLabel = "—";
    if (dateStr) {
      try {
        const d = new Date(dateStr);
        if (!Number.isNaN(d.getTime())) {
          const m = String(d.getMonth() + 1).padStart(2, "0");
          const day = String(d.getDate()).padStart(2, "0");
          shortLabel = `${m}-${day}`;
        } else shortLabel = dateStr.slice(5);
      } catch {
        shortLabel = dateStr.slice(5);
      }
    }
    const value = getLcrPercentFromRow(row);
    const hqla = getMetricFromRow(row, "hqla") ?? getMetricFromRow(row, "hqla_amount") ?? getMetricFromRow(row, "hqla_total") ?? getMetricFromRow(row, "hqla_value");
    const outflows = getMetricFromRow(row, "net_cash_outflows");
    return { dateStr, label, shortLabel, value, hqla, outflows };
  }).filter((d) => d.value != null);
}

/** Pick indices for x-axis labels so we show at most maxLabels, evenly spaced (first, last, and in between). */
function getXAxisLabelIndices(length, maxLabels = 6) {
  if (length <= maxLabels) return Array.from({ length }, (_, i) => i);
  const indices = [0];
  for (let i = 1; i < maxLabels - 1; i++) {
    indices.push(Math.round((i / (maxLabels - 1)) * (length - 1)));
  }
  indices.push(length - 1);
  return [...new Set(indices)].sort((a, b) => a - b);
}

/** Get value from row for a metric (HQLA, net_cash_outflows, cash_inflows). */
function getMetricFromRow(row, key) {
  const v = row[key];
  if (v != null && v !== "") return v;
  const lower = (key || "").toLowerCase();
  const keys = Object.keys(row || {});
  const found = keys.find((k) => k.toLowerCase().includes(lower.replace(/_/g, "")) || (lower === "hqla" && k.toLowerCase().includes("hqla")));
  return found != null ? row[found] : null;
}

function getColumnLabel(colKey) {
  if (COLUMN_LABELS[colKey]) return COLUMN_LABELS[colKey];
  const lower = colKey.toLowerCase();
  if (lower.includes("lcr")) return "LCR";
  if (lower.includes("hqla")) return "HQLA";
  return colKey.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function isLcrColumn(columnKey) {
  const lower = (columnKey || "").toLowerCase();
  return lower === "lcr" || lower.includes("lcr_");
}

function isMillionsColumn(columnKey) {
  const lower = (columnKey || "").toLowerCase();
  return lower.includes("hqla") || lower.includes("net_cash_outflow");
}

function formatCell(value, columnKey) {
  if (value == null || value === "") return "—";
  if (columnKey === "as_of_date") {
    const dateStr = typeof value === "string" ? value.split("T")[0] : String(value);
    try {
      const d = new Date(dateStr);
      if (Number.isNaN(d.getTime())) return dateStr;
      return d.toLocaleDateString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
      });
    } catch {
      return dateStr;
    }
  }
  if (isLcrColumn(columnKey)) {
    const num = Number(value);
    if (!Number.isFinite(num)) return String(value);
    const percent = num > 0 && num <= 2 ? num * 100 : num;
    return `${percent.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`;
  }
  if (isMillionsColumn(columnKey)) {
    const num = Number(value);
    if (!Number.isFinite(num)) return String(value);
    const millions =
      Math.abs(num) >= 1_000_000 ? num / 1_000_000 : num;
    return `${millions.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}M`;
  }
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return String(value);
}

function formatDateLabel(dateStr) {
  if (dateStr === "Unknown") return dateStr;
  try {
    const d = new Date(dateStr);
    if (Number.isNaN(d.getTime())) return dateStr;
    return d.toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  } catch {
    return dateStr;
  }
}

/** Normalize to YYYY-MM-DD for comparison (handles Date, ISO string, or "YYYY-MM-DD"). */
function normalizeAsOfDate(value) {
  if (value == null || value === "") return "";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value)) return value.split("T")[0];
  try {
    const d = new Date(value);
    if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  } catch {}
  return "";
}

/** Ensure content has paragraph breaks: if no \n\n, split by \n so we don't show one giant block. */
function ensureParagraphBreaks(content) {
  if (content == null || typeof content !== "string") return content;
  const trimmed = content.trim();
  if (!trimmed) return trimmed;
  if (/\n\n+/.test(trimmed)) return trimmed;
  return trimmed.split(/\n/).filter((line) => line.trim()).join("\n\n");
}

/** Markdown component overrides so assistant messages use our chat styles. */
const chatMarkdownComponents = {
  p: ({ node, children, ...props }) => {
    const flat = Array.isArray(children) ? children.join("") : String(children ?? "");
    const isToolLine = /🔧\s*Tool used\.?/.test(flat);
    return (
      <p
        {...props}
        className={`chat-message-p ${isToolLine ? "chat-tool-line" : ""}`}
      >
        {children}
      </p>
    );
  },
  strong: ({ children }) => <strong>{children}</strong>,
  code: ({ node, inline, className, children, ...props }) =>
    inline ? (
      <code className="chat-inline-code" {...props}>{children}</code>
    ) : (
      <code className="chat-code-block" {...props}>{children}</code>
    ),
  ul: ({ children }) => <ul className="chat-message-list">{children}</ul>,
  ol: ({ children }) => <ol className="chat-message-list">{children}</ol>,
  li: ({ children }) => <li className="chat-message-li">{children}</li>,
  h1: ({ children }) => <h3 className="chat-message-h">{children}</h3>,
  h2: ({ children }) => <h3 className="chat-message-h">{children}</h3>,
  h3: ({ children }) => <h3 className="chat-message-h">{children}</h3>,
  pre: ({ children }) => <pre className="chat-pre">{children}</pre>,
};

/** Format assistant chat content with markdown (headers, **bold**, `code`, lists) and paragraph structure. */
function formatChatMessage(content) {
  if (content == null || typeof content !== "string") return content;
  const withBreaks = ensureParagraphBreaks(content);
  if (!withBreaks) return withBreaks;
  return (
    <ReactMarkdown components={chatMarkdownComponents}>
      {withBreaks}
    </ReactMarkdown>
  );
}

const BREAKDOWN_HIDDEN_COLUMNS = ["calculation_timestamp", "snapshot_id"];

function getBreakdownColumns(breakdownRows) {
  if (!breakdownRows || !breakdownRows.length) return [];
  return Object.keys(breakdownRows[0]).filter(
    (k) => !BREAKDOWN_HIDDEN_COLUMNS.includes(k)
  );
}

function getBreakdownColumnLabel(colKey) {
  if (colKey === "as_of_date") return "Date";
  if (colKey === "gl_account" || colKey === "account_id") return "Account ID";
  return colKey.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatBreakdownCell(value, colKey) {
  if (value == null || value === "") return "—";
  if (colKey === "as_of_date") {
    const dateStr = typeof value === "string" ? value.split("T")[0] : String(value);
    try {
      const d = new Date(dateStr);
      if (Number.isNaN(d.getTime())) return dateStr;
      return d.toLocaleDateString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
      });
    } catch {
      return dateStr;
    }
  }
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return String(value);
}

function isBreakdownSumColumn(colKey, breakdownType) {
  const lower = (colKey || "").toLowerCase();
  if (breakdownType === "hqla") {
    return lower.includes("gross_balance") || lower.includes("net_hqla_value");
  }
  if (breakdownType === "cash_outflows") {
    return lower.includes("gross_balance") || lower.includes("cash_outflow");
  }
  return false;
}

function computeBreakdownTotals(breakdownRows, columns, breakdownType) {
  const totals = {};
  for (const col of columns) {
    if (!isBreakdownSumColumn(col, breakdownType)) continue;
    let sum = 0;
    for (const row of breakdownRows) {
      const v = row[col];
      const num = Number(v);
      if (Number.isFinite(num)) sum += num;
    }
    totals[col] = sum;
  }
  return totals;
}

/** Cash inflows from HQLA breakdown: same UI calculation as modal, with fallback for any numeric amount column. */
function cashInflowsFromHqlaBreakdown(breakdownRows) {
  if (!breakdownRows || !breakdownRows.length) return null;
  const cols = getBreakdownColumns(breakdownRows);
  const totals = computeBreakdownTotals(breakdownRows, cols, "hqla");
  const grossKey = Object.keys(totals).find((k) => k.toLowerCase().includes("gross_balance"));
  if (grossKey != null && totals[grossKey] != null) return totals[grossKey];
  const netKey = Object.keys(totals).find((k) => k.toLowerCase().includes("net_hqla"));
  if (netKey != null && totals[netKey] != null) return totals[netKey];
  const first = Object.values(totals).find((v) => v != null && Number.isFinite(v));
  if (first != null) return first;
  const skip = new Set(["as_of_date", "snapshot_id", "calculation_timestamp", "created_by", "date", "id"]);
  const amountLike = cols.filter(
    (c) => !skip.has(c.toLowerCase()) && /gross|amount|balance|value|inflow|hqla/.test(c.toLowerCase())
  );
  for (const col of amountLike) {
    let sum = 0;
    let hasAny = false;
    for (const row of breakdownRows) {
      const v = row[col];
      const num = Number(v);
      if (Number.isFinite(num)) {
        sum += num;
        hasAny = true;
      }
    }
    if (hasAny) return sum;
  }
  for (const col of cols) {
    if (skip.has(col.toLowerCase())) continue;
    let sum = 0;
    let hasAny = false;
    for (const row of breakdownRows) {
      const v = row[col];
      const num = Number(v);
      if (Number.isFinite(num)) {
        sum += num;
        hasAny = true;
      }
    }
    if (hasAny) return sum;
  }
  return null;
}

export default function App() {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [modal, setModal] = useState({
    open: false,
    type: null,
    title: "",
    row: null,
    loading: false,
    data: null,
    error: null,
  });
  const [chatMessages, setChatMessages] = useState([]);
  const [chatInput, setChatInput] = useState("");
  const [chatLoading, setChatLoading] = useState(false);
  const [chatError, setChatError] = useState("");
  const [streamingContent, setStreamingContent] = useState("");
  const [streamingStatus, setStreamingStatus] = useState("");
  const [streamingToolWorking, setStreamingToolWorking] = useState(false);
  const [reasoningSteps, setReasoningSteps] = useState([]);
  const [recoRecords, setRecoRecords] = useState([]);
  const [recoLoading, setRecoLoading] = useState(false);
  const [recoError, setRecoError] = useState("");
  const [summaryText, setSummaryText] = useState("");
  const [summaryLoading, setSummaryLoading] = useState(false);
  const [summaryError, setSummaryError] = useState("");
  const [suggestedQuestions, setSuggestedQuestions] = useState([]);
  const [suggestedQuestionsLoading, setSuggestedQuestionsLoading] = useState(false);
  const [intradayTab, setIntradayTab] = useState("chart");
  const [hqlaBreakdownLatest, setHqlaBreakdownLatest] = useState({ date: null, data: null });
  const [chatPopupOpen, setChatPopupOpen] = useState(false);
  const [chartTooltip, setChartTooltip] = useState(null);
  const [pdfReportLoading, setPdfReportLoading] = useState(false);
  const [pdfReportUrl, setPdfReportUrl] = useState(null);
  const [theme, setTheme] = useState(() => {
    try {
      return (typeof window !== "undefined" && window.localStorage.getItem("lcr-theme")) || "light";
    } catch {
      return "light";
    }
  });

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    try {
      window.localStorage.setItem("lcr-theme", theme);
    } catch {}
  }, [theme]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError("");
    fetch(`${API_BASE}/api/lcr/results`)
      .then((r) => {
        if (!r.ok) throw new Error(`Failed to load LCR results (${r.status})`);
        return r.json();
      })
      .then((data) => {
        if (!cancelled) setRows(Array.isArray(data) ? data : []);
      })
      .catch((e) => {
        if (!cancelled) setError(e.message || "Failed to load LCR results.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setSuggestedQuestionsLoading(true);
    fetch(`${API_BASE}/api/lcr/suggested-questions`)
      .then((r) => {
        if (!r.ok) throw new Error("Failed to load suggested questions");
        return r.json();
      })
      .then((data) => {
        if (!cancelled) setSuggestedQuestions(Array.isArray(data?.questions) ? data.questions : []);
      })
      .catch(() => {
        if (!cancelled) setSuggestedQuestions([]);
      })
      .finally(() => {
        if (!cancelled) setSuggestedQuestionsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setRecoLoading(true);
    setRecoError("");
    fetch(`${API_BASE}/api/lcr/reco`)
      .then((r) => {
        if (!r.ok) throw new Error(`Failed to load reco table (${r.status})`);
        return r.json();
      })
      .then((data) => {
        if (!cancelled) setRecoRecords(Array.isArray(data) ? data : []);
      })
      .catch((e) => {
        if (!cancelled) setRecoError(e.message || "Failed to load reco table.");
      })
      .finally(() => {
        if (!cancelled) setRecoLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setSummaryLoading(true);
    setSummaryError("");
    fetch(`${API_BASE}/api/lcr/executive-summary/summarized`)
      .then((r) => {
        if (!r.ok) throw new Error(r.status === 502 ? "Summary service unavailable." : `Failed to load summary (${r.status})`);
        return r.json();
      })
      .then((data) => {
        if (!cancelled) setSummaryText(data?.summary ?? "");
      })
      .catch((e) => {
        if (!cancelled) setSummaryError(e.message || "Failed to load summarized executive summary.");
      })
      .finally(() => {
        if (!cancelled) setSummaryLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!rows.length) {
      setHqlaBreakdownLatest({ date: null, data: null });
      return;
    }
    const sorted = [...rows].sort((a, b) => {
      const aDate = normalizeAsOfDate(a.as_of_date);
      const bDate = normalizeAsOfDate(b.as_of_date);
      return bDate.localeCompare(aDate);
    });
    const latestDateStr = normalizeAsOfDate(sorted[0].as_of_date);
    if (!latestDateStr) return;
    setHqlaBreakdownLatest((prev) => (prev.date === latestDateStr ? prev : { date: latestDateStr, data: null }));
    let cancelled = false;
    fetch(`${API_BASE}/api/lcr/hqla-breakdown?date=${encodeURIComponent(latestDateStr)}`)
      .then((r) => (r.ok ? r.json() : []))
      .then((data) => {
        if (!cancelled) setHqlaBreakdownLatest({ date: latestDateStr, data: Array.isArray(data) ? data : [] });
      })
      .catch(() => {
        if (!cancelled) setHqlaBreakdownLatest((prev) => (prev.date === latestDateStr ? { ...prev, data: [] } : prev));
      });
    return () => { cancelled = true; };
  }, [rows]);

  const byDate = useMemo(() => groupByDate(rows), [rows]);

  /** Reco records for the latest timestamp only. */
  const recoLatestOnly = useMemo(() => {
    if (!recoRecords.length) return [];
    const tsCol = "timestamp";
    let latest = recoRecords[0][tsCol];
    for (const row of recoRecords) {
      const t = row[tsCol];
      if (t != null && (latest == null || String(t) > String(latest))) latest = t;
    }
    if (latest == null) return recoRecords;
    return recoRecords.filter((row) => row[tsCol] == null || String(row[tsCol]) === String(latest));
  }, [recoRecords]);

  /** Group reco records by tag for Executive Summary (latest timestamp only). */
  const recoByTag = useMemo(() => {
    if (!recoLatestOnly.length) return [];
    const tagCol = RECO_TAG_COLUMN;
    const map = new Map();
    for (const row of recoLatestOnly) {
      const tag = row[tagCol] != null ? String(row[tagCol]).trim() : "Other";
      if (!map.has(tag)) map.set(tag, []);
      map.get(tag).push(row);
    }
    return Array.from(map.entries());
  }, [recoLatestOnly]);

  const openBreakdown = (type, row) => {
    const title =
      type === "hqla" ? "HQLA Breakdown" : "Cash Outflows Breakdown";
    setModal({
      open: true,
      type,
      title,
      row,
      loading: true,
      data: null,
      error: null,
    });
    const snapshotId = row.snapshot_id != null ? String(row.snapshot_id) : null;
    const rawDate = row.as_of_date;
    const dateParam =
      rawDate != null
        ? (typeof rawDate === "string" ? rawDate.split("T")[0] : String(rawDate))
        : null;
    const params = new URLSearchParams();
    if (snapshotId) params.set("snapshot_id", snapshotId);
    if (dateParam) params.set("date", dateParam);
    const path =
      type === "hqla"
        ? `${API_BASE}/api/lcr/hqla-breakdown`
        : `${API_BASE}/api/lcr/cash-outflows-breakdown`;
    const url = params.toString() ? `${path}?${params}` : path;
    fetch(url)
      .then((r) => {
        if (!r.ok) throw new Error(`Failed to load breakdown (${r.status})`);
        return r.json();
      })
      .then((data) => {
        setModal((m) =>
          m.open ? { ...m, loading: false, data: Array.isArray(data) ? data : [], error: null } : m
        );
      })
      .catch((e) => {
        setModal((m) =>
          m.open ? { ...m, loading: false, data: null, error: e.message } : m
        );
      });
  };

  const closeModal = () => {
    setModal((m) => ({ ...m, open: false }));
  };

  const openGLModal = (row) => {
    const rawDate = row.as_of_date;
    const dateParam =
      rawDate != null
        ? (typeof rawDate === "string" ? rawDate.split("T")[0] : String(rawDate))
        : null;
    const dateLabel = dateParam ? formatDateLabel(dateParam) : "Unknown";
    setModal({
      open: true,
      type: "gl",
      title: `Account Transaction Data — ${dateLabel}`,
      row,
      loading: true,
      data: null,
      error: null,
    });
    if (!dateParam) {
      setModal((m) => (m.open ? { ...m, loading: false, error: "No date for this row." } : m));
      return;
    }
    const url = `${API_BASE}/api/lcr/gl-data?date=${encodeURIComponent(dateParam)}`;
    fetch(url)
      .then(async (r) => {
        const body = await r.json().catch(() => null);
        if (!r.ok) {
          const msg = body?.detail ?? body?.message ?? `Failed to load GL data (${r.status})`;
          throw new Error(typeof msg === "string" ? msg : JSON.stringify(msg));
        }
        return body;
      })
      .then((data) => {
        setModal((m) =>
          m.open ? { ...m, loading: false, data: Array.isArray(data) ? data : [], error: null } : m
        );
      })
      .catch((e) => {
        setModal((m) =>
          m.open ? { ...m, loading: false, data: null, error: e.message } : m
        );
      });
  };

  const sendChat = async (optionalMessage) => {
    const text = (optionalMessage != null ? String(optionalMessage).trim() : (chatInput || "").trim());
    if (!text || chatLoading) return;
    setChatInput("");
    setChatMessages((prev) => [...prev, { role: "user", content: text }]);
    setChatLoading(true);
    setChatError("");
    setStreamingContent("");
    setStreamingStatus("");
    setStreamingToolWorking(false);
    setReasoningSteps([]);
    let accumulatedContent = "";
    let addedToMessages = false;
    const currentSteps = [];
    try {
      const res = await fetch(`${API_BASE}/api/lcr/chat/stream`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: text }),
      });
      if (!res.ok) {
        throw new Error(res.status === 502 ? "Chat service unavailable." : `Failed (${res.status})`);
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";
        for (const line of lines) {
          if (line.startsWith("data: ")) {
            try {
              const data = JSON.parse(line.slice(6));
              if (data.type === "status") {
                setStreamingStatus(data.message || "");
              } else if (data.type === "tool_call") {
                setStreamingToolWorking(true);
                if (data.sql || data.tool_name) {
                  currentSteps.push({
                    type: "call",
                    toolName: data.tool_name || "SQL Query",
                    sql: data.sql || "",
                    callId: data.call_id || "",
                    question: data.question || "",
                  });
                  setReasoningSteps([...currentSteps]);
                }
              } else if (data.type === "tool_result") {
                currentSteps.push({
                  type: "result",
                  callId: data.call_id || "",
                  output: data.output || "",
                });
                setReasoningSteps([...currentSteps]);
              } else if (data.type === "token") {
                setStreamingToolWorking(false);
                accumulatedContent += data.content ?? "";
                setStreamingContent(accumulatedContent);
              } else if (data.type === "done") {
                addedToMessages = true;
                setChatMessages((prev) => [
                  ...prev,
                  { role: "assistant", content: accumulatedContent, reasoning: [...currentSteps] },
                ]);
                setStreamingContent("");
                setStreamingStatus("");
                setStreamingToolWorking(false);
                setReasoningSteps([]);
              } else if (data.type === "error") {
                setChatError(data.message || "Something went wrong.");
                setStreamingContent("");
                setStreamingStatus("");
                setStreamingToolWorking(false);
              }
            } catch (_) {}
          }
        }
      }
      if (accumulatedContent && !addedToMessages) {
        setChatMessages((prev) => [
          ...prev,
          { role: "assistant", content: accumulatedContent, reasoning: [...currentSteps] },
        ]);
      }
    } catch (e) {
      setChatError(e.message || "Failed to send message.");
    } finally {
      setChatLoading(false);
      setStreamingContent("");
      setStreamingStatus("");
      setStreamingToolWorking(false);
    }
  };

  const renderCell = (row, col, value) => {
    const formatted = formatCell(value, col);
    if (isMillionsColumn(col)) {
      return (
        <button
          type="button"
          className="cell-link"
          onClick={() => openBreakdown(col.toLowerCase().includes("hqla") ? "hqla" : "cash_outflows", row)}
        >
          {formatted}
        </button>
      );
    }
    if (isLcrColumn(col)) {
      return (
        <button
          type="button"
          className="cell-link"
          onClick={() => openGLModal(row)}
          title="View GL data for this date"
        >
          {formatted}
        </button>
      );
    }
    return formatted;
  };

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-logo" aria-label="DataBank">
          <img className="app-logo-img" src={databankLogo} alt="DataBank" />
        </div>
        <h1 className="page-title">Intraday Liquidity Coverage Metrics and Analysis</h1>
        <a
          className="cfo-hub-link"
          href="https://cfo-office-hub-3036928383961086.aws.databricksapps.com/"
          target="_blank"
          rel="noopener noreferrer"
        >
          CFO Office Hub
        </a>
        <button
          type="button"
          className="chat-popup-trigger"
          onClick={() => setChatPopupOpen(true)}
          title="Chat with Liquidity Assistant"
          aria-label="Chat with Liquidity Assistant"
        >
          <span className="chat-popup-trigger-icon" aria-hidden>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
            </svg>
          </span>
          <span className="chat-popup-trigger-label">Chat with Liquidity Assistant</span>
        </button>
        <button
          type="button"
          className="theme-toggle"
          onClick={() => setTheme((t) => (t === "light" ? "dark" : "light"))}
          title={theme === "light" ? "Switch to dark mode" : "Switch to light mode"}
          aria-label={theme === "light" ? "Switch to dark mode" : "Switch to light mode"}
        >
          {theme === "light" ? (
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
          ) : (
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>
          )}
        </button>
        {error && <p className="error" style={{ margin: 0 }}>{error}</p>}
      </header>
      <div className="app-container">
        {/* Left top: Executive Summary – Gemini-formatted (Top 3, Risks, Forecasts) */}
        <div className="dashboard-section">
          <div className="dashboard-section-header">Executive Summary</div>
          <div className={`dashboard-section-body ${summaryLoading ? "loading" : ""}`}>
            {summaryLoading && "Summarizing with Databricks AI…"}
            {summaryError && <p className="error">{summaryError}</p>}
            {!summaryLoading && !summaryError && summaryText && (
              <div className="executive-summary-markdown">
                <ReactMarkdown
                  components={chatMarkdownComponents}
                  rehypePlugins={[rehypeRaw]}
                >
                  {applySummaryHighlights(summaryText)}
                </ReactMarkdown>
              </div>
            )}
            {!summaryLoading && !summaryError && !summaryText && !summaryError && (
              <p className="empty-message">No executive summary available.</p>
            )}
          </div>
        </div>
        {/* Bottom-left: LCR Ratio Trends (two tabs: Chart | Table) */}
        <div className="dashboard-section">
          <div className="dashboard-section-header dashboard-section-header-with-tabs">
            <span>LCR Ratio Trends</span>
            {!loading && !error && rows.length > 0 && (
              <div className="intraday-tabs">
                <button
                  type="button"
                  className={`intraday-tab ${intradayTab === "chart" ? "active" : ""}`}
                  onClick={() => setIntradayTab("chart")}
                >
                  Chart
                </button>
                <button
                  type="button"
                  className={`intraday-tab ${intradayTab === "table" ? "active" : ""}`}
                  onClick={() => setIntradayTab("table")}
                >
                  Table
                </button>
              </div>
            )}
          </div>
          <div className={`dashboard-section-body ${loading ? "loading" : ""}`}>
            {loading && "Loading LCR results…"}
            {!loading && error && <p className="error">{error}</p>}
            {!loading && !error && rows.length === 0 && <p className="empty-message">No LCR results found.</p>}
            {!loading && !error && rows.length > 0 && intradayTab === "chart" && (() => {
              const allRows = [...rows].sort((a, b) => {
                const aDate = a.as_of_date == null ? "" : String(a.as_of_date).split("T")[0];
                const bDate = b.as_of_date == null ? "" : String(b.as_of_date).split("T")[0];
                return bDate.localeCompare(aDate);
              });
              const latestRow = allRows[0];
              const latestDateStr = normalizeAsOfDate(latestRow.as_of_date);
              const chartData = getLcrChartData(rows);
              const hqlaVal = getMetricFromRow(latestRow, "hqla") ?? getMetricFromRow(latestRow, "hqla_amount") ?? getMetricFromRow(latestRow, "hqla_total") ?? getMetricFromRow(latestRow, "hqla_value");
              const breakdownMatch = latestDateStr && hqlaBreakdownLatest.date === latestDateStr && hqlaBreakdownLatest.data != null;
              const cashInflowsFromBreakdown = breakdownMatch && hqlaBreakdownLatest.data.length
                ? cashInflowsFromHqlaBreakdown(hqlaBreakdownLatest.data)
                : null;
              const cashInflowsVal = cashInflowsFromBreakdown ?? getMetricFromRow(latestRow, "cash_inflows") ?? getMetricFromRow(latestRow, "inflows") ?? hqlaVal;
              const cashOutflowsVal = getMetricFromRow(latestRow, "net_cash_outflows");
              const formatMetricB = (v) => {
                if (v == null || v === "") return "—";
                const num = Number(v);
                if (!Number.isFinite(num)) return String(v);
                if (Math.abs(num) >= 1_000_000_000) return `$${(num / 1_000_000_000).toFixed(1)}B`;
                if (Math.abs(num) >= 1_000_000) return `$${(num / 1_000_000).toFixed(1)}M`;
                return `$${num.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
              };
              const regulatoryStatus = latestRow.regulatory_status != null && String(latestRow.regulatory_status).trim() !== "" ? String(latestRow.regulatory_status).trim() : "—";
              const isSubmitted = /submitted|compliant|ok/i.test(regulatoryStatus);
              const renderChart = (opts) => {
                const { width: w, height: h, onPointHover } = opts;
                const pad = { top: 14, right: 14, bottom: 32, left: 36 };
                const cW = w - pad.left - pad.right;
                const cH = h - pad.top - pad.bottom;
                const values = chartData.map((d) => d.value);
                const yMin = Math.floor((Math.min(...values, 100) - 5) / 10) * 10;
                const yMax = Math.ceil((Math.max(...values, 100) + 5) / 10) * 10;
                const scaleY = (v) => pad.top + cH - ((v - yMin) / (yMax - yMin || 1)) * cH;
                const scaleX = (i) => pad.left + (i / Math.max(chartData.length - 1, 1)) * cW;
                const pathD = chartData.length ? `M ${chartData.map((d, i) => `${scaleX(i)} ${scaleY(d.value)}`).join(" L ")}` : "";
                const xLabelIndices = getXAxisLabelIndices(chartData.length, 6);
                const yTicks = [yMin, Math.round((yMin + yMax) / 2 / 10) * 10, yMax].filter((v) => v >= yMin && v <= yMax && (v === yMin || v === yMax || Math.abs(v - (yMin + yMax) / 2) < (yMax - yMin) * 0.6));
                if (yTicks.length === 2 && yMax - yMin > 20) yTicks.splice(1, 0, Math.round((yMin + yMax) / 2 / 10) * 10);
                const fontSizeAxis = 6;
                return (
                  <svg className="intraday-lcr-chart" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="xMidYMid meet">
                    <line x1={pad.left} y1={pad.top} x2={pad.left} y2={h - pad.bottom} stroke="var(--aura-border)" strokeWidth="1" />
                    <line x1={pad.left} y1={h - pad.bottom} x2={w - pad.right} y2={h - pad.bottom} stroke="var(--aura-border)" strokeWidth="1" />
                    {yTicks.map((v) => (
                      <g key={v}>
                        <line x1={pad.left} y1={scaleY(v)} x2={w - pad.right} y2={scaleY(v)} stroke="var(--aura-border)" strokeWidth="0.5" strokeDasharray="3,3" />
                        <text x={pad.left - 6} y={scaleY(v) + (fontSizeAxis / 2)} textAnchor="end" fontSize={fontSizeAxis} fill="var(--aura-muted)">{v}%</text>
                      </g>
                    ))}
                    {pathD && <path d={pathD} fill="none" stroke="var(--lcr-chart-line)" strokeWidth="1" strokeLinecap="round" strokeLinejoin="round" />}
                    {chartData.map((d, i) => (
                      <g
                        key={i}
                        onMouseEnter={(e) => onPointHover && onPointHover(e, i, d)}
                        onMouseLeave={() => onPointHover && onPointHover(null, null, null)}
                      >
                        <circle cx={scaleX(i)} cy={scaleY(d.value)} r={10} fill="transparent" />
                        <circle cx={scaleX(i)} cy={scaleY(d.value)} r={2.5} fill="var(--lcr-chart-line)" />
                      </g>
                    ))}
                    {xLabelIndices.map((i) => (
                      <text key={i} x={scaleX(i)} y={h - 8} textAnchor="middle" fontSize={fontSizeAxis} fill="var(--aura-muted)">{chartData[i].shortLabel}</text>
                    ))}
                  </svg>
                );
              };
              return (
                <>
                  <div className="intraday-overview">
                    <div className="intraday-top-row">
                      <div className="intraday-kpi-cards intraday-kpi-cards--small">
                        <div className="intraday-kpi-card intraday-kpi-card--hqla">
                          <div className="intraday-kpi-card-icon intraday-kpi-card-icon--blue" aria-hidden>
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"><rect x="4" y="4" width="16" height="6" rx="1"/><rect x="4" y="12" width="16" height="6" rx="1"/></svg>
                          </div>
                          <span className="intraday-kpi-card-title">HQLA</span>
                          <span className="intraday-kpi-card-value">{formatMetricB(hqlaVal)}</span>
                          <span className="intraday-kpi-card-desc">High Quality Liquid Assets | {latestDateStr || "—"}</span>
                        </div>
                        <div className="intraday-kpi-card intraday-kpi-card--inflows">
                          <div className="intraday-kpi-card-icon intraday-kpi-card-icon--green" aria-hidden>
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="5 12 12 19 19 12"/></svg>
                          </div>
                          <span className="intraday-kpi-card-title">CASH INFLOWS</span>
                          <span className="intraday-kpi-card-value">{formatMetricB(cashInflowsVal)}</span>
                          <span className="intraday-kpi-card-desc">Total gross (HQLA breakdown) | {latestDateStr || "—"}</span>
                        </div>
                        <div className="intraday-kpi-card intraday-kpi-card--outflows">
                          <div className="intraday-kpi-card-icon intraday-kpi-card-icon--red" aria-hidden>
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>
                          </div>
                          <span className="intraday-kpi-card-title">CASH OUTFLOWS</span>
                          <span className="intraday-kpi-card-value">{formatMetricB(cashOutflowsVal)}</span>
                          <span className="intraday-kpi-card-desc">Weighted outflows (LCR) | {latestDateStr || "—"}</span>
                        </div>
                      </div>
                      <div className="intraday-panel intraday-filing-panel intraday-filing-panel--side">
                        <h3 className="intraday-panel-title">2052a Filing Status</h3>
                        <div className="intraday-filing-date">{latestDateStr || "—"}</div>
                        <div className="intraday-filing-status-row">
                          <span className={`intraday-filing-status-badge ${isSubmitted ? "submitted" : ""}`}>
                            {isSubmitted ? "Submitted" : regulatoryStatus}
                          </span>
                          <button
                            type="button"
                            className="intraday-pdf-report-btn"
                            onClick={() => {
                              if (!latestDateStr) return;
                              setPdfReportLoading(true);
                              const url = `${API_BASE}/api/lcr/fr2052a-report?date=${encodeURIComponent(latestDateStr)}`;
                              fetch(url)
                                .then((r) => {
                                  if (!r.ok) throw new Error("Failed to generate PDF");
                                  return r.blob();
                                })
                                .then((blob) => {
                                  const u = URL.createObjectURL(blob);
                                  setPdfReportUrl(u);
                                })
                                .catch(() => {})
                                .finally(() => setPdfReportLoading(false));
                            }}
                            disabled={pdfReportLoading || !latestDateStr}
                            title="Download PDF report (LCR trends, HQLA, cash outflows for this date)"
                            aria-label="Download PDF report"
                          >
                            {pdfReportLoading ? (
                              <span className="intraday-pdf-report-spinner" aria-hidden />
                            ) : (
                              <>
                                <svg className="intraday-pdf-report-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden>
                                  <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
                                  <polyline points="14 2 14 8 20 8" />
                                  <path d="M12 18v-6" />
                                  <path d="M9 15h6" />
                                </svg>
                                <span className="intraday-pdf-report-label">PDF Report</span>
                              </>
                            )}
                          </button>
                        </div>
                      </div>
                    </div>
                    <div className="intraday-chart-row">
                      <div className="intraday-panel intraday-chart-panel">
                        <div
                          className="intraday-chart-wrap"
                          onMouseLeave={() => setChartTooltip(null)}
                        >
                          {chartTooltip && chartTooltip.d && (
                            <div
                              className="intraday-chart-tooltip"
                              style={{
                                position: "fixed",
                                left: chartTooltip.x + 12,
                                top: chartTooltip.y + 8,
                                pointerEvents: "none",
                              }}
                            >
                              <span className="intraday-chart-tooltip-lcr">
                                LCR: {chartTooltip.d.value != null ? `${Number(chartTooltip.d.value).toFixed(1)}%` : "—"}
                              </span>
                              <span className="intraday-chart-tooltip-hqla">HQLA: {formatMetricB(chartTooltip.d.hqla)}</span>
                              <span className="intraday-chart-tooltip-outflows">Outflows: {formatMetricB(chartTooltip.d.outflows)}</span>
                            </div>
                          )}
                          {chartData.length === 0 ? (
                            <p className="empty-message">No LCR ratio data to plot.</p>
                          ) : (
                            renderChart({
                              width: 320,
                              height: 180,
                              onPointHover: (e, index, d) => {
                                if (e == null) setChartTooltip(null);
                                else setChartTooltip({ x: e.clientX, y: e.clientY, d });
                              },
                            })
                          )}
                        </div>
                      </div>
                    </div>
                  </div>
                </>
              );
            })()}
            {!loading && !error && rows.length > 0 && intradayTab === "table" && (() => {
              const allRows = [...rows].sort((a, b) => {
                const aDate = a.as_of_date == null ? "" : String(a.as_of_date).split("T")[0];
                const bDate = b.as_of_date == null ? "" : String(b.as_of_date).split("T")[0];
                return bDate.localeCompare(aDate);
              });
              const intradayCols = getIntradayColumns(allRows);
              const firstRow = allRows[0];
              return (
                <div className="table-wrap">
                  <table className="table">
                    <thead>
                      <tr>
                        {intradayCols.map(({ label, clickable, dataKey }) => (
                          <th key={dataKey}>
                            {clickable === "gl" ? (
                              <button
                                type="button"
                                className="cell-link th-link"
                                onClick={() => openGLModal(firstRow)}
                                title="View GL data for this date"
                              >
                                {label}
                              </button>
                            ) : clickable === "hqla" ? (
                              <button
                                type="button"
                                className="cell-link th-link"
                                onClick={() => openBreakdown("hqla", firstRow)}
                                title="View HQLA breakdown"
                              >
                                {label}
                              </button>
                            ) : clickable === "cash_outflows" ? (
                              <button
                                type="button"
                                className="cell-link th-link"
                                onClick={() => openBreakdown("cash_outflows", firstRow)}
                                title="View cash outflows breakdown"
                              >
                                {label}
                              </button>
                            ) : (
                              label
                            )}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {allRows.map((row, i) => (
                        <tr key={i}>
                          {intradayCols.map(({ dataKey }) => (
                            <td key={dataKey}>{renderCell(row, dataKey, row[dataKey])}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
          </div>
        </div>
      </div>

      {chatPopupOpen && (
        <div className="chat-popup-overlay" onClick={() => setChatPopupOpen(false)} aria-label="Close chat">
          <div className="chat-popup" onClick={(e) => e.stopPropagation()}>
            <div className="chat-popup-header">
              <span className="chat-popup-title">Chat with the Liquidity Analyst</span>
              <span className="chat-powered-by">Powered by Databricks Genie MCP</span>
              <button type="button" className="chat-popup-close" onClick={() => setChatPopupOpen(false)} aria-label="Close">×</button>
            </div>
            <div className="chat-popup-body">
              {chatError && <p className="error" style={{ marginBottom: "0.5rem" }}>{chatError}</p>}
              <div className="chat-window">
                {chatMessages.length === 0 && !streamingContent && !streamingStatus && (
                  <div className="chat-suggested-questions">
                    <p className="chat-suggested-intro">
                      {suggestedQuestionsLoading ? "Loading suggested questions…" : "Ask a question about LCR, or try one of these:"}
                    </p>
                    {!suggestedQuestionsLoading && (() => {
                      const list = suggestedQuestions.length ? suggestedQuestions : DEFAULT_SUGGESTED_CHAT_QUESTIONS;
                      const withoutLcrOverview = list.filter(
                        (q) => (q.category || "").trim().toLowerCase() !== "lcr overview"
                      );
                      const bySection = withoutLcrOverview.reduce((acc, q) => {
                        const cat = q.category || "General";
                        if (!acc[cat]) acc[cat] = [];
                        acc[cat].push(q);
                        return acc;
                      }, {});
                      const sections = Object.entries(bySection);
                      return (
                        <div className="chat-suggested-sections">
                          {sections.map(([category, questions]) => (
                            <div key={category} className="chat-suggested-section">
                              <h4 className="chat-suggested-section-title">{category}</h4>
                              <ul className="chat-suggested-list" aria-label={`Suggested: ${category}`}>
                                {questions.map((q, i) => (
                                  <li key={`${category}-${i}`}>
                                    <button
                                      type="button"
                                      className="chat-suggested-question"
                                      onClick={() => sendChat(q.text)}
                                    >
                                      <span className="chat-suggested-text">{q.text}</span>
                                    </button>
                                  </li>
                                ))}
                              </ul>
                            </div>
                          ))}
                        </div>
                      );
                    })()}
                  </div>
                )}
                {chatMessages.map((msg, i) => (
                  <div key={i} className={`chat-message ${msg.role === "user" ? "chat-user" : "chat-assistant"}`}>
                    {msg.role === "assistant" ? (
                      <>
                        <div className="chat-message-body">{formatChatMessage(msg.content)}</div>
                        {msg.reasoning && msg.reasoning.length > 0 && (
                          <details className="chat-reasoning">
                            <summary className="chat-reasoning-summary">
                              View reasoning ({msg.reasoning.filter(s => s.type === "call").length} {msg.reasoning.filter(s => s.type === "call").length === 1 ? "query" : "queries"})
                            </summary>
                            <div className="chat-reasoning-steps">
                              {msg.reasoning.map((step, j) => (
                                <div key={j} className={`reasoning-step reasoning-step-${step.type}`}>
                                  {step.type === "call" && step.sql && (
                                    <>
                                      <div className="reasoning-label">SQL Query</div>
                                      <pre className="reasoning-sql">{step.sql}</pre>
                                    </>
                                  )}
                                  {step.type === "result" && step.output && (
                                    <>
                                      <div className="reasoning-label">Data Retrieved</div>
                                      <pre className="reasoning-output">{
                                        typeof step.output === "string"
                                          ? step.output.substring(0, 1000)
                                          : JSON.stringify(step.output, null, 2).substring(0, 1000)
                                      }{(typeof step.output === "string" ? step.output.length : JSON.stringify(step.output).length) > 1000 ? "\n..." : ""}</pre>
                                    </>
                                  )}
                                </div>
                              ))}
                            </div>
                          </details>
                        )}
                      </>
                    ) : (
                      msg.content
                    )}
                  </div>
                ))}
                {(streamingStatus || streamingContent || streamingToolWorking || reasoningSteps.length > 0) && (
                  <div className="chat-message chat-assistant chat-streaming">
                    {streamingStatus && (
                      <div className="chat-streaming-status">{streamingStatus}</div>
                    )}
                    {streamingToolWorking && (
                      <div className="chat-streaming-tool-badge" aria-live="polite">
                        System is thinking…
                      </div>
                    )}
                    {reasoningSteps.length > 0 && (
                      <details className="chat-reasoning chat-reasoning-live" open>
                        <summary className="chat-reasoning-summary">
                          Agent reasoning ({reasoningSteps.filter(s => s.type === "call").length} {reasoningSteps.filter(s => s.type === "call").length === 1 ? "query" : "queries"})
                        </summary>
                        <div className="chat-reasoning-steps">
                          {reasoningSteps.map((step, j) => (
                            <div key={j} className={`reasoning-step reasoning-step-${step.type}`}>
                              {step.type === "call" && step.sql && (
                                <>
                                  <div className="reasoning-label">SQL Query</div>
                                  <pre className="reasoning-sql">{step.sql}</pre>
                                </>
                              )}
                              {step.type === "result" && step.output && (
                                <>
                                  <div className="reasoning-label">Data Retrieved</div>
                                  <pre className="reasoning-output">{
                                    typeof step.output === "string"
                                      ? step.output.substring(0, 500)
                                      : JSON.stringify(step.output, null, 2).substring(0, 500)
                                  }</pre>
                                </>
                              )}
                            </div>
                          ))}
                        </div>
                      </details>
                    )}
                    {streamingContent && (
                      <div className="chat-streaming-content">
                        {formatChatMessage(streamingContent)}
                        <span className="chat-cursor" aria-hidden>▌</span>
                      </div>
                    )}
                  </div>
                )}
              </div>
              {chatMessages.length > 0 && !chatLoading && !streamingContent && !streamingStatus && (
                <div className="chat-back-row">
                  <button
                    type="button"
                    className="chat-back"
                    onClick={() => {
                      setChatMessages([]);
                      setChatError(null);
                      setChatInput("");
                    }}
                    title="Back to suggested questions"
                  >
                    ← Back to questions
                  </button>
                </div>
              )}
              <div className="chat-input-row">
                <textarea
                  className="chat-input"
                  placeholder="Type your message…"
                  value={chatInput}
                  onChange={(e) => setChatInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      sendChat();
                    }
                  }}
                  disabled={chatLoading}
                  rows={2}
                />
                <button type="button" className="chat-send" onClick={sendChat} disabled={chatLoading}>
                  {chatLoading ? "Sending…" : "Send"}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {pdfReportUrl && (
        <div
          className="pdf-report-popup-overlay"
          onClick={() => {
            URL.revokeObjectURL(pdfReportUrl);
            setPdfReportUrl(null);
          }}
          aria-label="Close PDF report"
        >
          <div className="pdf-report-popup" onClick={(e) => e.stopPropagation()}>
            <div className="pdf-report-popup-header">
              <span className="pdf-report-popup-title">2052a PDF Report</span>
              <button
                type="button"
                className="pdf-report-popup-close"
                onClick={() => {
                  URL.revokeObjectURL(pdfReportUrl);
                  setPdfReportUrl(null);
                }}
                aria-label="Close"
              >
                ×
              </button>
            </div>
            <div className="pdf-report-popup-body">
              <iframe title="2052a PDF Report" src={pdfReportUrl} className="pdf-report-iframe" />
            </div>
          </div>
        </div>
      )}

      {modal.open && (
          <div className="modal-overlay" onClick={closeModal}>
            <div className="modal-content" onClick={(e) => e.stopPropagation()}>
              <div className="modal-header">
                <h3>{modal.title}</h3>
                <button type="button" className="modal-close" onClick={closeModal} aria-label="Close">
                  ×
                </button>
              </div>
              <div className="modal-body">
                {modal.loading && <p className="loading-row">Loading…</p>}
                {modal.error && <p className="error">{modal.error}</p>}
                {!modal.loading && !modal.error && modal.data && modal.data.length === 0 && (
                  <p className="empty-message">
                    {modal.type === "gl" ? "No GL data found for this date." : "No breakdown data found."}
                  </p>
                )}
                {!modal.loading && !modal.error && modal.data && modal.data.length > 0 && (() => {
                  const cols = getBreakdownColumns(modal.data);
                  const totals = computeBreakdownTotals(modal.data, cols, modal.type);
                  return (
                    <div className="table-wrap">
                      <table className="table">
                        <thead>
                          <tr>
                            {cols.map((col) => (
                              <th key={col}>{getBreakdownColumnLabel(col)}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {modal.data.map((r, i) => (
                            <tr key={i}>
                              {cols.map((col) => (
                                <td key={col}>{formatBreakdownCell(r[col], col)}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                        <tfoot className="table-total-row">
                          <tr>
                            {cols.map((col, idx) => {
                              if (totals[col] != null) {
                                const num = totals[col];
                                const millions = Math.abs(num) >= 1_000_000 ? num / 1_000_000 : num;
                                return (
                                  <td key={col}>
                                    {millions.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}M
                                  </td>
                                );
                              }
                              if (idx === 0) return <td key={col}>Total</td>;
                              return <td key={col} />;
                            })}
                          </tr>
                        </tfoot>
                      </table>
                    </div>
                  );
                })()}
              </div>
            </div>
          </div>
        )}
    </div>
  );
}
