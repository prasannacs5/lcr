# LCR Liquidity Analysis - Databricks Asset Bundle

Intraday Liquidity Coverage Ratio (LCR) metrics, analysis, and AI-powered insights for Aura Bank.

## Components

| Component | Description |
|-----------|-------------|
| **LCR App** | FastAPI + React dashboard with LCR metrics, HQLA breakdown, chat assistant, PDF reports |
| **Hourly GL Pipeline** | Hydrates general ledger data and validates accounting equation every hour |
| **Daily Snapshot Job** | Captures LCR snapshot, calculates ratios, generates AI executive summaries daily at 6 AM ET |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- SQL Warehouse (serverless recommended)
- Access to foundation model serving endpoints (Claude Sonnet 4, Gemini 2.5 Pro)
- Genie Space created with the LCR tables (manual step post-deploy)

## Deployment

### Configure variables

Edit `databricks.yml` or pass variables at deploy time:

```bash
databricks bundle deploy -t dev \
  --var warehouse_id=<your-warehouse-id> \
  --var catalog=<your-catalog> \
  --var schema=aura_bank
```

### Deploy

```bash
# Deploy all resources
databricks bundle deploy -t dev

# Run initial data setup (first time only)
databricks bundle run lcr_hourly_gl_pipeline -t dev

# Run daily snapshot (first time only)
databricks bundle run lcr_daily_snapshot -t dev
```

### Post-deploy

1. Create a Genie Space in the Databricks UI with the 7 LCR tables
2. Update the `genie_space_id` variable with the new Space ID
3. Redeploy: `databricks bundle deploy -t dev`

## Data Model

| Table | Description |
|-------|-------------|
| `general_ledger` | Account balances with asset/liability/equity classifications |
| `haircuts` | Regulatory haircut rates for HQLA calculation |
| `runoff_rates` | Fed runoff rates for cash outflow calculation |
| `lcr_results` | Daily LCR snapshots with ratio, HQLA, outflows, compliance status |
| `hqla_breakdown` | Detailed HQLA breakdown by asset type per snapshot |
| `cash_outflows_breakdown` | Detailed cash outflow breakdown by liability type per snapshot |
| `reco_table` | AI-generated executive summaries, risk assessments, forecasts |

## Architecture

```
[Hourly Job] → gl_hydrate → gl_validation → general_ledger table
[Daily Job]  → LCR_Calculation (UC function + stored procedure) → lcr_results, hqla_breakdown, cash_outflows_breakdown
             → generate_recommendations (Gemini) → reco_table
[App]        → FastAPI reads from tables → React dashboard + Chat (LLM + SQL/Genie)
```
