# LCR

LCR app with Python FastAPI backend and React frontend, deployable to Databricks Apps.

## Auth

- **Local**: use `DATABRICKS_TOKEN` (Personal Access Token).
- **Databricks Apps**: OAuth M2M with the app’s service principal (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` are set by the Apps platform; optionally use `DATABRICKS_APP_APPLICATION_ID` in `app.yaml`).

## Environment Variables

For local development, set:

```bash
export DATABRICKS_HOST="https://<your-workspace-host>"
export DATABRICKS_TOKEN="<your-personal-access-token>"
```

For Databricks Apps (service principal), the platform provides:

- `DATABRICKS_HOST`
- `DATABRICKS_CLIENT_ID`
- `DATABRICKS_CLIENT_SECRET`

No PAT is required when running as an App.

## Install & Run Locally

### Backend (FastAPI)

```bash
cd databricks/lcr
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend (React + Vite)

```bash
cd databricks/lcr/frontend
npm install
npm run dev
```

The frontend dev server proxies `/api` to `http://localhost:8000`.

## Build Frontend for Deployment

```bash
cd databricks/lcr/frontend
npm run build
```

Output is in `frontend/dist`. The FastAPI app serves:

- `/` → `frontend/dist/index.html`
- `/assets/*` → `frontend/dist/assets/*`

## Databricks Apps Deployment

1. Create your app in the Databricks workspace and note its **Application ID**.
2. In `app.yaml`, set `DATABRICKS_APP_APPLICATION_ID` to that value (replace `<your-databricks-app-application-id>`).
3. Ensure `frontend/dist` exists (run `npm run build` in `frontend`).
4. Sync to the workspace, e.g.:
   ```bash
   databricks sync --watch . /Workspace/Users/<user>/lcr
   ```

## Data pipeline and operational serving

The LCR data journey runs raw to governed gold through a Lakeflow pipeline, and the app
can serve reads from a Lakebase operational store.

### Lakeflow Declarative Pipeline (raw to gold)

`notebook/lcr_lakeflow_pipeline` ingests the raw general ledger, HQLA haircut, and runoff
feeds with Auto Loader and transforms them through a bronze / silver / gold medallion,
applying Basel III haircuts and runoff rates in gold. The accounting identity
`Assets = Liabilities + Equity` is enforced as a declarative data-quality expectation:
imbalanced GL snapshots are quarantined for review rather than patched with a plug entry.

The pipeline targets the parallel schema `cfo.aura_bank_lakeflow`, isolated from the live
`cfo.aura_bank` tables the app serves.

```bash
# 1. Land raw files once (reads cfo.aura_bank, writes to a new landing volume)
#    run notebook/lcr_lakeflow_seed
# 2. Create and run the pipeline
databricks pipelines create --json @resources/lcr_lakeflow_pipeline.json
```

### Lakebase operational serving (optional, flag-gated)

`notebook/lcr_lakebase_serving` provisions a Lakebase (managed Postgres) instance and
syncs the governed gold LCR results into it for low-latency reads. The app reads from
Lakebase only when `LCR_USE_LAKEBASE=true`; otherwise it uses the SQL warehouse exactly as
before. Any Lakebase error falls back to the warehouse. The read path is in
`backend/lib/lakebase_client.py`.

```bash
# app.yaml env to enable (default off):
LCR_USE_LAKEBASE=true
LCR_LAKEBASE_INSTANCE=lcr-lakebase
LCR_LAKEBASE_HOST=<instance read_write_dns>
LCR_LAKEBASE_DATABASE=databricks_postgres
LCR_LAKEBASE_TABLE=public.lcr_results_latest
```
