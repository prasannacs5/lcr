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
