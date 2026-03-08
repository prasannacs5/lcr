from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.routes import health, lcr

app = FastAPI()
app.include_router(health.router, prefix="/api")
app.include_router(lcr.router, prefix="/api")

static_dir = Path(__file__).resolve().parent.parent / "frontend" / "dist"
assets_dir = static_dir / "assets"
if static_dir.exists():
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    index_file = static_dir / "index.html"

    @app.get("/")
    def serve_frontend() -> FileResponse:
        return FileResponse(index_file)
