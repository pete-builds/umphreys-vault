"""Tiny read-only FastAPI status endpoint.

Exposes ``/healthz`` (always 200 if process is up) and ``/status`` with
``{schema_version, row_counts, last_etl_run}``. Bound to LAN/Tailscale only;
never exposes secrets.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from umphreys_vault import __version__, db
from umphreys_vault.config import get_settings


@asynccontextmanager
async def _lifespan(app: FastAPI) -> Any:
    settings = get_settings()
    pool = await db.connect(settings)
    app.state.pool = pool
    app.state.settings = settings
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(
    title="umphreys-vault status",
    version=__version__,
    lifespan=_lifespan,
)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/status")
async def status() -> dict[str, Any]:
    pool = app.state.pool
    return {
        "version": __version__,
        "schema_version": await db.schema_version(pool),
        "row_counts": await db.table_row_counts(pool),
        "last_etl_run": await db.last_etl_run(pool),
    }
