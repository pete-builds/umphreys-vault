"""Status endpoint handler tests.

We call the route handlers directly with a stubbed pool rather than spinning
up the lifespan (which would connect to a real Postgres).
"""

from __future__ import annotations

from typing import Any

import pytest

from umphreys_vault import status as status_mod


@pytest.mark.asyncio
async def test_healthz_ok() -> None:
    body = await status_mod.healthz()
    assert body["status"] == "ok"
    assert "version" in body


@pytest.mark.asyncio
async def test_status_handler_reads_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _schema_version(_pool: Any) -> int:
        return 1

    async def _row_counts(_pool: Any) -> dict[str, int]:
        return {"shows": 3}

    async def _last_run(_pool: Any) -> dict[str, Any] | None:
        return None

    monkeypatch.setattr(status_mod.db, "schema_version", _schema_version)
    monkeypatch.setattr(status_mod.db, "table_row_counts", _row_counts)
    monkeypatch.setattr(status_mod.db, "last_etl_run", _last_run)
    status_mod.app.state.pool = object()

    body = await status_mod.status()
    assert body["schema_version"] == 1
    assert body["row_counts"] == {"shows": 3}
