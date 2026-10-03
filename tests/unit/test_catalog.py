"""The venue catalog loader must hand the stored venue_id -> slug map to the upsert.

Without it, upsert_venues cannot tell a renamed venue from a new one, and a slug
change upstream kills every refresh on the venue_id unique constraint (2026-09-28).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from umphreys_vault.etl import catalog


@pytest.mark.asyncio
async def test_load_venues_passes_the_stored_slug_map(monkeypatch: pytest.MonkeyPatch) -> None:
    client = MagicMock()
    client.venues = AsyncMock(return_value=[{"venue_id": 1547, "slug": "new-slug"}])
    stored = {1547: "old-slug"}
    monkeypatch.setattr(catalog, "venue_id_slug_map", AsyncMock(return_value=stored))
    upsert = AsyncMock(return_value=1)
    monkeypatch.setattr(catalog, "upsert_venues", upsert)

    conn = object()
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    assert await catalog.load_venues(client, pool) == 1
    args: tuple[Any, ...] = upsert.await_args.args
    assert args[0] is conn
    assert args[2] == stored


@pytest.mark.asyncio
async def test_load_venues_dry_run_touches_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    client = MagicMock()
    client.venues = AsyncMock(return_value=[{}, {}])
    lookup = AsyncMock()
    monkeypatch.setattr(catalog, "venue_id_slug_map", lookup)

    assert await catalog.load_venues(client, MagicMock(), dry_run=True) == 2
    lookup.assert_not_awaited()
