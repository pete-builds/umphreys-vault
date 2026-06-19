"""ATU enrichment: jam charts and guest appearances.

Runs AFTER shows + setlists are loaded so foreign keys resolve. Both the
``/jamcharts.json`` and ``/appearances.json`` endpoints return their full
corpus in one call, so each is a single fetch + idempotent table replace.
"""

from __future__ import annotations

import logging

import asyncpg

from umphreys_vault.clients.atu import ATUClient
from umphreys_vault.etl.upserts import upsert_appearances, upsert_jam_chart_entries

log = logging.getLogger(__name__)


async def load_jam_charts(client: ATUClient, pool: asyncpg.Pool, dry_run: bool = False) -> int:
    """Pull the full ATU jam chart and replace the table contents."""
    rows = await client.jamcharts()
    log.info("Fetched jam chart", extra={"count": len(rows)})
    if dry_run:
        return len(rows)
    async with pool.acquire() as conn:
        return await upsert_jam_chart_entries(conn, rows)


async def load_appearances(client: ATUClient, pool: asyncpg.Pool, dry_run: bool = False) -> int:
    """Pull the full ATU appearances list and replace the table contents."""
    rows = await client.appearances()
    log.info("Fetched appearances", extra={"count": len(rows)})
    if dry_run:
        return len(rows)
    async with pool.acquire() as conn:
        return await upsert_appearances(conn, rows)
