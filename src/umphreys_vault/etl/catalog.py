"""Catalog ETL: the full song and venue catalogs.

These pull the complete ``/songs.json`` and ``/venues.json`` lists so the
``songs`` and ``venues`` tables hold every catalog row, not just the ones
referenced by loaded setlists. Run after (or alongside) the setlist load so
the richer catalog fields (e.g. a venue's canonical slug, a song's
``original_artist``) land even for songs/venues not yet performed in the
loaded window.
"""

from __future__ import annotations

import logging

import asyncpg

from umphreys_vault.clients.atu import ATUClient
from umphreys_vault.etl.upserts import upsert_songs, upsert_venues

log = logging.getLogger(__name__)


async def load_songs(client: ATUClient, pool: asyncpg.Pool, dry_run: bool = False) -> int:
    """Pull the full ATU song catalog and upsert it (~1128 rows)."""
    songs = await client.songs()
    log.info("Fetched songs", extra={"count": len(songs)})
    if dry_run:
        return len(songs)
    async with pool.acquire() as conn:
        return await upsert_songs(conn, songs)


async def load_venues(client: ATUClient, pool: asyncpg.Pool, dry_run: bool = False) -> int:
    """Pull the full ATU venue catalog and upsert it."""
    venues = await client.venues()
    log.info("Fetched venues", extra={"count": len(venues)})
    if dry_run:
        return len(venues)
    async with pool.acquire() as conn:
        return await upsert_venues(conn, venues)
