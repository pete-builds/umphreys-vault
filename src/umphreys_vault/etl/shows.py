"""Setlist ETL: turn a flat list of ATU setlist rows into normalized records.

ATU's ``setlists`` methods return one row per song performance, with the
show, venue, and tour denormalized onto every row. The efficient backfill
unit is one year (``/setlists/showyear/{year}.json``): a single call returns
every song row for every show that year.

This module groups those rows by show date and, per show, upserts the venue,
tour, the song stubs referenced, the show record, and the ``setlist_entries``
in one transaction so a partial failure never leaves half a setlist.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Any

import asyncpg

from umphreys_vault.etl.upserts import (
    replace_setlist_entries_for_show,
    slugify,
    upsert_show,
    upsert_songs,
    upsert_tours,
    upsert_venues,
)

log = logging.getLogger(__name__)


def group_rows_by_show(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group flat setlist rows by ``showdate`` (ISO ``YYYY-MM-DD``).

    Rows missing a usable ``showdate`` are dropped. Within each show the rows
    are sorted by ``position`` so persistence order is stable.
    """
    out: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        date = r.get("showdate")
        if not isinstance(date, str) or len(date) < 10:
            continue
        out.setdefault(date[:10], []).append(r)
    for date_rows in out.values():
        date_rows.sort(key=lambda r: _safe_int(r.get("position")) or 0)
    return out


def _safe_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _venue_slug_for(row: dict[str, Any]) -> str | None:
    """Derive a venue slug from a setlist row.

    Setlist rows carry ``venuename``/``city``/``state``/``country`` but no
    venue slug, so we synthesise the ATU-style ``name-city-state-country``
    slug to match the ``/venues.json`` catalog slug (e.g.
    ``the-tabernacle-atlanta-ga-usa``).
    """
    name = row.get("venuename")
    if not name:
        return None
    parts = [name, row.get("city"), row.get("state"), row.get("country")]
    joined = " ".join(str(p) for p in parts if p)
    return slugify(joined)


def _venue_stub(row: dict[str, Any], slug: str) -> dict[str, Any]:
    return {
        "slug": slug,
        "venue_id": row.get("venue_id"),
        "venuename": row.get("venuename"),
        "city": row.get("city"),
        "state": row.get("state"),
        "country": row.get("country"),
    }


def _tour_slug_for(row: dict[str, Any]) -> str | None:
    name = row.get("tourname")
    return slugify(str(name)) if name else None


def _song_stub(row: dict[str, Any]) -> dict[str, Any] | None:
    slug = row.get("slug")
    if not slug:
        return None
    return {
        "slug": slug,
        "song_id": row.get("song_id"),
        "songname": row.get("songname"),
        "isoriginal": row.get("isoriginal", 1),
        "original_artist": row.get("original_artist"),
    }


def _resolve_venue_slug(row: dict[str, Any], venue_map: dict[int, str]) -> str | None:
    """Prefer the canonical catalog slug (by venue_id); fall back to synthesis.

    Resolving by ``venue_id`` keeps one row per venue even when the synthesised
    name-based slug differs from ATU's canonical ``/venues.json`` slug.
    """
    venue_id = _safe_int(row.get("venue_id"))
    if venue_id is not None and venue_id in venue_map:
        return venue_map[venue_id]
    return _venue_slug_for(row)


def _show_record(
    date: str, rows: list[dict[str, Any]], venue_map: dict[int, str]
) -> dict[str, Any]:
    """Synthesise a ``shows`` record from a show's setlist rows."""
    head = rows[0]
    venue_slug = _resolve_venue_slug(head, venue_map)
    tour_slug = _tour_slug_for(head)
    return {
        "date": date,
        "show_id": head.get("show_id"),
        "show_order": head.get("showorder"),
        "show_title": head.get("showtitle"),
        "venue_slug": venue_slug,
        "tour_slug": tour_slug,
        "show_notes": head.get("shownotes"),
        "permalink": head.get("permalink"),
        "show_year": head.get("showyear"),
    }


async def _persist_one_show(
    pool: asyncpg.Pool, date: str, rows: list[dict[str, Any]], venue_map: dict[int, str]
) -> dict[str, int]:
    """Write one show + its venue/tour/songs/setlist_entries in a transaction."""
    head = rows[0]
    show = _show_record(date, rows, venue_map)

    venue_slug = show["venue_slug"]
    venue_id = _safe_int(head.get("venue_id"))
    # Only stub-insert a venue the catalog doesn't already cover. When the
    # venue_id is in the catalog map, the canonical row already exists (with
    # richer fields) and the show simply references it — inserting a stub under
    # a divergent slug would duplicate the venue / collide on venue_id.
    in_catalog = venue_id is not None and venue_id in venue_map
    venue_stub = _venue_stub(head, venue_slug) if (venue_slug and not in_catalog) else None
    tour_slug = show["tour_slug"]
    tour_stub = (
        {"slug": tour_slug, "tour_id": head.get("tour_id"), "tourname": head.get("tourname")}
        if tour_slug
        else None
    )

    song_stubs: dict[str, dict[str, Any]] = {}
    for r in rows:
        stub = _song_stub(r)
        if stub is not None:
            song_stubs[stub["slug"]] = stub

    counts = {"shows": 1, "setlist_entries": 0}
    show_date = dt.date.fromisoformat(date)

    async with pool.acquire() as conn, conn.transaction():
        if venue_stub:
            await upsert_venues(conn, [venue_stub])
        if tour_stub:
            await upsert_tours(conn, [tour_stub])
        if song_stubs:
            await upsert_songs(conn, list(song_stubs.values()))
        await upsert_show(conn, show)
        counts["setlist_entries"] = await replace_setlist_entries_for_show(conn, show_date, rows)
    return counts


async def load_setlist_rows(
    pool: asyncpg.Pool,
    rows: list[dict[str, Any]],
    venue_map: dict[int, str] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Persist a batch of flat ATU setlist rows.

    Groups by show date, then writes each show in its own transaction. Used
    for both a full year (backfill) and a single show (refresh). ``venue_map``
    (venue_id -> canonical slug, from the venue catalog) lets shows reference
    canonical venues; when omitted, venue slugs are synthesised from the name.
    """
    venue_map = venue_map or {}
    grouped = group_rows_by_show(rows)
    totals: dict[str, int] = {"shows": 0, "setlist_entries": 0, "errors": 0}

    for date, date_rows in sorted(grouped.items()):
        try:
            if dry_run:
                totals["shows"] += 1
                totals["setlist_entries"] += len(date_rows)
                continue
            counts = await _persist_one_show(pool, date, date_rows, venue_map)
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v
        except Exception as exc:
            totals["errors"] += 1
            log.exception("setlist ETL failed", extra={"date": date, "error": str(exc)})
    return totals


async def find_max_show_date(pool: asyncpg.Pool) -> str | None:
    async with pool.acquire() as conn:
        row = await conn.fetchval("SELECT TO_CHAR(MAX(date), 'YYYY-MM-DD') FROM shows")
    return str(row) if row is not None else None
