"""Idempotent upsert helpers used by every ETL module.

Each helper accepts a ``Connection`` and a list of dicts shaped to match the
corresponding table. They are intentionally simple — bulk INSERT ... ON
CONFLICT (...) DO UPDATE statements with explicit column lists so the binding
never silently drops a field.

ATU value quirks handled here
-----------------------------
- Booleans arrive as ``0``/``1`` integers (``isoriginal``, ``isjamchart``,
  ``isreprise``, ``isjam``) — coerced via :func:`_to_bool`.
- Timestamps arrive in space-separated SQL form (``"2023-02-26 04:29:59"``),
  not ISO-T, and use a ``"1000-01-01 ..."`` sentinel for "unknown" — both are
  handled by :func:`_to_ts`.
- ``personname`` and some text fields carry trailing newlines — trimmed.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import asyncpg

# Sentinel ATU uses for "no real timestamp". Treat as NULL.
_PLACEHOLDER_TS_PREFIX = "1000-01-01"


def _to_date(s: Any) -> dt.date | None:
    if not s or not isinstance(s, str):
        return None
    try:
        return dt.date.fromisoformat(s[:10])
    except ValueError:
        return None


def _to_ts(s: Any) -> dt.datetime | None:
    """Parse an ATU timestamp.

    ATU emits ``"YYYY-MM-DD HH:MM:SS"`` (space separator, naive). We treat the
    ``1000-01-01`` placeholder as NULL and assume UTC for the rest.
    """
    if not s or not isinstance(s, str):
        return None
    if s.startswith(_PLACEHOLDER_TS_PREFIX):
        return None
    try:
        return dt.datetime.fromisoformat(s)
    except ValueError:
        return None


def _to_text(v: Any) -> str | None:
    """Coerce arbitrary upstream values to a TEXT-bindable str (or None).

    asyncpg's encoder rejects an int where the column is TEXT, so every
    payload bound to a TEXT column flows through this helper. Empty strings
    are preserved (ATU uses ``""`` distinctly from null in places).
    """
    if v is None:
        return None
    if isinstance(v, str):
        return v.strip("\r\n") if v else v
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _to_int(v: Any) -> int | None:
    """Coerce arbitrary upstream values to int (or None) for INTEGER columns."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_bool(v: Any) -> bool:
    """Coerce ATU's 0/1 (or "0"/"1", or real bools) to a Python bool."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "t"}
    return False


def slugify(s: str) -> str:
    """ATU/phish.in slug convention: lowercase, hyphens, alphanumerics."""
    out: list[str] = []
    prev_dash = False
    for c in s.lower():
        if c.isalnum():
            out.append(c)
            prev_dash = False
        elif not prev_dash:
            out.append("-")
            prev_dash = True
    return "".join(out).strip("-")


# ---------------------------------------------------------------------------
# Venues
# ---------------------------------------------------------------------------


async def upsert_venues(conn: asyncpg.Connection, rows: list[dict[str, Any]]) -> int:
    """Upsert venue rows shaped from ``/venues.json`` setlist-derived stubs.

    Each row may come from the ``/venues.json`` catalog (``venuename``,
    ``venue_id``, ``slug``, ``city``, ``state``, ``country``) or from a
    setlist row stub (``venue_id``, ``venuename``, ``city``, ``state``,
    ``country`` — slug derived). The slug is the PK; ``venue_id`` is unique.
    """
    if not rows:
        return 0
    seen: dict[str, dict[str, Any]] = {}
    for r in rows:
        name = r.get("venuename") or r.get("name") or ""
        slug = r.get("slug") or (slugify(name) if name else None)
        if not slug:
            continue
        seen[slug] = {**r, "_slug": slug, "_name": name}

    payload = [
        (
            r["_slug"],
            _to_int(r.get("venue_id")),
            _to_text(r["_name"]) or "",
            _to_text(r.get("city")),
            _to_text(r.get("state")),
            _to_text(r.get("country")),
            _to_text(r.get("location")),
            _to_ts(r.get("created_at")),
            _to_ts(r.get("updated_at")),
        )
        for r in seen.values()
    ]
    await conn.executemany(
        """
        INSERT INTO venues (
            slug, venue_id, name, city, state, country, location,
            upstream_created_at, upstream_updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (slug) DO UPDATE SET
            venue_id = COALESCE(EXCLUDED.venue_id, venues.venue_id),
            name = EXCLUDED.name,
            city = COALESCE(EXCLUDED.city, venues.city),
            state = COALESCE(EXCLUDED.state, venues.state),
            country = COALESCE(EXCLUDED.country, venues.country),
            location = COALESCE(EXCLUDED.location, venues.location),
            upstream_created_at =
                COALESCE(EXCLUDED.upstream_created_at, venues.upstream_created_at),
            upstream_updated_at =
                COALESCE(EXCLUDED.upstream_updated_at, venues.upstream_updated_at),
            fetched_at = now()
        """,
        payload,
    )
    return len(payload)


# ---------------------------------------------------------------------------
# Tours
# ---------------------------------------------------------------------------


async def upsert_tours(conn: asyncpg.Connection, rows: list[dict[str, Any]]) -> int:
    """Upsert tour rows. ATU gives ``tour_id`` + ``tourname`` on setlist rows;
    slug is derived from the name (the slug is the PK)."""
    if not rows:
        return 0
    seen: dict[str, dict[str, Any]] = {}
    for r in rows:
        name = r.get("tourname") or r.get("name") or ""
        slug = r.get("slug") or (slugify(name) if name else None)
        if not slug:
            continue
        seen[slug] = {**r, "_slug": slug, "_name": name}

    payload = [
        (
            r["_slug"],
            _to_int(r.get("tour_id")),
            _to_text(r["_name"]) or "",
        )
        for r in seen.values()
    ]
    await conn.executemany(
        """
        INSERT INTO tours (slug, tour_id, name)
        VALUES ($1,$2,$3)
        ON CONFLICT (slug) DO UPDATE SET
            tour_id = COALESCE(EXCLUDED.tour_id, tours.tour_id),
            name = EXCLUDED.name,
            fetched_at = now()
        """,
        payload,
    )
    return len(payload)


# ---------------------------------------------------------------------------
# Songs
# ---------------------------------------------------------------------------


async def upsert_songs(conn: asyncpg.Connection, rows: list[dict[str, Any]]) -> int:
    """Upsert song catalog rows.

    Accepts either ``/songs.json`` rows (``id``, ``name``, ``slug``,
    ``isoriginal``, ``original_artist``, ``created_at``, ``updated_at``) or
    setlist-derived stubs (``song_id``, ``songname``, ``slug``,
    ``isoriginal``, ``original_artist``). The computed gap/debut/last/count
    columns are left untouched here — the aggregate pass owns them.
    """
    if not rows:
        return 0
    seen: dict[str, dict[str, Any]] = {}
    for r in rows:
        slug = r.get("slug")
        if not slug:
            continue
        seen[slug] = r

    payload = [
        (
            slug,
            _to_int(r.get("id") if r.get("id") is not None else r.get("song_id")),
            _to_text(r.get("name") or r.get("songname")) or slug,
            _to_bool(r.get("isoriginal", True)),
            _to_text(r.get("original_artist")) or None,
            _to_ts(r.get("created_at")),
            _to_ts(r.get("updated_at")),
        )
        for slug, r in seen.items()
    ]
    await conn.executemany(
        """
        INSERT INTO songs (
            slug, song_id, title, original, original_artist,
            upstream_created_at, upstream_updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (slug) DO UPDATE SET
            song_id = COALESCE(EXCLUDED.song_id, songs.song_id),
            title = EXCLUDED.title,
            original = EXCLUDED.original,
            original_artist = EXCLUDED.original_artist,
            upstream_created_at = COALESCE(EXCLUDED.upstream_created_at, songs.upstream_created_at),
            upstream_updated_at = COALESCE(EXCLUDED.upstream_updated_at, songs.upstream_updated_at),
            fetched_at = now()
        """,
        payload,
    )
    return len(payload)


# ---------------------------------------------------------------------------
# Shows
# ---------------------------------------------------------------------------


async def upsert_show(conn: asyncpg.Connection, show: dict[str, Any]) -> None:
    """Upsert one show record. Caller resolves venue/tour slugs first.

    ``show`` is a synthesised dict (see :func:`umphreys_vault.etl.shows`) with
    keys: ``date``, ``show_id``, ``show_order``, ``show_title``,
    ``venue_slug``, ``tour_slug``, ``show_notes``, ``permalink``,
    ``show_year``.
    """
    await conn.execute(
        """
        INSERT INTO shows (
            date, show_id, show_order, show_title, venue_slug, tour_slug,
            show_notes, permalink, show_year
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (date) DO UPDATE SET
            show_id = COALESCE(EXCLUDED.show_id, shows.show_id),
            show_order = EXCLUDED.show_order,
            show_title = EXCLUDED.show_title,
            venue_slug = COALESCE(EXCLUDED.venue_slug, shows.venue_slug),
            tour_slug = COALESCE(EXCLUDED.tour_slug, shows.tour_slug),
            show_notes = EXCLUDED.show_notes,
            permalink = EXCLUDED.permalink,
            show_year = EXCLUDED.show_year,
            fetched_at = now()
        """,
        _to_date(show.get("date")),
        _to_int(show.get("show_id")),
        _to_int(show.get("show_order")) or 1,
        _to_text(show.get("show_title")),
        show.get("venue_slug"),
        show.get("tour_slug"),
        _to_text(show.get("show_notes")),
        _to_text(show.get("permalink")),
        _to_int(show.get("show_year")),
    )


async def replace_setlist_entries_for_show(
    conn: asyncpg.Connection,
    show_date: dt.date,
    rows: list[dict[str, Any]],
) -> int:
    """Wipe + reinsert setlist_entries for one show. Idempotent.

    Setlist rows are tightly bound to a show; replacing the full set is
    simpler and safer than per-row upsert. ``unique_id`` carries the stable
    ATU ``uniqueid`` so individual performances remain addressable.
    """
    await conn.execute("DELETE FROM setlist_entries WHERE show_date = $1", show_date)
    if not rows:
        return 0
    payload = [
        (
            show_date,
            _to_int(r.get("uniqueid")),
            _to_int(r.get("position")) or 0,
            _to_text(r.get("settype")),
            _to_text(r.get("setnumber")),
            _to_text(r.get("slug")),
            _to_text(r.get("songname")) or "",
            _to_int(r.get("transition_id")),
            _to_text(r.get("transition")),
            _to_text(r.get("footnote")),
            _to_bool(r.get("isjamchart")),
            _to_text(r.get("jamchart_notes")),
            _to_bool(r.get("isoriginal", True)),
            _to_text(r.get("original_artist")) or None,
            _to_bool(r.get("isreprise")),
            _to_bool(r.get("isjam")),
        )
        for r in rows
    ]
    await conn.executemany(
        """
        INSERT INTO setlist_entries (
            show_date, unique_id, position, set_type, set_number,
            song_slug, song_name, transition_id, transition, footnote,
            is_jamchart, jamchart_notes, is_original, original_artist,
            is_reprise, is_jam
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
        )
        ON CONFLICT (unique_id) DO NOTHING
        """,
        payload,
    )
    return len(payload)


# ---------------------------------------------------------------------------
# Jam charts (native ATU `jamcharts` method)
# ---------------------------------------------------------------------------


async def upsert_jam_chart_entries(
    conn: asyncpg.Connection, rows: list[dict[str, Any]] | None
) -> int:
    """Wipe and re-insert the entire jam-chart corpus.

    ATU's jam-chart endpoint returns the full list, so idempotent re-run is
    simplest as delete + insert. Entries referencing a show date we don't
    have are dropped (``show_date`` is NOT NULL + FK-required); entries whose
    ``song_slug`` we don't catalog keep the row with a null FK (``song_name``
    preserved).
    """
    if rows is None:
        rows = []

    async with conn.transaction():
        await conn.execute("DELETE FROM jam_chart_entries")
        if not rows:
            return 0

        known_show_dates: set[Any] = {r["date"] for r in await conn.fetch("SELECT date FROM shows")}
        known_song_slugs: set[str] = {r["slug"] for r in await conn.fetch("SELECT slug FROM songs")}

        payload = []
        for r in rows:
            show_date = _to_date(r.get("showdate"))
            if show_date is None or show_date not in known_show_dates:
                continue
            slug = r.get("song_slug") or r.get("slug")
            if slug is not None and slug not in known_song_slugs:
                slug = None  # preserve row, drop dangling FK
            payload.append(
                (
                    show_date,
                    slug,
                    _to_text(r.get("songname") or r.get("song")),
                    _to_text(r.get("jamchartnote") or r.get("jamchart_notes") or r.get("notes")),
                    json.dumps(r),
                )
            )
        if not payload:
            return 0
        await conn.executemany(
            """
            INSERT INTO jam_chart_entries (
                show_date, song_slug, song_name, notes, raw_json
            )
            VALUES ($1,$2,$3,$4,$5::jsonb)
            """,
            payload,
        )
    return len(payload)


# ---------------------------------------------------------------------------
# Appearances (native ATU `appearances` method)
# ---------------------------------------------------------------------------


async def upsert_appearances(conn: asyncpg.Connection, rows: list[dict[str, Any]] | None) -> int:
    """Wipe and re-insert the appearances corpus, keeping only rows whose
    show date we already have.

    ATU returns the full list. Idempotent re-run is a delete + insert. The
    unique constraint is ``(show_date, person_slug, notes)``; we de-dupe in
    Python so an executemany batch never trips it.
    """
    if rows is None:
        rows = []

    async with conn.transaction():
        await conn.execute("DELETE FROM appearances")
        if not rows:
            return 0

        known_show_dates: set[Any] = {r["date"] for r in await conn.fetch("SELECT date FROM shows")}

        seen: dict[tuple[Any, Any, Any], tuple[Any, ...]] = {}
        for r in rows:
            show_date = _to_date(r.get("showdate"))
            if show_date is None or show_date not in known_show_dates:
                continue
            person_slug = _to_text(r.get("slug"))
            notes = _to_text(r.get("notes"))
            key = (show_date, person_slug, notes)
            seen[key] = (
                show_date,
                _to_int(r.get("person_id")),
                _to_text(r.get("personname") or r.get("person_name")) or "",
                person_slug,
                _to_text(r.get("appearance_type")),
                notes,
            )
        payload = list(seen.values())
        if not payload:
            return 0
        await conn.executemany(
            """
            INSERT INTO appearances (
                show_date, person_id, person_name, person_slug,
                appearance_type, notes
            )
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (show_date, person_slug, notes) DO UPDATE SET
                person_id = EXCLUDED.person_id,
                person_name = EXCLUDED.person_name,
                appearance_type = EXCLUDED.appearance_type,
                fetched_at = now()
            """,
            payload,
        )
    return len(payload)
