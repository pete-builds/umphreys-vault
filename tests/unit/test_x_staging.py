"""x_staging tests: title->slug resolution + idempotent staging upsert.

No real Postgres. The songs catalog is supplied through an AsyncMock conn whose
``.fetch`` returns canned ``songs`` rows, mirroring the style in test_upserts.py.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import AsyncMock

import pytest

from umphreys_vault.x_staging import (
    main,
    resolve_titles_conn,
    upsert_staging_conn,
)

# A small slice of the real UM catalog (slug/title/alias as in the songs table).
_SONGS = [
    {"slug": "in-the-kitchen", "title": "In the Kitchen", "alias": None},
    {"slug": "all-in-time", "title": "All in Time", "alias": None},
    {"slug": "bridgeless", "title": "Bridgeless", "alias": None},
    {"slug": "1348", "title": "1348", "alias": "Thirteen Forty-Eight"},
    {"slug": "red-baron", "title": "Red Baron", "alias": None},
]


def _songs_conn(songs: list[dict[str, Any]]) -> AsyncMock:
    """AsyncMock conn whose .fetch returns the songs catalog rows."""
    conn = AsyncMock()

    async def _fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
        if "FROM songs" in sql:
            return songs
        return []

    conn.fetch.side_effect = _fetch
    return conn


# ---------------------------------------------------------------------------
# resolve_titles
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_titles_matches_known_setlist() -> None:
    conn = _songs_conn(_SONGS)
    # Real-ish X-post titles with casing/punctuation noise.
    titles = ["In The Kitchen", "all in time!", "Bridgeless"]
    out = await resolve_titles_conn(conn, titles)
    assert [r["matched"] for r in out] == [True, True, True]
    assert [r["song_slug"] for r in out] == ["in-the-kitchen", "all-in-time", "bridgeless"]
    # Canonical title comes back from the catalog, not the raw input.
    assert out[0]["song_name"] == "In the Kitchen"
    assert out[1]["song_name"] == "All in Time"


@pytest.mark.asyncio
async def test_resolve_titles_reports_match_method_for_fast_paths() -> None:
    # A catalog row whose slug is NOT the slugified title, so the slug fast path
    # is the only one that can hit a bare-slug input.
    songs = [*_SONGS, {"slug": "in-the-kitchen-1998", "title": "Kitchen 98", "alias": None}]
    conn = _songs_conn(songs)
    out = await resolve_titles_conn(
        conn, ["In The Kitchen", "Thirteen Forty-Eight", "in-the-kitchen-1998"]
    )
    # Exact title, alias, and slug fast paths each report their own method and
    # win before fuzzy ever runs.
    assert [r["match_method"] for r in out] == ["exact", "alias", "slug"]


@pytest.mark.asyncio
async def test_resolve_titles_via_alias() -> None:
    conn = _songs_conn(_SONGS)
    out = await resolve_titles_conn(conn, ["Thirteen Forty-Eight"])
    assert out[0]["matched"] is True
    assert out[0]["song_slug"] == "1348"
    assert out[0]["song_name"] == "1348"
    assert out[0]["match_method"] == "alias"


@pytest.mark.asyncio
async def test_resolve_titles_fuzzy_fallback_resolves_noisy_title() -> None:
    # "Bridgless" (dropped 'e') misses exact/alias/slug but is well above the
    # fuzzy threshold against "Bridgeless", so it resolves via fuzzy.
    conn = _songs_conn(_SONGS)
    out = await resolve_titles_conn(conn, ["Bridgless"])
    assert out[0]["matched"] is True
    assert out[0]["song_slug"] == "bridgeless"
    assert out[0]["song_name"] == "Bridgeless"
    assert out[0]["match_method"] == "fuzzy"


@pytest.mark.asyncio
async def test_resolve_titles_fuzzy_handles_word_reorder_and_noise() -> None:
    # Extra surrounding noise + a small typo on a multi-word title still lands
    # on the right song via fuzzy (and never via the exact/alias/slug paths).
    conn = _songs_conn(_SONGS)
    out = await resolve_titles_conn(conn, ["In the Kithen"])
    assert out[0]["matched"] is True
    assert out[0]["song_slug"] == "in-the-kitchen"
    assert out[0]["match_method"] == "fuzzy"


@pytest.mark.asyncio
async def test_resolve_titles_fuzzy_rejects_below_threshold() -> None:
    # A genuinely unknown title scores below threshold against every catalog
    # row, so it stays a clean miss rather than mis-crediting a wrong song.
    conn = _songs_conn(_SONGS)
    out = await resolve_titles_conn(conn, ["Some Totally Different Jam"])
    assert out[0]["matched"] is False
    assert out[0]["song_slug"] is None
    assert out[0]["match_method"] is None


@pytest.mark.asyncio
async def test_resolve_titles_fuzzy_threshold_env_knob(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Cranking the threshold to 0.99 rejects an otherwise-acceptable fuzzy hit,
    # proving the X_FUZZY_THRESHOLD env knob is honored.
    monkeypatch.setenv("X_FUZZY_THRESHOLD", "0.99")
    conn = _songs_conn(_SONGS)
    out = await resolve_titles_conn(conn, ["Bridgless"])
    assert out[0]["matched"] is False
    assert out[0]["match_method"] is None


@pytest.mark.asyncio
async def test_resolve_titles_rejects_fake_title() -> None:
    conn = _songs_conn(_SONGS)
    out = await resolve_titles_conn(conn, ["Totally Not A Real Song", ""])
    assert out[0]["matched"] is False
    assert out[0]["song_slug"] is None
    assert out[0]["song_name"] is None
    # Empty string is a clean miss, not a crash.
    assert out[1]["matched"] is False


@pytest.mark.asyncio
async def test_resolve_titles_via_slug_input() -> None:
    # If a title already arrives as a catalog slug, it still resolves.
    conn = _songs_conn(_SONGS)
    out = await resolve_titles_conn(conn, ["red-baron"])
    assert out[0]["matched"] is True
    assert out[0]["song_slug"] == "red-baron"


# ---------------------------------------------------------------------------
# upsert_staging
# ---------------------------------------------------------------------------


def _rows() -> list[dict[str, Any]]:
    return [
        {
            "song_slug": "in-the-kitchen",
            "song_name": "In the Kitchen",
            "set_number_hint": "1",
            "position_hint": 3,
            "confidence": 0.80,
            "source_post_id": "tweet-1",
        },
        {
            "song_slug": "all-in-time",
            "song_name": "All in Time",
            "set_number_hint": "2",
            "position_hint": 1,
            "confidence": 0.65,
            "source_post_id": "tweet-2",
        },
    ]


@pytest.mark.asyncio
async def test_upsert_staging_writes_resolved_rows() -> None:
    conn = AsyncMock()
    n = await upsert_staging_conn(conn, "2026-07-01", _rows())
    assert n == 2
    payload = conn.executemany.await_args.args[1]
    # Tuple: (show_date, song_slug, song_name, set_number_hint, position_hint,
    #   confidence, source_post_id)
    by_slug = {r[1]: r for r in payload}
    kitchen = by_slug["in-the-kitchen"]
    # show_date must be bound as a real date object (asyncpg encodes the DATE
    # param before the ::date cast, so a str would raise DataError at runtime).
    assert kitchen[0] == date(2026, 7, 1)
    assert isinstance(kitchen[0], date)
    assert kitchen[2] == "In the Kitchen"
    assert kitchen[3] == "1"
    assert kitchen[4] == 3
    assert kitchen[5] == pytest.approx(0.80)
    assert kitchen[6] == "tweet-1"


@pytest.mark.asyncio
async def test_upsert_staging_is_idempotent_on_batch() -> None:
    # Same slug twice in one batch must collapse to a single payload row,
    # so the executemany never trips the (show_date, song_slug) unique key.
    conn = AsyncMock()
    dupes = _rows() + _rows()
    n = await upsert_staging_conn(conn, "2026-07-01", dupes)
    assert n == 2
    payload = conn.executemany.await_args.args[1]
    slugs = sorted(r[1] for r in payload)
    assert slugs == ["all-in-time", "in-the-kitchen"]


@pytest.mark.asyncio
async def test_upsert_staging_sql_bumps_last_seen_and_keeps_max_confidence() -> None:
    # Assert the ON CONFLICT clause encodes the idempotency contract:
    # re-running identical input bumps last_seen_at and keeps the higher
    # confidence rather than inserting a duplicate.
    conn = AsyncMock()
    await upsert_staging_conn(conn, "2026-07-01", _rows())
    sql = conn.executemany.await_args.args[0]
    assert "ON CONFLICT (show_date, song_slug) DO UPDATE" in sql
    assert "last_seen_at = now()" in sql
    assert "GREATEST(x_setlist_staging.confidence, EXCLUDED.confidence)" in sql
    assert "COALESCE(EXCLUDED.set_number_hint" in sql


@pytest.mark.asyncio
async def test_upsert_staging_empty_returns_zero() -> None:
    conn = AsyncMock()
    n = await upsert_staging_conn(conn, "2026-07-01", [])
    assert n == 0
    conn.executemany.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_staging_skips_rows_without_slug() -> None:
    conn = AsyncMock()
    rows = [{"song_name": "Orphan", "confidence": 0.5}]  # no song_slug
    n = await upsert_staging_conn(conn, "2026-07-01", rows)
    assert n == 0
    conn.executemany.assert_not_called()


# ---------------------------------------------------------------------------
# CLI surface
# ---------------------------------------------------------------------------


def test_cli_usage_on_no_args(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main([])
    assert rc == 2
    err = capsys.readouterr().err
    assert "usage:" in err


def test_cli_bad_json(capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["resolve", "{not json"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "bad_input" in err
