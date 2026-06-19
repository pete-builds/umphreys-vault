"""Aggregate gap-computation tests.

The aggregate pass is pure SQL (``etl/aggregate._AGGREGATE_SQL``), so we can't
run it without a live Postgres. Instead we verify the *algorithm* with a
faithful Python reimplementation of the same CTE logic against a small
hand-built fixture of shows + setlist_entries, asserting hand-computed
debut / last-play / times-played / gap values.

A guard test below pins the column names referenced by the real SQL so the
reference implementation and the SQL can't silently drift apart.

The ``_rowcount`` command-tag parser is unit-tested directly, and the
dry-run path of ``recompute_song_stats`` is exercised against an AsyncMock
pool.
"""

from __future__ import annotations

import datetime as dt
from typing import Any
from unittest.mock import AsyncMock

import pytest

from umphreys_vault.etl import aggregate

# ---------------------------------------------------------------------------
# Fixture: 5 shows, several songs with known gap/debut/last/count.
# ---------------------------------------------------------------------------
# Distinct ordered show dates (ascending):
#   2024-01-01, 2024-02-01, 2024-03-01, 2024-04-01, 2024-05-01
# newest-first rank (shows_after) == number of shows strictly newer:
#   2024-05-01 -> 0
#   2024-04-01 -> 1
#   2024-03-01 -> 2
#   2024-02-01 -> 3
#   2024-01-01 -> 4

_SHOWS = [
    dt.date(2024, 1, 1),
    dt.date(2024, 2, 1),
    dt.date(2024, 3, 1),
    dt.date(2024, 4, 1),
    dt.date(2024, 5, 1),
]

# (song_slug, show_date) performances.
_ENTRIES = [
    # "opener": played at shows 1, 3, 5. Last play 2024-05-01 -> gap 0.
    ("opener", dt.date(2024, 1, 1)),
    ("opener", dt.date(2024, 3, 1)),
    ("opener", dt.date(2024, 5, 1)),
    # "rarity": played only at show 1. Last play 2024-01-01 -> gap 4.
    ("rarity", dt.date(2024, 1, 1)),
    # "midrun": played at shows 2 and 3. Last play 2024-03-01 -> gap 2.
    ("midrun", dt.date(2024, 2, 1)),
    ("midrun", dt.date(2024, 3, 1)),
    # "dupe-in-show": played twice in the same show (e.g. reprise). Counts as 2
    # performances but the show only counts once for gap. Last play 2024-04-01
    # -> gap 1, times_played 2.
    ("dupe-in-show", dt.date(2024, 4, 1)),
    ("dupe-in-show", dt.date(2024, 4, 1)),
    # A NULL-slug entry (unmatched song) must be ignored entirely.
    (None, dt.date(2024, 2, 1)),
]

# Songs catalogued. "never-played" has a catalog row but no performances.
_SONG_SLUGS = ["opener", "rarity", "midrun", "dupe-in-show", "never-played"]


def _reference_song_stats() -> dict[str, dict[str, Any]]:
    """Pure-Python mirror of ``_AGGREGATE_SQL``.

    shows_ranked: rank each distinct show date newest-first (0-based) ==
                  count of shows strictly newer.
    song_stats:   per song with a non-null slug, debut/last/count.
    joined:       gap = shows_ranked rank of the song's last_play_date.
    Unplayed songs -> all None (mirrors the LEFT-JOIN reset).
    """
    ordered = sorted(_SHOWS, reverse=True)
    shows_after = {d: i for i, d in enumerate(ordered)}

    stats: dict[str, dict[str, Any]] = {}
    for slug, date in _ENTRIES:
        if slug is None:
            continue
        s = stats.setdefault(
            slug, {"debut_date": date, "last_play_date": date, "times_played": 0}
        )
        s["debut_date"] = min(s["debut_date"], date)
        s["last_play_date"] = max(s["last_play_date"], date)
        s["times_played"] += 1

    out: dict[str, dict[str, Any]] = {}
    for slug in _SONG_SLUGS:
        if slug in stats:
            s = stats[slug]
            out[slug] = {
                "debut_date": s["debut_date"],
                "last_play_date": s["last_play_date"],
                "times_played": s["times_played"],
                "gap_current": shows_after[s["last_play_date"]],
            }
        else:
            out[slug] = {
                "debut_date": None,
                "last_play_date": None,
                "times_played": None,
                "gap_current": None,
            }
    return out


def test_gap_and_counts_against_hand_computed_values() -> None:
    stats = _reference_song_stats()

    assert stats["opener"] == {
        "debut_date": dt.date(2024, 1, 1),
        "last_play_date": dt.date(2024, 5, 1),
        "times_played": 3,
        "gap_current": 0,
    }
    assert stats["rarity"] == {
        "debut_date": dt.date(2024, 1, 1),
        "last_play_date": dt.date(2024, 1, 1),
        "times_played": 1,
        "gap_current": 4,
    }
    assert stats["midrun"] == {
        "debut_date": dt.date(2024, 2, 1),
        "last_play_date": dt.date(2024, 3, 1),
        "times_played": 2,
        "gap_current": 2,
    }


def test_same_show_repeat_counts_play_not_show() -> None:
    # Two performances in one show: times_played=2 but the gap is computed off
    # the single distinct show, so gap=1 (one show newer than 2024-04-01).
    stats = _reference_song_stats()
    assert stats["dupe-in-show"]["times_played"] == 2
    assert stats["dupe-in-show"]["gap_current"] == 1


def test_never_played_song_is_null() -> None:
    stats = _reference_song_stats()
    assert stats["never-played"] == {
        "debut_date": None,
        "last_play_date": None,
        "times_played": None,
        "gap_current": None,
    }


def test_null_slug_entries_ignored() -> None:
    # The NULL-slug entry at 2024-02-01 must never create a song row.
    stats = _reference_song_stats()
    assert None not in stats
    assert set(stats) == set(_SONG_SLUGS)


def test_most_recent_show_song_has_zero_gap() -> None:
    stats = _reference_song_stats()
    played_latest = [
        slug for slug, s in stats.items() if s["last_play_date"] == max(_SHOWS)
    ]
    for slug in played_latest:
        assert stats[slug]["gap_current"] == 0


# ---------------------------------------------------------------------------
# Guard: the reference logic above must stay aligned with the real SQL.
# ---------------------------------------------------------------------------


def test_aggregate_sql_references_expected_columns() -> None:
    sql = aggregate._AGGREGATE_SQL
    # The four computed columns must all be written.
    for col in ("debut_date", "last_play_date", "times_played", "gap_current"):
        assert col in sql, f"aggregate SQL no longer writes {col}"
    # Gap is derived from a newest-first row number over shows.
    assert "ROW_NUMBER() OVER (ORDER BY s.date DESC)" in sql
    # Only played shows (with a setlist) count toward gap — scheduled future
    # shows live in `shows` too and must not inflate gaps.
    assert "EXISTS (SELECT 1 FROM setlist_entries se WHERE se.show_date = s.date)" in sql
    # Per-song stats come from setlist_entries joined to shows.
    assert "FROM setlist_entries" in sql
    assert "se.song_slug IS NOT NULL" in sql


def test_reset_sql_targets_unplayed_songs() -> None:
    sql = aggregate._RESET_UNPLAYED_SQL
    assert "NOT EXISTS" in sql
    assert "setlist_entries" in sql


def test_rowcount_parses_command_tag() -> None:
    assert aggregate._rowcount("UPDATE 42") == 42
    assert aggregate._rowcount("UPDATE 0") == 0
    assert aggregate._rowcount("weird") == 0
    assert aggregate._rowcount("") == 0


@pytest.mark.asyncio
async def test_recompute_dry_run_counts_without_writing() -> None:
    conn = AsyncMock()
    conn.fetchval.return_value = 4

    class _Acquire:
        async def __aenter__(self) -> AsyncMock:
            return conn

        async def __aexit__(self, *_: Any) -> None:
            return None

    pool = AsyncMock()
    pool.acquire = lambda: _Acquire()

    result = await aggregate.recompute_song_stats(pool, dry_run=True)
    assert result["songs_updated"] == 0
    assert result["songs_with_plays"] == 4
    conn.execute.assert_not_called()
