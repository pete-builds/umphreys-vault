"""Aggregate pass: compute per-song stats from the full setlist corpus.

This is the novel part of the Umphrey's vault. The ATU API exposes no
gap / times-played / debut data on its ``songs`` method, so we derive those
four columns from the loaded ``setlist_entries`` joined to the distinct,
ordered list of shows, and write them onto ``songs``:

- ``debut_date``     — earliest show date the song appears.
- ``last_play_date`` — latest show date the song appears.
- ``times_played``   — count of performances (setlist_entries rows for the song).
- ``gap_current``    — "shows since last played": the number of distinct shows
                       in the full ordered show list that occurred strictly
                       AFTER the song's ``last_play_date``.

Songs that have never been played (in the loaded corpus) are reset to NULL on
all four columns, so a song catalogued but not yet performed reads clean.

Gap definition
--------------
``gap_current`` counts *shows*, not days. If a song last appeared at show N
and there have since been 7 distinct shows, the gap is 7. A song played at the
most recent show has a gap of 0. This matches the standard jam-band "gap"
convention (shows since last played).

Everything is one SQL statement so it's a single pass over ``setlist_entries``
and the distinct show list — no per-song round trips.
"""

from __future__ import annotations

import logging

import asyncpg

log = logging.getLogger(__name__)


# One statement, three CTEs:
#   shows_ranked  — every distinct show date, newest-first rank (0-based).
#                   rank R == number of shows strictly newer than this one,
#                   which is exactly the gap for a song last played here.
#   song_stats    — per song: debut, last play, play count, derived from
#                   setlist_entries (song_slug NOT NULL).
#   joined        — attach the rank of each song's last_play_date as the gap.
# Then UPDATE songs: matched songs get their stats; unmatched songs (never
# played in the corpus) are reset to NULL via the LEFT JOIN.
_AGGREGATE_SQL = """
WITH shows_ranked AS (
    -- Only PLAYED shows (those with a setlist) count toward gap. Scheduled
    -- future shows live in `shows` too (for the predict form) but have no
    -- setlist_entries, so they must not inflate every song's gap.
    SELECT
        s.date,
        (ROW_NUMBER() OVER (ORDER BY s.date DESC) - 1) AS shows_after
    FROM shows s
    WHERE EXISTS (SELECT 1 FROM setlist_entries se WHERE se.show_date = s.date)
),
song_stats AS (
    SELECT
        se.song_slug                AS slug,
        MIN(s.date)                 AS debut_date,
        MAX(s.date)                 AS last_play_date,
        COUNT(*)                    AS times_played
    FROM setlist_entries se
    JOIN shows s ON s.date = se.show_date
    WHERE se.song_slug IS NOT NULL
    GROUP BY se.song_slug
),
joined AS (
    SELECT
        ss.slug,
        ss.debut_date,
        ss.last_play_date,
        ss.times_played,
        sr.shows_after AS gap_current
    FROM song_stats ss
    JOIN shows_ranked sr ON sr.date = ss.last_play_date
)
UPDATE songs sg
SET
    debut_date     = j.debut_date,
    last_play_date = j.last_play_date,
    times_played   = j.times_played,
    gap_current    = j.gap_current,
    fetched_at     = now()
FROM joined j
WHERE sg.slug = j.slug
"""

# Reset songs that have no performances in the corpus, so a re-run after rows
# are removed doesn't leave stale stats on a now-unplayed song.
_RESET_UNPLAYED_SQL = """
UPDATE songs sg
SET
    debut_date     = NULL,
    last_play_date = NULL,
    times_played   = NULL,
    gap_current    = NULL,
    fetched_at     = now()
WHERE NOT EXISTS (
    SELECT 1 FROM setlist_entries se
    WHERE se.song_slug = sg.slug
)
AND (
    sg.times_played IS NOT NULL
    OR sg.debut_date IS NOT NULL
    OR sg.last_play_date IS NOT NULL
    OR sg.gap_current IS NOT NULL
)
"""


async def recompute_song_stats(pool: asyncpg.Pool, dry_run: bool = False) -> dict[str, int]:
    """Recompute debut / last-play / times-played / gap for every song.

    Returns a small summary of rows touched. The whole pass runs in one
    transaction so readers never see a half-updated ``songs`` table.
    """
    if dry_run:
        async with pool.acquire() as conn:
            played = await conn.fetchval(
                "SELECT COUNT(DISTINCT song_slug) FROM setlist_entries WHERE song_slug IS NOT NULL"
            )
        log.info("Aggregate dry-run", extra={"songs_with_plays": int(played or 0)})
        return {"songs_updated": 0, "songs_reset": 0, "songs_with_plays": int(played or 0)}

    async with pool.acquire() as conn, conn.transaction():
        updated_tag = await conn.execute(_AGGREGATE_SQL)
        reset_tag = await conn.execute(_RESET_UNPLAYED_SQL)

    updated = _rowcount(updated_tag)
    reset = _rowcount(reset_tag)
    log.info("Aggregate complete", extra={"songs_updated": updated, "songs_reset": reset})
    return {"songs_updated": updated, "songs_reset": reset}


def _rowcount(command_tag: str) -> int:
    """Parse asyncpg's ``UPDATE <n>`` command tag into the row count."""
    try:
        return int(command_tag.rsplit(" ", 1)[1])
    except (IndexError, ValueError):
        return 0
