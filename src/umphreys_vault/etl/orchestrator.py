"""Orchestrate full backfill and incremental refresh runs.

Mirrors the phish-vault orchestrator pattern: bracket each run with
``etl_runs`` audit rows, and never let an exception swallow the audit row.

Backfill order:
  1. Setlists, per year (the heavy step): venues, tours, songs, shows,
     setlist_entries, all derived from each year's flat setlist rows.
  2. Catalog: full ``/songs.json`` + ``/venues.json`` so catalog-only rows
     (songs/venues not yet performed in the loaded window) also land.
  3. Enrichment: ``/jamcharts.json`` and ``/appearances.json`` (one call each).
  4. Aggregate: compute per-song debut/last-play/times-played/gap from the
     full corpus.

Refresh order:
  1. Latest show (``/latest.json``) plus the current year's setlists, to pick
     up any backfilled corrections within the year.
  2. Recent-window re-pull: re-fetch + upsert setlists for every show in a
     trailing window (default 14 days) so a show whose setlist ATU enters AFTER
     our daily run still backfills on the next run. ATU is the source of record.
  3. Enrichment, then aggregate.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any

import asyncpg

from umphreys_vault.clients.atu import ATUClient
from umphreys_vault.etl import aggregate, catalog, enrichment, shows

log = logging.getLogger(__name__)

# Floor year used only if ``list_years`` returns nothing usable. Umphrey's
# McGee's first show was 1998-01-21; 1997 is a safe lower bound.
_YEAR_FLOOR = 1997

# Default trailing window (days) the refresh re-pulls setlists for, so a show
# whose setlist ATU enters late still backfills on the next daily run. Override
# via ``REFRESH_RECENT_DAYS`` (settings.refresh_recent_days). 0 disables.
_REFRESH_RECENT_DAYS = 14


async def _start_run(pool: asyncpg.Pool, mode: str, source: str, args: dict[str, Any]) -> int:
    async with pool.acquire() as conn:
        run_id = await conn.fetchval(
            """
            INSERT INTO etl_runs (mode, source, args, status)
            VALUES ($1, $2, $3::jsonb, 'running')
            RETURNING id
            """,
            mode,
            source,
            json.dumps(args),
        )
    return int(run_id)


async def _finish_run(
    pool: asyncpg.Pool,
    run_id: int,
    rows_added: int,
    rows_updated: int,
    error: str | None,
    summary: dict[str, Any],
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE etl_runs SET
                finished_at = now(),
                rows_added = $2,
                rows_updated = $3,
                status = $4,
                error_message = $5,
                summary = $6::jsonb
            WHERE id = $1
            """,
            run_id,
            rows_added,
            rows_updated,
            "ok" if error is None else "error",
            error,
            json.dumps(summary, default=str),
        )


async def _resolve_years(atu: ATUClient, year: int | None) -> list[int]:
    """Resolve the list of years to backfill.

    A specific ``--year`` short-circuits. Otherwise enumerate via
    ``/list/year.json``; if that comes back empty (the endpoint occasionally
    returns a shape the client can't parse), fall back to a static range from
    the floor year to the current year.
    """
    if year is not None:
        return [year]
    years = await atu.list_years()
    if years:
        return years
    log.warning("list_years returned nothing; falling back to static year range")
    return list(range(_YEAR_FLOOR, dt.date.today().year + 1))


async def run_backfill(
    pool: asyncpg.Pool,
    atu: ATUClient,
    *,
    year: int | None = None,
    concurrency: int = 4,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Full historical backfill (or a single year with ``year=``).

    ``concurrency`` is accepted for CLI/signature parity with the Phish vault;
    years are loaded serially here (one cheap call per year), and within a
    year the per-show transactions are sequential.
    """
    args = {"year": year, "concurrency": concurrency, "dry_run": dry_run}
    run_id = await _start_run(pool, "backfill", "atu_v2", args)
    summary: dict[str, Any] = {"args": args}
    error: str | None = None
    rows_added = 0
    try:
        years = await _resolve_years(atu, year)
        summary["years"] = years
        log.info("Backfill: years queued", extra={"count": len(years)})

        # Load the venue catalog FIRST so shows resolve to the canonical venue
        # slug by venue_id. A slug synthesised from the venue name can diverge
        # from ATU's canonical slug and would then collide on the
        # venues.venue_id unique constraint.
        summary["venues"] = await catalog.load_venues(atu, pool, dry_run=dry_run)
        venue_map = {} if dry_run else await catalog.venue_id_slug_map(pool)

        per_year: dict[str, dict[str, int]] = {}
        for y in years:
            rows = await atu.setlists_by_year(y)
            totals = await shows.load_setlist_rows(
                pool, rows, venue_map=venue_map, dry_run=dry_run
            )
            per_year[str(y)] = totals
            rows_added += int(totals.get("setlist_entries", 0))
        summary["setlists_by_year"] = per_year

        summary["songs"] = await catalog.load_songs(atu, pool, dry_run=dry_run)
        rows_added += int(summary["songs"]) + int(summary["venues"])

        # Scheduled future shows (no setlist) so the predict form has a target.
        summary["upcoming"] = await shows.load_upcoming_shows(
            pool, await atu.upcoming_shows(), venue_map, dry_run=dry_run
        )
        rows_added += int(summary["upcoming"])

        summary["jam_chart_entries"] = await enrichment.load_jam_charts(atu, pool, dry_run=dry_run)
        summary["appearances"] = await enrichment.load_appearances(atu, pool, dry_run=dry_run)
        rows_added += int(summary["jam_chart_entries"]) + int(summary["appearances"])

        summary["aggregate"] = await _run_aggregate(pool, dry_run=dry_run)
    except Exception as exc:
        error = repr(exc)
        log.exception("Backfill run failed")
        summary["error"] = error
        raise
    finally:
        await _finish_run(pool, run_id, rows_added, 0, error, summary)
    return summary


async def run_refresh(
    pool: asyncpg.Pool,
    atu: ATUClient,
    *,
    concurrency: int = 4,
    dry_run: bool = False,
    recent_days: int = _REFRESH_RECENT_DAYS,
) -> dict[str, Any]:
    """Incremental refresh: latest show + current year + recent-window re-pull.

    ``concurrency`` is accepted for signature parity with backfill.
    ``recent_days`` sets the trailing window re-pulled so late-entered setlists
    backfill on the next run (``REFRESH_RECENT_DAYS``; 0 disables).
    """
    args = {"concurrency": concurrency, "dry_run": dry_run, "recent_days": recent_days}
    run_id = await _start_run(pool, "refresh", "atu_v2", args)
    summary: dict[str, Any] = {"args": args}
    error: str | None = None
    rows_added = 0
    try:
        # Refresh the venue catalog + build the id->slug map first, same reason
        # as backfill: resolve shows to canonical venue slugs.
        summary["venues"] = await catalog.load_venues(atu, pool, dry_run=dry_run)
        venue_map = {} if dry_run else await catalog.venue_id_slug_map(pool)

        latest_rows = await atu.latest()
        latest_totals = await shows.load_setlist_rows(
            pool, latest_rows, venue_map=venue_map, dry_run=dry_run
        )
        summary["latest"] = latest_totals
        rows_added += int(latest_totals.get("setlist_entries", 0))

        # Also pull the full current year so within-year corrections land.
        year = _year_of(latest_rows) or dt.date.today().year
        summary["year"] = year
        year_rows = await atu.setlists_by_year(year)
        year_totals = await shows.load_setlist_rows(
            pool, year_rows, venue_map=venue_map, dry_run=dry_run
        )
        summary["year_setlists"] = year_totals
        rows_added += int(year_totals.get("setlist_entries", 0))

        # Re-pull setlists for the trailing window so a show whose setlist ATU
        # enters AFTER our daily run (the late-entry bug) backfills next run.
        recent = await _repull_recent_setlists(
            pool, atu, venue_map, recent_days, dry_run=dry_run
        )
        summary["recent_window"] = recent
        rows_added += int(recent.get("setlist_entries", 0))

        summary["upcoming"] = await shows.load_upcoming_shows(
            pool, await atu.upcoming_shows(), venue_map, dry_run=dry_run
        )

        summary["jam_chart_entries"] = await enrichment.load_jam_charts(atu, pool, dry_run=dry_run)
        summary["appearances"] = await enrichment.load_appearances(atu, pool, dry_run=dry_run)

        summary["aggregate"] = await _run_aggregate(pool, dry_run=dry_run)
    except Exception as exc:
        error = repr(exc)
        log.exception("Refresh run failed")
        summary["error"] = error
        raise
    finally:
        await _finish_run(pool, run_id, rows_added, 0, error, summary)
    return summary


async def run_aggregate_only(pool: asyncpg.Pool, *, dry_run: bool = False) -> dict[str, Any]:
    """Run only the per-song aggregate pass, with its own audit row."""
    args = {"dry_run": dry_run}
    run_id = await _start_run(pool, "aggregate", "computed", args)
    summary: dict[str, Any] = {"args": args}
    error: str | None = None
    rows_updated = 0
    try:
        result = await aggregate.recompute_song_stats(pool, dry_run=dry_run)
        summary["aggregate"] = result
        rows_updated = int(result.get("songs_updated", 0))
    except Exception as exc:
        error = repr(exc)
        log.exception("Aggregate run failed")
        summary["error"] = error
        raise
    finally:
        await _finish_run(pool, run_id, 0, rows_updated, error, summary)
    return summary


async def _run_aggregate(pool: asyncpg.Pool, dry_run: bool) -> dict[str, int]:
    """Aggregate pass used inside backfill/refresh (no separate audit row;
    the parent run records it in its summary)."""
    return await aggregate.recompute_song_stats(pool, dry_run=dry_run)


async def _recent_show_dates(
    pool: asyncpg.Pool, recent_days: int, today: dt.date | None = None
) -> list[str]:
    """ISO dates of shows whose ``date`` falls in the trailing window.

    Window is ``[today - recent_days, today]`` inclusive. Returns existing
    ``shows`` rows only, so a single bounded query (no per-show DB calls). A
    show that has a row but zero setlist_entries (the late-entry bug) is still
    selected here, which is the whole point.
    """
    if recent_days <= 0:
        return []
    today = today or dt.date.today()
    cutoff = today - dt.timedelta(days=recent_days)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT TO_CHAR(date, 'YYYY-MM-DD') AS d FROM shows "
            "WHERE date >= $1 AND date <= $2 ORDER BY date",
            cutoff,
            today,
        )
    return [str(r["d"]) for r in rows]


async def _repull_recent_setlists(
    pool: asyncpg.Pool,
    atu: ATUClient,
    venue_map: dict[int, str],
    recent_days: int,
    *,
    dry_run: bool,
    today: dt.date | None = None,
) -> dict[str, Any]:
    """Re-fetch + upsert setlists for every show in the trailing window.

    Reuses the same ATU fetch (``setlists_by_date``) and persist path
    (``shows.load_setlist_rows``) the rest of the ETL uses; idempotent via the
    underlying upserts, so a show that already has its setlist is a no-op and a
    late-entered one gets backfilled. Per-date failures are logged and counted,
    never fatal to the refresh.
    """
    dates = await _recent_show_dates(pool, recent_days, today=today)
    out: dict[str, Any] = {
        "recent_days": recent_days,
        "dates_checked": len(dates),
        "setlist_entries": 0,
        "errors": 0,
    }
    for d in dates:
        try:
            rows = await atu.setlists_by_date(d)
            if not rows:
                continue
            totals = await shows.load_setlist_rows(
                pool, rows, venue_map=venue_map, dry_run=dry_run
            )
            out["setlist_entries"] += int(totals.get("setlist_entries", 0))
            out["errors"] += int(totals.get("errors", 0))
        except Exception as exc:
            out["errors"] += 1
            log.exception("recent-window re-pull failed", extra={"date": d, "error": str(exc)})
    return out


def _year_of(rows: list[dict[str, Any]]) -> int | None:
    for r in rows:
        y = r.get("showyear")
        try:
            return int(y) if y is not None else None
        except (TypeError, ValueError):
            continue
    return None
