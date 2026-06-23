"""Orchestrator wiring tests with stubbed clients + DB.

``run_backfill`` / ``run_refresh`` are lightweight glue. We stub the ETL
submodules and the ATU client, then assert the call ordering and the
``etl_runs`` audit writes.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from umphreys_vault.etl import orchestrator


class FakePool:
    """Minimal asyncpg.Pool stand-in for orchestrator audit writes."""

    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.fetchval_returns = iter([42, 0, 0, 0])
        # Canned ``shows`` rows the recent-window query selects (default empty).
        self.recent_show_rows: list[dict[str, str]] = []

    def acquire(self) -> FakePool:
        return self

    async def __aenter__(self) -> FakePool:
        return self

    async def __aexit__(self, *_: Any) -> None:
        return None

    async def execute(self, sql: str, *args: Any) -> str:
        self.executed.append((sql, args))
        return "UPDATE 1"

    async def fetchval(self, sql: str, *args: Any) -> int:
        self.executed.append((sql, args))
        return next(self.fetchval_returns)

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, str]]:
        self.executed.append((sql, args))
        if "FROM shows" in sql:
            return self.recent_show_rows
        return []


def _fake_atu() -> MagicMock:
    atu = MagicMock()
    atu.list_years = AsyncMock(return_value=[1998])
    atu.setlists_by_year = AsyncMock(return_value=[{"showdate": "1998-01-21"}])
    atu.latest = AsyncMock(return_value=[{"showdate": "2026-06-18", "showyear": 2026}])
    atu.upcoming_shows = AsyncMock(return_value=[])
    atu.setlists_by_date = AsyncMock(return_value=[])
    return atu


@pytest.mark.asyncio
async def test_run_backfill_records_audit_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = FakePool()

    async def fake_load_setlist_rows(*a: Any, **kw: Any) -> dict[str, int]:
        return {"shows": 1, "setlist_entries": 3, "errors": 0}

    async def fake_load_songs(*a: Any, **kw: Any) -> int:
        return 7

    async def fake_load_venues(*a: Any, **kw: Any) -> int:
        return 5

    async def fake_load_jam(*a: Any, **kw: Any) -> int:
        return 2

    async def fake_load_app(*a: Any, **kw: Any) -> int:
        return 1

    async def fake_aggregate(*a: Any, **kw: Any) -> dict[str, int]:
        return {"songs_updated": 9, "songs_reset": 0}

    monkeypatch.setattr(orchestrator.shows, "load_setlist_rows", fake_load_setlist_rows)
    monkeypatch.setattr(orchestrator.catalog, "load_songs", fake_load_songs)
    monkeypatch.setattr(orchestrator.catalog, "load_venues", fake_load_venues)
    monkeypatch.setattr(orchestrator.enrichment, "load_jam_charts", fake_load_jam)
    monkeypatch.setattr(orchestrator.enrichment, "load_appearances", fake_load_app)
    monkeypatch.setattr(orchestrator.aggregate, "recompute_song_stats", fake_aggregate)

    summary = await orchestrator.run_backfill(
        pool,  # type: ignore[arg-type]
        _fake_atu(),
        year=1998,
        concurrency=2,
        dry_run=True,
    )

    assert summary["years"] == [1998]
    assert summary["songs"] == 7
    assert summary["venues"] == 5
    assert summary["jam_chart_entries"] == 2
    assert summary["appearances"] == 1
    assert summary["aggregate"]["songs_updated"] == 9
    sqls = [s for s, _ in pool.executed]
    assert any("INSERT INTO etl_runs" in s for s in sqls)
    assert any("UPDATE etl_runs SET" in s for s in sqls)


@pytest.mark.asyncio
async def test_run_backfill_falls_back_to_year_range(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = FakePool()
    atu = _fake_atu()
    atu.list_years = AsyncMock(return_value=[])  # endpoint unparseable

    captured_years: list[int] = []

    async def fake_setlists_by_year(year: int) -> list[dict[str, Any]]:
        captured_years.append(year)
        return []

    atu.setlists_by_year = AsyncMock(side_effect=fake_setlists_by_year)

    async def noop_rows(*a: Any, **kw: Any) -> dict[str, int]:
        return {"shows": 0, "setlist_entries": 0, "errors": 0}

    async def zero(*a: Any, **kw: Any) -> int:
        return 0

    async def agg(*a: Any, **kw: Any) -> dict[str, int]:
        return {"songs_updated": 0, "songs_reset": 0}

    monkeypatch.setattr(orchestrator.shows, "load_setlist_rows", noop_rows)
    monkeypatch.setattr(orchestrator.catalog, "load_songs", zero)
    monkeypatch.setattr(orchestrator.catalog, "load_venues", zero)
    monkeypatch.setattr(orchestrator.enrichment, "load_jam_charts", zero)
    monkeypatch.setattr(orchestrator.enrichment, "load_appearances", zero)
    monkeypatch.setattr(orchestrator.aggregate, "recompute_song_stats", agg)

    summary = await orchestrator.run_backfill(pool, atu, year=None, dry_run=True)  # type: ignore[arg-type]

    # Fallback range starts at the floor year and ends at the current year.
    assert captured_years[0] == orchestrator._YEAR_FLOOR
    assert summary["years"][0] == orchestrator._YEAR_FLOOR


@pytest.mark.asyncio
async def test_run_refresh_uses_latest_year(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = FakePool()
    atu = _fake_atu()

    async def noop_rows(*a: Any, **kw: Any) -> dict[str, int]:
        return {"shows": 1, "setlist_entries": 2, "errors": 0}

    async def zero(*a: Any, **kw: Any) -> int:
        return 0

    async def agg(*a: Any, **kw: Any) -> dict[str, int]:
        return {"songs_updated": 1, "songs_reset": 0}

    async def empty_vmap(*a: Any, **kw: Any) -> dict[int, str]:
        return {}

    monkeypatch.setattr(orchestrator.shows, "load_setlist_rows", noop_rows)
    monkeypatch.setattr(orchestrator.shows, "load_upcoming_shows", zero)
    monkeypatch.setattr(orchestrator.catalog, "load_venues", zero)
    monkeypatch.setattr(orchestrator.catalog, "venue_id_slug_map", empty_vmap)
    monkeypatch.setattr(orchestrator.enrichment, "load_jam_charts", zero)
    monkeypatch.setattr(orchestrator.enrichment, "load_appearances", zero)
    monkeypatch.setattr(orchestrator.aggregate, "recompute_song_stats", agg)

    summary = await orchestrator.run_refresh(pool, atu, dry_run=False)  # type: ignore[arg-type]
    assert summary["year"] == 2026
    atu.setlists_by_year.assert_awaited_with(2026)


@pytest.mark.asyncio
async def test_run_refresh_repulls_recent_window(monkeypatch: pytest.MonkeyPatch) -> None:
    # The late-entry bug fix: refresh must re-fetch + upsert setlists for every
    # show in the trailing window, not just the latest one.
    pool = FakePool()
    pool.recent_show_rows = [{"d": "2026-06-18"}, {"d": "2026-06-19"}]
    atu = _fake_atu()
    # 2026-06-19 (Fairport) had a show row but ATU entered its setlist late.
    atu.setlists_by_date = AsyncMock(
        return_value=[{"showdate": "2026-06-19", "slug": "all-in-time", "position": 1}]
    )

    seen_dates: list[str] = []

    async def fake_load_setlist_rows(_pool: Any, rows: Any, **kw: Any) -> dict[str, int]:
        for r in rows:
            if r.get("showdate"):
                seen_dates.append(r["showdate"])
        return {"shows": 1, "setlist_entries": len(rows), "errors": 0}

    async def zero(*a: Any, **kw: Any) -> int:
        return 0

    async def agg(*a: Any, **kw: Any) -> dict[str, int]:
        return {"songs_updated": 1, "songs_reset": 0}

    async def empty_vmap(*a: Any, **kw: Any) -> dict[int, str]:
        return {}

    monkeypatch.setattr(orchestrator.shows, "load_setlist_rows", fake_load_setlist_rows)
    monkeypatch.setattr(orchestrator.shows, "load_upcoming_shows", zero)
    monkeypatch.setattr(orchestrator.catalog, "load_venues", zero)
    monkeypatch.setattr(orchestrator.catalog, "venue_id_slug_map", empty_vmap)
    monkeypatch.setattr(orchestrator.enrichment, "load_jam_charts", zero)
    monkeypatch.setattr(orchestrator.enrichment, "load_appearances", zero)
    monkeypatch.setattr(orchestrator.aggregate, "recompute_song_stats", agg)

    summary = await orchestrator.run_refresh(pool, atu, dry_run=False)  # type: ignore[arg-type]

    # Both recent dates were checked against ATU (source of record) ...
    assert atu.setlists_by_date.await_count == 2
    awaited = {c.args[0] for c in atu.setlists_by_date.await_args_list}
    assert awaited == {"2026-06-18", "2026-06-19"}
    # ... and the late-entered Fairport setlist flowed into the persist path.
    assert "2026-06-19" in seen_dates
    assert summary["recent_window"]["dates_checked"] == 2
    assert summary["recent_window"]["recent_days"] == orchestrator._REFRESH_RECENT_DAYS


@pytest.mark.asyncio
async def test_recent_show_dates_respects_window_and_disable() -> None:
    pool = FakePool()
    pool.recent_show_rows = [{"d": "2026-06-19"}]
    dates = await orchestrator._recent_show_dates(pool, 14)  # type: ignore[arg-type]
    assert dates == ["2026-06-19"]
    # recent_days=0 disables the re-pull entirely (no query, empty result).
    pool2 = FakePool()
    assert await orchestrator._recent_show_dates(pool2, 0) == []  # type: ignore[arg-type]
    assert pool2.executed == []


@pytest.mark.asyncio
async def test_aggregate_only_records_computed_source() -> None:
    pool = FakePool()
    pool.fetchval_returns = iter([99])

    rid = await orchestrator._start_run(pool, "aggregate", "computed", {"a": 1})  # type: ignore[arg-type]
    assert rid == 99
    insert_sql, insert_args = pool.executed[0]
    assert "INSERT INTO etl_runs" in insert_sql
    assert insert_args[1] == "computed"
