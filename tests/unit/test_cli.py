"""CLI smoke tests via Click's CliRunner.

DB layer + orchestrator are monkey-patched; the CLI surface (arg wiring, JSON
output) is what we exercise.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import pytest
from click.testing import CliRunner

from umphreys_vault import cli as cli_mod


@asynccontextmanager
async def _fake_pool_ctx(_settings: Any):
    yield "pool-stub"


async def _fake_run_migrations(_pool: Any) -> list[str]:
    return ["001_initial.sql"]


async def _fake_schema_version(_pool: Any) -> int:
    return 1


async def _fake_table_row_counts(_pool: Any) -> dict[str, int]:
    return {"shows": 0, "songs": 0, "setlist_entries": 0}


async def _fake_last_etl_run(_pool: Any) -> dict[str, Any] | None:
    return None


@pytest.fixture(autouse=True)
def _patch_db(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_mod.db, "pool_ctx", _fake_pool_ctx)
    monkeypatch.setattr(cli_mod.db, "run_migrations", _fake_run_migrations)
    monkeypatch.setattr(cli_mod.db, "schema_version", _fake_schema_version)
    monkeypatch.setattr(cli_mod.db, "table_row_counts", _fake_table_row_counts)
    monkeypatch.setattr(cli_mod.db, "last_etl_run", _fake_last_etl_run)


def test_cli_init() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["init"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == 1
    assert "001_initial.sql" in payload["applied"]


def test_cli_stats() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["stats"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema_version"] == 1
    assert payload["row_counts"]["setlist_entries"] == 0


def test_cli_backfill_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_run_backfill(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"years": [1998], "songs": 1, "aggregate": {"songs_updated": 0}}

    monkeypatch.setattr("umphreys_vault.etl.orchestrator.run_backfill", _fake_run_backfill)
    monkeypatch.setattr(cli_mod, "_make_atu", lambda s: _AsyncCloseable())

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["backfill", "--year", "1998", "--dry-run"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["years"] == [1998]


def test_cli_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_run_refresh(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"year": 2026}

    monkeypatch.setattr("umphreys_vault.etl.orchestrator.run_refresh", _fake_run_refresh)
    monkeypatch.setattr(cli_mod, "_make_atu", lambda s: _AsyncCloseable())

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["refresh"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["year"] == 2026


def test_cli_aggregate(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_agg(*a: Any, **kw: Any) -> dict[str, Any]:
        return {"aggregate": {"songs_updated": 9, "songs_reset": 0}}

    monkeypatch.setattr("umphreys_vault.etl.orchestrator.run_aggregate_only", _fake_agg)

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["aggregate"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["aggregate"]["songs_updated"] == 9


def test_cli_help_exits_clean() -> None:
    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["--help"])
    assert result.exit_code == 0
    assert "Umphrey's vault" in result.output


def test_cli_main_handles_unexpected(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = RuntimeError("boom")

    def _raise(*a: Any, **kw: Any) -> None:
        raise sentinel

    monkeypatch.setattr(cli_mod, "cli", _raise)
    with pytest.raises(SystemExit) as e:
        cli_mod.main()
    assert e.value.code == 2


class _AsyncCloseable:
    async def aclose(self) -> None:
        return None
