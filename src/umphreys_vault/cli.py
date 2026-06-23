"""Click CLI for umphreys-vault.

Subcommands::

    umphreys-vault init                  # apply migrations
    umphreys-vault backfill [--year N]   # full historical backfill (or one year)
    umphreys-vault refresh               # latest show + current year + enrich + aggregate
    umphreys-vault aggregate             # recompute per-song debut/last/count/gap
    umphreys-vault stats                 # JSON row counts per table
    umphreys-vault serve-status          # tiny FastAPI status endpoint
"""

from __future__ import annotations

import asyncio
import json
import sys
from typing import Any

import click

from umphreys_vault import __version__, db
from umphreys_vault.clients.atu import ATUClient
from umphreys_vault.config import Settings, get_settings
from umphreys_vault.logging_setup import configure_logging
from umphreys_vault.throttle import TokenBucket


def _settings(ctx: click.Context) -> Settings:
    s: Settings = ctx.obj["settings"]
    return s


def _make_atu(settings: Settings) -> ATUClient:
    bucket = TokenBucket(settings.etl_throttle_atu_rps)
    return ATUClient(
        throttle=bucket,
        base_url=settings.atu_base_url,
        artist_id=settings.atu_artist_id,
        timeout=settings.etl_request_timeout_s,
    )


@click.group()
@click.version_option(__version__)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Umphrey's vault: Postgres data store + ETL for ATU setlists."""
    ctx.ensure_object(dict)
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    ctx.obj["settings"] = settings


# -------- init -----------------------------------------------------


@cli.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Apply migrations to the configured Postgres."""

    async def _run() -> None:
        settings = _settings(ctx)
        async with db.pool_ctx(settings) as pool:
            applied = await db.run_migrations(pool)
            v = await db.schema_version(pool)
        click.echo(json.dumps({"schema_version": v, "applied": applied}, indent=2))

    asyncio.run(_run())


# -------- backfill -------------------------------------------------


@cli.command()
@click.option("--year", type=int, default=None, help="Limit to one year.")
@click.option("--dry-run", is_flag=True, help="Fetch + log, write nothing.")
@click.pass_context
def backfill(ctx: click.Context, year: int | None, dry_run: bool) -> None:
    """Full historical backfill (slow). Use --year to limit to one year."""
    from umphreys_vault.etl.orchestrator import run_backfill

    async def _run() -> dict[str, Any]:
        settings = _settings(ctx)
        atu = _make_atu(settings)
        try:
            async with db.pool_ctx(settings) as pool:
                await db.run_migrations(pool)
                return await run_backfill(
                    pool,
                    atu,
                    year=year,
                    concurrency=settings.etl_concurrency,
                    dry_run=dry_run,
                )
        finally:
            await atu.aclose()

    summary = asyncio.run(_run())
    click.echo(json.dumps(summary, indent=2, default=str))


# -------- refresh --------------------------------------------------


@cli.command()
@click.option("--dry-run", is_flag=True, help="Fetch + log, write nothing.")
@click.pass_context
def refresh(ctx: click.Context, dry_run: bool) -> None:
    """Incremental refresh: latest show + current year, then enrich + aggregate."""
    from umphreys_vault.etl.orchestrator import run_refresh

    async def _run() -> dict[str, Any]:
        settings = _settings(ctx)
        atu = _make_atu(settings)
        try:
            async with db.pool_ctx(settings) as pool:
                await db.run_migrations(pool)
                return await run_refresh(
                    pool,
                    atu,
                    concurrency=settings.etl_concurrency,
                    dry_run=dry_run,
                    recent_days=settings.refresh_recent_days,
                )
        finally:
            await atu.aclose()

    summary = asyncio.run(_run())
    click.echo(json.dumps(summary, indent=2, default=str))


# -------- aggregate ------------------------------------------------


@cli.command()
@click.option("--dry-run", is_flag=True, help="Compute counts but write nothing.")
@click.pass_context
def aggregate(ctx: click.Context, dry_run: bool) -> None:
    """Recompute per-song debut / last-play / times-played / gap."""
    from umphreys_vault.etl.orchestrator import run_aggregate_only

    async def _run() -> dict[str, Any]:
        settings = _settings(ctx)
        async with db.pool_ctx(settings) as pool:
            await db.run_migrations(pool)
            return await run_aggregate_only(pool, dry_run=dry_run)

    summary = asyncio.run(_run())
    click.echo(json.dumps(summary, indent=2, default=str))


# -------- stats ----------------------------------------------------


@cli.command()
@click.pass_context
def stats(ctx: click.Context) -> None:
    """JSON status snapshot: row counts per table + last ETL run."""

    async def _run() -> dict[str, Any]:
        settings = _settings(ctx)
        async with db.pool_ctx(settings) as pool:
            return {
                "version": __version__,
                "schema_version": await db.schema_version(pool),
                "row_counts": await db.table_row_counts(pool),
                "last_etl_run": await db.last_etl_run(pool),
            }

    snap = asyncio.run(_run())
    click.echo(json.dumps(snap, indent=2, default=str))


# -------- serve-status --------------------------------------------


@cli.command(name="serve-status")
@click.option("--host", default=None)
@click.option("--port", type=int, default=None)
@click.pass_context
def serve_status(ctx: click.Context, host: str | None, port: int | None) -> None:
    """Tiny FastAPI status endpoint (read-only)."""
    import uvicorn

    settings = _settings(ctx)
    uvicorn.run(
        "umphreys_vault.status:app",
        host=host or settings.status_host,
        port=port or settings.status_port,
        log_level=settings.log_level.lower(),
    )


def main() -> None:
    try:
        cli(standalone_mode=True)
    except SystemExit:
        raise
    except Exception as exc:
        click.echo(f"umphreys-vault error: {exc}", err=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
