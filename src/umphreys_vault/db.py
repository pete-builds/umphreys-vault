"""Postgres connection + migration runner.

Uses asyncpg for the actual ETL work. Migrations are plain .sql files in
``migrations/`` applied in lexical order; we keep them boring on purpose.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import asyncpg

from umphreys_vault.config import Settings

log = logging.getLogger(__name__)

MIGRATIONS_PACKAGE = "umphreys_vault"  # we use repo-rooted migrations/ via Path


def _migrations_dir() -> Path:
    """Locate the migrations/ directory relative to the package source.

    This works in both editable installs and the Docker image, where the
    package and migrations/ are siblings under /app.
    """
    here = Path(__file__).resolve().parent
    # src/umphreys_vault/db.py → repo root migrations/
    candidates = [
        here.parent.parent / "migrations",
        here.parent / "migrations",
        Path("/app/migrations"),
    ]
    for c in candidates:
        if c.is_dir():
            return c
    raise FileNotFoundError(f"Could not locate migrations directory; looked in: {candidates!r}")


async def connect(settings: Settings) -> asyncpg.Pool:
    """Build an asyncpg pool. Caller closes."""
    return cast(
        asyncpg.Pool,
        await asyncpg.create_pool(
            dsn=settings.pg_dsn,
            min_size=1,
            max_size=max(2, settings.etl_concurrency + 1),
            command_timeout=60,
        ),
    )


@asynccontextmanager
async def pool_ctx(settings: Settings) -> AsyncIterator[asyncpg.Pool]:
    pool = await connect(settings)
    try:
        yield pool
    finally:
        await pool.close()


async def run_migrations(pool: asyncpg.Pool) -> list[str]:
    """Apply all .sql files in migrations/ in lexical order.

    Each file is treated as one transaction. ``schema_version`` records
    the highest applied integer at the bottom of each file; we read that
    table to skip already-applied migrations.
    """
    mdir = _migrations_dir()
    files = sorted(p for p in mdir.iterdir() if p.suffix == ".sql")
    if not files:
        log.warning("No migrations found", extra={"dir": str(mdir)})
        return []

    applied: list[str] = []
    async with pool.acquire() as conn:
        # Bootstrap schema_version if missing.
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                version    INTEGER PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        current = await conn.fetchval("SELECT COALESCE(MAX(version), 0) FROM schema_version")
        for f in files:
            # Filename convention: NNN_description.sql where NNN is integer.
            num = int(f.stem.split("_", 1)[0])
            if num <= current:
                continue
            sql = f.read_text(encoding="utf-8")
            async with conn.transaction():
                await conn.execute(sql)
            applied.append(f.name)
            log.info("Applied migration", extra={"file": f.name, "version": num})
    return applied


async def schema_version(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        v = await conn.fetchval("SELECT COALESCE(MAX(version), 0) FROM schema_version")
    return int(v or 0)


async def table_row_counts(pool: asyncpg.Pool) -> dict[str, int]:
    """Return row counts for the persisted tables (skip pg_*, information_schema)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )
        out: dict[str, int] = {}
        for r in rows:
            name = r["table_name"]
            n = await conn.fetchval(f'SELECT COUNT(*) FROM "{name}"')
            out[name] = int(n)
    return out


async def last_etl_run(pool: asyncpg.Pool) -> dict[str, object] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM etl_runs ORDER BY id DESC LIMIT 1")
    return dict(row) if row else None
