"""Tiny read-only FastAPI status endpoint + X-setlist ingest write path.

Exposes ``/healthz`` (always 200 if process is up) and ``/status`` with
``{schema_version, row_counts, last_etl_run}``. Bound to LAN/Tailscale only;
never exposes secrets.

Also exposes ``POST /ingest/x-setlist`` — the HTTP integration boundary the
n8n workflow POSTs extracted X/Twitter setlist titles to. n8n runs in its own
container and cannot import the vault package or reach Postgres cleanly, so
this thin endpoint wraps :mod:`umphreys_vault.x_staging` (resolve titles to
catalog slugs, then idempotently upsert the matched rows into
``x_setlist_staging``). The X-derived titles are treated strictly as data:
they only flow into the parameterised resolver/upsert in x_staging, never into
eval/exec or string-built SQL.

Auth: a shared secret in the ``X-Ingest-Secret`` header, constant-time-compared
to ``INGEST_SECRET``. If ``INGEST_SECRET`` is unset the route is fail-closed
(503) so a misconfigured deploy never accepts unauthenticated writes; status +
health stay available.
"""

from __future__ import annotations

import secrets
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, field_validator

from umphreys_vault import __version__, db, x_staging
from umphreys_vault.config import get_settings


@asynccontextmanager
async def _lifespan(app: FastAPI) -> Any:
    settings = get_settings()
    pool = await db.connect(settings)
    app.state.pool = pool
    app.state.settings = settings
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(
    title="umphreys-vault status",
    version=__version__,
    lifespan=_lifespan,
)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@app.get("/status")
async def status() -> dict[str, Any]:
    pool = app.state.pool
    return {
        "version": __version__,
        "schema_version": await db.schema_version(pool),
        "row_counts": await db.table_row_counts(pool),
        "last_etl_run": await db.last_etl_run(pool),
    }


# ---------------------------------------------------------------------------
# X-setlist ingest (write path wrapping x_staging)
# ---------------------------------------------------------------------------

_VALID_SET_HINTS = {"1", "2", "3", "e"}


class IngestSong(BaseModel):
    """One song extracted from an X post. ``raw_title`` is untrusted text."""

    raw_title: str
    set_number_hint: str | None = None
    position_hint: int | None = None
    confidence: float = Field(default=1.0)

    @field_validator("raw_title")
    @classmethod
    def _strip_title(cls, v: str) -> str:
        return v.strip()

    @field_validator("set_number_hint")
    @classmethod
    def _norm_set_hint(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip().lower()
        if v == "":
            return None
        # Drop unrecognised hints rather than reject the whole request; the
        # set number is advisory only and x_staging tolerates a null hint.
        return v if v in _VALID_SET_HINTS else None

    @field_validator("confidence")
    @classmethod
    def _clamp_confidence(cls, v: float) -> float:
        # Clamp (not reject): out-of-range values from a flaky extractor
        # shouldn't drop an otherwise-good setlist row.
        if v < 0.0:
            return 0.0
        if v > 1.0:
            return 1.0
        return v


class IngestRequest(BaseModel):
    """POST /ingest/x-setlist body."""

    show_date: str
    songs: list[IngestSong] = Field(default_factory=list)
    source_post_id: str | None = None

    @field_validator("show_date")
    @classmethod
    def _validate_date(cls, v: str) -> str:
        try:
            date.fromisoformat(v.strip())
        except (ValueError, AttributeError) as exc:
            raise ValueError("show_date must be YYYY-MM-DD") from exc
        return v.strip()


def _check_auth(provided: str | None) -> None:
    """Constant-time shared-secret check. Fail-closed if INGEST_SECRET unset."""
    settings = get_settings()
    expected = settings.ingest_secret.get_secret_value()
    if not expected:
        raise HTTPException(status_code=503, detail="ingest disabled: INGEST_SECRET not configured")
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="invalid or missing X-Ingest-Secret")


@app.post("/ingest/x-setlist")
async def ingest_x_setlist(
    body: IngestRequest,
    x_ingest_secret: str | None = Header(default=None, alias="X-Ingest-Secret"),
) -> dict[str, Any]:
    """Resolve X-sourced raw titles to slugs and upsert matched rows.

    Returns ``{ok, written, matched: [slug...], rejected: [raw_title...]}``.
    Unmatched titles are dropped and reported in ``rejected`` (never invented
    as slugs). An empty ``songs`` list is a no-op success (written=0).
    """
    _check_auth(x_ingest_secret)

    if not body.songs:
        return {"ok": True, "written": 0, "matched": [], "rejected": []}

    titles = [s.raw_title for s in body.songs]
    resolved = await x_staging.resolve_titles(titles)

    # resolve_titles preserves input order, so zip back to the per-song hints.
    upsert_rows: list[dict[str, Any]] = []
    matched: list[str] = []
    rejected: list[str] = []
    for song, res in zip(body.songs, resolved, strict=True):
        if not res.get("matched"):
            rejected.append(song.raw_title)
            continue
        slug = res["song_slug"]
        matched.append(slug)
        upsert_rows.append(
            {
                "song_slug": slug,
                "song_name": res.get("song_name"),
                "set_number_hint": song.set_number_hint,
                "position_hint": song.position_hint,
                "confidence": song.confidence,
                "source_post_id": body.source_post_id,
            }
        )

    written = await x_staging.upsert_staging(body.show_date, upsert_rows)
    return {"ok": True, "written": written, "matched": matched, "rejected": rejected}
