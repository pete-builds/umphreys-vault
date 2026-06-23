"""X/Twitter setlist staging: resolve raw titles to catalog slugs + upsert.

This is the write path mcp-umphreys uses to land ADVISORY X-sourced setlist
rows during a live show. Two deterministic functions plus a small JSON CLI so
n8n can drive it via Execute Command:

- :func:`resolve_titles` maps each raw X title to an existing ``songs.slug``
  (exact title -> alias -> normalized/slugified -> fuzzy match). Unresolvable
  titles are returned with ``matched=False`` so the caller can reject them; we
  never invent a slug, because ``x_setlist_staging.song_slug`` is FK-gated on
  ``songs``.
- :func:`upsert_staging` writes resolved rows idempotently on the
  ``(show_date, song_slug)`` unique key.

The normalized-match step reuses :func:`umphreys_vault.etl.upserts.slugify` (the
canonical ATU slug convention) so a title like "In The Kitchen" resolves to the
catalog slug "in-the-kitchen". When exact/alias/slug all miss, a conservative
FUZZY fallback scores the noisy title against every catalog title+alias and
accepts the best candidate only above :data:`X_FUZZY_THRESHOLD` (so a wrong
fuzzy match never mis-credits a song; we drop rather than guess). Fuzzy uses
``rapidfuzz`` when importable and falls back to stdlib ``difflib`` otherwise, so
there's no new hard dependency.

Everything here is deterministic (no LLM, no reasoning). Logs go to stderr via
the package logger; data goes to stdout as JSON in CLI mode.
"""

from __future__ import annotations

import asyncio
import difflib
import json
import logging
import os
import sys
from datetime import date
from typing import Any

import asyncpg

from umphreys_vault import db
from umphreys_vault.config import get_settings
from umphreys_vault.etl.upserts import _to_int, _to_text, slugify
from umphreys_vault.logging_setup import configure_logging

log = logging.getLogger(__name__)

# rapidfuzz is preferred (faster, better partial scoring) but optional — guard
# the import so difflib (stdlib) is the no-extra-dependency fallback.
try:  # pragma: no cover - exercised by whichever lib is installed
    from rapidfuzz import fuzz as _rf_fuzz  # type: ignore[import-not-found]

    _HAS_RAPIDFUZZ = True
except ImportError:  # pragma: no cover
    _rf_fuzz = None
    _HAS_RAPIDFUZZ = False


def _fuzzy_threshold() -> float:
    """Minimum 0..1 similarity for a fuzzy match. Env-overridable.

    A title scoring below this is rejected (``matched=False``): a wrong fuzzy
    match could mis-score the game, so we keep this conservative and drop rather
    than guess. ``X_FUZZY_THRESHOLD`` (a 0..1 ratio) overrides the default.
    """
    raw = os.getenv("X_FUZZY_THRESHOLD")
    if raw:
        try:
            return float(raw)
        except ValueError:
            log.warning("ignoring invalid X_FUZZY_THRESHOLD=%r", raw)
    return X_FUZZY_THRESHOLD


# Default 0..1 similarity floor for the fuzzy fallback (rapidfuzz 0-100 scores
# are scaled to 0..1 before comparison, so one threshold covers both backends).
X_FUZZY_THRESHOLD = 0.86


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def _norm(s: str) -> str:
    """Normalize a title for matching: reuse the catalog slug convention.

    Two titles that slugify to the same value are considered the same song
    (e.g. "In the Kitchen" / "In The Kitchen!" -> "in-the-kitchen").
    """
    return slugify(s or "")


def _fuzzy_norm(s: str) -> str:
    """Normalize for fuzzy scoring: slug words rejoined with spaces.

    Reuses :func:`slugify` (lowercase, strip punctuation, collapse runs) then
    swaps hyphens for spaces so token-oriented scorers see real word tokens
    (e.g. "In The Kitchen!!" -> "in the kitchen").
    """
    return slugify(s or "").replace("-", " ").strip()


def _fuzzy_score(a: str, b: str) -> float:
    """Similarity in 0..1 between two pre-normalized strings.

    rapidfuzz ``WRatio`` (0-100) scaled to 0..1 when available; otherwise the
    stdlib :class:`difflib.SequenceMatcher` ratio. Both already return 0..1 after
    scaling, so callers compare against a single 0..1 threshold.
    """
    if not a or not b:
        return 0.0
    if _HAS_RAPIDFUZZ:
        return float(_rf_fuzz.WRatio(a, b)) / 100.0
    return difflib.SequenceMatcher(None, a, b).ratio()


async def _load_song_index(conn: asyncpg.Connection) -> dict[str, Any]:
    """Build lookup indexes over the songs catalog.

    Returns ``by_title``/``by_alias``/``by_slug`` (each keyed by normalized
    string -> ``{"slug": ..., "title": ...}``) for the exact fast paths, plus
    ``candidates``: a flat list of ``(fuzzy_norm_string, entry)`` over every
    title AND alias, scanned only by the fuzzy fallback. Title/alias lookups go
    through :func:`_norm` so casing/punctuation differ harmlessly. ``by_slug``
    lets a title that already arrives as a slug resolve.
    """
    rows = await conn.fetch("SELECT slug, title, alias FROM songs")
    by_title: dict[str, dict[str, str]] = {}
    by_alias: dict[str, dict[str, str]] = {}
    by_slug: dict[str, dict[str, str]] = {}
    candidates: list[tuple[str, dict[str, str]]] = []
    for r in rows:
        slug = str(r["slug"])
        title = str(r["title"])
        entry = {"slug": slug, "title": title}
        by_slug[slug] = entry
        if title:
            by_title.setdefault(_norm(title), entry)
            fn = _fuzzy_norm(title)
            if fn:
                candidates.append((fn, entry))
        alias = r["alias"]
        if alias:
            by_alias.setdefault(_norm(str(alias)), entry)
            fn = _fuzzy_norm(str(alias))
            if fn:
                candidates.append((fn, entry))
    return {
        "by_title": by_title,
        "by_alias": by_alias,
        "by_slug": by_slug,
        "candidates": candidates,
    }


def _resolve_one(raw_title: str, index: dict[str, Any]) -> dict[str, Any]:
    """Resolve a single raw title against the prebuilt song index.

    Match order: exact title -> alias -> normalized slug -> fuzzy fallback. The
    exact paths win whenever they hit; fuzzy runs only when all three miss and
    accepts the best candidate only at/above the threshold. Returns a dict with
    ``raw_title``, ``song_slug``, ``song_name`` (canonical), ``matched``, and
    ``match_method`` ("exact"|"alias"|"slug"|"fuzzy"|None).
    """
    key = _norm(raw_title)
    miss = {
        "raw_title": raw_title,
        "song_slug": None,
        "song_name": None,
        "matched": False,
        "match_method": None,
    }
    if not key:
        return miss
    method_for = {"by_title": "exact", "by_alias": "alias", "by_slug": "slug"}
    for sub in ("by_title", "by_alias", "by_slug"):
        hit = index[sub].get(key)
        if hit:
            return {
                "raw_title": raw_title,
                "song_slug": hit["slug"],
                "song_name": hit["title"],
                "matched": True,
                "match_method": method_for[sub],
            }
    return _resolve_fuzzy(raw_title, index, miss)


def _resolve_fuzzy(
    raw_title: str, index: dict[str, Any], miss: dict[str, Any]
) -> dict[str, Any]:
    """Fuzzy fallback: best title/alias candidate above the threshold, else miss.

    Conservative on purpose: a sub-threshold best score returns ``miss`` so we
    drop the title rather than mis-credit a wrong song.
    """
    needle = _fuzzy_norm(raw_title)
    if not needle:
        return miss
    threshold = _fuzzy_threshold()
    best_score = 0.0
    best_entry: dict[str, str] | None = None
    for cand_norm, entry in index["candidates"]:
        score = _fuzzy_score(needle, cand_norm)
        if score > best_score:
            best_score = score
            best_entry = entry
            if best_score >= 1.0:
                break
    if best_entry is not None and best_score >= threshold:
        return {
            "raw_title": raw_title,
            "song_slug": best_entry["slug"],
            "song_name": best_entry["title"],
            "matched": True,
            "match_method": "fuzzy",
        }
    return miss


async def resolve_titles_conn(
    conn: asyncpg.Connection, titles: list[str]
) -> list[dict[str, Any]]:
    """Connection-bound resolver (testable with a mock conn)."""
    index = await _load_song_index(conn)
    return [_resolve_one(t, index) for t in titles]


async def resolve_titles(titles: list[str]) -> list[dict[str, Any]]:
    """Resolve raw X titles to catalog slugs.

    For each raw title: exact title -> alias -> normalized -> fuzzy match
    against the ``songs`` catalog. Returns one dict per input title:
    ``[{raw_title, song_slug, song_name, matched, match_method}]``. Unresolvable
    titles have ``matched=False`` and null slug/name so the caller can reject
    them.
    """
    settings = get_settings()
    async with db.pool_ctx(settings) as pool, pool.acquire() as conn:
        return await resolve_titles_conn(conn, titles)


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------


async def upsert_staging_conn(
    conn: asyncpg.Connection, show_date: str, rows: list[dict[str, Any]]
) -> int:
    """Connection-bound staging upsert (testable with a mock conn).

    ``rows`` are ``[{song_slug, song_name, set_number_hint, position_hint,
    confidence, source_post_id}]`` (already resolved). Idempotent on
    ``(show_date, song_slug)``: re-running identical input bumps ``last_seen_at``
    and keeps the higher confidence without creating duplicate rows.
    """
    if not rows:
        return 0
    # De-dupe on song_slug within the batch so executemany never trips the
    # unique constraint (last write wins, matching the catalog upsert pattern).
    seen: dict[str, dict[str, Any]] = {}
    for r in rows:
        slug = r.get("song_slug")
        if not slug:
            continue
        seen[str(slug)] = r
    if not seen:
        return 0

    # asyncpg encodes the DATE bind before the ::date cast applies, so it needs a
    # real date object, not the ISO string the HTTP/CLI callers pass in.
    sd = date.fromisoformat(show_date) if isinstance(show_date, str) else show_date
    payload = [
        (
            sd,
            slug,
            _to_text(r.get("song_name")) or slug,
            _to_text(r.get("set_number_hint")),
            _to_int(r.get("position_hint")),
            float(r["confidence"]),
            _to_text(r.get("source_post_id")),
        )
        for slug, r in seen.items()
    ]
    await conn.executemany(
        """
        INSERT INTO x_setlist_staging (
            show_date, song_slug, song_name, set_number_hint, position_hint,
            confidence, source_post_id
        )
        VALUES ($1::date,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (show_date, song_slug) DO UPDATE SET
            last_seen_at = now(),
            confidence = GREATEST(x_setlist_staging.confidence, EXCLUDED.confidence),
            set_number_hint = COALESCE(EXCLUDED.set_number_hint, x_setlist_staging.set_number_hint),
            position_hint = COALESCE(EXCLUDED.position_hint, x_setlist_staging.position_hint),
            song_name = EXCLUDED.song_name,
            source_post_id = COALESCE(EXCLUDED.source_post_id, x_setlist_staging.source_post_id)
        """,
        payload,
    )
    return len(payload)


async def upsert_staging(show_date: str, rows: list[dict[str, Any]]) -> int:
    """Upsert resolved X-staging rows for ``show_date``. Returns rows written.

    See :func:`upsert_staging_conn` for the row shape and idempotency contract.
    """
    settings = get_settings()
    async with db.pool_ctx(settings) as pool, pool.acquire() as conn:
        return await upsert_staging_conn(conn, show_date, rows)


# ---------------------------------------------------------------------------
# CLI (JSON in / JSON out, for n8n Execute Command)
# ---------------------------------------------------------------------------


def _usage() -> str:
    return (
        "usage:\n"
        "  python -m umphreys_vault.x_staging resolve '<json list of titles>'\n"
        "  python -m umphreys_vault.x_staging upsert <show_date> '<json list of rows>'"
    )


def main(argv: list[str] | None = None) -> int:
    """Entry point. Emits JSON to stdout; logs to stderr. Exit codes:

    0 success, 2 usage/parse error, 3 runtime error.
    """
    args = list(sys.argv[1:] if argv is None else argv)
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)

    if not args:
        print(_usage(), file=sys.stderr)
        return 2

    cmd = args[0]
    try:
        if cmd == "resolve":
            if len(args) != 2:
                print(_usage(), file=sys.stderr)
                return 2
            titles = json.loads(args[1])
            if not isinstance(titles, list):
                raise ValueError("resolve expects a JSON list of titles")
            result = asyncio.run(resolve_titles([str(t) for t in titles]))
            print(json.dumps({"resolved": result}, default=str))
            return 0

        if cmd == "upsert":
            if len(args) != 3:
                print(_usage(), file=sys.stderr)
                return 2
            show_date = args[1]
            rows = json.loads(args[2])
            if not isinstance(rows, list):
                raise ValueError("upsert expects a JSON list of rows")
            written = asyncio.run(upsert_staging(show_date, rows))
            print(json.dumps({"show_date": show_date, "written": written}))
            return 0

        print(_usage(), file=sys.stderr)
        return 2
    except (json.JSONDecodeError, ValueError) as exc:
        print(json.dumps({"error_code": "bad_input", "message": str(exc)}), file=sys.stderr)
        return 2
    except Exception as exc:  # surface as structured JSON, not a stack trace
        log.exception("x_staging failed")
        print(json.dumps({"error_code": "runtime_error", "message": str(exc)}), file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main())
