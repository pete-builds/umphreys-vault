"""POST /ingest/x-setlist tests.

The route wraps x_staging.resolve_titles + upsert_staging. We monkeypatch those
two module-level coroutines (the same DB boundary the rest of the suite mocks)
and the INGEST_SECRET setting, then drive the route via FastAPI's TestClient.

TestClient is NOT used as a context manager here, so the app lifespan (which
connects to a real Postgres pool) never fires. The ingest route doesn't touch
app.state.pool, so this is sufficient and keeps the test DB-free.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from umphreys_vault import status as status_mod
from umphreys_vault.config import Settings

_SECRET = "test-shared-secret"

# Catalog the fake resolver knows about: title -> slug.
_KNOWN = {
    "In The Kitchen": ("in-the-kitchen", "In the Kitchen"),
    "All in Time": ("all-in-time", "All in Time"),
}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with INGEST_SECRET set and x_staging fully mocked."""

    def _settings_with_secret() -> Settings:
        s = Settings()
        object.__setattr__(s, "ingest_secret", SecretStr(_SECRET))
        return s

    # get_settings is imported into status as a name; patch it there.
    monkeypatch.setattr(status_mod, "get_settings", _settings_with_secret)

    async def _fake_resolve(titles: list[str]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for t in titles:
            hit = _KNOWN.get(t)
            if hit:
                out.append(
                    {"raw_title": t, "song_slug": hit[0], "song_name": hit[1], "matched": True}
                )
            else:
                out.append(
                    {"raw_title": t, "song_slug": None, "song_name": None, "matched": False}
                )
        return out

    captured: dict[str, Any] = {}

    async def _fake_upsert(show_date: str, rows: list[dict[str, Any]]) -> int:
        captured["show_date"] = show_date
        captured["rows"] = rows
        return len(rows)

    monkeypatch.setattr(status_mod.x_staging, "resolve_titles", _fake_resolve)
    monkeypatch.setattr(status_mod.x_staging, "upsert_staging", _fake_upsert)

    c = TestClient(status_mod.app)
    c._captured = captured  # type: ignore[attr-defined]
    return c


def _hdr(secret: str | None = _SECRET) -> dict[str, str]:
    return {"X-Ingest-Secret": secret} if secret is not None else {}


def test_happy_path_two_matched_one_unmatched(client: TestClient) -> None:
    body = {
        "show_date": "2026-07-01",
        "source_post_id": "tweet-99",
        "songs": [
            {"raw_title": "In The Kitchen", "set_number_hint": "1", "position_hint": 3,
             "confidence": 0.9},
            {"raw_title": "All in Time", "set_number_hint": "2", "position_hint": 1,
             "confidence": 0.7},
            {"raw_title": "Totally Fake Song", "confidence": 0.5},
        ],
    }
    r = client.post("/ingest/x-setlist", json=body, headers=_hdr())
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["written"] == 2
    assert sorted(data["matched"]) == ["all-in-time", "in-the-kitchen"]
    assert data["rejected"] == ["Totally Fake Song"]

    # Hints + source carried into the upsert rows.
    rows = client._captured["rows"]  # type: ignore[attr-defined]
    by_slug = {row["song_slug"]: row for row in rows}
    assert by_slug["in-the-kitchen"]["set_number_hint"] == "1"
    assert by_slug["in-the-kitchen"]["position_hint"] == 3
    assert by_slug["in-the-kitchen"]["confidence"] == pytest.approx(0.9)
    assert by_slug["in-the-kitchen"]["source_post_id"] == "tweet-99"
    assert client._captured["show_date"] == "2026-07-01"  # type: ignore[attr-defined]


def test_auth_missing_secret_rejected(client: TestClient) -> None:
    body = {"show_date": "2026-07-01", "songs": [{"raw_title": "All in Time"}]}
    r = client.post("/ingest/x-setlist", json=body, headers=_hdr(secret=None))
    assert r.status_code == 401


def test_auth_wrong_secret_rejected(client: TestClient) -> None:
    body = {"show_date": "2026-07-01", "songs": [{"raw_title": "All in Time"}]}
    r = client.post("/ingest/x-setlist", json=body, headers=_hdr(secret="nope"))
    assert r.status_code == 401


def test_empty_songs_is_noop_success(client: TestClient) -> None:
    body = {"show_date": "2026-07-01", "songs": []}
    r = client.post("/ingest/x-setlist", json=body, headers=_hdr())
    assert r.status_code == 200
    data = r.json()
    assert data == {"ok": True, "written": 0, "matched": [], "rejected": []}


def test_malformed_body_rejected(client: TestClient) -> None:
    # songs items must be objects with raw_title; a bare string is invalid.
    body = {"show_date": "2026-07-01", "songs": ["just a string"]}
    r = client.post("/ingest/x-setlist", json=body, headers=_hdr())
    assert r.status_code == 422


def test_bad_date_rejected(client: TestClient) -> None:
    body = {"show_date": "07/01/2026", "songs": [{"raw_title": "All in Time"}]}
    r = client.post("/ingest/x-setlist", json=body, headers=_hdr())
    # Pydantic validation failure on the body field => 422.
    assert r.status_code == 422


def test_confidence_clamped(client: TestClient) -> None:
    body = {
        "show_date": "2026-07-01",
        "songs": [{"raw_title": "All in Time", "confidence": 5.0}],
    }
    r = client.post("/ingest/x-setlist", json=body, headers=_hdr())
    assert r.status_code == 200
    rows = client._captured["rows"]  # type: ignore[attr-defined]
    assert rows[0]["confidence"] == pytest.approx(1.0)


def test_ingest_disabled_when_secret_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    # No INGEST_SECRET => fail-closed 503, even with a header present.
    def _settings_no_secret() -> Settings:
        s = Settings()
        object.__setattr__(s, "ingest_secret", SecretStr(""))
        return s

    monkeypatch.setattr(status_mod, "get_settings", _settings_no_secret)
    c = TestClient(status_mod.app)
    body = {"show_date": "2026-07-01", "songs": [{"raw_title": "All in Time"}]}
    r = c.post("/ingest/x-setlist", json=body, headers={"X-Ingest-Secret": "anything"})
    assert r.status_code == 503
