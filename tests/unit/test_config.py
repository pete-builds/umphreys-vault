"""Settings smoke tests."""

from __future__ import annotations

import os

import pytest

from umphreys_vault.config import Settings


def test_settings_dsn_uses_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PG_HOST", "h")
    monkeypatch.setenv("PG_PORT", "5555")
    monkeypatch.setenv("PG_DB", "d")
    monkeypatch.setenv("PG_USER", "u")
    monkeypatch.setenv("PG_PASSWORD", "secret-pw")
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert s.pg_dsn == "postgresql://u:secret-pw@h:5555/d"


def test_settings_defaults_present(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure no stale env from other tests leaks in.
    for k in ("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    s = Settings(_env_file=None)  # type: ignore[call-arg]
    assert s.etl_concurrency >= 1
    assert s.etl_throttle_atu_rps > 0
    assert "allthings.umphreys.com" in s.atu_base_url
    assert s.atu_artist_id == 1
    assert s.status_port == 3719


def test_settings_does_not_log_secret() -> None:
    s = Settings(
        pg_password="hunter2",  # type: ignore[arg-type]
        _env_file=None,  # type: ignore[call-arg]
    )
    # repr() for SecretStr is masked; defense in depth.
    assert "hunter2" not in repr(s)
    # Unwrap explicitly only via the .get_secret_value() boundary.
    assert s.pg_password.get_secret_value() == "hunter2"


def test_get_settings_callable() -> None:
    from umphreys_vault.config import get_settings

    # Should not raise even when the env is empty (all fields have defaults).
    s = get_settings()
    assert s is not None
    assert isinstance(s.etl_concurrency, int)
    # Make sure os import is referenced so ruff doesn't strip it.
    assert "PATH" in os.environ
