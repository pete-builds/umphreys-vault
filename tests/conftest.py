"""Shared pytest fixtures."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _load(name: str) -> Any:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def venues_atu() -> dict[str, Any]:
    """ATU /api/v2/venues.json envelope captured during probe."""
    return _load("atu_venues.json")


@pytest.fixture(scope="session")
def songs_atu() -> dict[str, Any]:
    """ATU /api/v2/songs.json envelope captured during probe."""
    return _load("atu_songs.json")


@pytest.fixture(scope="session")
def setlists_1998_atu() -> dict[str, Any]:
    """ATU /api/v2/setlists/showyear/1998.json envelope (first 3 rows)."""
    return _load("atu_setlists_1998.json")


@pytest.fixture(scope="session")
def jamcharts_atu() -> dict[str, Any]:
    """ATU /api/v2/jamcharts.json envelope captured during probe."""
    return _load("atu_jamcharts.json")


@pytest.fixture(scope="session")
def appearances_atu() -> dict[str, Any]:
    """ATU /api/v2/appearances.json envelope captured during probe."""
    return _load("atu_appearances.json")


@pytest.fixture(scope="session")
def latest_atu() -> dict[str, Any]:
    """ATU /api/v2/latest.json envelope captured during probe."""
    return _load("atu_latest.json")
