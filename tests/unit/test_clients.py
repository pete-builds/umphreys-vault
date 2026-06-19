"""ATU client tests: envelope unwrapping, error handling, year coercion.

Uses respx to mock httpx. Fixtures are real ATU responses captured during
the build probe (see tests/fixtures/atu_*.json).
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
import respx

from umphreys_vault.clients.atu import ATUClient, ATUError, _coerce_year
from umphreys_vault.throttle import TokenBucket

BASE = "https://allthings.umphreys.com/api/v2"


def _bucket() -> TokenBucket:
    return TokenBucket(rps=100)


@pytest.mark.asyncio
async def test_venues_unwraps_envelope(venues_atu: dict[str, Any]) -> None:
    with respx.mock(base_url=BASE, assert_all_called=True) as mock:
        mock.get("/venues.json").mock(return_value=httpx.Response(200, json=venues_atu))
        client = ATUClient(throttle=_bucket())
        try:
            data = await client.venues()
        finally:
            await client.aclose()
    assert isinstance(data, list)
    assert data[0]["slug"] == "the-tabernacle-atlanta-ga-usa"
    assert len(data) == 2


@pytest.mark.asyncio
async def test_setlists_by_year_returns_data_rows(setlists_1998_atu: dict[str, Any]) -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/setlists/showyear/1998.json").mock(
            return_value=httpx.Response(200, json=setlists_1998_atu)
        )
        client = ATUClient(throttle=_bucket())
        try:
            rows = await client.setlists_by_year(1998)
        finally:
            await client.aclose()
    assert len(rows) == 3
    assert rows[0]["songname"] == "Bob"
    assert rows[0]["uniqueid"] == "511"


@pytest.mark.asyncio
async def test_latest_unwraps(latest_atu: dict[str, Any]) -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/latest.json").mock(return_value=httpx.Response(200, json=latest_atu))
        client = ATUClient(throttle=_bucket())
        try:
            rows = await client.latest()
        finally:
            await client.aclose()
    assert rows[0]["showdate"] == "2026-06-18"
    assert rows[0]["settype"] == "One Set"


@pytest.mark.asyncio
async def test_error_envelope_raises() -> None:
    body = {"error": True, "error_message": "bad request", "data": []}
    with respx.mock(base_url=BASE) as mock:
        mock.get("/songs.json").mock(return_value=httpx.Response(200, json=body))
        client = ATUClient(throttle=_bucket())
        try:
            with pytest.raises(ATUError, match="bad request"):
                await client.songs()
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_error_string_envelope_raises() -> None:
    # ATU's `error` is sometimes a truthy string rather than a bool.
    body = {"error": "ValueError", "error_message": "", "data": []}
    with respx.mock(base_url=BASE) as mock:
        mock.get("/venues.json").mock(return_value=httpx.Response(200, json=body))
        client = ATUClient(throttle=_bucket())
        try:
            with pytest.raises(ATUError, match="ValueError"):
                await client.venues()
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_404_returns_empty_list() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/setlists/showdate/9999-99-99.json").mock(return_value=httpx.Response(404))
        client = ATUClient(throttle=_bucket())
        try:
            rows = await client.setlists_by_date("9999-99-99")
        finally:
            await client.aclose()
    assert rows == []


@pytest.mark.asyncio
async def test_5xx_raises() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/songs.json").mock(return_value=httpx.Response(500, text="oops"))
        client = ATUClient(throttle=_bucket())
        try:
            with pytest.raises(ATUError):
                await client.songs()
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_invalid_json_raises() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/venues.json").mock(return_value=httpx.Response(200, text="<<not json>>"))
        client = ATUClient(throttle=_bucket())
        try:
            with pytest.raises(ATUError):
                await client.venues()
        finally:
            await client.aclose()


@pytest.mark.asyncio
async def test_missing_data_key_returns_empty() -> None:
    body = {"error": False, "error_message": ""}
    with respx.mock(base_url=BASE) as mock:
        mock.get("/songs.json").mock(return_value=httpx.Response(200, json=body))
        client = ATUClient(throttle=_bucket())
        try:
            data = await client.songs()
        finally:
            await client.aclose()
    assert data == []


@pytest.mark.asyncio
async def test_list_years_coerces_scalar_and_object_shapes() -> None:
    # ATU has returned both bare scalars and objects keyed by `year`.
    body = {
        "error": False,
        "error_message": "",
        "data": ["1998", 1999, {"year": "2000"}, {"name": 2001}, "garbage", 1234567],
    }
    with respx.mock(base_url=BASE) as mock:
        mock.get("/list/year.json").mock(return_value=httpx.Response(200, json=body))
        client = ATUClient(throttle=_bucket())
        try:
            years = await client.list_years()
        finally:
            await client.aclose()
    assert years == [1998, 1999, 2000, 2001]


def test_coerce_year_edge_cases() -> None:
    assert _coerce_year("2024") == 2024
    assert _coerce_year(2024) == 2024
    assert _coerce_year({"year": 2024}) == 2024
    assert _coerce_year({"value": "2024"}) == 2024
    assert _coerce_year("not-a-year") is None
    assert _coerce_year(99) is None
    assert _coerce_year(None) is None


@pytest.mark.asyncio
async def test_list_years_passes_artist_id() -> None:
    captured: dict[str, str] = {}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"error": False, "error_message": "", "data": []})

    with respx.mock(base_url=BASE) as mock:
        mock.get("/list/year.json").mock(side_effect=_capture)
        client = ATUClient(throttle=_bucket(), artist_id=7)
        try:
            await client.list_years()
        finally:
            await client.aclose()
    assert "artist=7" in captured["url"]
