"""Async client for the All Things Umphreys (ATU) REST API v2.

The single upstream source for this vault. Public — no auth, no API key.
Probe-confirmed shapes are captured in PROBE-NOTES.md.

Envelope
--------
Every endpoint wraps its payload::

    {"error": false, "error_message": "", "data": [...]}

On failure ``error`` is truthy (a string or ``true``) and ``data`` is empty.
:meth:`ATUClient._get` unwraps this and raises :class:`ATUError` on error, so
every public method returns the bare ``data`` list.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from umphreys_vault.throttle import TokenBucket

log = logging.getLogger(__name__)


class ATUError(RuntimeError):
    """Raised on a non-2xx response, transport failure, or ``error`` envelope."""


class ATUClient:
    """Thin async wrapper around the ATU v2 API.

    The API is public, so there is no key to send. ``artist_id`` defaults to
    1 (Umphrey's McGee) and is forwarded on the methods that accept it.
    """

    def __init__(
        self,
        throttle: TokenBucket,
        base_url: str = "https://allthings.umphreys.com/api/v2",
        artist_id: int = 1,
        timeout: float = 20.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.artist_id = artist_id
        self._throttle = throttle
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Accept": "application/json"},
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> list[Any]:
        """GET ``path``, unwrap the ATU envelope, return the ``data`` list.

        Raises :class:`ATUError` on transport failure, non-2xx, malformed
        JSON, or a truthy ``error`` field. A 404 is treated as "no data"
        (empty list) rather than an error, matching the phish-vault client's
        tolerance for missing-resource calls.
        """
        await self._throttle.acquire()
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_exc: Exception | None = None
        for attempt in range(2):
            try:
                resp = await self._client.get(url, params=params)
            except (httpx.ConnectError, httpx.RemoteProtocolError) as exc:
                last_exc = exc
                if attempt == 0:
                    log.warning(
                        "ATU connection error, retrying once",
                        extra={"path": path, "error": str(exc)},
                    )
                    continue
                raise ATUError(f"ATU connection failed: {exc}") from exc
            except httpx.HTTPError as exc:
                raise ATUError(f"ATU transport error: {exc}") from exc

            if resp.status_code == 404:
                return []
            if resp.status_code >= 400:
                raise ATUError(f"ATU GET {path} returned {resp.status_code}: {resp.text[:300]}")
            try:
                body = resp.json()
            except ValueError as exc:
                raise ATUError(f"ATU returned invalid JSON: {exc}") from exc
            return _unwrap(body, path)

        raise ATUError(  # pragma: no cover — defensive
            f"ATU request exhausted retries: {last_exc}"
        )

    # ---- backfill driver ---------------------------------------------

    async def list_years(self) -> list[int]:
        """Enumerate every year the band has played.

        ``/list/year.json`` returns ``data`` as a list. ATU has historically
        returned either bare year scalars (``["1998", 1999, ...]``) or small
        objects keyed by ``year``/``name``; we coerce either shape to a sorted
        list of ints and silently drop anything unparseable.
        """
        data = await self._get("list/year.json", params={"artist": self.artist_id})
        years: set[int] = set()
        for item in data:
            y = _coerce_year(item)
            if y is not None:
                years.add(y)
        return sorted(years)

    # ---- setlists -----------------------------------------------------

    async def setlists_by_year(self, year: int) -> list[dict[str, Any]]:
        """All setlist rows for one year. The efficient backfill unit."""
        return _as_dicts(await self._get(f"setlists/showyear/{year}.json"))

    async def setlists_by_date(self, date: str) -> list[dict[str, Any]]:
        """All setlist rows for one show date (``YYYY-MM-DD``)."""
        return _as_dicts(await self._get(f"setlists/showdate/{date}.json"))

    async def latest(self) -> list[dict[str, Any]]:
        """Setlist rows for the most recent show (drives ``refresh``)."""
        return _as_dicts(await self._get("latest.json"))

    # ---- catalog ------------------------------------------------------

    async def songs(self) -> list[dict[str, Any]]:
        """Full song catalog (~1128 rows)."""
        return _as_dicts(await self._get("songs.json"))

    async def venues(self) -> list[dict[str, Any]]:
        """Venue catalog."""
        return _as_dicts(await self._get("venues.json"))

    async def jamcharts(self) -> list[dict[str, Any]]:
        """Jam chart entries."""
        return _as_dicts(await self._get("jamcharts.json"))

    async def appearances(self) -> list[dict[str, Any]]:
        """Guest sit-ins / appearances."""
        return _as_dicts(await self._get("appearances.json"))


def _unwrap(body: Any, path: str) -> list[Any]:
    """Unwrap the ATU ``{error, error_message, data}`` envelope to ``data``."""
    if not isinstance(body, dict):
        raise ATUError(f"ATU {path}: expected an object envelope, got {type(body).__name__}")
    err = body.get("error")
    # ``error`` is false/"" on success; truthy (bool true or a message) on error.
    if err:
        msg = body.get("error_message") or err
        raise ATUError(f"ATU {path} error: {msg}")
    data = body.get("data")
    if data is None:
        return []
    if not isinstance(data, list):
        # Some methods could conceivably return a single object; normalise.
        return [data]
    return data


def _as_dicts(data: list[Any]) -> list[dict[str, Any]]:
    """Filter a ``data`` list down to dict rows (defensive)."""
    return [r for r in data if isinstance(r, dict)]


def _coerce_year(item: Any) -> int | None:
    """Coerce a ``/list/year`` element (scalar or object) to an int year."""
    if isinstance(item, int):
        return item if 1900 <= item <= 2100 else None
    if isinstance(item, str):
        s = item.strip()
        return int(s) if s.isdigit() and 1900 <= int(s) <= 2100 else None
    if isinstance(item, dict):
        for key in ("year", "name", "value"):
            v = item.get(key)
            y = _coerce_year(v)
            if y is not None:
                return y
    return None
