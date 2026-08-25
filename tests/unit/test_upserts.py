"""Upsert mapping + coercion tests (no real Postgres; AsyncMock connection).

These assert the ATU-field -> schema-column mapping, including the 0/1->bool
coercions, the space-separated timestamp parsing, the 1000-01-01 placeholder,
and slug derivation.
"""

from __future__ import annotations

import datetime as dt
from typing import Any
from unittest.mock import AsyncMock

import pytest

from umphreys_vault.etl.upserts import (
    _to_bool,
    _to_int,
    _to_text,
    _to_ts,
    replace_setlist_entries_for_show,
    slugify,
    upsert_appearances,
    upsert_jam_chart_entries,
    upsert_songs,
    upsert_venues,
)


def test_to_bool_handles_0_1_and_strings() -> None:
    assert _to_bool(1) is True
    assert _to_bool(0) is False
    assert _to_bool("1") is True
    assert _to_bool("0") is False
    assert _to_bool(True) is True
    assert _to_bool(None) is False
    assert _to_bool("") is False


def test_to_ts_parses_space_separated_and_drops_placeholder() -> None:
    assert _to_ts("2023-02-26 04:29:59") == dt.datetime(2023, 2, 26, 4, 29, 59)
    assert _to_ts("1000-01-01 00:00:00") is None
    assert _to_ts("") is None
    assert _to_ts(None) is None
    assert _to_ts("garbage") is None


def test_to_text_trims_trailing_newlines() -> None:
    assert _to_text("Steve Krojniewski\n") == "Steve Krojniewski"
    assert _to_text(42) == "42"
    assert _to_text(None) is None
    assert _to_text("") == ""


def test_to_int_coerces() -> None:
    assert _to_int("511") == 511
    assert _to_int(511) == 511
    assert _to_int("") is None
    assert _to_int(None) is None
    assert _to_int("nope") is None


def test_slugify_matches_atu_convention() -> None:
    assert slugify("Bridget McGuire's Filling Station South Bend IN USA") == (
        "bridget-mcguire-s-filling-station-south-bend-in-usa"
    )
    assert slugify("#5") == "5"
    assert slugify("Red Baron") == "red-baron"


@pytest.mark.asyncio
async def test_upsert_songs_maps_id_and_isoriginal(songs_atu: dict[str, Any]) -> None:
    conn = AsyncMock()
    n = await upsert_songs(conn, songs_atu["data"])
    assert n == 2
    payload = conn.executemany.await_args.args[1]
    # Tuple: (slug, song_id, title, original, original_artist, created, updated)
    by_slug = {r[0]: r for r in payload}
    row5, row13 = by_slug["5"], by_slug["13-days"]
    assert row13[0] == "13-days"
    assert row13[1] == 3  # song_id from `id`
    assert row13[2] == "13 Days"
    assert row13[3] is True  # original from isoriginal=1
    assert row13[4] == "Umphrey's McGee"
    # 1000-01-01 created_at maps to None; real updated_at parses.
    assert row5[5] is None
    assert row13[6] == dt.datetime(2023, 2, 26, 4, 29, 59)


@pytest.mark.asyncio
async def test_upsert_venues_derives_slug_when_missing() -> None:
    conn = AsyncMock()
    # A setlist-derived stub has no slug; it must be synthesised.
    rows = [
        {
            "venue_id": 8,
            "venuename": "Bridget McGuire's Filling Station",
            "city": "South Bend",
            "state": "IN",
            "country": "USA",
        }
    ]
    n = await upsert_venues(conn, rows)
    assert n == 1
    payload = conn.executemany.await_args.args[1]
    slug = payload[0][0]
    assert slug == slugify("Bridget McGuire's Filling Station")
    assert payload[0][1] == 8  # venue_id


@pytest.mark.asyncio
async def test_replace_setlist_entries_maps_fields(setlists_1998_atu: dict[str, Any]) -> None:
    conn = AsyncMock()
    rows = setlists_1998_atu["data"]
    n = await replace_setlist_entries_for_show(conn, dt.date(1998, 1, 21), rows)
    assert n == 3
    # DELETE then bulk INSERT.
    conn.execute.assert_awaited_once()
    payload = conn.executemany.await_args.args[1]
    # Tuple order: (show_date, unique_id, position, set_type, set_number,
    #   song_slug, song_name, transition_id, transition, footnote,
    #   is_jamchart, jamchart_notes, is_original, original_artist,
    #   is_reprise, is_jam)
    bob = payload[0]
    assert bob[1] == 511  # uniqueid -> unique_id (int)
    assert bob[2] == 1  # position
    assert bob[3] == "Set"  # set_type from settype
    assert bob[4] == "1"  # set_number from setnumber (raw TEXT)
    assert bob[5] == "bob"  # song_slug from slug
    assert bob[6] == "Bob"
    assert bob[10] is False  # is_jamchart from isjamchart=0
    assert bob[12] is True  # is_original from isoriginal=1
    # Red Baron is a cover.
    red_baron = payload[1]
    assert red_baron[12] is False  # isoriginal=0
    assert red_baron[13] == "Billy Cobham"  # original_artist


@pytest.mark.asyncio
async def test_replace_setlist_entries_empty() -> None:
    conn = AsyncMock()
    n = await replace_setlist_entries_for_show(conn, dt.date(1998, 1, 21), [])
    assert n == 0
    conn.execute.assert_awaited_once()  # DELETE still fires
    conn.executemany.assert_not_called()


@pytest.mark.asyncio
async def test_upsert_appearances_filters_unknown_shows_and_trims_name(
    appearances_atu: dict[str, Any],
) -> None:
    # Build a conn whose transaction context returns known show dates that
    # include only the first appearance's date.
    conn = _appearances_conn(known_dates={dt.date(1998, 1, 31)})
    n = await upsert_appearances(conn, appearances_atu["data"])
    assert n == 1
    payload = conn.executemany.await_args.args[1]
    row = payload[0]
    # Tuple: (show_date, person_id, person_name, person_slug, type, notes)
    assert row[0] == dt.date(1998, 1, 31)
    assert row[2] == "Steve Krojniewski"  # trailing \n trimmed
    assert row[3] == "steve-krojniewski"
    assert row[5] == "on percussion"


def _appearances_conn(known_dates: set[dt.date]) -> AsyncMock:
    """An AsyncMock conn whose .fetch returns the given known show dates and
    whose .transaction() is an async context manager."""
    conn = AsyncMock()

    async def _fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
        if "FROM shows" in sql:
            return [{"date": d} for d in known_dates]
        return []

    conn.fetch.side_effect = _fetch

    class _Txn:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *_: Any) -> None:
            return None

    conn.transaction = lambda: _Txn()
    return conn


# --- the empty-payload guard on the two full-corpus loaders ----------------
#
# Both loaders re-pull the ENTIRE corpus every run and replace it wholesale.
# They used to DELETE first and inspect the payload second, and a `return`
# inside `async with conn.transaction()` is not an exception -- asyncpg sees a
# clean exit and COMMITS. So the wipe was durable, nothing raised, and the run
# recorded status='ok'.
#
# This repo reaches that branch more easily than its phish-vault sibling: the
# ATU client returns a bare [] on 404 and when the `data` key is absent, and
# _to_date here swallows a ValueError instead of raising, so a renamed field
# AND a changed date format both skip every row and land here.


def _corpus_conn(existing: int, known_dates: set[dt.date] | None = None) -> AsyncMock:
    """A conn reporting `existing` rows already in the target table."""
    conn = _appearances_conn(known_dates or {dt.date(1998, 1, 31)})
    conn.fetchval.return_value = existing
    return conn


@pytest.mark.asyncio
async def test_empty_payload_refuses_to_wipe_a_populated_appearances_table() -> None:
    conn = _corpus_conn(existing=4127)
    with pytest.raises(ValueError, match="refusing to replace 4127"):
        await upsert_appearances(conn, [])
    conn.execute.assert_not_called()  # the DELETE never fires


@pytest.mark.asyncio
async def test_empty_payload_refuses_to_wipe_a_populated_jam_chart_table() -> None:
    conn = _corpus_conn(existing=812)
    with pytest.raises(ValueError, match="refusing to replace 812"):
        await upsert_jam_chart_entries(conn, [])
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_field_drift_is_refused_the_same_as_an_empty_response() -> None:
    """The likelier failure: rows arrive, and not one of them is usable.

    A renamed key or a changed date format produces exactly this, and _to_date
    swallowing the parse error is what turns it into a silent full skip rather
    than a raise.
    """
    conn = _corpus_conn(existing=99)
    drifted = [{"show_date": "1998-01-31", "person_name": "Someone"}] * 5
    with pytest.raises(ValueError, match="5 rows fetched, none usable"):
        await upsert_appearances(conn, drifted)
    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_an_empty_payload_against_an_empty_table_is_a_no_op() -> None:
    """First run, or a genuinely empty corpus. Nothing to protect, so no error."""
    conn = _corpus_conn(existing=0)
    assert await upsert_appearances(conn, []) == 0
    assert await upsert_jam_chart_entries(conn, []) == 0
    conn.execute.assert_not_called()
