"""Setlist grouping + show-record synthesis tests."""

from __future__ import annotations

from typing import Any

from umphreys_vault.etl import shows
from umphreys_vault.etl.shows import (
    _show_record,
    _tour_slug_for,
    _venue_slug_for,
    group_rows_by_show,
)


def test_group_rows_by_show_sorts_by_position() -> None:
    rows: list[dict[str, Any]] = [
        {"showdate": "1998-01-21", "position": 3, "songname": "Divisions"},
        {"showdate": "1998-01-21", "position": 1, "songname": "Bob"},
        {"showdate": "1998-01-21", "position": 2, "songname": "Red Baron"},
        {"showdate": "1998-02-11", "position": 1, "songname": "Phil's Farm"},
        {"position": 1, "songname": "no date - dropped"},
    ]
    grouped = group_rows_by_show(rows)
    assert set(grouped) == {"1998-01-21", "1998-02-11"}
    assert [r["songname"] for r in grouped["1998-01-21"]] == ["Bob", "Red Baron", "Divisions"]


def test_venue_slug_derived_from_setlist_row(setlists_1998_atu: dict[str, Any]) -> None:
    head = setlists_1998_atu["data"][0]
    slug = _venue_slug_for(head)
    assert slug == "bridget-mcguire-s-filling-station-south-bend-in-usa"


def test_tour_slug_derived(setlists_1998_atu: dict[str, Any]) -> None:
    head = setlists_1998_atu["data"][0]
    assert _tour_slug_for(head) == "no-tour-name"


def test_show_record_pulls_head_fields(setlists_1998_atu: dict[str, Any]) -> None:
    rows = setlists_1998_atu["data"]
    rec = _show_record("1998-01-21", rows)
    assert rec["date"] == "1998-01-21"
    assert rec["show_id"] == 1327337854
    assert rec["show_year"] == 1998
    assert rec["show_notes"] == "First Umphrey's McGee show"
    assert rec["venue_slug"] == "bridget-mcguire-s-filling-station-south-bend-in-usa"
    assert rec["tour_slug"] == "no-tour-name"


async def test_load_setlist_rows_dry_run_counts(setlists_1998_atu: dict[str, Any]) -> None:
    # Dry-run never touches the pool, so a bare object stands in.
    totals = await shows.load_setlist_rows(object(), setlists_1998_atu["data"], dry_run=True)  # type: ignore[arg-type]
    assert totals["shows"] == 1
    assert totals["setlist_entries"] == 3
    assert totals["errors"] == 0
