# umphreys-vault

A Postgres 16 data vault plus async ETL for Umphrey's McGee setlist data.

Templated from `phish-vault` but rewritten for a single upstream source with
no audio and no reviews. It loads the band's full performance history into a
normalized schema and computes per-song stats (debut, last played, times
played, current gap) that the source API does not expose.

## Source

All data comes from the public **All Things Umphreys (ATU) REST API v2**
(`https://allthings.umphreys.com/api/v2`). No auth, no API key. The ETL is
throttled to a polite request rate because there is no key to lift the cap.

Every endpoint wraps its payload as `{"error": false, "error_message": "", "data": [...]}`.

Endpoints used:

| Method | Use |
|---|---|
| `/list/year.json?artist=1` | enumerate every year, drives backfill |
| `/setlists/showyear/{year}.json` | all setlist rows for a year (the backfill unit) |
| `/setlists/showdate/{YYYY-MM-DD}.json` | one show's setlist rows |
| `/latest.json` | most recent show, drives `refresh` |
| `/songs.json` | full song catalog (~1128 rows) |
| `/venues.json` | venue catalog |
| `/jamcharts.json` | jam chart entries |
| `/appearances.json` | guest sit-ins |

## Schema

`migrations/001_initial.sql` is the authoritative contract. Tables: `venues`,
`tours`, `songs`, `shows`, `setlist_entries`, `jam_chart_entries`,
`appearances`, `etl_runs`, `schema_version`. There are no audio (`tracks`) or
`reviews` tables: the ATU API has no analog for either.

The `songs` columns `debut_date`, `last_play_date`, `times_played`, and
`gap_current` are NULL until the aggregate pass computes them from the full
`setlist_entries` corpus.

## Gap computation

`gap_current` is "shows since last played": the number of distinct shows in
the full ordered show list that occurred strictly after a song's last
performance. A song played at the most recent show has a gap of 0. Songs never
played (in the loaded corpus) are NULL. See `etl/aggregate.py`; it runs in one
SQL statement over `setlist_entries` joined to the distinct ordered show list.

## Commands

```bash
umphreys-vault init                  # apply migrations
umphreys-vault backfill              # full historical backfill
umphreys-vault backfill --year 2024  # one year only
umphreys-vault refresh               # latest show + current year + enrich + aggregate
umphreys-vault aggregate             # recompute per-song stats only
umphreys-vault stats                 # JSON row counts per table
umphreys-vault serve-status          # read-only FastAPI status endpoint
```

All ETL commands accept `--dry-run` (fetch + log, write nothing).

## Configuration

Copy `.env.example` to `.env`. Key vars: `PG_*`, `ATU_BASE_URL`,
`ATU_ARTIST_ID`, `ETL_CONCURRENCY`, `ETL_THROTTLE_ATU_RPS` (default 3),
`STATUS_PORT` (default 3716).

## Deploy

```bash
docker compose up -d postgres                       # database
docker compose run --rm umphreys-vault-etl init     # migrations
docker compose run --rm umphreys-vault-etl backfill # initial load
docker compose --profile status up -d               # status endpoint on :3716
```

A cron entry runs `docker compose --profile cron run --rm umphreys-vault-etl refresh`
daily. Postgres binds to `127.0.0.1:5435` on the host by default.

## Development

```bash
pip install -e '.[dev]'
ruff check . && mypy && pytest
```
