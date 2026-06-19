-- Umphreys Vault — initial schema (v1)
-- Postgres 16+. Idempotent: safe to re-run.
--
-- Single upstream source: the All Things Umphreys (ATU) public REST API v2
-- (https://allthings.umphreys.com/api/v2, no auth/key). Unlike the Phish vault
-- this lineage was templated from, there is NO audio source (phish.in has no UM
-- analog) and NO reviews method, so the audio (tracks/track_songs) and reviews
-- tables are intentionally absent. Per-song gap/times-played/debut are NOT in
-- the ATU API; the ETL computes them from the full setlist corpus and writes
-- them onto `songs` (see etl/aggregate.py).

CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Reference tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS venues (
    slug                    TEXT PRIMARY KEY,
    venue_id                INTEGER UNIQUE,                 -- ATU venue_id
    name                    TEXT NOT NULL,
    other_names             TEXT[] NOT NULL DEFAULT '{}',
    latitude                DOUBLE PRECISION,
    longitude               DOUBLE PRECISION,
    city                    TEXT,
    state                   TEXT,
    country                 TEXT,
    location                TEXT,
    shows_count             INTEGER NOT NULL DEFAULT 0,
    upstream_created_at     TIMESTAMPTZ,
    upstream_updated_at     TIMESTAMPTZ,
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_venues_city_state ON venues (city, state);
CREATE INDEX IF NOT EXISTS idx_venues_country    ON venues (country);

CREATE TABLE IF NOT EXISTS tours (
    slug                    TEXT PRIMARY KEY,
    tour_id                 INTEGER UNIQUE,                 -- ATU tour_id
    name                    TEXT NOT NULL,
    shows_count             INTEGER NOT NULL DEFAULT 0,
    starts_on               DATE,
    ends_on                 DATE,
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tours_starts_on ON tours (starts_on);

CREATE TABLE IF NOT EXISTS songs (
    slug                            TEXT PRIMARY KEY,
    song_id                         INTEGER UNIQUE,         -- ATU song_id
    title                           TEXT NOT NULL,
    alias                           TEXT,
    original                        BOOLEAN NOT NULL DEFAULT TRUE,
    original_artist                 TEXT,                   -- cover source; blank if original
    -- COMPUTED by the ETL aggregate pass from the full setlist corpus
    -- (ATU exposes none of these on the songs method). Nullable until the
    -- aggregate pass runs.
    debut_date                      DATE,
    last_play_date                  DATE,
    times_played                    INTEGER,
    gap_current                     INTEGER,
    upstream_created_at             TIMESTAMPTZ,
    upstream_updated_at             TIMESTAMPTZ,
    fetched_at                      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_songs_title    ON songs (title);
CREATE INDEX IF NOT EXISTS idx_songs_original ON songs (original);
CREATE INDEX IF NOT EXISTS idx_songs_gap      ON songs (gap_current DESC NULLS LAST);

-- ---------------------------------------------------------------------------
-- Shows
-- ---------------------------------------------------------------------------
-- Canonical id is the show date (UM plays one show per date in practice).
-- `show_order` disambiguates the rare multi-show day; `show_id` is ATU's stable
-- per-show id, retained for joins and embed links.

CREATE TABLE IF NOT EXISTS shows (
    date                            DATE PRIMARY KEY,
    show_id                         BIGINT UNIQUE,          -- ATU show_id
    show_order                      INTEGER NOT NULL DEFAULT 1,
    show_title                      TEXT,
    venue_slug                      TEXT REFERENCES venues(slug),
    tour_slug                       TEXT REFERENCES tours(slug),
    show_notes                      TEXT,                   -- ATU shownotes (e.g. "entire show without Jake")
    permalink                       TEXT,
    show_year                       INTEGER,
    upstream_created_at             TIMESTAMPTZ,
    upstream_updated_at             TIMESTAMPTZ,
    fetched_at                      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_shows_venue ON shows (venue_slug);
CREATE INDEX IF NOT EXISTS idx_shows_tour  ON shows (tour_slug);
CREATE INDEX IF NOT EXISTS idx_shows_year  ON shows (show_year);

-- ---------------------------------------------------------------------------
-- Setlist rows (the heart of the data: one row per song performance)
-- ---------------------------------------------------------------------------
-- Mirrors an ATU `setlists` row. set_label is normalized at the MCP boundary;
-- here we store the raw ATU values so we never lose fidelity. Encore detection:
-- set_number = 'e'.

CREATE TABLE IF NOT EXISTS setlist_entries (
    id                              BIGSERIAL PRIMARY KEY,
    show_date                       DATE NOT NULL REFERENCES shows(date) ON DELETE CASCADE,
    unique_id                       BIGINT UNIQUE,          -- ATU uniqueid (stable per performance)
    position                        INTEGER NOT NULL,       -- global within show
    set_type                        TEXT,                   -- "Set", "One Set", "Encore label source"
    set_number                      TEXT,                   -- "1","2","3","e"
    song_slug                       TEXT REFERENCES songs(slug),
    song_name                       TEXT NOT NULL,
    transition_id                   INTEGER,
    transition                      TEXT,                   -- ", ", " > ", "  "
    footnote                        TEXT,                   -- teases/debuts/guest notes
    is_jamchart                     BOOLEAN NOT NULL DEFAULT FALSE,
    jamchart_notes                  TEXT,
    is_original                     BOOLEAN NOT NULL DEFAULT TRUE,
    original_artist                 TEXT,
    is_reprise                      BOOLEAN NOT NULL DEFAULT FALSE,
    is_jam                          BOOLEAN NOT NULL DEFAULT FALSE,
    fetched_at                      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_setlist_show_pos ON setlist_entries (show_date, position);
CREATE INDEX IF NOT EXISTS idx_setlist_song     ON setlist_entries (song_slug);
CREATE INDEX IF NOT EXISTS idx_setlist_jamchart ON setlist_entries (is_jamchart) WHERE is_jamchart;

-- ---------------------------------------------------------------------------
-- Jam charts (native ATU `jamcharts` method)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS jam_chart_entries (
    id                              BIGSERIAL PRIMARY KEY,
    show_date                       DATE NOT NULL REFERENCES shows(date) ON DELETE CASCADE,
    song_slug                       TEXT REFERENCES songs(slug),
    song_name                       TEXT,
    notes                           TEXT,
    raw_json                        JSONB NOT NULL,
    fetched_at                      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_jam_chart_show ON jam_chart_entries (show_date);
CREATE INDEX IF NOT EXISTS idx_jam_chart_song ON jam_chart_entries (song_slug);

-- ---------------------------------------------------------------------------
-- Guest appearances / sit-ins (native ATU `appearances` method)
-- New capability the Phish lineage lacked. Powers sit-in tooling (e.g. tracking
-- a guest guitarist covering for an absent member).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS appearances (
    id                              BIGSERIAL PRIMARY KEY,
    show_date                       DATE NOT NULL REFERENCES shows(date) ON DELETE CASCADE,
    person_id                       INTEGER,
    person_name                     TEXT NOT NULL,
    person_slug                     TEXT,
    appearance_type                 TEXT,                   -- "guest musician", etc.
    notes                           TEXT,                   -- "on percussion", "on guitar"
    fetched_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (show_date, person_slug, notes)
);
CREATE INDEX IF NOT EXISTS idx_appearances_show   ON appearances (show_date);
CREATE INDEX IF NOT EXISTS idx_appearances_person ON appearances (person_slug);

-- ---------------------------------------------------------------------------
-- ETL audit
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl_runs (
    id                              BIGSERIAL PRIMARY KEY,
    started_at                      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at                     TIMESTAMPTZ,
    mode                            TEXT NOT NULL CHECK (mode IN ('backfill', 'refresh', 'enrichment', 'aggregate', 'venues', 'songs')),
    source                          TEXT NOT NULL DEFAULT 'atu_v2' CHECK (source IN ('atu_v2', 'computed')),
    args                            JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                          TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'ok', 'error')),
    rows_added                      INTEGER NOT NULL DEFAULT 0,
    rows_updated                    INTEGER NOT NULL DEFAULT 0,
    error_message                   TEXT,
    summary                         JSONB
);
CREATE INDEX IF NOT EXISTS idx_etl_runs_started ON etl_runs (started_at DESC);

INSERT INTO schema_version (version) VALUES (1)
    ON CONFLICT (version) DO NOTHING;
