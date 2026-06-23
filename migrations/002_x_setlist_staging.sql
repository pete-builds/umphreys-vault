-- Umphreys Vault — X/Twitter setlist staging (v2)
-- Postgres 16+. Idempotent: safe to re-run.
--
-- Holds ADVISORY X/Twitter-sourced setlist rows that mcp-umphreys merges with
-- authoritative ATU data during a live show. This table never feeds scoring or
-- canonical history; it is a low-confidence, fast-moving side channel.
--
-- Design notes
-- ------------
-- * show_date is NOT a foreign key to shows(date): the upcoming show row may
--   not exist yet when X posts start arriving for it.
-- * song_slug IS a foreign key to songs(slug): the DB itself is the catalog
--   gate, so an off-catalog song is physically unwritable. Callers resolve raw
--   X titles to a real songs.slug before insert (see x_staging.resolve_titles).
-- * set_number_hint / position_hint are ADVISORY/display only and must never be
--   used for scoring.

CREATE TABLE IF NOT EXISTS x_setlist_staging (
    id              BIGSERIAL PRIMARY KEY,
    show_date       DATE NOT NULL,                          -- NOT FK to shows(date): upcoming show row may not exist yet
    song_slug       TEXT NOT NULL REFERENCES songs(slug),   -- DB-level catalog gate: off-catalog songs physically unwritable
    song_name       TEXT NOT NULL,                          -- canonical title from songs at resolve time
    set_number_hint TEXT,                                   -- "1","2","3","e" or NULL; ADVISORY/display only, never used for scoring
    position_hint   INTEGER,                                -- advisory ordering within the X post
    provenance      TEXT NOT NULL DEFAULT 'x',
    confidence      NUMERIC(3,2) NOT NULL,
    source_post_id  TEXT,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (show_date, song_slug)
);
CREATE INDEX IF NOT EXISTS idx_x_staging_show ON x_setlist_staging (show_date);

INSERT INTO schema_version (version) VALUES (2)
    ON CONFLICT (version) DO NOTHING;
