-- osu! scout platform schema (v1)
-- Every source (sheet, CSV, forum, website...) ends up in these same tables.
-- Raw osu! API responses are cached so analytics can be recomputed without refetching.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tournaments (
    id          INTEGER PRIMARY KEY,
    slug        TEXT NOT NULL UNIQUE,          -- e.g. vrso-2027 (URL key)
    name        TEXT NOT NULL,
    acronym     TEXT,
    start_date  TEXT,                          -- ISO date, derived from matches if unknown
    end_date    TEXT,
    warmups     INTEGER NOT NULL DEFAULT 0,    -- games to skip at the start of each lobby when no mappool is loaded
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Where links were discovered. Lets us re-scan a source later.
CREATE TABLE IF NOT EXISTS tournament_sources (
    id             INTEGER PRIMARY KEY,
    tournament_id  INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    kind           TEXT NOT NULL,              -- google_sheet | csv | xlsx | manual | forum | website
    location       TEXT NOT NULL,              -- URL or file path
    scanned_at     TEXT NOT NULL DEFAULT (datetime('now')),
    links_found    INTEGER NOT NULL DEFAULT 0,
    UNIQUE (tournament_id, location)
);

-- One row per osu! multiplayer lobby that belongs to a tournament.
-- status drives a resumable import: pending -> imported | failed | not_found
CREATE TABLE IF NOT EXISTS tournament_matches (
    id             INTEGER PRIMARY KEY,
    tournament_id  INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    osu_match_id   INTEGER NOT NULL,
    round          TEXT,                       -- canonical code: Q, GS, RO32, RO16, QF, SF, F, GF ...
    round_raw      TEXT,                       -- whatever text the source had
    name           TEXT,                       -- lobby title, e.g. "VRSO: (Team A) vs (Team B)"
    team_red       TEXT,                       -- parsed from title when possible
    team_blue      TEXT,
    start_time     TEXT,
    end_time       TEXT,
    status         TEXT NOT NULL DEFAULT 'pending',
    error          TEXT,
    source_context TEXT,                       -- row text the link was found in (debugging)
    fetched_at     TEXT,
    UNIQUE (tournament_id, osu_match_id)
);

CREATE TABLE IF NOT EXISTS raw_match_cache (
    osu_match_id  INTEGER PRIMARY KEY,
    payload       BLOB NOT NULL,               -- zlib-compressed JSON of the full (paginated) response
    fetched_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS players (
    user_id       INTEGER PRIMARY KEY,         -- osu! user id is the canonical key
    username      TEXT NOT NULL,
    country_code  TEXT,
    avatar_url    TEXT,
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS beatmaps (
    beatmap_id     INTEGER PRIMARY KEY,
    beatmapset_id  INTEGER,
    artist         TEXT,
    title          TEXT,
    version        TEXT,                       -- difficulty name
    star_rating    REAL,                       -- nomod SR as reported by the API
    mode           TEXT
);

-- Optional: tournament mappool (beatmap -> slot). Makes mod detection and warmup
-- detection exact instead of inferred.
CREATE TABLE IF NOT EXISTS mappool (
    tournament_id  INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    round          TEXT NOT NULL DEFAULT '',   -- '' = applies to every round
    beatmap_id     INTEGER NOT NULL,
    slot           TEXT NOT NULL,              -- NM1, HD2, DT3, FM1, TB ...
    PRIMARY KEY (tournament_id, round, beatmap_id)
);

CREATE TABLE IF NOT EXISTS match_games (
    id             INTEGER PRIMARY KEY,
    match_id       INTEGER NOT NULL REFERENCES tournament_matches(id) ON DELETE CASCADE,
    osu_game_id    INTEGER NOT NULL,
    order_index    INTEGER NOT NULL,           -- 0-based order inside the lobby
    beatmap_id     INTEGER,
    mods           TEXT NOT NULL DEFAULT '',   -- lobby-level mods, comma separated
    mod_bucket     TEXT,                       -- NM HD HR DT EZ FL FM TB (pool slot wins over inference)
    pool_slot      TEXT,
    scoring_type   TEXT,                       -- score | scorev2 | accuracy | combo
    team_type      TEXT,                       -- head-to-head | team-vs | tag-coop | tag-team-vs
    start_time     TEXT,
    end_time       TEXT,
    is_warmup      INTEGER NOT NULL DEFAULT 0,
    excluded       INTEGER NOT NULL DEFAULT 0, -- 1 = ignore in analytics (warmup, abort, duplicate...)
    exclude_reason TEXT,
    UNIQUE (match_id, osu_game_id)
);

CREATE TABLE IF NOT EXISTS game_scores (
    id          INTEGER PRIMARY KEY,
    game_id     INTEGER NOT NULL REFERENCES match_games(id) ON DELETE CASCADE,
    user_id     INTEGER NOT NULL REFERENCES players(user_id),
    score       INTEGER NOT NULL,
    accuracy    REAL,                          -- 0..1
    max_combo   INTEGER,
    count_300   INTEGER,
    count_100   INTEGER,
    count_50    INTEGER,
    count_miss  INTEGER,
    mods        TEXT NOT NULL DEFAULT '',      -- player's mods (includes freemod picks)
    team        TEXT,                          -- red | blue | none
    slot        INTEGER,
    passed      INTEGER NOT NULL DEFAULT 1,
    UNIQUE (game_id, user_id)
);

CREATE INDEX IF NOT EXISTS ix_matches_tournament ON tournament_matches(tournament_id, status);
CREATE INDEX IF NOT EXISTS ix_games_match ON match_games(match_id);
CREATE INDEX IF NOT EXISTS ix_games_beatmap ON match_games(beatmap_id);
CREATE INDEX IF NOT EXISTS ix_scores_game ON game_scores(game_id);
CREATE INDEX IF NOT EXISTS ix_scores_user ON game_scores(user_id);

-- ---- v2: tournament format + tournament-scoped teams -----------------------
-- A team only exists *inside* a tournament (a country cup's "Vietnam" is not part of
-- the global player identity). Players stay global; memberships are per tournament.
CREATE TABLE IF NOT EXISTS tournament_teams (
    id             INTEGER PRIMARY KEY,
    tournament_id  INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    slug           TEXT NOT NULL,
    name           TEXT NOT NULL,
    country_code   TEXT,                       -- set when the team is (or is mostly) one country
    UNIQUE (tournament_id, slug)
);

-- Everyone who played at least one counted game in the tournament.
CREATE TABLE IF NOT EXISTS tournament_players (
    tournament_id  INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    user_id        INTEGER NOT NULL REFERENCES players(user_id),
    PRIMARY KEY (tournament_id, user_id)
);

CREATE TABLE IF NOT EXISTS team_memberships (
    tournament_id  INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
    team_id        INTEGER NOT NULL REFERENCES tournament_teams(id) ON DELETE CASCADE,
    user_id        INTEGER NOT NULL REFERENCES players(user_id),
    source         TEXT NOT NULL DEFAULT 'match',   -- match | country
    PRIMARY KEY (tournament_id, user_id)
);

CREATE INDEX IF NOT EXISTS ix_teams_tournament ON tournament_teams(tournament_id);
CREATE INDEX IF NOT EXISTS ix_members_team ON team_memberships(team_id);
