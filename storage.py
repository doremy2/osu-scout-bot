from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "osu_scout.db"

CREATE_MATCHES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player TEXT NOT NULL,
    opponent TEXT,
    event TEXT NOT NULL,
    stage TEXT,
    source TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_file TEXT,
    date TEXT,
    mod TEXT NOT NULL,
    slot TEXT NOT NULL,
    score INTEGER,
    accuracy REAL,
    result TEXT,
    star_rating REAL,
    beatmap_id INTEGER,
    map_name TEXT,
    difficulty_name TEXT,
    player_team TEXT,
    opponent_team TEXT,
    match_id TEXT,
    import_batch TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE
);
"""

# Teams metadata: small lookup so team_code ('US') can render as
# team_name ('United States') in UI.
CREATE_TEAMS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS teams (
    team_code TEXT PRIMARY KEY,
    team_name TEXT NOT NULL,
    event TEXT,
    import_batch TEXT NOT NULL
);
"""

CREATE_TOURNAMENT_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tournament_events (
    event TEXT PRIMARY KEY,
    display_name TEXT,
    short_name TEXT,
    tier TEXT,
    start_date TEXT,
    end_date TEXT,
    source TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_file TEXT,
    source_url TEXT,
    metadata_json TEXT,
    import_batch TEXT NOT NULL
);
"""

CREATE_TOURNAMENT_EVENTS_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_tournament_events_source ON tournament_events(source, source_type);",
    "CREATE INDEX IF NOT EXISTS idx_tournament_events_dates ON tournament_events(start_date, end_date);",
]

CREATE_TOURNAMENT_STAGES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tournament_stages (
    event TEXT NOT NULL,
    stage TEXT NOT NULL,
    stage_order INTEGER,
    stage_type TEXT,
    starts_at TEXT,
    ends_at TEXT,
    source TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_file TEXT,
    source_url TEXT,
    metadata_json TEXT,
    import_batch TEXT NOT NULL,
    PRIMARY KEY (event, stage)
);
"""

CREATE_TOURNAMENT_STAGES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_tournament_stages_event_order ON tournament_stages(event, stage_order);",
]

CREATE_TOURNAMENT_PLAYERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tournament_players (
    event TEXT NOT NULL,
    player TEXT NOT NULL,
    team_code TEXT NOT NULL DEFAULT '',
    user_id INTEGER,
    country_code TEXT,
    seed INTEGER,
    source TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_file TEXT,
    source_url TEXT,
    metadata_json TEXT,
    import_batch TEXT NOT NULL,
    PRIMARY KEY (event, player, team_code)
);
"""

CREATE_TOURNAMENT_PLAYERS_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_tournament_players_event_team ON tournament_players(event, team_code);",
    "CREATE INDEX IF NOT EXISTS idx_tournament_players_user_id ON tournament_players(user_id);",
]

CREATE_TOURNAMENT_MAP_POOL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tournament_map_pool (
    event TEXT NOT NULL,
    stage TEXT NOT NULL,
    slot TEXT NOT NULL,
    mod TEXT,
    map_name TEXT,
    difficulty_name TEXT,
    beatmap_id INTEGER,
    star_rating REAL,
    source TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_file TEXT,
    source_url TEXT,
    metadata_json TEXT,
    import_batch TEXT NOT NULL,
    PRIMARY KEY (event, stage, slot)
);
"""

CREATE_TOURNAMENT_MAP_POOL_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_tournament_map_pool_event_stage ON tournament_map_pool(event, stage);",
    "CREATE INDEX IF NOT EXISTS idx_tournament_map_pool_beatmap_id ON tournament_map_pool(beatmap_id);",
]

CREATE_PLAYER_ALIASES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS player_aliases (
    alias TEXT PRIMARY KEY COLLATE NOCASE,
    alias_key TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    canonical_key TEXT NOT NULL,
    user_id INTEGER,
    source TEXT NOT NULL,
    import_batch TEXT NOT NULL
);
"""

CREATE_PLAYER_ALIASES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_player_aliases_alias_key ON player_aliases(alias_key);",
    "CREATE INDEX IF NOT EXISTS idx_player_aliases_canonical_key ON player_aliases(canonical_key);",
    "CREATE INDEX IF NOT EXISTS idx_player_aliases_user_id ON player_aliases(user_id);",
]

CREATE_EXTERNAL_RATINGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS external_ratings (
    source TEXT NOT NULL,
    lookup_name TEXT NOT NULL,
    lookup_key TEXT NOT NULL,
    canonical_name TEXT,
    canonical_key TEXT,
    user_id INTEGER,
    display_value TEXT,
    payload_json TEXT,
    status TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (source, lookup_key)
);
"""

CREATE_EXTERNAL_RATINGS_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_external_ratings_canonical_key ON external_ratings(canonical_key);",
    "CREATE INDEX IF NOT EXISTS idx_external_ratings_user_id ON external_ratings(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_external_ratings_fetched_at ON external_ratings(fetched_at);",
]

CREATE_OSU_USER_PROFILES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS osu_user_profiles (
    lookup_name TEXT PRIMARY KEY COLLATE NOCASE,
    lookup_key TEXT NOT NULL,
    user_id INTEGER,
    profile_username TEXT,
    profile_key TEXT,
    country_code TEXT,
    bancho_rank INTEGER,
    pp REAL,
    country_rank INTEGER,
    lazer_rank INTEGER,
    payload_json TEXT,
    status TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);
"""

CREATE_OSU_USER_PROFILES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_osu_user_profiles_lookup_key ON osu_user_profiles(lookup_key);",
    "CREATE INDEX IF NOT EXISTS idx_osu_user_profiles_user_id ON osu_user_profiles(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_osu_user_profiles_profile_key ON osu_user_profiles(profile_key);",
    "CREATE INDEX IF NOT EXISTS idx_osu_user_profiles_fetched_at ON osu_user_profiles(fetched_at);",
]

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_matches_player ON matches(player);",
    "CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date);",
    "CREATE INDEX IF NOT EXISTS idx_matches_slot ON matches(slot);",
    "CREATE INDEX IF NOT EXISTS idx_matches_source ON matches(source);",
]

CREATE_PLAYER_SCORES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS player_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    player TEXT NOT NULL,
    player_team TEXT,
    event TEXT NOT NULL,
    stage TEXT,
    source TEXT NOT NULL,
    rank INTEGER,
    pscore REAL,
    played_count INTEGER,
    played_total INTEGER,
    counted_count INTEGER,
    counted_total INTEGER,
    avg_score INTEGER,
    avg_accuracy REAL,
    highest_slot TEXT,
    highest_score INTEGER,
    import_batch TEXT NOT NULL,
    UNIQUE(player, event, stage, source)
);
"""

CREATE_PLAYER_SCORES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_player_scores_player ON player_scores(player);",
    "CREATE INDEX IF NOT EXISTS idx_player_scores_event ON player_scores(event, stage);",
]

# tournament_matches stores match-level (BO9/BO11/BO13) rows. One row per
# (team, opponent, stage, match_index) so we can later join two complementary
# scorelines into a single resolved match if we recover opponent linking.
CREATE_TOURNAMENT_MATCHES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tournament_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event TEXT NOT NULL,
    stage TEXT,
    source TEXT NOT NULL,
    source_type TEXT,
    source_file TEXT,
    source_url TEXT,
    team TEXT NOT NULL,
    team_code TEXT,
    opponent_team TEXT,
    team_score INTEGER,
    opponent_score INTEGER,
    result TEXT,
    match_link TEXT,
    match_index INTEGER,
    date TEXT,
    import_batch TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE
);
"""

CREATE_TOURNAMENT_MATCHES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_tm_team ON tournament_matches(team);",
    "CREATE INDEX IF NOT EXISTS idx_tm_event ON tournament_matches(event, stage);",
]

# match_games / match_scores: match-detail layer populated by the osu!
# multiplayer API (/matches/{id}). One `match_games` row per map played
# within a multiplayer lobby, one `match_scores` row per player per map.
# These replace the flaky leaderboard-CSV path as the authoritative
# source for per-map per-player data and unlock real Map WR / Match WR.
CREATE_MATCH_GAMES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS match_games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,            -- osu! mp match id (/matches/{id})
    game_id INTEGER NOT NULL,             -- osu! game id, unique per map in match
    event TEXT,
    stage TEXT,
    red_team_code TEXT,                   -- resolved from match.name via teams table
    blue_team_code TEXT,
    beatmap_id INTEGER,
    beatmap_title TEXT,
    beatmap_version TEXT,
    star_rating REAL,
    mode TEXT,
    scoring_type TEXT,
    team_type TEXT,
    mods TEXT,                            -- json array
    start_time TEXT,
    end_time TEXT,
    winning_team TEXT,                    -- 'red' / 'blue' / NULL
    red_total INTEGER,
    blue_total INTEGER,
    import_batch TEXT NOT NULL,
    UNIQUE(match_id, game_id)
);
"""

CREATE_MATCH_SCORES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS match_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    match_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT,
    score INTEGER,
    accuracy REAL,
    max_combo INTEGER,
    mods TEXT,                            -- json array
    team TEXT,                            -- 'red' / 'blue' / NULL
    team_code TEXT,                       -- resolved from match_games.{red,blue}_team_code
    passed INTEGER NOT NULL DEFAULT 0,    -- 0/1
    slot INTEGER,                         -- lobby slot, not mappool slot
    import_batch TEXT NOT NULL,
    UNIQUE(match_id, game_id, user_id)
);
"""

CREATE_MATCH_GAMES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_mg_match ON match_games(match_id);",
    "CREATE INDEX IF NOT EXISTS idx_mg_event ON match_games(event, stage);",
    "CREATE INDEX IF NOT EXISTS idx_mg_beatmap ON match_games(beatmap_id);",
    "CREATE INDEX IF NOT EXISTS idx_ms_match_game ON match_scores(match_id, game_id);",
    "CREATE INDEX IF NOT EXISTS idx_ms_user ON match_scores(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_ms_username ON match_scores(username);",
]

# ─── Discovered Tournaments (Forum 55 scraper) ──────────────────
CREATE_DISCOVERED_TOURNAMENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS discovered_tournaments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    forum_thread_id INTEGER UNIQUE,          -- osu! forum thread id
    name TEXT NOT NULL,                       -- tournament name
    forum_url TEXT NOT NULL,                  -- full thread URL
    posted_date TEXT,                         -- thread creation date
    updated_date TEXT,                        -- last reply/update date
    format TEXT,                              -- 1v1, 2v2, 3v3, 4v4, etc.
    rank_range TEXT,                          -- e.g. "10k-50k", "no rank limit"
    game_mode TEXT DEFAULT 'osu',             -- osu, taiko, catch, mania
    spreadsheet_links TEXT,                   -- JSON array
    bracket_links TEXT,                       -- JSON array
    mappool_links TEXT,                       -- JSON array
    discord_links TEXT,                       -- JSON array
    match_links TEXT,                         -- JSON array of osu! mp links
    registration_url TEXT,
    status TEXT DEFAULT 'discovered',         -- discovered, active, completed, cancelled
    source TEXT NOT NULL DEFAULT 'forum_55',  -- provenance
    scrape_batch TEXT,
    scraped_at TEXT,
    notes TEXT,
    UNIQUE(forum_url)
);
"""

CREATE_DISCOVERED_TOURNAMENTS_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_dt_name ON discovered_tournaments(name);",
    "CREATE INDEX IF NOT EXISTS idx_dt_status ON discovered_tournaments(status);",
    "CREATE INDEX IF NOT EXISTS idx_dt_posted ON discovered_tournaments(posted_date);",
    "CREATE INDEX IF NOT EXISTS idx_dt_thread ON discovered_tournaments(forum_thread_id);",
]

# ─── Match History (multi-source recent match lookup) ────────────
CREATE_TOURNAMENT_SOURCES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tournament_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tournament_key TEXT NOT NULL UNIQUE,
    tournament_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    source_url TEXT NOT NULL,
    forum_url TEXT,
    wiki_url TEXT,
    spreadsheet_url TEXT,
    bracket_url TEXT,
    discord_url TEXT,
    forum_author TEXT,
    created_at TEXT,
    last_post_at TEXT,
    rank_range TEXT,
    team_size TEXT,
    format TEXT,
    status TEXT NOT NULL DEFAULT 'discovered',
    last_checked_at TEXT,
    data_quality TEXT NOT NULL DEFAULT 'partial',
    notes TEXT,
    source TEXT NOT NULL,
    source_type TEXT,
    linked_match_urls TEXT,
    lazer_room_urls TEXT,
    linked_source_key TEXT,
    priority_score INTEGER NOT NULL DEFAULT 0,
    start_date TEXT,
    end_date TEXT,
    game_mode TEXT,
    player_count INTEGER,
    match_count INTEGER,
    verified_ratio REAL,
    stage_url TEXT,
    classification TEXT,
    metadata_json TEXT,
    discovered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

CREATE_TOURNAMENT_SOURCES_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_ts_year ON tournament_sources(year);",
    "CREATE INDEX IF NOT EXISTS idx_ts_status ON tournament_sources(status);",
    "CREATE INDEX IF NOT EXISTS idx_ts_quality ON tournament_sources(data_quality);",
    "CREATE INDEX IF NOT EXISTS idx_ts_source ON tournament_sources(source);",
    "CREATE INDEX IF NOT EXISTS idx_ts_name ON tournament_sources(tournament_name);",
]

CREATE_MATCH_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS match_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,                          -- osu! user id
    username TEXT NOT NULL,                    -- osu! username at time of match
    tournament_name TEXT,                      -- tournament name
    tournament_id TEXT,                        -- FK to discovered_tournaments or tournaments
    stage TEXT,                                -- Group Stage, Finals, etc.
    match_date TEXT,                           -- ISO date or YYYY-MM
    opponent_name TEXT,                        -- opponent username or team name
    opponent_id INTEGER,                       -- opponent user/team id
    team_name TEXT,                            -- player's team name
    opponent_team_name TEXT,                   -- opponent team name
    result TEXT,                               -- win, loss, draw, unknown
    player_score INTEGER,                      -- team/player score in series
    opponent_score INTEGER,                    -- opponent score in series
    match_link TEXT,                           -- osu! mp link or bracket link
    match_id INTEGER,                          -- osu! mp match id
    source TEXT NOT NULL,                      -- forum_55, osu_api, manual, csv_import, etc.
    source_url TEXT,                           -- where we found this data
    data_quality TEXT NOT NULL DEFAULT 'partial', -- verified, partial, inferred, sample
    scraped_at TEXT,
    import_batch TEXT,
    fingerprint TEXT UNIQUE                    -- dedup key
);
"""

CREATE_MATCH_HISTORY_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_mh_user ON match_history(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_mh_username ON match_history(username COLLATE NOCASE);",
    "CREATE INDEX IF NOT EXISTS idx_mh_tournament ON match_history(tournament_name);",
    "CREATE INDEX IF NOT EXISTS idx_mh_date ON match_history(match_date);",
    "CREATE INDEX IF NOT EXISTS idx_mh_quality ON match_history(data_quality);",
]


UPSERT_MATCH_SQL = """
INSERT INTO matches (
    player,
    opponent,
    event,
    stage,
    source,
    source_type,
    source_file,
    date,
    mod,
    slot,
    score,
    accuracy,
    result,
    star_rating,
    beatmap_id,
    map_name,
    difficulty_name,
    player_team,
    opponent_team,
    match_id,
    import_batch,
    fingerprint
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fingerprint) DO UPDATE SET
    opponent = excluded.opponent,
    event = excluded.event,
    stage = excluded.stage,
    source = excluded.source,
    source_type = excluded.source_type,
    source_file = excluded.source_file,
    date = excluded.date,
    mod = excluded.mod,
    slot = excluded.slot,
    score = excluded.score,
    accuracy = excluded.accuracy,
    result = excluded.result,
    star_rating = COALESCE(excluded.star_rating, matches.star_rating),
    beatmap_id = COALESCE(excluded.beatmap_id, matches.beatmap_id),
    map_name = excluded.map_name,
    difficulty_name = COALESCE(excluded.difficulty_name, matches.difficulty_name),
    player_team = excluded.player_team,
    opponent_team = excluded.opponent_team,
    match_id = excluded.match_id,
    import_batch = excluded.import_batch;
"""

UPSERT_PLAYER_ALIAS_SQL = """
INSERT INTO player_aliases (
    alias,
    alias_key,
    canonical_name,
    canonical_key,
    user_id,
    source,
    import_batch
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(alias) DO UPDATE SET
    alias_key = excluded.alias_key,
    canonical_name = excluded.canonical_name,
    canonical_key = excluded.canonical_key,
    user_id = COALESCE(excluded.user_id, player_aliases.user_id),
    source = excluded.source,
    import_batch = excluded.import_batch;
"""


def _resolve_db_path(db_path: str | Path | None = None) -> Path:
    return Path(db_path) if db_path is not None else DB_PATH


def _ensure_data_dir(db_path: str | Path | None = None) -> None:
    _resolve_db_path(db_path).parent.mkdir(parents=True, exist_ok=True)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_player_key(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    normalized = "".join(ch for ch in text.casefold() if ch.isalnum())
    return normalized or text.casefold()


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    return int(float(text))


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("%", "").replace(",", "")
    if not text:
        return None
    return float(text)


def _infer_source_type(source_file: str | None) -> str:
    if not source_file:
        return "manual"
    suffix = Path(source_file).suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix == ".csv":
        return "csv"
    return "manual"


_STAGE_ORDER = {
    "Qualifiers": 0,
    "Group Stage": 1,
    "Round of 16": 2,
    "Round of 32": 2,
    "Quarterfinals": 3,
    "Semifinals": 4,
    "Finals": 5,
    "Grand Finals": 6,
}

_STAGE_PATTERNS = (
    ("grand finals", "Grand Finals"),
    ("lower round 3", "Lower Round 3"),
    ("losers round 3", "Lower Round 3"),
    ("lower round 2", "Lower Round 2"),
    ("losers round 2", "Lower Round 2"),
    ("lower round 1", "Lower Round 1"),
    ("losers round 1", "Lower Round 1"),
    ("semifinals", "Semifinals"),
    ("quarterfinals", "Quarterfinals"),
    ("round of 16", "Round of 16"),
    ("round of 32", "Round of 32"),
    ("group stage", "Group Stage"),
    ("finals", "Finals"),
    ("qualifier", "Qualifiers"),
)


def canonicalize_stage(stage: str | None, *, source_file: str | None = None) -> str | None:
    cleaned = _clean_text(stage)
    haystack_parts = [cleaned or ""]
    if source_file:
        haystack_parts.append(Path(source_file).stem)
    normalized = re.sub(r"[_()\-]+", " ", " ".join(haystack_parts).lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized:
        return cleaned
    lower_round_match = re.search(r"\blr\s*([1-9][0-9]*)\b", normalized)
    if lower_round_match:
        return f"Lower Round {lower_round_match.group(1)}"
    for needle, label in _STAGE_PATTERNS:
        if needle in normalized:
            return label
    return cleaned


def _stage_order_value(stage: str | None, *, source_file: str | None = None) -> int:
    return _STAGE_ORDER.get(canonicalize_stage(stage, source_file=source_file), -1)


def _canonical_owc_match_id(
    event: str | None,
    stage: str | None,
    match_id: str | None,
) -> str | None:
    cleaned_match_id = _clean_text(match_id)
    if _clean_text(event) != "OWC 2025":
        return cleaned_match_id
    canonical_stage = canonicalize_stage(stage)
    if canonical_stage is None:
        return cleaned_match_id
    return f"owc-2025-{canonical_stage.lower().replace(' ', '-')}"


def _canonical_event(event: str | None, *, source_file: str | None = None) -> str | None:
    cleaned = _clean_text(event)
    source_text = (source_file or "").lower()
    if not cleaned:
        return cleaned
    if cleaned.startswith("OWC 2025") or "world cup 2025" in source_text or "owc_2025" in source_text:
        return "OWC 2025"
    return cleaned


def _build_fingerprint(normalized: dict[str, Any]) -> str:
    parts = [
        normalized["player"] or "",
        normalized["opponent"] or "",
        normalized["event"] or "",
        normalized["stage"] or "",
        normalized["source"] or "",
        normalized["date"] or "",
        normalized["mod"] or "",
        normalized["slot"] or "",
        str(normalized["score"] or ""),
        str(normalized["accuracy"] or ""),
        normalized["result"] or "",
        str(normalized["beatmap_id"] or ""),
        normalized["match_id"] or "",
    ]
    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_match(
    match: dict[str, Any],
    *,
    source_file: str | None = None,
    source_type: str | None = None,
    import_batch: str | None = None,
) -> dict[str, Any]:
    resolved_source_file = _clean_text(match.get("source_file")) or source_file
    stage = canonicalize_stage(
        match.get("stage"),
        source_file=resolved_source_file,
    )
    normalized = {
        "player": _clean_text(match.get("player")),
        "opponent": _clean_text(match.get("opponent")),
        "event": _canonical_event(match.get("event"), source_file=resolved_source_file),
        "stage": stage,
        "source": _clean_text(match.get("source")) or "manual",
        "source_type": _clean_text(match.get("source_type")) or source_type or _infer_source_type(source_file),
        "source_file": resolved_source_file,
        "date": _clean_text(match.get("date")),
        "mod": _clean_text(match.get("mod")),
        "slot": _clean_text(match.get("slot")),
        "score": _to_int(match.get("score")),
        "accuracy": _to_float(match.get("accuracy")),
        "result": (_clean_text(match.get("result")) or "unknown").lower(),
        "star_rating": _to_float(match.get("star_rating")),
        "beatmap_id": _to_int(match.get("beatmap_id")),
        "map_name": _clean_text(match.get("map_name")),
        "difficulty_name": _clean_text(match.get("difficulty_name")),
        "player_team": _clean_text(match.get("player_team")),
        "opponent_team": _clean_text(match.get("opponent_team")),
        "match_id": _canonical_owc_match_id(
            match.get("event"),
            stage,
            match.get("match_id"),
        ),
        "import_batch": import_batch or _clean_text(match.get("import_batch")) or _utc_now_iso(),
    }

    required_fields = ["player", "event", "source", "mod", "slot"]
    missing = [field for field in required_fields if not normalized[field]]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)} | row={match}")

    normalized["fingerprint"] = _clean_text(match.get("fingerprint")) or _build_fingerprint(normalized)
    return normalized


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    resolved_db_path = _resolve_db_path(db_path)
    _ensure_data_dir(resolved_db_path)
    connection = sqlite3.connect(resolved_db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _migrate_add_column_if_missing(
    connection: sqlite3.Connection,
    table: str,
    column: str,
    column_type: str,
) -> None:
    """Add a column if it's not already present. Used for lightweight
    forward migrations on tables that predate new fields."""
    existing = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def init_db(db_path: str | Path | None = None) -> None:
    with get_connection(db_path) as connection:
        connection.execute(CREATE_MATCHES_TABLE_SQL)
        for statement in CREATE_INDEXES_SQL:
            connection.execute(statement)

        # Lightweight forward-migrations.
        _migrate_add_column_if_missing(connection, "matches", "difficulty_name", "TEXT")

        connection.execute(CREATE_PLAYER_SCORES_TABLE_SQL)
        for statement in CREATE_PLAYER_SCORES_INDEXES_SQL:
            connection.execute(statement)

        connection.execute(CREATE_TOURNAMENT_MATCHES_TABLE_SQL)
        for statement in CREATE_TOURNAMENT_MATCHES_INDEXES_SQL:
            connection.execute(statement)
        _migrate_add_column_if_missing(connection, "tournament_matches", "source_type", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_matches", "source_file", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_matches", "source_url", "TEXT")

        connection.execute(CREATE_TEAMS_TABLE_SQL)
        connection.execute(CREATE_TOURNAMENT_EVENTS_TABLE_SQL)
        for statement in CREATE_TOURNAMENT_EVENTS_INDEXES_SQL:
            connection.execute(statement)
        connection.execute(CREATE_TOURNAMENT_STAGES_TABLE_SQL)
        for statement in CREATE_TOURNAMENT_STAGES_INDEXES_SQL:
            connection.execute(statement)
        connection.execute(CREATE_TOURNAMENT_PLAYERS_TABLE_SQL)
        for statement in CREATE_TOURNAMENT_PLAYERS_INDEXES_SQL:
            connection.execute(statement)
        connection.execute(CREATE_TOURNAMENT_MAP_POOL_TABLE_SQL)
        for statement in CREATE_TOURNAMENT_MAP_POOL_INDEXES_SQL:
            connection.execute(statement)
        connection.execute(CREATE_PLAYER_ALIASES_TABLE_SQL)
        for statement in CREATE_PLAYER_ALIASES_INDEXES_SQL:
            connection.execute(statement)
        connection.execute(CREATE_EXTERNAL_RATINGS_TABLE_SQL)
        for statement in CREATE_EXTERNAL_RATINGS_INDEXES_SQL:
            connection.execute(statement)
        connection.execute(CREATE_OSU_USER_PROFILES_TABLE_SQL)
        for statement in CREATE_OSU_USER_PROFILES_INDEXES_SQL:
            connection.execute(statement)

        connection.execute(CREATE_MATCH_GAMES_TABLE_SQL)
        connection.execute(CREATE_MATCH_SCORES_TABLE_SQL)
        for statement in CREATE_MATCH_GAMES_INDEXES_SQL:
            connection.execute(statement)

        connection.execute(CREATE_DISCOVERED_TOURNAMENTS_TABLE_SQL)
        for statement in CREATE_DISCOVERED_TOURNAMENTS_INDEXES_SQL:
            connection.execute(statement)
        connection.execute(CREATE_TOURNAMENT_SOURCES_TABLE_SQL)
        for statement in CREATE_TOURNAMENT_SOURCES_INDEXES_SQL:
            connection.execute(statement)
        _migrate_add_column_if_missing(connection, "tournament_sources", "forum_author", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_sources", "created_at", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_sources", "last_post_at", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_sources", "linked_source_key", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_sources", "priority_score", "INTEGER NOT NULL DEFAULT 0")
        _migrate_add_column_if_missing(connection, "tournament_sources", "start_date", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_sources", "end_date", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_sources", "game_mode", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_sources", "player_count", "INTEGER")
        _migrate_add_column_if_missing(connection, "tournament_sources", "match_count", "INTEGER")
        _migrate_add_column_if_missing(connection, "tournament_sources", "verified_ratio", "REAL")
        _migrate_add_column_if_missing(connection, "tournament_sources", "stage_url", "TEXT")
        _migrate_add_column_if_missing(connection, "tournament_sources", "classification", "TEXT")
        connection.execute(CREATE_MATCH_HISTORY_TABLE_SQL)
        for statement in CREATE_MATCH_HISTORY_INDEXES_SQL:
            connection.execute(statement)

        connection.commit()

    # Backfill star ratings from cached match JSONs (runs once, fast no-op after)
    _backfill_star_ratings_from_cache()


def _backfill_star_ratings_from_cache() -> None:
    """One-shot backfill: read cached match JSONs to set star_rating + beatmap_id
    on legacy matches rows that are still NULL.  Runs at startup inside init_db()
    but is a no-op once all rows already have star_rating populated.
    """
    cache_dir = DATA_DIR / "cache" / "owc_2025" / "matches"
    if not cache_dir.exists():
        return

    with get_connection() as connection:
        needs_backfill = connection.execute(
            "SELECT COUNT(*) FROM matches WHERE star_rating IS NULL"
        ).fetchone()[0]
        if needs_backfill == 0:
            return

    # Build title (lowercase) -> {beatmap_id, star_rating}
    lookup: dict[str, dict[str, Any]] = {}
    for f in sorted(cache_dir.glob("*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        for g in data.get("games", []):
            bid = g.get("beatmap_id")
            sr = g.get("star_rating")
            title = (g.get("beatmap_title") or "").strip()
            if not bid or not sr or not title:
                continue
            lookup[title.lower()] = {"beatmap_id": bid, "star_rating": round(sr, 2)}

    if not lookup:
        return

    with get_connection() as connection:
        distinct_maps = connection.execute(
            "SELECT DISTINCT map_name FROM matches WHERE map_name IS NOT NULL AND star_rating IS NULL"
        ).fetchall()
        updated = 0
        for row in distinct_maps:
            mn = row[0] or ""
            parts = mn.split(" - ", 1)
            title_part = parts[-1].strip().lower() if len(parts) > 1 else mn.strip().lower()
            match = lookup.get(title_part) or lookup.get(mn.strip().lower())
            if match:
                connection.execute(
                    "UPDATE matches SET star_rating = ?, beatmap_id = ? WHERE map_name = ? AND star_rating IS NULL",
                    (match["star_rating"], match["beatmap_id"], mn),
                )
                updated += 1
        if updated:
            connection.commit()


def _dedupe_preserve_order(values: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = _clean_text(value)
        if text is None:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def _build_name_in_clause(column: str, names: Iterable[str]) -> tuple[str, list[str]]:
    cleaned = _dedupe_preserve_order(names)
    if not cleaned:
        return "1 = 0", []
    lowered = [name.casefold() for name in cleaned]
    placeholders = ", ".join("?" for _ in lowered)
    return f"lower(trim({column})) IN ({placeholders})", lowered


def _normalize_team_code(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    normalized = text.upper().replace(":", "").strip()
    return normalized or None


def _build_team_filter_clause(column: str, team_codes: Iterable[str | None] | None) -> tuple[str, list[str]]:
    cleaned = _dedupe_preserve_order(_normalize_team_code(code) for code in (team_codes or []))
    if not cleaned:
        return "", []
    placeholders = ", ".join("?" for _ in cleaned)
    return f" AND UPPER(REPLACE(TRIM({column}), ':', '')) IN ({placeholders})", cleaned


def _score_signature(
    *,
    stage: Any,
    score: Any,
    accuracy: Any,
) -> tuple[str, int, float] | None:
    stage_text = canonicalize_stage(_clean_text(stage))
    score_value = _to_int(score)
    accuracy_value = _to_float(accuracy)
    if stage_text is None or score_value is None or accuracy_value is None:
        return None
    return (stage_text, score_value, round(accuracy_value, 2))


def _is_confident_score_alias_match(
    *,
    best_overlap: int,
    best_csv_count: int,
    best_detail_count: int,
    second_best_overlap: int,
) -> bool:
    comparable = min(best_csv_count, best_detail_count)
    if comparable <= 0:
        return False
    coverage = best_overlap / comparable
    if best_overlap < 5:
        return False
    if coverage < 0.75:
        return False
    if second_best_overlap > 0 and best_overlap < (second_best_overlap * 2):
        return False
    return True


def _collect_inferred_player_alias_rows(
    connection: sqlite3.Connection,
    *,
    team_codes: Iterable[str | None] | None = None,
) -> list[dict[str, Any]]:
    matches_team_clause, matches_team_params = _build_team_filter_clause("player_team", team_codes)
    match_scores_team_clause, match_scores_team_params = _build_team_filter_clause("team_code", team_codes)

    score_rows = connection.execute(
        f"""
        SELECT DISTINCT
            username,
            user_id,
            UPPER(REPLACE(TRIM(team_code), ':', '')) AS team_code
        FROM match_scores
        WHERE username IS NOT NULL
          AND trim(username) <> ''
          AND team_code IS NOT NULL
          AND trim(team_code) <> ''
          {match_scores_team_clause}
        """,
        match_scores_team_params,
    ).fetchall()

    csv_rows = connection.execute(
        f"""
        SELECT DISTINCT
            player,
            UPPER(REPLACE(TRIM(player_team), ':', '')) AS team_code
        FROM matches
        WHERE player IS NOT NULL
          AND trim(player) <> ''
          AND player_team IS NOT NULL
          AND trim(player_team) <> ''
          {matches_team_clause}
        """,
        matches_team_params,
    ).fetchall()

    by_team: dict[str, list[tuple[str, int | None, str | None]]] = {}
    for row in score_rows:
        team_code = _normalize_team_code(row["team_code"])
        username = _clean_text(row["username"])
        if team_code is None or username is None:
            continue
        by_team.setdefault(team_code, []).append(
            (
                username,
                _to_int(row["user_id"]),
                _normalize_player_key(username),
            )
        )

    inferred_by_alias: dict[str, dict[str, Any]] = {}
    for row in csv_rows:
        alias = _clean_text(row["player"])
        team_code = _normalize_team_code(row["team_code"])
        if alias is None or team_code is None:
            continue
        alias_key = _normalize_player_key(alias)
        if alias_key is None:
            continue
        candidates = [
            (username, user_id)
            for username, user_id, username_key in by_team.get(team_code, [])
            if username_key == alias_key
        ]
        if len(candidates) != 1:
            continue
        canonical_name, user_id = candidates[0]
        if canonical_name.casefold() == alias.casefold():
            continue
        inferred_by_alias[alias.casefold()] = {
            "alias": alias,
            "canonical_name": canonical_name,
            "user_id": user_id,
            "source": "team_normalized_match",
        }

    csv_score_rows = connection.execute(
        f"""
        SELECT
            player,
            stage,
            score,
            accuracy,
            UPPER(REPLACE(TRIM(player_team), ':', '')) AS team_code
        FROM matches
        WHERE player IS NOT NULL
          AND trim(player) <> ''
          AND player_team IS NOT NULL
          AND trim(player_team) <> ''
          AND score IS NOT NULL
          AND accuracy IS NOT NULL
          {matches_team_clause}
        """,
        matches_team_params,
    ).fetchall()
    detail_score_rows = connection.execute(
        f"""
        SELECT
            ms.username,
            ms.user_id,
            mg.stage,
            ms.score,
            ms.accuracy,
            UPPER(REPLACE(TRIM(ms.team_code), ':', '')) AS team_code
        FROM match_scores ms
        JOIN match_games mg
          ON mg.match_id = ms.match_id
         AND mg.game_id = ms.game_id
        WHERE ms.username IS NOT NULL
          AND trim(ms.username) <> ''
          AND ms.team_code IS NOT NULL
          AND trim(ms.team_code) <> ''
          AND ms.score IS NOT NULL
          AND ms.accuracy IS NOT NULL
          {match_scores_team_clause}
        """,
        match_scores_team_params,
    ).fetchall()

    direct_usernames_by_team: dict[str, set[str]] = {}
    csv_signatures_by_player: dict[tuple[str, str], set[tuple[str, int, float]]] = {}
    for row in csv_score_rows:
        team_code = _normalize_team_code(row["team_code"])
        player = _clean_text(row["player"])
        signature = _score_signature(
            stage=row["stage"],
            score=row["score"],
            accuracy=row["accuracy"],
        )
        if team_code is None or player is None or signature is None:
            continue
        csv_signatures_by_player.setdefault((team_code, player), set()).add(signature)

    detail_signatures_by_user: dict[tuple[str, str, int | None], set[tuple[str, int, float]]] = {}
    for row in detail_score_rows:
        team_code = _normalize_team_code(row["team_code"])
        username = _clean_text(row["username"])
        signature = _score_signature(
            stage=row["stage"],
            score=row["score"],
            accuracy=row["accuracy"],
        )
        if team_code is None or username is None or signature is None:
            continue
        direct_usernames_by_team.setdefault(team_code, set()).add(username.casefold())
        detail_signatures_by_user.setdefault(
            (team_code, username, _to_int(row["user_id"])),
            set(),
        ).add(signature)

    for (team_code, alias), csv_signatures in csv_signatures_by_player.items():
        if alias.casefold() in direct_usernames_by_team.get(team_code, set()):
            continue
        if alias.casefold() in inferred_by_alias:
            continue

        best_candidate: tuple[int, str, int | None, int] | None = None
        second_best_overlap = 0
        for candidate_team_code, username, user_id in detail_signatures_by_user:
            if candidate_team_code != team_code:
                continue
            detail_signatures = detail_signatures_by_user[(candidate_team_code, username, user_id)]
            overlap = len(csv_signatures & detail_signatures)
            if overlap <= 0:
                continue
            if best_candidate is None or overlap > best_candidate[0]:
                if best_candidate is not None:
                    second_best_overlap = max(second_best_overlap, best_candidate[0])
                best_candidate = (overlap, username, user_id, len(detail_signatures))
            else:
                second_best_overlap = max(second_best_overlap, overlap)

        if best_candidate is None:
            continue

        best_overlap, canonical_name, user_id, detail_signature_count = best_candidate
        if not _is_confident_score_alias_match(
            best_overlap=best_overlap,
            best_csv_count=len(csv_signatures),
            best_detail_count=detail_signature_count,
            second_best_overlap=second_best_overlap,
        ):
            continue

        inferred_by_alias[alias.casefold()] = {
            "alias": alias,
            "canonical_name": canonical_name,
            "user_id": user_id,
            "source": "team_score_signature_match",
        }

    return list(inferred_by_alias.values())


def _refresh_inferred_player_aliases(team_codes: Iterable[str | None]) -> int:
    init_db()
    normalized_team_codes = [
        team_code
        for team_code in (
            _normalize_team_code(code)
            for code in team_codes
        )
        if team_code is not None
    ]
    if not normalized_team_codes:
        return 0
    with get_connection() as connection:
        inferred_rows = _collect_inferred_player_alias_rows(
            connection,
            team_codes=normalized_team_codes,
        )
    return insert_or_update_player_aliases(inferred_rows)


def insert_or_update_player_aliases(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    import_batch = _utc_now_iso()
    values = []
    for row in rows:
        alias = _clean_text(row.get("alias"))
        canonical_name = _clean_text(row.get("canonical_name")) or alias
        if not alias or not canonical_name:
            continue
        values.append(
            (
                alias,
                _normalize_player_key(alias) or alias.casefold(),
                canonical_name,
                _normalize_player_key(canonical_name) or canonical_name.casefold(),
                _to_int(row.get("user_id")),
                _clean_text(row.get("source")) or "manual",
                import_batch,
            )
        )
    if not values:
        return 0
    with get_connection() as connection:
        connection.executemany(UPSERT_PLAYER_ALIAS_SQL, values)
        connection.commit()
    return len(values)


def _ensure_player_aliases_seeded(connection: sqlite3.Connection | None = None) -> None:
    init_db()
    own_connection = False
    if connection is None:
        connection = get_connection()
        own_connection = True
    try:
        alias_count = connection.execute(
            "SELECT COUNT(*) AS c FROM player_aliases"
        ).fetchone()["c"]
        if alias_count:
            return
        has_source_data = connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM matches) AS matches_count,
                (SELECT COUNT(*) FROM match_scores) AS match_scores_count
            """
        ).fetchone()
        if (has_source_data["matches_count"] or 0) <= 0 and (has_source_data["match_scores_count"] or 0) <= 0:
            return
    finally:
        if own_connection:
            connection.close()

    backfill_player_aliases()


def resolve_player_identity(
    username: str,
    *,
    connection: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Resolve one input name to a canonical player identity.

    Returns:
        {
            "input": "Sobu-",
            "canonical_name": "Sobu",
            "names": ["Sobu", "Sobu-"],
            "user_ids": [13872272],
        }
    """
    init_db()
    cleaned = _clean_text(username)
    if cleaned is None:
        return {"input": username, "canonical_name": username, "names": [], "user_ids": []}

    own_connection = False
    if connection is None:
        connection = get_connection()
        own_connection = True

    try:
        _ensure_player_aliases_seeded(connection)
        key = _normalize_player_key(cleaned) or cleaned.casefold()
        seed_rows = connection.execute(
            """
            SELECT alias, canonical_name, user_id
            FROM player_aliases
            WHERE alias_key = ? OR canonical_key = ?
            """,
            (key, key),
        ).fetchall()

        canonical_name = cleaned
        canonical_key = key
        seed_user_ids = {
            int(row["user_id"])
            for row in seed_rows
            if row["user_id"] is not None
        }

        for row in seed_rows:
            if (
                (_normalize_player_key(row["alias"]) or row["alias"].casefold()) == key
                or (_normalize_player_key(row["canonical_name"]) or row["canonical_name"].casefold()) == key
            ):
                canonical_name = row["canonical_name"] or canonical_name
                canonical_key = _normalize_player_key(canonical_name) or canonical_key
                break

        cluster_rows = list(seed_rows)
        if canonical_key or seed_user_ids:
            query = """
            SELECT alias, canonical_name, user_id
            FROM player_aliases
            WHERE canonical_key = ?
            """
            params: list[Any] = [canonical_key]
            if seed_user_ids:
                placeholders = ", ".join("?" for _ in seed_user_ids)
                query += f" OR user_id IN ({placeholders})"
                params.extend(sorted(seed_user_ids))
            cluster_rows = connection.execute(query, params).fetchall()

        names = _dedupe_preserve_order(
            [
                canonical_name,
                cleaned,
                *[row["alias"] for row in cluster_rows],
                *[row["canonical_name"] for row in cluster_rows],
            ]
        )
        user_ids = {
            int(row["user_id"])
            for row in cluster_rows
            if row["user_id"] is not None
        }

        if not user_ids:
            exact_user_rows = connection.execute(
                """
                SELECT DISTINCT user_id
                FROM match_scores
                WHERE user_id IS NOT NULL
                  AND lower(trim(username)) = lower(trim(?))
                """,
                (cleaned,),
            ).fetchall()
            user_ids = {int(row["user_id"]) for row in exact_user_rows}

        return {
            "input": cleaned,
            "canonical_name": canonical_name,
            "names": names,
            "user_ids": sorted(user_ids),
        }
    finally:
        if own_connection:
            connection.close()


UPSERT_EXTERNAL_RATING_SQL = """
INSERT INTO external_ratings (
    source,
    lookup_name,
    lookup_key,
    canonical_name,
    canonical_key,
    user_id,
    display_value,
    payload_json,
    status,
    fetched_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source, lookup_key) DO UPDATE SET
    lookup_name = excluded.lookup_name,
    canonical_name = COALESCE(excluded.canonical_name, external_ratings.canonical_name),
    canonical_key = COALESCE(excluded.canonical_key, external_ratings.canonical_key),
    user_id = COALESCE(excluded.user_id, external_ratings.user_id),
    display_value = excluded.display_value,
    payload_json = excluded.payload_json,
    status = excluded.status,
    fetched_at = excluded.fetched_at;
"""


UPSERT_OSU_USER_PROFILE_SQL = """
INSERT INTO osu_user_profiles (
    lookup_name,
    lookup_key,
    user_id,
    profile_username,
    profile_key,
    country_code,
    bancho_rank,
    pp,
    country_rank,
    lazer_rank,
    payload_json,
    status,
    fetched_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(lookup_name) DO UPDATE SET
    lookup_key = excluded.lookup_key,
    user_id = COALESCE(excluded.user_id, osu_user_profiles.user_id),
    profile_username = COALESCE(excluded.profile_username, osu_user_profiles.profile_username),
    profile_key = COALESCE(excluded.profile_key, osu_user_profiles.profile_key),
    country_code = COALESCE(excluded.country_code, osu_user_profiles.country_code),
    bancho_rank = COALESCE(excluded.bancho_rank, osu_user_profiles.bancho_rank),
    pp = COALESCE(excluded.pp, osu_user_profiles.pp),
    country_rank = COALESCE(excluded.country_rank, osu_user_profiles.country_rank),
    lazer_rank = COALESCE(excluded.lazer_rank, osu_user_profiles.lazer_rank),
    payload_json = excluded.payload_json,
    status = excluded.status,
    fetched_at = excluded.fetched_at;
"""


def upsert_external_ratings(
    rows: Iterable[dict[str, Any]],
    *,
    db_path: str | Path | None = None,
) -> int:
    init_db(db_path)
    values = []
    for row in rows:
        source = _clean_text(row.get("source"))
        lookup_name = _clean_text(row.get("lookup_name"))
        lookup_key = _normalize_player_key(lookup_name)
        status = _clean_text(row.get("status")) or "ok"
        if not source or not lookup_name or not lookup_key:
            continue
        canonical_name = _clean_text(row.get("canonical_name"))
        canonical_key = _normalize_player_key(canonical_name)
        payload = row.get("payload_json")
        if payload is not None and not isinstance(payload, str):
            payload = json.dumps(payload, ensure_ascii=True)
        values.append(
            (
                source,
                lookup_name,
                lookup_key,
                canonical_name,
                canonical_key,
                _to_int(row.get("user_id")),
                _clean_text(row.get("display_value")),
                payload,
                status,
                _clean_text(row.get("fetched_at")) or _utc_now_iso(),
            )
        )

    if not values:
        return 0

    with get_connection(db_path) as connection:
        connection.executemany(UPSERT_EXTERNAL_RATING_SQL, values)
        connection.commit()
    return len(values)


def upsert_osu_user_profiles(
    rows: Iterable[dict[str, Any]],
    *,
    db_path: str | Path | None = None,
) -> int:
    init_db(db_path)
    values = []
    for row in rows:
        lookup_name = _clean_text(row.get("lookup_name"))
        lookup_key = _normalize_player_key(lookup_name)
        status = _clean_text(row.get("status")) or "ok"
        if not lookup_name or not lookup_key:
            continue
        payload = row.get("payload_json")
        if payload is not None and not isinstance(payload, str):
            payload = json.dumps(payload, ensure_ascii=True)
        profile_username = _clean_text(row.get("profile_username"))
        values.append(
            (
                lookup_name,
                lookup_key,
                _to_int(row.get("user_id")),
                profile_username,
                _normalize_player_key(profile_username),
                _clean_text(row.get("country_code")),
                _to_int(row.get("bancho_rank")),
                _to_float(row.get("pp")),
                _to_int(row.get("country_rank")),
                _to_int(row.get("lazer_rank")),
                payload,
                status,
                _clean_text(row.get("fetched_at")) or _utc_now_iso(),
            )
        )

    if not values:
        return 0

    with get_connection(db_path) as connection:
        connection.executemany(UPSERT_OSU_USER_PROFILE_SQL, values)
        connection.commit()
    return len(values)


def fetch_cached_osu_user_profile(
    *,
    names: Iterable[str] | None = None,
    user_ids: Iterable[int] | None = None,
    max_age_hours: float | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    init_db(db_path)
    keys = _dedupe_preserve_order(
        _normalize_player_key(name)
        for name in (names or [])
    )
    resolved_user_ids = [int(user_id) for user_id in (user_ids or []) if user_id is not None]
    if not keys and not resolved_user_ids:
        return None

    clauses: list[str] = []
    params: list[Any] = []
    if keys:
        placeholders = ", ".join("?" for _ in keys)
        clauses.append(f"(lookup_key IN ({placeholders}) OR profile_key IN ({placeholders}))")
        params.extend(keys)
        params.extend(keys)
    if resolved_user_ids:
        placeholders = ", ".join("?" for _ in resolved_user_ids)
        clauses.append(f"user_id IN ({placeholders})")
        params.extend(resolved_user_ids)

    query = f"""
    SELECT
        lookup_name,
        lookup_key,
        user_id,
        profile_username,
        profile_key,
        country_code,
        bancho_rank,
        pp,
        country_rank,
        lazer_rank,
        payload_json,
        status,
        fetched_at
    FROM osu_user_profiles
    WHERE {' OR '.join(clauses)}
    """

    with get_connection(db_path) as connection:
        rows = [dict(row) for row in connection.execute(query, params).fetchall()]

    cutoff = None
    if max_age_hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)

    key_set = {key for key in keys if key}
    user_id_set = set(resolved_user_ids)
    best_row: dict[str, Any] | None = None
    best_rank: tuple[int, int, int, float] | None = None

    for row in rows:
        fetched_at = _parse_iso_datetime(row.get("fetched_at"))
        if cutoff is not None and (fetched_at is None or fetched_at < cutoff):
            continue
        rank = (
            0 if row.get("status") == "ok" else 1,
            0 if row.get("user_id") in user_id_set else 1,
            0 if row.get("profile_key") in key_set else 1,
            -(fetched_at.timestamp() if fetched_at else 0.0),
        )
        if best_rank is None or rank < best_rank:
            best_rank = rank
            best_row = row

    return best_row


def fetch_cached_external_ratings(
    username: str,
    *,
    sources: Iterable[str] | None = None,
    max_age_hours: float | None = None,
) -> dict[str, dict[str, Any]]:
    init_db()
    with get_connection() as connection:
        identity = resolve_player_identity(username, connection=connection)
        keys = _dedupe_preserve_order(
            _normalize_player_key(name)
            for name in [
                identity.get("input"),
                identity.get("canonical_name"),
                *(identity.get("names") or []),
            ]
        )
        user_ids = [int(user_id) for user_id in identity.get("user_ids") or []]

        if not keys and not user_ids:
            return {}

        clauses: list[str] = []
        params: list[Any] = []
        if keys:
            key_placeholders = ", ".join("?" for _ in keys)
            clauses.append(f"(lookup_key IN ({key_placeholders}) OR canonical_key IN ({key_placeholders}))")
            params.extend(keys)
            params.extend(keys)
        if user_ids:
            id_placeholders = ", ".join("?" for _ in user_ids)
            clauses.append(f"user_id IN ({id_placeholders})")
            params.extend(user_ids)

        query = """
        SELECT
            source,
            lookup_name,
            lookup_key,
            canonical_name,
            canonical_key,
            user_id,
            display_value,
            payload_json,
            status,
            fetched_at
        FROM external_ratings
        WHERE {where_clause}
        """
        if sources:
            source_list = [_clean_text(source) for source in sources if _clean_text(source)]
            if source_list:
                source_placeholders = ", ".join("?" for _ in source_list)
                query += f"\n  AND source IN ({source_placeholders})"
                params.extend(source_list)
        rows = connection.execute(
            query.format(where_clause=" OR ".join(clauses)),
            params,
        ).fetchall()

    cutoff = None
    if max_age_hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)

    key_set = {key for key in keys if key}
    user_id_set = set(user_ids)
    best_rows: dict[str, dict[str, Any]] = {}
    best_ranks: dict[str, tuple[int, int, int, float]] = {}

    for raw_row in rows:
        row = dict(raw_row)
        fetched_at = _parse_iso_datetime(row.get("fetched_at"))
        if cutoff is not None and (fetched_at is None or fetched_at < cutoff):
            continue
        source = row.get("source")
        if not source:
            continue
        rank = (
            0 if row.get("status") == "ok" else 1,
            0 if row.get("user_id") in user_id_set else 1,
            0 if row.get("canonical_key") in key_set else 1,
            -(fetched_at.timestamp() if fetched_at else 0.0),
        )
        if source not in best_ranks or rank < best_ranks[source]:
            best_rows[source] = row
            best_ranks[source] = rank

    return best_rows


def _build_player_identity_clause(
    *,
    identity: dict[str, Any],
    name_column: str | None = None,
    user_id_column: str | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if user_id_column and identity.get("user_ids"):
        user_ids = [int(user_id) for user_id in identity["user_ids"]]
        placeholders = ", ".join("?" for _ in user_ids)
        clauses.append(f"{user_id_column} IN ({placeholders})")
        params.extend(user_ids)

    if name_column:
        name_clause, name_params = _build_name_in_clause(
            name_column,
            identity.get("names") or [],
        )
        if name_params:
            clauses.append(name_clause)
            params.extend(name_params)

    if not clauses:
        return "1 = 0", []
    return "(" + " OR ".join(clauses) + ")", params


def backfill_player_aliases() -> dict[str, int]:
    """Seed alias rows from existing match/player data and safe team-local inferences."""
    init_db()
    self_rows: list[dict[str, Any]] = []

    with get_connection() as connection:
        score_rows = connection.execute(
            """
            SELECT DISTINCT username, user_id, team_code
            FROM match_scores
            WHERE username IS NOT NULL
              AND trim(username) <> ''
            """
        ).fetchall()
        for row in score_rows:
            self_rows.append(
                {
                    "alias": row["username"],
                    "canonical_name": row["username"],
                    "user_id": row["user_id"],
                    "source": "match_scores_self",
                }
            )

        match_player_rows = connection.execute(
            """
            SELECT DISTINCT player
            FROM matches
            WHERE player IS NOT NULL
              AND trim(player) <> ''
            """
        ).fetchall()
        for row in match_player_rows:
            self_rows.append(
                {
                    "alias": row["player"],
                    "canonical_name": row["player"],
                    "source": "matches_self",
                }
            )

        player_score_rows = connection.execute(
            """
            SELECT DISTINCT player
            FROM player_scores
            WHERE player IS NOT NULL
              AND trim(player) <> ''
            """
        ).fetchall()
        for row in player_score_rows:
            self_rows.append(
                {
                    "alias": row["player"],
                    "canonical_name": row["player"],
                    "source": "player_scores_self",
                }
            )
        inferred_rows = _collect_inferred_player_alias_rows(connection)

    self_written = insert_or_update_player_aliases(self_rows)
    inferred_written = insert_or_update_player_aliases(inferred_rows)
    return {
        "self_aliases_seen": len(self_rows),
        "self_aliases_written": self_written,
        "inferred_aliases_seen": len(inferred_rows),
        "inferred_aliases_written": inferred_written,
    }


def insert_matches(
    matches: Iterable[dict[str, Any]],
    *,
    source_file: str | None = None,
    source_type: str | None = None,
) -> int:
    init_db()
    import_batch = _utc_now_iso()

    normalized_rows = [
        normalize_match(
            match,
            source_file=source_file,
            source_type=source_type,
            import_batch=import_batch,
        )
        for match in matches
    ]

    values = [
        (
            row["player"],
            row["opponent"],
            row["event"],
            row["stage"],
            row["source"],
            row["source_type"],
            row["source_file"],
            row["date"],
            row["mod"],
            row["slot"],
            row["score"],
            row["accuracy"],
            row["result"],
            row["star_rating"],
            row["beatmap_id"],
            row["map_name"],
            row["difficulty_name"],
            row["player_team"],
            row["opponent_team"],
            row["match_id"],
            row["import_batch"],
            row["fingerprint"],
        )
        for row in normalized_rows
    ]

    with get_connection() as connection:
        connection.executemany(UPSERT_MATCH_SQL, values)
        connection.commit()

    insert_or_update_player_aliases(
        {
            "alias": row["player"],
            "canonical_name": row["player"],
            "source": "matches_self",
        }
        for row in normalized_rows
    )
    _refresh_inferred_player_aliases(row.get("player_team") for row in normalized_rows)

    return len(normalized_rows)


def fetch_all_matches() -> list[dict[str, Any]]:
    init_db()
    query = """
    SELECT
        player,
        opponent,
        event,
        stage,
        source,
        source_type,
        source_file,
        date,
        mod,
        slot,
        score,
        accuracy,
        result,
        star_rating,
        beatmap_id,
        map_name,
        difficulty_name,
        player_team,
        opponent_team,
        match_id,
        import_batch,
        fingerprint
    FROM matches
    ORDER BY date DESC, id DESC
    """
    with get_connection() as connection:
        rows = connection.execute(query).fetchall()
    return [dict(row) for row in rows]


def fetch_player_matches(username: str) -> list[dict[str, Any]]:
    init_db()
    base_query = """
    SELECT
        id,
        player,
        opponent,
        event,
        stage,
        source,
        source_type,
        source_file,
        date,
        mod,
        slot,
        score,
        accuracy,
        result,
        star_rating,
        beatmap_id,
        map_name,
        difficulty_name,
        player_team,
        opponent_team,
        match_id,
        import_batch,
        fingerprint
    FROM matches
    """
    with get_connection() as connection:
        identity = resolve_player_identity(username, connection=connection)
        where_clause, params = _build_player_identity_clause(
            identity=identity,
            name_column="player",
        )
        query = base_query + f"\nWHERE {where_clause}"
        rows = connection.execute(query, params).fetchall()
    parsed_rows = [dict(row) for row in rows]
    for row in parsed_rows:
        row["stage"] = canonicalize_stage(
            row.get("stage"),
            source_file=row.get("source_file"),
        )
        row["match_id"] = _canonical_owc_match_id(
            row.get("event"),
            row.get("stage"),
            row.get("match_id"),
        )
    parsed_rows.sort(
        key=lambda row: (
            1 if _clean_text(row.get("date")) else 0,
            _clean_text(row.get("date")) or "",
            _stage_order_value(row.get("stage"), source_file=row.get("source_file")),
            int(row.get("id") or 0),
        ),
        reverse=True,
    )
    return parsed_rows


def fetch_recent_player_maps(username: str, limit: int = 5) -> list[dict[str, Any]]:
    """Return recent MAP-level rows, preferring match-detail chronology."""
    init_db()
    detail_query = """
    WITH slot_lookup AS (
        SELECT
            event,
            stage,
            beatmap_id,
            MIN(slot) AS slot,
            MIN(map_name) AS map_name,
            MIN(difficulty_name) AS difficulty_name
        FROM matches
        WHERE beatmap_id IS NOT NULL
          AND slot IS NOT NULL
        GROUP BY event, stage, beatmap_id
    )
    SELECT
        ms.username AS player,
        NULL AS opponent,
        mg.event,
        mg.stage,
        'osu_api_match_detail' AS source,
        'json' AS source_type,
        NULL AS source_file,
        COALESCE(mg.end_time, mg.start_time) AS date,
        NULL AS mod,
        sl.slot AS slot,
        ms.score,
        ms.accuracy,
        CASE
            WHEN lower(ms.team) = lower(mg.winning_team) THEN 'win'
            WHEN lower(ms.team) IN ('red', 'blue') AND lower(mg.winning_team) IN ('red', 'blue') THEN 'loss'
            ELSE 'unknown'
        END AS result,
        mg.star_rating,
        mg.beatmap_id,
        COALESCE(
            sl.map_name,
            CASE
                WHEN mg.beatmap_title IS NOT NULL
                 AND mg.beatmap_version IS NOT NULL
                 AND trim(mg.beatmap_version) <> ''
                THEN mg.beatmap_title || ' [' || mg.beatmap_version || ']'
                ELSE mg.beatmap_title
            END
        ) AS map_name,
        sl.difficulty_name AS difficulty_name,
        ms.team_code AS player_team,
        CASE
            WHEN lower(ms.team) = 'red' THEN mg.blue_team_code
            WHEN lower(ms.team) = 'blue' THEN mg.red_team_code
            ELSE NULL
        END AS opponent_team,
        CAST(ms.match_id AS TEXT) AS match_id,
        mg.import_batch,
        NULL AS fingerprint
    FROM match_scores ms
    JOIN match_games mg
      ON ms.match_id = mg.match_id
     AND ms.game_id = mg.game_id
    LEFT JOIN slot_lookup sl
      ON sl.event IS mg.event
     AND sl.stage IS mg.stage
     AND sl.beatmap_id = mg.beatmap_id
    ORDER BY COALESCE(mg.end_time, mg.start_time) DESC, mg.match_id DESC, mg.game_id DESC
    LIMIT ?
    """
    with get_connection() as connection:
        identity = resolve_player_identity(username, connection=connection)
        where_clause, params = _build_player_identity_clause(
            identity=identity,
            name_column="ms.username",
            user_id_column="ms.user_id",
        )
        query = detail_query.replace(
            "ORDER BY COALESCE(mg.end_time, mg.start_time) DESC, mg.match_id DESC, mg.game_id DESC",
            f"WHERE {where_clause}\n    ORDER BY COALESCE(mg.end_time, mg.start_time) DESC, mg.match_id DESC, mg.game_id DESC",
        )
        detail_rows = connection.execute(query, [*params, limit]).fetchall()
    if detail_rows:
        rows = [dict(row) for row in detail_rows]
        for row in rows:
            row["stage"] = canonicalize_stage(row.get("stage"))
            row["match_id"] = _canonical_owc_match_id(
                row.get("event"),
                row.get("stage"),
                row.get("match_id"),
            )
        return rows
    return fetch_player_matches(username)[:limit]


def fetch_unenriched_map_keys() -> list[dict[str, Any]]:
    """Return distinct (event, stage, slot, map_name) groups that still
    need beatmap_id / star_rating enrichment.

    Used by importers/osu_api.py so we only call the osu! API once per
    unique map instead of once per row.
    """
    init_db()
    query = """
    SELECT
        event,
        stage,
        slot,
        map_name,
        COUNT(*) AS row_count
    FROM matches
    WHERE map_name IS NOT NULL
      AND TRIM(map_name) <> ''
      AND (beatmap_id IS NULL OR star_rating IS NULL)
    GROUP BY event, stage, slot, map_name
    ORDER BY event, stage, slot, map_name
    """
    with get_connection() as connection:
        rows = connection.execute(query).fetchall()
    return [dict(row) for row in rows]


def update_enrichment_for_map(
    *,
    event: str | None,
    stage: str | None,
    slot: str | None,
    map_name: str | None,
    beatmap_id: int | None,
    star_rating: float | None,
    difficulty_name: str | None = None,
) -> int:
    """Write enriched beatmap_id / star_rating / difficulty_name back to
    all rows that share the same (event, stage, slot, map_name) key.
    Returns rows updated.

    Only fills NULLs — never overwrites existing values, so this is safe
    to re-run.
    """
    init_db()
    query = """
    UPDATE matches
    SET
        beatmap_id = COALESCE(beatmap_id, ?),
        star_rating = COALESCE(star_rating, ?),
        difficulty_name = COALESCE(difficulty_name, ?)
    WHERE map_name IS ?
      AND (event IS ? OR (event IS NULL AND ? IS NULL))
      AND (stage IS ? OR (stage IS NULL AND ? IS NULL))
      AND (slot IS ? OR (slot IS NULL AND ? IS NULL))
    """
    params = (
        beatmap_id,
        star_rating,
        difficulty_name,
        map_name,
        event, event,
        stage, stage,
        slot, slot,
    )
    with get_connection() as connection:
        cursor = connection.execute(query, params)
        connection.commit()
        return cursor.rowcount


# ============================================================
# teams: team_code -> team_name lookup
# ============================================================

UPSERT_TEAM_SQL = """
INSERT INTO teams (team_code, team_name, event, import_batch)
VALUES (?, ?, ?, ?)
ON CONFLICT(team_code) DO UPDATE SET
    team_name = excluded.team_name,
    event = COALESCE(excluded.event, teams.event),
    import_batch = excluded.import_batch;
"""


def insert_or_update_teams(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    import_batch = _utc_now_iso()
    values = []
    for row in rows:
        team_code = _clean_text(row.get("team_code"))
        team_name = _clean_text(row.get("team_name"))
        if not team_code or not team_name:
            continue
        values.append((team_code, team_name, _clean_text(row.get("event")), import_batch))
    with get_connection() as connection:
        connection.executemany(UPSERT_TEAM_SQL, values)
        connection.commit()
    return len(values)


def fetch_team_name_map() -> dict[str, str]:
    """Return {team_code: team_name} for UI rendering."""
    init_db()
    with get_connection() as connection:
        rows = connection.execute("SELECT team_code, team_name FROM teams").fetchall()
    return {row["team_code"]: row["team_name"] for row in rows}


# ============================================================
# tournament metadata package tables
# ============================================================

UPSERT_TOURNAMENT_EVENT_SQL = """
INSERT INTO tournament_events (
    event, display_name, short_name, tier,
    start_date, end_date,
    source, source_type, source_file, source_url,
    metadata_json, import_batch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(event) DO UPDATE SET
    display_name = COALESCE(excluded.display_name, tournament_events.display_name),
    short_name = COALESCE(excluded.short_name, tournament_events.short_name),
    tier = COALESCE(excluded.tier, tournament_events.tier),
    start_date = COALESCE(excluded.start_date, tournament_events.start_date),
    end_date = COALESCE(excluded.end_date, tournament_events.end_date),
    source = excluded.source,
    source_type = excluded.source_type,
    source_file = COALESCE(excluded.source_file, tournament_events.source_file),
    source_url = COALESCE(excluded.source_url, tournament_events.source_url),
    metadata_json = COALESCE(excluded.metadata_json, tournament_events.metadata_json),
    import_batch = excluded.import_batch;
"""

UPSERT_TOURNAMENT_STAGE_SQL = """
INSERT INTO tournament_stages (
    event, stage, stage_order, stage_type,
    starts_at, ends_at,
    source, source_type, source_file, source_url,
    metadata_json, import_batch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(event, stage) DO UPDATE SET
    stage_order = COALESCE(excluded.stage_order, tournament_stages.stage_order),
    stage_type = COALESCE(excluded.stage_type, tournament_stages.stage_type),
    starts_at = COALESCE(excluded.starts_at, tournament_stages.starts_at),
    ends_at = COALESCE(excluded.ends_at, tournament_stages.ends_at),
    source = excluded.source,
    source_type = excluded.source_type,
    source_file = COALESCE(excluded.source_file, tournament_stages.source_file),
    source_url = COALESCE(excluded.source_url, tournament_stages.source_url),
    metadata_json = COALESCE(excluded.metadata_json, tournament_stages.metadata_json),
    import_batch = excluded.import_batch;
"""

UPSERT_TOURNAMENT_PLAYER_SQL = """
INSERT INTO tournament_players (
    event, player, team_code, user_id, country_code, seed,
    source, source_type, source_file, source_url,
    metadata_json, import_batch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(event, player, team_code) DO UPDATE SET
    user_id = COALESCE(excluded.user_id, tournament_players.user_id),
    country_code = COALESCE(excluded.country_code, tournament_players.country_code),
    seed = COALESCE(excluded.seed, tournament_players.seed),
    source = excluded.source,
    source_type = excluded.source_type,
    source_file = COALESCE(excluded.source_file, tournament_players.source_file),
    source_url = COALESCE(excluded.source_url, tournament_players.source_url),
    metadata_json = COALESCE(excluded.metadata_json, tournament_players.metadata_json),
    import_batch = excluded.import_batch;
"""

UPSERT_TOURNAMENT_MAP_POOL_SQL = """
INSERT INTO tournament_map_pool (
    event, stage, slot, mod, map_name, difficulty_name,
    beatmap_id, star_rating,
    source, source_type, source_file, source_url,
    metadata_json, import_batch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(event, stage, slot) DO UPDATE SET
    mod = COALESCE(excluded.mod, tournament_map_pool.mod),
    map_name = COALESCE(excluded.map_name, tournament_map_pool.map_name),
    difficulty_name = COALESCE(excluded.difficulty_name, tournament_map_pool.difficulty_name),
    beatmap_id = COALESCE(excluded.beatmap_id, tournament_map_pool.beatmap_id),
    star_rating = COALESCE(excluded.star_rating, tournament_map_pool.star_rating),
    source = excluded.source,
    source_type = excluded.source_type,
    source_file = COALESCE(excluded.source_file, tournament_map_pool.source_file),
    source_url = COALESCE(excluded.source_url, tournament_map_pool.source_url),
    metadata_json = COALESCE(excluded.metadata_json, tournament_map_pool.metadata_json),
    import_batch = excluded.import_batch;
"""


def _serialize_metadata_json(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=True)


def upsert_tournament_events(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    import_batch = _utc_now_iso()
    values = []
    for row in rows:
        event = _clean_text(row.get("event"))
        if event is None:
            continue
        values.append(
            (
                event,
                _clean_text(row.get("display_name")) or event,
                _clean_text(row.get("short_name")),
                _clean_text(row.get("tier")),
                _clean_text(row.get("start_date")),
                _clean_text(row.get("end_date")),
                _clean_text(row.get("source")) or "manual",
                _clean_text(row.get("source_type")) or "json",
                _clean_text(row.get("source_file")),
                _clean_text(row.get("source_url")),
                _serialize_metadata_json(row.get("metadata_json") or row.get("metadata")),
                import_batch,
            )
        )
    if not values:
        return 0
    with get_connection() as connection:
        connection.executemany(UPSERT_TOURNAMENT_EVENT_SQL, values)
        connection.commit()
    return len(values)


def upsert_tournament_stages(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    import_batch = _utc_now_iso()
    values = []
    for row in rows:
        event = _clean_text(row.get("event"))
        stage = canonicalize_stage(_clean_text(row.get("stage")))
        if event is None or stage is None:
            continue
        values.append(
            (
                event,
                stage,
                _to_int(row.get("stage_order")),
                _clean_text(row.get("stage_type")),
                _clean_text(row.get("starts_at")),
                _clean_text(row.get("ends_at")),
                _clean_text(row.get("source")) or "manual",
                _clean_text(row.get("source_type")) or "json",
                _clean_text(row.get("source_file")),
                _clean_text(row.get("source_url")),
                _serialize_metadata_json(row.get("metadata_json") or row.get("metadata")),
                import_batch,
            )
        )
    if not values:
        return 0
    with get_connection() as connection:
        connection.executemany(UPSERT_TOURNAMENT_STAGE_SQL, values)
        connection.commit()
    return len(values)


def upsert_tournament_players(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    import_batch = _utc_now_iso()
    values = []
    alias_rows = []
    for row in rows:
        event = _clean_text(row.get("event"))
        player = _clean_text(row.get("player"))
        if event is None or player is None:
            continue
        team_code = _normalize_team_code(row.get("team_code")) or ""
        user_id = _to_int(row.get("user_id"))
        canonical_name = _clean_text(row.get("canonical_name")) or player
        alias_rows.append(
            {
                "alias": player,
                "canonical_name": canonical_name,
                "user_id": user_id,
                "source": _clean_text(row.get("source")) or "tournament_package",
            }
        )
        values.append(
            (
                event,
                player,
                team_code,
                user_id,
                _clean_text(row.get("country_code")),
                _to_int(row.get("seed")),
                _clean_text(row.get("source")) or "manual",
                _clean_text(row.get("source_type")) or "json",
                _clean_text(row.get("source_file")),
                _clean_text(row.get("source_url")),
                _serialize_metadata_json(row.get("metadata_json") or row.get("metadata")),
                import_batch,
            )
        )
    if not values:
        return 0
    with get_connection() as connection:
        connection.executemany(UPSERT_TOURNAMENT_PLAYER_SQL, values)
        connection.commit()
    insert_or_update_player_aliases(alias_rows)
    return len(values)


def upsert_tournament_map_pool(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    import_batch = _utc_now_iso()
    values = []
    for row in rows:
        event = _clean_text(row.get("event"))
        stage = canonicalize_stage(_clean_text(row.get("stage")))
        slot = _clean_text(row.get("slot"))
        if event is None or stage is None or slot is None:
            continue
        slot = slot.upper()
        mod = _clean_text(row.get("mod"))
        if mod is None:
            mod = "".join(ch for ch in slot if ch.isalpha()).upper() or None
        values.append(
            (
                event,
                stage,
                slot,
                mod,
                _clean_text(row.get("map_name")),
                _clean_text(row.get("difficulty_name")),
                _to_int(row.get("beatmap_id")),
                _to_float(row.get("star_rating")),
                _clean_text(row.get("source")) or "manual",
                _clean_text(row.get("source_type")) or "json",
                _clean_text(row.get("source_file")),
                _clean_text(row.get("source_url")),
                _serialize_metadata_json(row.get("metadata_json") or row.get("metadata")),
                import_batch,
            )
        )
    if not values:
        return 0
    with get_connection() as connection:
        connection.executemany(UPSERT_TOURNAMENT_MAP_POOL_SQL, values)
        connection.commit()
    return len(values)


# ============================================================
# player_scores: real Performance Score values per player per round
# ============================================================

UPSERT_PLAYER_SCORE_SQL = """
INSERT INTO player_scores (
    player, player_team, event, stage, source, rank, pscore,
    played_count, played_total, counted_count, counted_total,
    avg_score, avg_accuracy, highest_slot, highest_score, import_batch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(player, event, stage, source) DO UPDATE SET
    player_team = excluded.player_team,
    rank = excluded.rank,
    pscore = excluded.pscore,
    played_count = excluded.played_count,
    played_total = excluded.played_total,
    counted_count = excluded.counted_count,
    counted_total = excluded.counted_total,
    avg_score = excluded.avg_score,
    avg_accuracy = excluded.avg_accuracy,
    highest_slot = excluded.highest_slot,
    highest_score = excluded.highest_score,
    import_batch = excluded.import_batch;
"""


def insert_player_scores(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    import_batch = _utc_now_iso()
    rows = list(rows)
    player_alias_rows = []
    values = [
        (
            _clean_text(row.get("player")),
            _clean_text(row.get("player_team")),
            _clean_text(row.get("event")),
            canonicalize_stage(_clean_text(row.get("stage"))),
            _clean_text(row.get("source")) or "manual",
            _to_int(row.get("rank")),
            (float(row["pscore"]) if row.get("pscore") is not None else None),
            _to_int(row.get("played_count")),
            _to_int(row.get("played_total")),
            _to_int(row.get("counted_count")),
            _to_int(row.get("counted_total")),
            _to_int(row.get("avg_score")),
            _to_float(row.get("avg_accuracy")),
            _clean_text(row.get("highest_slot")),
            _to_int(row.get("highest_score")),
            import_batch,
        )
        for row in rows
        if _clean_text(row.get("player")) and _clean_text(row.get("event"))
    ]
    for row in rows:
        player = _clean_text(row.get("player"))
        if player is None:
            continue
        player_alias_rows.append(
            {
                "alias": player,
                "canonical_name": player,
                "source": "player_scores_self",
            }
        )
    with get_connection() as connection:
        connection.executemany(UPSERT_PLAYER_SCORE_SQL, values)
        connection.commit()
    insert_or_update_player_aliases(player_alias_rows)
    return len(values)


def fetch_player_scores(username: str) -> list[dict[str, Any]]:
    """Return all per-round Performance Score rows for one player, newest
    rounds first (best-effort: by id desc, since these CSVs have no date)."""
    init_db()
    base_query = """
    SELECT *
    FROM player_scores
    ORDER BY id DESC
    """
    with get_connection() as connection:
        identity = resolve_player_identity(username, connection=connection)
        where_clause, params = _build_player_identity_clause(
            identity=identity,
            name_column="player",
        )
        query = base_query.replace(
            "ORDER BY id DESC",
            f"WHERE {where_clause}\n    ORDER BY id DESC",
        )
        rows = connection.execute(query, params).fetchall()
    return [dict(row) for row in rows]


# ============================================================
# tournament_matches: match-level (BO9/BO11/BO13) rows
# ============================================================

def _build_tournament_match_fingerprint(row: dict[str, Any]) -> str:
    parts = [
        row.get("event") or "",
        row.get("stage") or "",
        row.get("source") or "",
        row.get("team") or "",
        row.get("team_code") or "",
        str(row.get("team_score") or ""),
        str(row.get("opponent_score") or ""),
        str(row.get("match_index") or ""),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


UPSERT_TOURNAMENT_MATCH_SQL = """
INSERT INTO tournament_matches (
    event, stage, source, source_type, source_file, source_url, team, team_code, opponent_team,
    team_score, opponent_score, result, match_link, match_index,
    date, import_batch, fingerprint
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fingerprint) DO UPDATE SET
    source = excluded.source,
    source_type = COALESCE(excluded.source_type, tournament_matches.source_type),
    source_file = COALESCE(excluded.source_file, tournament_matches.source_file),
    source_url = COALESCE(excluded.source_url, tournament_matches.source_url),
    opponent_team = COALESCE(excluded.opponent_team, tournament_matches.opponent_team),
    match_link = COALESCE(excluded.match_link, tournament_matches.match_link),
    date = COALESCE(excluded.date, tournament_matches.date),
    import_batch = excluded.import_batch;
"""


def insert_tournament_matches(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    import_batch = _utc_now_iso()
    values = []
    for row in rows:
        if not _clean_text(row.get("team")) or not _clean_text(row.get("event")):
            continue
        normalized_row = dict(row)
        normalized_row["stage"] = canonicalize_stage(_clean_text(row.get("stage")))
        fingerprint = _build_tournament_match_fingerprint(normalized_row)
        values.append(
            (
                _clean_text(row.get("event")),
                normalized_row["stage"],
                _clean_text(row.get("source")) or "manual",
                _clean_text(row.get("source_type")) or "manual",
                _clean_text(row.get("source_file")),
                _clean_text(row.get("source_url")),
                _clean_text(row.get("team")),
                _clean_text(row.get("team_code")),
                _clean_text(row.get("opponent_team")),
                _to_int(row.get("team_score")),
                _to_int(row.get("opponent_score")),
                _clean_text(row.get("result")) or "unknown",
                _clean_text(row.get("match_link")),
                _to_int(row.get("match_index")),
                _clean_text(row.get("date")),
                import_batch,
                fingerprint,
            )
        )
    with get_connection() as connection:
        connection.executemany(UPSERT_TOURNAMENT_MATCH_SQL, values)
        connection.commit()
    return len(values)


def backfill_tournament_match_metadata(
    *,
    event: str | None,
    stage: str | None,
    team_code: str | None,
    match_index: int | None,
    opponent_team: str | None = None,
    match_link: str | None = None,
    date: str | None = None,
    team_score: int | None = None,
    opponent_score: int | None = None,
    result: str | None = None,
) -> int:
    """Authoritative backfill for an existing tournament_matches row keyed by
    (event, stage, team_code, match_index).

    The manual matches.csv is treated as the canonical override source, so
    every field it supplies is written through. Specifically, whenever the
    caller passes a non-null value, the DB row is overwritten (COALESCE
    semantics: CSV value wins when present, existing DB value is kept when
    the CSV cell is blank).

    Fields overwritten:
        opponent_team, match_link, date,
        team_score, opponent_score, result

    This fixes a previous bug where only opponent_team / match_link / date
    were backfilled. Rows originally ingested from an OWC team-stats CSV
    could end up with an opponent_team from matches.csv glued onto a
    scoreline from a completely different match at the same match_index,
    producing 'Denmark 1-5' / 'Taiwan 5-3' style mismatches. Writing the
    scoreline at the same time forces the whole row to agree.

    If no row exists for the key but team_score + opponent_score are
    provided, a fresh row is inserted so manual metadata can stand alone.
    """
    init_db()
    import_batch = _utc_now_iso()
    stage = canonicalize_stage(stage)

    # Derive result from the scoreline when the caller didn't supply one.
    # Explicit results still win (useful for future sources that record
    # walkovers / forfeits).
    derived_result: str | None = _clean_text(result)
    if derived_result is None and team_score is not None and opponent_score is not None:
        if team_score > opponent_score:
            derived_result = "win"
        elif team_score < opponent_score:
            derived_result = "loss"
        else:
            derived_result = "draw"

    with get_connection() as connection:
        update_sql = """
        UPDATE tournament_matches
        SET
            opponent_team  = COALESCE(?, opponent_team),
            match_link     = COALESCE(?, match_link),
            date           = COALESCE(?, date),
            team_score     = COALESCE(?, team_score),
            opponent_score = COALESCE(?, opponent_score),
            result         = COALESCE(?, result),
            import_batch   = ?
        WHERE (event       IS ? OR (event       IS NULL AND ? IS NULL))
          AND (stage       IS ? OR (stage       IS NULL AND ? IS NULL))
          AND (team_code   IS ? OR (team_code   IS NULL AND ? IS NULL))
          AND (match_index IS ? OR (match_index IS NULL AND ? IS NULL))
        """
        cursor = connection.execute(
            update_sql,
            (
                opponent_team,
                match_link,
                date,
                team_score,
                opponent_score,
                derived_result,
                import_batch,
                event, event,
                stage, stage,
                team_code, team_code,
                match_index, match_index,
            ),
        )
        updated = cursor.rowcount

        # Refresh fingerprint(s) for any row whose scoreline we just rewrote.
        # team_score / opponent_score / match_index feed into the fingerprint
        # hash; leaving the old hash in place would cause a subsequent
        # re-import of the OWC team-stats CSV (which rebuilds the OLD hash)
        # to miss this row and insert a duplicate.
        if updated and (team_score is not None or opponent_score is not None):
            affected = connection.execute(
                """
                SELECT id, event, stage, source, team, team_code,
                       team_score, opponent_score, match_index
                FROM tournament_matches
                WHERE (event       IS ? OR (event       IS NULL AND ? IS NULL))
                  AND (stage       IS ? OR (stage       IS NULL AND ? IS NULL))
                  AND (team_code   IS ? OR (team_code   IS NULL AND ? IS NULL))
                  AND (match_index IS ? OR (match_index IS NULL AND ? IS NULL))
                """,
                (
                    event, event,
                    stage, stage,
                    team_code, team_code,
                    match_index, match_index,
                ),
            ).fetchall()
            for row in affected:
                new_fp = _build_tournament_match_fingerprint(dict(row))
                connection.execute(
                    "UPDATE tournament_matches SET fingerprint = ? WHERE id = ?",
                    (new_fp, row["id"]),
                )

        if updated == 0 and team_score is not None and opponent_score is not None:
            # No existing row; insert a fresh one so manual metadata can
            # stand alone if Team Stats never covered this match.
            row = {
                "event": event,
                "stage": stage,
                "source": "manual_metadata",
                "team": team_code,  # fallback: team column required, use code
                "team_code": team_code,
                "opponent_team": opponent_team,
                "team_score": team_score,
                "opponent_score": opponent_score,
                "result": (
                    "win" if (team_score or 0) > (opponent_score or 0)
                    else "loss" if (team_score or 0) < (opponent_score or 0)
                    else "draw"
                ),
                "match_link": match_link,
                "match_index": match_index,
                "date": date,
            }
            insert_tournament_matches([row])
            updated = 1

        connection.commit()
        return updated


def fetch_player_tournament_matches(username: str, limit: int = 5) -> list[dict[str, Any]]:
    """Return match-level rows that involve the player's team(s).

    We don't have a true players-by-team table, so we discover the player's
    teams by joining against the map-level `matches` table on player name.
    Anything that team played in tournament_matches counts as a match the
    player participated in. Newest rounds first; falls back to id-order
    when dates are missing (which is the common case for OWC CSVs).
    """
    init_db()
    query = """
    WITH player_team_counts AS (
        SELECT
            team_code,
            COUNT(*) AS c
        FROM (
            SELECT
                UPPER(REPLACE(TRIM(player_team), ':', '')) AS team_code
            FROM matches
            WHERE {matches_identity_clause}
              AND player_team IS NOT NULL
              AND TRIM(player_team) <> ''

            UNION ALL

            SELECT
                UPPER(REPLACE(TRIM(team_code), ':', '')) AS team_code
            FROM match_scores
            WHERE {match_scores_identity_clause}
              AND team_code IS NOT NULL
              AND TRIM(team_code) <> ''
        )
        GROUP BY team_code
    ),
    chosen_team AS (
        SELECT team_code
        FROM player_team_counts
        ORDER BY c DESC, team_code
        LIMIT 1
    ),
    match_meta AS (
        SELECT
            mg.match_id,
            mg.event,
            mg.stage,
            mg.red_team_code,
            mg.blue_team_code,
            SUM(CASE WHEN lower(mg.winning_team) = 'red' THEN 1 ELSE 0 END) AS red_score,
            SUM(CASE WHEN lower(mg.winning_team) = 'blue' THEN 1 ELSE 0 END) AS blue_score,
            MIN(COALESCE(mg.start_time, mg.end_time)) AS match_start_time
        FROM match_games mg
        GROUP BY
            mg.match_id,
            mg.event,
            mg.stage,
            mg.red_team_code,
            mg.blue_team_code
    ),
    match_meta_team_base AS (
        SELECT
            event,
            stage,
            red_team_code AS team_code,
            blue_team_code AS opponent_team,
            red_score AS team_score,
            blue_score AS opponent_score,
            match_id,
            match_start_time
        FROM match_meta
        WHERE red_team_code IS NOT NULL
        UNION ALL
        SELECT
            event,
            stage,
            blue_team_code AS team_code,
            red_team_code AS opponent_team,
            blue_score AS team_score,
            red_score AS opponent_score,
            match_id,
            match_start_time
        FROM match_meta
        WHERE blue_team_code IS NOT NULL
    ),
    match_meta_team AS (
        SELECT
            event,
            stage,
            team_code,
            opponent_team,
            team_score,
            opponent_score,
            match_id,
            match_start_time,
            ROW_NUMBER() OVER (
                PARTITION BY event, stage, team_code
                ORDER BY match_start_time, match_id
            ) - 1 AS match_index
        FROM match_meta_team_base
    )
    SELECT
        tm.*,
        own.team_name AS team_name,
        opp.team_name AS opponent_team_name,
        mmt.opponent_team AS detail_opponent_team,
        detail_opp.team_name AS detail_opponent_team_name,
        mmt.team_score AS detail_team_score,
        mmt.opponent_score AS detail_opponent_score,
        mmt.match_id AS detail_match_id,
        mmt.match_start_time AS detail_match_start_time
    FROM tournament_matches tm
    LEFT JOIN teams own ON own.team_code = tm.team_code
    LEFT JOIN teams opp ON opp.team_code = tm.opponent_team
    LEFT JOIN match_meta_team mmt
      ON mmt.event IS tm.event
     AND mmt.stage IS tm.stage
     AND mmt.team_code = tm.team_code
     AND mmt.match_index = tm.match_index
    LEFT JOIN teams detail_opp ON detail_opp.team_code = mmt.opponent_team
    WHERE UPPER(REPLACE(TRIM(tm.team_code), ':', '')) IN (SELECT team_code FROM chosen_team)
    ORDER BY
        CASE COALESCE(tm.stage, '')
            WHEN 'Grand Finals' THEN 6
            WHEN 'Finals' THEN 5
            WHEN 'Semifinals' THEN 4
            WHEN 'Quarterfinals' THEN 3
            WHEN 'Round of 16' THEN 2
            WHEN 'Round of 32' THEN 1
            WHEN 'Group Stage' THEN 0
            ELSE -1
        END DESC,
        tm.date DESC,
        tm.id DESC
    LIMIT ?
    """
    with get_connection() as connection:
        identity = resolve_player_identity(username, connection=connection)
        matches_identity_clause, matches_identity_params = _build_player_identity_clause(
            identity=identity,
            name_column="player",
        )
        match_scores_identity_clause, match_scores_identity_params = _build_player_identity_clause(
            identity=identity,
            name_column="username",
            user_id_column="user_id",
        )
        rendered_query = query.format(
            matches_identity_clause=matches_identity_clause,
            match_scores_identity_clause=match_scores_identity_clause,
        )
        rows = connection.execute(
            rendered_query,
            [
                *matches_identity_params,
                *match_scores_identity_params,
                limit,
            ],
        ).fetchall()
    parsed_rows = [dict(row) for row in rows]
    for row in parsed_rows:
        row["stage"] = canonicalize_stage(row.get("stage"))
        if row.get("detail_opponent_team"):
            row["opponent_team"] = row["detail_opponent_team"]
        if row.get("detail_opponent_team_name"):
            row["opponent_team_name"] = row["detail_opponent_team_name"]
        if not row.get("date") and row.get("detail_match_start_time"):
            row["date"] = str(row["detail_match_start_time"])[:10]
        if not row.get("match_link") and row.get("detail_match_id") is not None:
            row["match_link"] = (
                f"https://osu.ppy.sh/community/matches/{row['detail_match_id']}"
            )
    return parsed_rows


def export_all_matches_to_json(output_path: str | Path) -> Path:
    rows = fetch_all_matches()
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return output


def compute_real_winrates(username: str) -> dict[str, Any]:
    """Compute true map-level and match-level win rates from match_games/match_scores.

    Returns a dict with keys:
        map_wr, map_wins, maps_total,
        match_wr, match_wins, matches_total
    Values are None when no data is available.
    """
    init_db()
    result: dict[str, Any] = {
        "map_wr": None, "map_wins": 0, "maps_total": 0,
        "match_wr": None, "match_wins": 0, "matches_total": 0,
    }

    with get_connection() as connection:
        # Check if match_scores table has data
        try:
            count = connection.execute("SELECT COUNT(*) FROM match_scores").fetchone()[0]
        except Exception:
            return result
        if count == 0:
            return result

        identity = resolve_player_identity(username, connection=connection)
        where_clause, params = _build_player_identity_clause(
            identity=identity,
            name_column="ms.username",
            user_id_column="ms.user_id",
        )

        # Map-level WR: did this player's team win this game?
        map_query = f"""
        SELECT
            SUM(CASE WHEN lower(ms.team) = lower(mg.winning_team) THEN 1 ELSE 0 END) AS wins,
            COUNT(*) AS total
        FROM match_scores ms
        JOIN match_games mg ON ms.match_id = mg.match_id AND ms.game_id = mg.game_id
        WHERE {where_clause}
          AND mg.winning_team IS NOT NULL
          AND mg.winning_team != ''
        """
        row = connection.execute(map_query, params).fetchone()
        if row and row[1] > 0:
            result["map_wins"] = row[0]
            result["maps_total"] = row[1]
            result["map_wr"] = round(row[0] / row[1] * 100, 1)

    return result


# ────────────────────────────────────────────────────────────────
# Discovered Tournaments CRUD
# ────────────────────────────────────────────────────────────────

def upsert_discovered_tournament(row: dict[str, Any]) -> None:
    init_db()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO discovered_tournaments (
                forum_thread_id, name, forum_url, posted_date, updated_date,
                format, rank_range, game_mode,
                spreadsheet_links, bracket_links, mappool_links,
                discord_links, match_links, registration_url,
                status, source, scrape_batch, scraped_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(forum_url) DO UPDATE SET
                name = COALESCE(excluded.name, discovered_tournaments.name),
                posted_date = COALESCE(excluded.posted_date, discovered_tournaments.posted_date),
                updated_date = COALESCE(excluded.updated_date, discovered_tournaments.updated_date),
                format = COALESCE(excluded.format, discovered_tournaments.format),
                rank_range = COALESCE(excluded.rank_range, discovered_tournaments.rank_range),
                spreadsheet_links = COALESCE(excluded.spreadsheet_links, discovered_tournaments.spreadsheet_links),
                bracket_links = COALESCE(excluded.bracket_links, discovered_tournaments.bracket_links),
                mappool_links = COALESCE(excluded.mappool_links, discovered_tournaments.mappool_links),
                discord_links = COALESCE(excluded.discord_links, discovered_tournaments.discord_links),
                match_links = COALESCE(excluded.match_links, discovered_tournaments.match_links),
                registration_url = COALESCE(excluded.registration_url, discovered_tournaments.registration_url),
                scraped_at = excluded.scraped_at
            """,
            (
                row.get("forum_thread_id"),
                row["name"],
                row["forum_url"],
                row.get("posted_date"),
                row.get("updated_date"),
                row.get("format"),
                row.get("rank_range"),
                row.get("game_mode", "osu"),
                json.dumps(row["spreadsheet_links"]) if row.get("spreadsheet_links") else None,
                json.dumps(row["bracket_links"]) if row.get("bracket_links") else None,
                json.dumps(row["mappool_links"]) if row.get("mappool_links") else None,
                json.dumps(row["discord_links"]) if row.get("discord_links") else None,
                json.dumps(row["match_links"]) if row.get("match_links") else None,
                row.get("registration_url"),
                row.get("status", "discovered"),
                row.get("source", "forum_55"),
                row.get("scrape_batch"),
                row.get("scraped_at") or _utc_now_iso(),
                row.get("notes"),
            ),
        )
        connection.commit()


def fetch_discovered_tournaments(
    *,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    init_db()
    query = "SELECT * FROM discovered_tournaments"
    params: list[Any] = []
    if status:
        query += " WHERE status = ?"
        params.append(status)
    query += " ORDER BY COALESCE(updated_date, posted_date, scraped_at) DESC LIMIT ?"
    params.append(limit)
    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()
    results = []
    for row in rows:
        d = dict(row)
        for json_col in ("spreadsheet_links", "bracket_links", "mappool_links",
                         "discord_links", "match_links"):
            if d.get(json_col) and isinstance(d[json_col], str):
                try:
                    d[json_col] = json.loads(d[json_col])
                except (json.JSONDecodeError, TypeError):
                    pass
        results.append(d)
    return results


# ────────────────────────────────────────────────────────────────
# Match History CRUD (multi-source recent match lookup)
# ────────────────────────────────────────────────────────────────

UPSERT_TOURNAMENT_SOURCE_SQL = """
INSERT INTO tournament_sources (
    tournament_key, tournament_name, year, source_url,
    forum_url, wiki_url, spreadsheet_url, bracket_url, discord_url,
    forum_author, created_at, last_post_at,
    rank_range, team_size, format, status, last_checked_at, data_quality,
    notes, source, source_type, linked_match_urls, lazer_room_urls,
    linked_source_key, priority_score,
    start_date, end_date, game_mode, player_count, match_count, verified_ratio, stage_url, classification,
    metadata_json, discovered_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(tournament_key) DO UPDATE SET
    tournament_name = COALESCE(excluded.tournament_name, tournament_sources.tournament_name),
    year = COALESCE(excluded.year, tournament_sources.year),
    source_url = COALESCE(excluded.source_url, tournament_sources.source_url),
    forum_url = COALESCE(excluded.forum_url, tournament_sources.forum_url),
    wiki_url = COALESCE(excluded.wiki_url, tournament_sources.wiki_url),
    spreadsheet_url = COALESCE(excluded.spreadsheet_url, tournament_sources.spreadsheet_url),
    bracket_url = COALESCE(excluded.bracket_url, tournament_sources.bracket_url),
    discord_url = COALESCE(excluded.discord_url, tournament_sources.discord_url),
    forum_author = COALESCE(excluded.forum_author, tournament_sources.forum_author),
    created_at = COALESCE(excluded.created_at, tournament_sources.created_at),
    last_post_at = COALESCE(excluded.last_post_at, tournament_sources.last_post_at),
    rank_range = COALESCE(excluded.rank_range, tournament_sources.rank_range),
    team_size = COALESCE(excluded.team_size, tournament_sources.team_size),
    format = COALESCE(excluded.format, tournament_sources.format),
    status = COALESCE(excluded.status, tournament_sources.status),
    last_checked_at = COALESCE(excluded.last_checked_at, tournament_sources.last_checked_at),
    data_quality = COALESCE(excluded.data_quality, tournament_sources.data_quality),
    notes = COALESCE(excluded.notes, tournament_sources.notes),
    source = COALESCE(excluded.source, tournament_sources.source),
    source_type = COALESCE(excluded.source_type, tournament_sources.source_type),
    linked_match_urls = COALESCE(excluded.linked_match_urls, tournament_sources.linked_match_urls),
    lazer_room_urls = COALESCE(excluded.lazer_room_urls, tournament_sources.lazer_room_urls),
    linked_source_key = COALESCE(excluded.linked_source_key, tournament_sources.linked_source_key),
    priority_score = MAX(excluded.priority_score, tournament_sources.priority_score),
    start_date = COALESCE(excluded.start_date, tournament_sources.start_date),
    end_date = COALESCE(excluded.end_date, tournament_sources.end_date),
    game_mode = COALESCE(excluded.game_mode, tournament_sources.game_mode),
    player_count = COALESCE(excluded.player_count, tournament_sources.player_count),
    match_count = COALESCE(excluded.match_count, tournament_sources.match_count),
    verified_ratio = COALESCE(excluded.verified_ratio, tournament_sources.verified_ratio),
    stage_url = COALESCE(excluded.stage_url, tournament_sources.stage_url),
    classification = COALESCE(excluded.classification, tournament_sources.classification),
    metadata_json = COALESCE(excluded.metadata_json, tournament_sources.metadata_json),
    updated_at = excluded.updated_at
"""


def _json_dump_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def upsert_tournament_sources(rows: Iterable[dict[str, Any]]) -> int:
    init_db()
    now = _utc_now_iso()
    values: list[tuple[Any, ...]] = []
    for row in rows:
        tournament_key = _clean_text(row.get("tournament_key"))
        tournament_name = _clean_text(row.get("tournament_name"))
        source_url = _clean_text(row.get("source_url"))
        year = row.get("year")
        if not tournament_key or not tournament_name or not source_url or not year:
            continue
        values.append(
            (
                tournament_key,
                tournament_name,
                int(year),
                source_url,
                _clean_text(row.get("forum_url")),
                _clean_text(row.get("wiki_url")),
                _clean_text(row.get("spreadsheet_url")),
                _clean_text(row.get("bracket_url")),
                _clean_text(row.get("discord_url")),
                _clean_text(row.get("forum_author")),
                _clean_text(row.get("created_at")),
                _clean_text(row.get("last_post_at")),
                _clean_text(row.get("rank_range")),
                _clean_text(row.get("team_size")),
                _clean_text(row.get("format")),
                _clean_text(row.get("status")) or "discovered",
                _clean_text(row.get("last_checked_at")) or now,
                _clean_text(row.get("data_quality")) or "partial",
                _clean_text(row.get("notes")),
                _clean_text(row.get("source")) or "unknown",
                _clean_text(row.get("source_type")),
                _json_dump_or_none(row.get("linked_match_urls")),
                _json_dump_or_none(row.get("lazer_room_urls")),
                _clean_text(row.get("linked_source_key")),
                int(row.get("priority_score") or 0),
                _clean_text(row.get("start_date")),
                _clean_text(row.get("end_date")),
                _clean_text(row.get("game_mode")),
                _to_int(row.get("player_count")),
                _to_int(row.get("match_count")),
                _to_float(row.get("verified_ratio")),
                _clean_text(row.get("stage_url")),
                _clean_text(row.get("classification")),
                _json_dump_or_none(row.get("metadata_json")),
                _clean_text(row.get("discovered_at")) or now,
                now,
            )
        )

    if not values:
        return 0
    with get_connection() as connection:
        connection.executemany(UPSERT_TOURNAMENT_SOURCE_SQL, values)
        connection.commit()
    return len(values)


def fetch_tournament_sources(
    *,
    year: int | None = None,
    status: str | None = None,
    data_quality: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    init_db()
    query = "SELECT * FROM tournament_sources"
    clauses: list[str] = []
    params: list[Any] = []
    if year is not None:
        clauses.append("year = ?")
        params.append(year)
    if status:
        clauses.append("status = ?")
        params.append(status)
    if data_quality:
        clauses.append("data_quality = ?")
        params.append(data_quality)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += """
        ORDER BY
            year DESC,
            CASE data_quality
                WHEN 'verified' THEN 0
                WHEN 'high' THEN 1
                WHEN 'partial' THEN 2
                WHEN 'low' THEN 3
                ELSE 4
            END,
            priority_score DESC,
            tournament_name COLLATE NOCASE
        LIMIT ?
    """
    params.append(limit)
    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for json_col in ("linked_match_urls", "lazer_room_urls", "metadata_json"):
            if item.get(json_col) and isinstance(item[json_col], str):
                try:
                    item[json_col] = json.loads(item[json_col])
                except (json.JSONDecodeError, TypeError):
                    pass
        results.append(item)
    return results


def build_tournament_sources_review_report(
    *,
    year: int | None = None,
    limit: int = 5000,
) -> dict[str, Any]:
    rows = fetch_tournament_sources(year=year, limit=limit)
    issues: list[dict[str, Any]] = []
    quality_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for row in rows:
        quality = row.get("data_quality") or "unknown"
        status = row.get("status") or "unknown"
        quality_counts[quality] = quality_counts.get(quality, 0) + 1
        status_counts[status] = status_counts.get(status, 0) + 1
        missing: list[str] = []
        if not row.get("spreadsheet_url"):
            missing.append("spreadsheet_url")
        if not row.get("bracket_url"):
            missing.append("bracket_url")
        if not row.get("linked_match_urls"):
            missing.append("match_links")
        if not row.get("rank_range"):
            missing.append("rank_range")
        if not row.get("team_size") and not row.get("format"):
            missing.append("team_size_or_format")
        if missing:
            issues.append(
                {
                    "tournament_key": row.get("tournament_key"),
                    "tournament_name": row.get("tournament_name"),
                    "year": row.get("year"),
                    "data_quality": quality,
                    "status": status,
                    "missing": missing,
                    "source_url": row.get("source_url"),
                    "notes": row.get("notes"),
                }
            )
    return {
        "generated_at": _utc_now_iso(),
        "year": year,
        "total": len(rows),
        "quality_counts": quality_counts,
        "status_counts": status_counts,
        "manual_review_count": len(issues),
        "manual_review": issues,
    }


def _match_history_fingerprint(row: dict[str, Any]) -> str:
    parts = [
        str(row.get("user_id") or row.get("username", "")).lower(),
        str(row.get("tournament_name", "")).lower(),
        str(row.get("match_date", "")),
        str(row.get("opponent_name", "")).lower(),
        str(row.get("match_id") or ""),
        str(row.get("source", "")),
    ]
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def upsert_match_history(rows: list[dict[str, Any]]) -> int:
    init_db()
    inserted = 0
    with get_connection() as connection:
        for row in rows:
            fp = row.get("fingerprint") or _match_history_fingerprint(row)
            connection.execute(
                """
                INSERT INTO match_history (
                    user_id, username, tournament_name, tournament_id,
                    stage, match_date, opponent_name, opponent_id,
                    team_name, opponent_team_name,
                    result, player_score, opponent_score,
                    match_link, match_id,
                    source, source_url, data_quality,
                    scraped_at, import_batch, fingerprint
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fingerprint) DO UPDATE SET
                    result = COALESCE(excluded.result, match_history.result),
                    player_score = COALESCE(excluded.player_score, match_history.player_score),
                    opponent_score = COALESCE(excluded.opponent_score, match_history.opponent_score),
                    match_link = COALESCE(excluded.match_link, match_history.match_link),
                    data_quality = CASE
                        WHEN excluded.data_quality = 'verified' THEN 'verified'
                        WHEN match_history.data_quality = 'verified' THEN 'verified'
                        ELSE excluded.data_quality
                    END
                """,
                (
                    row.get("user_id"),
                    row.get("username"),
                    row.get("tournament_name"),
                    row.get("tournament_id"),
                    row.get("stage"),
                    row.get("match_date"),
                    row.get("opponent_name"),
                    row.get("opponent_id"),
                    row.get("team_name"),
                    row.get("opponent_team_name"),
                    row.get("result"),
                    row.get("player_score"),
                    row.get("opponent_score"),
                    row.get("match_link"),
                    row.get("match_id"),
                    row.get("source", "unknown"),
                    row.get("source_url"),
                    row.get("data_quality", "partial"),
                    row.get("scraped_at") or _utc_now_iso(),
                    row.get("import_batch"),
                    fp,
                ),
            )
            inserted += 1
        connection.commit()
    return inserted


def fetch_recent_match_history(
    username: str,
    *,
    limit: int = 20,
    data_quality: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch recent matches for a player from the match_history table.

    Searches by username (case-insensitive) and optionally by user_id
    via the player identity system.  Returns newest first.

    Deduplication:
    - When the same match appears from multiple sources (e.g. tournament_matches
      + match_scores), only the best-quality row is kept.
    - Dedup key: (tournament_name, stage, opponent, match_date) or match_id.
    """
    init_db()
    with get_connection() as connection:
        identity = resolve_player_identity(username, connection=connection)
        names = [username]
        if identity.get("canonical_name"):
            names.append(identity["canonical_name"])
        names.extend(identity.get("names") or [])
        # Deduplicate name list (case-insensitive)
        seen_names: set[str] = set()
        unique_names: list[str] = []
        for n in names:
            low = n.lower()
            if low not in seen_names:
                seen_names.add(low)
                unique_names.append(n)
        names = unique_names

        user_ids = identity.get("user_ids") or []

        # Build WHERE clause
        clauses = []
        params: list[Any] = []
        if names:
            placeholders = ", ".join("?" for _ in names)
            clauses.append(f"lower(mh.username) IN ({placeholders})")
            params.extend(n.lower() for n in names)
        if user_ids:
            placeholders = ", ".join("?" for _ in user_ids)
            clauses.append(f"mh.user_id IN ({placeholders})")
            params.extend(user_ids)

        where = " OR ".join(clauses) if clauses else "1=0"

        quality_filter = ""
        if data_quality:
            quality_filter = " AND mh.data_quality = ?"
            params.append(data_quality)

        # Fetch more than needed so we can dedup in Python
        fetch_limit = limit * 4
        query = f"""
        SELECT
            mh.*,
            CASE mh.data_quality
                WHEN 'verified' THEN 1
                WHEN 'partial' THEN 2
                WHEN 'inferred' THEN 3
                WHEN 'sample' THEN 4
                ELSE 5
            END AS quality_rank
        FROM match_history mh
        WHERE ({where}){quality_filter}
        ORDER BY mh.match_date DESC, quality_rank ASC
        LIMIT ?
        """
        params.append(fetch_limit)
        rows = connection.execute(query, params).fetchall()

    # ── Deduplicate ───────────────────────────────────────────
    # Prefer the row with best data_quality. Two rows represent the
    # same match if they share match_id, or if they share the same
    # (tournament, stage, opponent, date) tuple.
    quality_order = {"verified": 0, "partial": 1, "inferred": 2, "sample": 3}

    def _dedup_key(row: dict) -> str:
        mid = row.get("match_id")
        if mid:
            return f"mid:{mid}"
        parts = [
            (row.get("tournament_name") or "").lower(),
            (row.get("stage") or "").lower(),
            (row.get("opponent_name") or row.get("opponent_team_name") or "").lower(),
            (row.get("match_date") or ""),
        ]
        return "|".join(parts)

    best: dict[str, dict] = {}
    for row in rows:
        d = dict(row)
        key = _dedup_key(d)
        existing = best.get(key)
        if existing is None:
            best[key] = d
        else:
            # Keep the row with better quality
            d_rank = quality_order.get(d.get("data_quality", ""), 9)
            e_rank = quality_order.get(existing.get("data_quality", ""), 9)
            if d_rank < e_rank:
                best[key] = d

    # Re-sort deduped results by date descending
    deduped = sorted(
        best.values(),
        key=lambda r: (r.get("match_date") or "", -quality_order.get(r.get("data_quality", ""), 9)),
        reverse=True,
    )

    return deduped[:limit]


def backfill_match_history_from_legacy() -> int:
    """Populate match_history from existing tournament_matches and match_scores.

    This bridges older imported data into the new unified lookup table.
    Only inserts rows that don't already exist (by fingerprint).

    Strategy:
    1. Build a team_code → [player_usernames] mapping from the matches and
       match_scores tables so we can attribute team-level results to individual
       players.
    2. For each tournament_matches row, create one match_history entry per
       player on that team (not one per team).
    3. From match_scores, group by (match_id, user_id) to get one row per
       player per match (not per game).
    """
    init_db()
    inserted = 0
    rows_to_insert: list[dict[str, Any]] = []

    with get_connection() as connection:
        # ── Build team_code → player list mapping ──────────────
        team_players: dict[str, set[tuple[str, int | None]]] = {}

        # From matches table (map-level rows with player + player_team)
        try:
            mp_rows = connection.execute("""
                SELECT DISTINCT
                    player,
                    UPPER(REPLACE(TRIM(player_team), ':', '')) AS team_code
                FROM matches
                WHERE player IS NOT NULL AND TRIM(player) <> ''
                  AND player_team IS NOT NULL AND TRIM(player_team) <> ''
            """).fetchall()
            for row in mp_rows:
                tc = row["team_code"]
                if tc:
                    team_players.setdefault(tc, set()).add((row["player"], None))
        except Exception:
            pass

        # From match_scores (has user_id + team side, joined to match_games for team codes)
        try:
            ms_team_rows = connection.execute("""
                SELECT DISTINCT
                    ms.username,
                    ms.user_id,
                    CASE lower(ms.team)
                        WHEN 'red'  THEN UPPER(REPLACE(TRIM(mg.red_team_code),  ':', ''))
                        WHEN 'blue' THEN UPPER(REPLACE(TRIM(mg.blue_team_code), ':', ''))
                        ELSE NULL
                    END AS team_code
                FROM match_scores ms
                JOIN match_games mg ON ms.match_id = mg.match_id AND ms.game_id = mg.game_id
                WHERE ms.username IS NOT NULL AND TRIM(ms.username) <> ''
            """).fetchall()
            for row in ms_team_rows:
                tc = row["team_code"]
                if tc:
                    team_players.setdefault(tc, set()).add(
                        (row["username"], row["user_id"])
                    )
        except Exception:
            pass

        # ── From tournament_matches → per-player rows ─────────
        tm_rows = connection.execute("""
            SELECT
                tm.event, tm.stage, tm.team, tm.team_code,
                tm.opponent_team, tm.team_score, tm.opponent_score,
                tm.result, tm.match_link, tm.date, tm.source,
                opp.team_name AS opponent_team_name,
                own.team_name AS own_team_name
            FROM tournament_matches tm
            LEFT JOIN teams opp ON opp.team_code = tm.opponent_team
            LEFT JOIN teams own ON own.team_code = tm.team_code
        """).fetchall()

        for tm in tm_rows:
            d = dict(tm)
            tc = (d.get("team_code") or "").upper().replace(":", "").strip()
            players = team_players.get(tc, set())
            opp_display = d.get("opponent_team_name") or d.get("opponent_team") or "Unknown"
            team_display = d.get("own_team_name") or d.get("team") or tc or "Unknown"

            if players:
                # Create one entry per player on this team
                for player_name, player_uid in players:
                    rows_to_insert.append({
                        "user_id": player_uid,
                        "username": player_name,
                        "tournament_name": d.get("event"),
                        "stage": d.get("stage"),
                        "match_date": d.get("date"),
                        "opponent_name": opp_display,
                        "team_name": team_display,
                        "opponent_team_name": opp_display,
                        "result": d.get("result"),
                        "player_score": d.get("team_score"),
                        "opponent_score": d.get("opponent_score"),
                        "match_link": d.get("match_link"),
                        "source": d.get("source") or "legacy_tournament_matches",
                        "data_quality": "partial",
                    })
            else:
                # No player mapping found — store under team name as fallback
                rows_to_insert.append({
                    "username": team_display,
                    "tournament_name": d.get("event"),
                    "stage": d.get("stage"),
                    "match_date": d.get("date"),
                    "opponent_name": opp_display,
                    "team_name": team_display,
                    "opponent_team_name": opp_display,
                    "result": d.get("result"),
                    "player_score": d.get("team_score"),
                    "opponent_score": d.get("opponent_score"),
                    "match_link": d.get("match_link"),
                    "source": d.get("source") or "legacy_tournament_matches",
                    "data_quality": "partial",
                })

        # ── From match_scores: one row per (match_id, user_id) ──
        try:
            ms_rows = connection.execute("""
                SELECT
                    ms.user_id,
                    ms.username,
                    mg.event,
                    mg.stage,
                    MIN(COALESCE(mg.end_time, mg.start_time)) AS match_date,
                    ms.match_id,
                    ms.team,
                    mg.red_team_code,
                    mg.blue_team_code
                FROM match_scores ms
                JOIN match_games mg ON ms.match_id = mg.match_id AND ms.game_id = mg.game_id
                GROUP BY ms.match_id, ms.user_id
            """).fetchall()

            for ms in ms_rows:
                d = dict(ms)
                team_side = (d.get("team") or "").lower()
                own_code = d.get("red_team_code") if team_side == "red" else d.get("blue_team_code")
                opp_code = d.get("blue_team_code") if team_side == "red" else d.get("red_team_code")

                # Resolve team names from teams table
                own_name = None
                opp_name = None
                if own_code:
                    r = connection.execute(
                        "SELECT team_name FROM teams WHERE team_code = ?",
                        (own_code,),
                    ).fetchone()
                    own_name = r["team_name"] if r else own_code
                if opp_code:
                    r = connection.execute(
                        "SELECT team_name FROM teams WHERE team_code = ?",
                        (opp_code,),
                    ).fetchone()
                    opp_name = r["team_name"] if r else opp_code

                rows_to_insert.append({
                    "user_id": d.get("user_id"),
                    "username": d.get("username") or "Unknown",
                    "tournament_name": d.get("event"),
                    "stage": d.get("stage"),
                    "match_date": (d.get("match_date") or "")[:10],
                    "opponent_name": opp_name or opp_code,
                    "team_name": own_name or own_code,
                    "opponent_team_name": opp_name or opp_code,
                    "match_id": d.get("match_id"),
                    "match_link": f"https://osu.ppy.sh/community/matches/{d['match_id']}" if d.get("match_id") else None,
                    "source": "osu_api_match_detail",
                    "data_quality": "verified",
                })
        except Exception:
            pass  # match_scores may not exist yet

    if rows_to_insert:
        inserted = upsert_match_history(rows_to_insert)
    return inserted
