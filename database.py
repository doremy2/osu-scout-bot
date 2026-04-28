"""Normalized database layer for osu! scout v2.

This module owns the NEW normalized tables.  The OLD tables in storage.py
continue to work — nothing is deleted.  Bot commands can be migrated one
at a time to read from the new tables.

Design principles:
  - Every table has source provenance columns (source, source_url, imported_at)
  - Every write is an UPSERT keyed on natural keys — re-running is safe
  - Partial data is fine — NULLs everywhere are tolerated
  - All text comparisons are case-insensitive via COLLATE NOCASE or LOWER()
  - JSON arrays stored as TEXT (json.dumps)

Tables:
  players           — canonical identity (user_id PK)
  tournaments       — one row per event
  tournament_entries — player × tournament (placement, team)
  v2_matches        — one BO-N series (prefixed to avoid collision with old 'matches')
  v2_games          — one map inside a match
  v2_scores         — one player's line on one map
  ratings_snapshots — external ratings over time
  source_links      — provenance for any record
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# Re-use the same DB file as storage.py — co-located tables.
DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "osu_scout.db"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_list(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, str):
        return val
    return json.dumps(list(val))


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# =====================================================================
#  Schema DDL
# =====================================================================

_SCHEMA_SQL = """

-- Canonical player identity
CREATE TABLE IF NOT EXISTS players (
    user_id       INTEGER PRIMARY KEY,
    username      TEXT COLLATE NOCASE,
    country_code  TEXT,
    source        TEXT,
    source_url    TEXT,
    imported_at   TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_players_username ON players(username COLLATE NOCASE);


-- Tournaments / events
CREATE TABLE IF NOT EXISTS tournaments (
    tournament_id TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    abbreviation  TEXT,
    year          INTEGER,
    format        TEXT,
    team_size     INTEGER,
    tier          TEXT,
    start_date    TEXT,
    end_date      TEXT,
    source        TEXT,
    source_url    TEXT,
    imported_at   TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);


-- Player participation in a tournament (placement, team)
CREATE TABLE IF NOT EXISTS tournament_entries (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    tournament_id  TEXT NOT NULL REFERENCES tournaments(tournament_id),
    player_id      INTEGER REFERENCES players(user_id),
    team_name      TEXT,
    team_code      TEXT,
    seed           INTEGER,
    placement      INTEGER,
    placement_text TEXT,
    source         TEXT,
    imported_at    TEXT NOT NULL,
    UNIQUE(tournament_id, player_id)
);
CREATE INDEX IF NOT EXISTS idx_te_player ON tournament_entries(player_id);
CREATE INDEX IF NOT EXISTS idx_te_tournament ON tournament_entries(tournament_id);


-- BO-N match series  (prefixed v2_ to avoid collision with legacy 'matches')
CREATE TABLE IF NOT EXISTS v2_matches (
    match_id      INTEGER PRIMARY KEY,    -- osu! mp id
    tournament_id TEXT REFERENCES tournaments(tournament_id),
    event         TEXT,                   -- human fallback
    stage         TEXT,
    match_name    TEXT,
    team_a        TEXT,
    team_b        TEXT,
    score_a       INTEGER,
    score_b       INTEGER,
    result        TEXT,                   -- "team_a" / "team_b" / "draw"
    match_link    TEXT,
    start_time    TEXT,
    end_time      TEXT,
    source        TEXT,
    source_url    TEXT,
    imported_at   TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_v2m_tournament ON v2_matches(tournament_id);
CREATE INDEX IF NOT EXISTS idx_v2m_event ON v2_matches(event);
CREATE INDEX IF NOT EXISTS idx_v2m_team_a ON v2_matches(team_a);
CREATE INDEX IF NOT EXISTS idx_v2m_team_b ON v2_matches(team_b);


-- One map play inside a match
CREATE TABLE IF NOT EXISTS v2_games (
    game_id        INTEGER NOT NULL,
    match_id       INTEGER NOT NULL REFERENCES v2_matches(match_id),
    beatmap_id     INTEGER,
    beatmap_title  TEXT,
    beatmap_version TEXT,
    star_rating    REAL,
    slot           TEXT,                  -- mappool slot: "NM1", "HD2"
    mods           TEXT,                  -- JSON array
    mode           TEXT,
    scoring_type   TEXT,
    team_type      TEXT,
    winning_team   TEXT,
    red_total      INTEGER,
    blue_total     INTEGER,
    start_time     TEXT,
    end_time       TEXT,
    imported_at    TEXT NOT NULL,
    PRIMARY KEY (match_id, game_id)
);
CREATE INDEX IF NOT EXISTS idx_v2g_beatmap ON v2_games(beatmap_id);


-- One player's score on one map
CREATE TABLE IF NOT EXISTS v2_scores (
    match_id   INTEGER NOT NULL,
    game_id    INTEGER NOT NULL,
    user_id    INTEGER NOT NULL,
    username   TEXT COLLATE NOCASE,
    score      INTEGER,
    accuracy   REAL,
    max_combo  INTEGER,
    count_300  INTEGER,
    count_100  INTEGER,
    count_50   INTEGER,
    count_miss INTEGER,
    mods       TEXT,                      -- JSON array
    team       TEXT,                      -- "red" / "blue"
    team_code  TEXT,
    passed     INTEGER NOT NULL DEFAULT 1,
    slot       INTEGER,                   -- lobby slot
    imported_at TEXT NOT NULL,
    PRIMARY KEY (match_id, game_id, user_id),
    FOREIGN KEY (match_id, game_id) REFERENCES v2_games(match_id, game_id)
);
CREATE INDEX IF NOT EXISTS idx_v2s_user ON v2_scores(user_id);
CREATE INDEX IF NOT EXISTS idx_v2s_username ON v2_scores(username COLLATE NOCASE);
CREATE INDEX IF NOT EXISTS idx_v2s_team_code ON v2_scores(team_code);


-- External rating snapshots
CREATE TABLE IF NOT EXISTS ratings_snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id       INTEGER REFERENCES players(user_id),
    username      TEXT COLLATE NOCASE,
    source        TEXT NOT NULL,           -- "romai", "elitebotix", "skillissue"
    rating_type   TEXT NOT NULL,           -- "elo", "duel", "1v1", "4v4"
    value         REAL,
    display_value TEXT,
    rank          INTEGER,
    peak_value    REAL,
    payload       TEXT,                    -- raw JSON
    fetched_at    TEXT NOT NULL,
    UNIQUE(user_id, source, rating_type, fetched_at)
);
CREATE INDEX IF NOT EXISTS idx_rs_user ON ratings_snapshots(user_id);
CREATE INDEX IF NOT EXISTS idx_rs_source ON ratings_snapshots(source, rating_type);


-- Provenance: every imported record can have 0+ source links
CREATE TABLE IF NOT EXISTS source_links (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    record_type  TEXT NOT NULL,            -- "match", "game", "player", "tournament"
    record_id    TEXT NOT NULL,
    source       TEXT NOT NULL,
    source_id    TEXT,
    source_url   TEXT,
    imported_at  TEXT NOT NULL,
    updated_at   TEXT,
    UNIQUE(record_type, record_id, source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_sl_record ON source_links(record_type, record_id);
"""


def init_v2_db() -> None:
    """Create all v2 tables (idempotent)."""
    with get_connection() as conn:
        conn.executescript(_SCHEMA_SQL)
        conn.commit()


# =====================================================================
#  UPSERT helpers
# =====================================================================

def upsert_player(
    user_id: int,
    *,
    username: str | None = None,
    country_code: str | None = None,
    source: str | None = None,
    source_url: str | None = None,
) -> None:
    now = _utc_now()
    init_v2_db()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO players (user_id, username, country_code, source, source_url, imported_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username     = COALESCE(excluded.username, players.username),
                country_code = COALESCE(excluded.country_code, players.country_code),
                source       = COALESCE(excluded.source, players.source),
                source_url   = COALESCE(excluded.source_url, players.source_url),
                updated_at   = excluded.updated_at
            """,
            (user_id, username, country_code, source, source_url, now, now),
        )
        conn.commit()


def upsert_tournament(
    tournament_id: str,
    *,
    name: str,
    abbreviation: str | None = None,
    year: int | None = None,
    format: str | None = None,
    team_size: int | None = None,
    tier: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    source: str | None = None,
    source_url: str | None = None,
) -> None:
    now = _utc_now()
    init_v2_db()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO tournaments (tournament_id, name, abbreviation, year, format,
                                     team_size, tier, start_date, end_date, source, source_url,
                                     imported_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tournament_id) DO UPDATE SET
                name         = COALESCE(excluded.name, tournaments.name),
                abbreviation = COALESCE(excluded.abbreviation, tournaments.abbreviation),
                year         = COALESCE(excluded.year, tournaments.year),
                format       = COALESCE(excluded.format, tournaments.format),
                team_size    = COALESCE(excluded.team_size, tournaments.team_size),
                tier         = COALESCE(excluded.tier, tournaments.tier),
                start_date   = COALESCE(excluded.start_date, tournaments.start_date),
                end_date     = COALESCE(excluded.end_date, tournaments.end_date),
                source       = COALESCE(excluded.source, tournaments.source),
                source_url   = COALESCE(excluded.source_url, tournaments.source_url),
                updated_at   = excluded.updated_at
            """,
            (tournament_id, name, abbreviation, year, format, team_size, tier,
             start_date, end_date, source, source_url, now, now),
        )
        conn.commit()


def upsert_match(
    match_id: int,
    *,
    tournament_id: str | None = None,
    event: str | None = None,
    stage: str | None = None,
    match_name: str | None = None,
    team_a: str | None = None,
    team_b: str | None = None,
    score_a: int | None = None,
    score_b: int | None = None,
    result: str | None = None,
    match_link: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    source: str | None = None,
    source_url: str | None = None,
) -> None:
    now = _utc_now()
    init_v2_db()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO v2_matches (match_id, tournament_id, event, stage, match_name,
                                    team_a, team_b, score_a, score_b, result,
                                    match_link, start_time, end_time, source, source_url,
                                    imported_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                tournament_id = COALESCE(excluded.tournament_id, v2_matches.tournament_id),
                event         = COALESCE(excluded.event, v2_matches.event),
                stage         = COALESCE(excluded.stage, v2_matches.stage),
                match_name    = COALESCE(excluded.match_name, v2_matches.match_name),
                team_a        = COALESCE(excluded.team_a, v2_matches.team_a),
                team_b        = COALESCE(excluded.team_b, v2_matches.team_b),
                score_a       = COALESCE(excluded.score_a, v2_matches.score_a),
                score_b       = COALESCE(excluded.score_b, v2_matches.score_b),
                result        = COALESCE(excluded.result, v2_matches.result),
                match_link    = COALESCE(excluded.match_link, v2_matches.match_link),
                start_time    = COALESCE(excluded.start_time, v2_matches.start_time),
                end_time      = COALESCE(excluded.end_time, v2_matches.end_time),
                source        = COALESCE(excluded.source, v2_matches.source),
                source_url    = COALESCE(excluded.source_url, v2_matches.source_url),
                updated_at    = excluded.updated_at
            """,
            (match_id, tournament_id, event, stage, match_name,
             team_a, team_b, score_a, score_b, result,
             match_link, start_time, end_time, source, source_url, now, now),
        )
        conn.commit()


def upsert_games(rows: Iterable[dict[str, Any]]) -> int:
    """Bulk upsert game rows (one per map in a match)."""
    now = _utc_now()
    init_v2_db()
    values = []
    for r in rows:
        values.append((
            r["match_id"], r["game_id"],
            r.get("beatmap_id"), r.get("beatmap_title"), r.get("beatmap_version"),
            r.get("star_rating"), r.get("slot"), _json_list(r.get("mods")),
            r.get("mode"), r.get("scoring_type"), r.get("team_type"),
            r.get("winning_team"), r.get("red_total"), r.get("blue_total"),
            r.get("start_time"), r.get("end_time"), now,
        ))
    if not values:
        return 0
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO v2_games (match_id, game_id, beatmap_id, beatmap_title, beatmap_version,
                                  star_rating, slot, mods, mode, scoring_type, team_type,
                                  winning_team, red_total, blue_total, start_time, end_time,
                                  imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id, game_id) DO UPDATE SET
                beatmap_id      = COALESCE(excluded.beatmap_id, v2_games.beatmap_id),
                beatmap_title   = COALESCE(excluded.beatmap_title, v2_games.beatmap_title),
                beatmap_version = COALESCE(excluded.beatmap_version, v2_games.beatmap_version),
                star_rating     = COALESCE(excluded.star_rating, v2_games.star_rating),
                slot            = COALESCE(excluded.slot, v2_games.slot),
                mods            = COALESCE(excluded.mods, v2_games.mods),
                winning_team    = COALESCE(excluded.winning_team, v2_games.winning_team),
                red_total       = COALESCE(excluded.red_total, v2_games.red_total),
                blue_total      = COALESCE(excluded.blue_total, v2_games.blue_total),
                imported_at     = excluded.imported_at
            """,
            values,
        )
        conn.commit()
    return len(values)


def upsert_scores(rows: Iterable[dict[str, Any]]) -> int:
    """Bulk upsert player-game-score rows."""
    now = _utc_now()
    init_v2_db()
    values = []
    for r in rows:
        passed = r.get("passed")
        passed_int = 1 if passed in (1, True, "1", "true") else 0
        values.append((
            r["match_id"], r["game_id"], r["user_id"],
            r.get("username"), r.get("score"), r.get("accuracy"),
            r.get("max_combo"), r.get("count_300"), r.get("count_100"),
            r.get("count_50"), r.get("count_miss"),
            _json_list(r.get("mods")), r.get("team"), r.get("team_code"),
            passed_int, r.get("slot"), now,
        ))
    if not values:
        return 0
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO v2_scores (match_id, game_id, user_id, username, score, accuracy,
                                   max_combo, count_300, count_100, count_50, count_miss,
                                   mods, team, team_code, passed, slot, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(match_id, game_id, user_id) DO UPDATE SET
                username   = COALESCE(excluded.username, v2_scores.username),
                score      = COALESCE(excluded.score, v2_scores.score),
                accuracy   = COALESCE(excluded.accuracy, v2_scores.accuracy),
                max_combo  = COALESCE(excluded.max_combo, v2_scores.max_combo),
                count_300  = COALESCE(excluded.count_300, v2_scores.count_300),
                count_100  = COALESCE(excluded.count_100, v2_scores.count_100),
                count_50   = COALESCE(excluded.count_50, v2_scores.count_50),
                count_miss = COALESCE(excluded.count_miss, v2_scores.count_miss),
                mods       = COALESCE(excluded.mods, v2_scores.mods),
                team       = COALESCE(excluded.team, v2_scores.team),
                team_code  = COALESCE(excluded.team_code, v2_scores.team_code),
                passed     = excluded.passed,
                imported_at = excluded.imported_at
            """,
            values,
        )
        conn.commit()
    return len(values)


def upsert_source_link(
    record_type: str,
    record_id: str,
    source: str,
    source_id: str | None = None,
    source_url: str | None = None,
) -> None:
    now = _utc_now()
    init_v2_db()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO source_links (record_type, record_id, source, source_id, source_url,
                                      imported_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(record_type, record_id, source, source_id) DO UPDATE SET
                source_url = COALESCE(excluded.source_url, source_links.source_url),
                updated_at = excluded.updated_at
            """,
            (record_type, record_id, source, source_id, source_url, now, now),
        )
        conn.commit()


# =====================================================================
#  Query helpers
# =====================================================================

def fetch_player(user_id: int) -> dict[str, Any] | None:
    init_v2_db()
    with get_connection() as conn:
        row = conn.execute("SELECT * FROM players WHERE user_id = ?", (user_id,)).fetchone()
    return dict(row) if row else None


def find_player_by_username(username: str) -> dict[str, Any] | None:
    init_v2_db()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM players WHERE username = ? COLLATE NOCASE", (username,)
        ).fetchone()
    return dict(row) if row else None


def fetch_player_match_history(
    user_id: int | None = None,
    username: str | None = None,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Get a player's recent match-level history from v2 tables."""
    init_v2_db()
    if user_id is not None:
        where = "s.user_id = ?"
        param = user_id
    elif username is not None:
        where = "s.username = ? COLLATE NOCASE"
        param = username
    else:
        return []

    query = f"""
    SELECT DISTINCT
        m.match_id, m.tournament_id, m.event, m.stage,
        m.team_a, m.team_b, m.score_a, m.score_b, m.result,
        m.match_link, m.start_time, m.end_time,
        s.team AS player_team_color,
        s.team_code AS player_team_code
    FROM v2_scores s
    JOIN v2_matches m ON s.match_id = m.match_id
    WHERE {where}
    ORDER BY m.start_time DESC, m.match_id DESC
    LIMIT ?
    """
    with get_connection() as conn:
        rows = conn.execute(query, (param, limit)).fetchall()
    return [dict(r) for r in rows]


def fetch_player_game_scores(
    user_id: int | None = None,
    username: str | None = None,
    *,
    limit: int = 200,
) -> list[dict[str, Any]]:
    """Get a player's per-map scores from v2 tables.

    Returns rows with game + score + match context joined.
    """
    init_v2_db()
    if user_id is not None:
        where = "s.user_id = ?"
        param = user_id
    elif username is not None:
        where = "s.username = ? COLLATE NOCASE"
        param = username
    else:
        return []

    query = f"""
    SELECT
        s.match_id, s.game_id, s.user_id, s.username,
        s.score, s.accuracy, s.max_combo,
        s.count_300, s.count_100, s.count_50, s.count_miss,
        s.mods AS player_mods, s.team, s.team_code, s.passed,
        g.beatmap_id, g.beatmap_title, g.beatmap_version, g.star_rating,
        g.slot, g.mods AS game_mods, g.winning_team,
        g.red_total, g.blue_total,
        m.event, m.stage, m.team_a, m.team_b, m.start_time AS match_start
    FROM v2_scores s
    JOIN v2_games g ON s.match_id = g.match_id AND s.game_id = g.game_id
    JOIN v2_matches m ON s.match_id = m.match_id
    WHERE {where}
    ORDER BY m.start_time DESC, g.start_time DESC
    LIMIT ?
    """
    with get_connection() as conn:
        rows = conn.execute(query, (param, limit)).fetchall()
    return [dict(r) for r in rows]


def fetch_player_slot_stats(
    user_id: int | None = None,
    username: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Aggregate per-slot stats from v2_scores + v2_games.

    Returns {slot: {played, wins, losses, avg_score, avg_accuracy, scores[]}}
    """
    rows = fetch_player_game_scores(user_id=user_id, username=username, limit=5000)
    buckets: dict[str, list[dict]] = {}
    for r in rows:
        slot = r.get("slot") or "?"
        buckets.setdefault(slot, []).append(r)

    result: dict[str, dict[str, Any]] = {}
    for slot, games in sorted(buckets.items()):
        scores = [g["score"] for g in games if g.get("score") is not None]
        accs = [g["accuracy"] for g in games if g.get("accuracy") is not None]
        wins = sum(
            1 for g in games
            if g.get("team") and g.get("winning_team")
            and g["team"].lower() == g["winning_team"].lower()
        )
        losses = sum(
            1 for g in games
            if g.get("team") and g.get("winning_team")
            and g["team"].lower() != g["winning_team"].lower()
        )
        result[slot] = {
            "played": len(games),
            "wins": wins,
            "losses": losses,
            "winrate": round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else None,
            "avg_score": round(sum(scores) / len(scores)) if scores else None,
            "avg_accuracy": round(sum(accs) / len(accs) * 100, 2) if accs else None,
            "scores": scores,
        }
    return result


# =====================================================================
#  Ingest from cached match JSON (osu! API /matches/{id})
# =====================================================================

def ingest_match_json(payload: dict[str, Any], *, event: str | None = None, stage: str | None = None) -> dict[str, int]:
    """Ingest a single cached match JSON into v2 tables.

    Parses match.name for team codes, writes v2_matches + v2_games +
    v2_scores. Also upserts players for every user seen.
    """
    import re
    init_v2_db()

    match_id = payload.get("match_id")
    name = payload.get("name") or ""
    start_time = payload.get("start_time")
    end_time = payload.get("end_time")
    red_score = payload.get("red_score")
    blue_score = payload.get("blue_score")
    users: dict = payload.get("users") or {}

    # Parse team codes from name
    pair_re = re.compile(r"\(([^()]+)\)\s*(?:vs\.?|v\.?)\s*\(([^()]+)\)", re.IGNORECASE)
    m = pair_re.search(name)

    # Build name→code lookup from teams table
    with get_connection() as conn:
        team_rows = conn.execute(
            "SELECT team_code, team_name FROM teams WHERE team_name IS NOT NULL"
        ).fetchall()
    name_to_code = {(r["team_name"] or "").strip().lower(): r["team_code"] for r in team_rows}

    def resolve(n: str | None) -> str | None:
        if not n:
            return None
        key = n.strip().lower()
        if key in name_to_code:
            return name_to_code[key]
        u = n.strip().upper()
        return u if 2 <= len(u) <= 4 and u.isalpha() else None

    team_a = resolve(m.group(1)) if m else None
    team_b = resolve(m.group(2)) if m else None

    # Determine result
    result = None
    if red_score is not None and blue_score is not None:
        if red_score > blue_score:
            result = "team_a"
        elif blue_score > red_score:
            result = "team_b"
        else:
            result = "draw"

    match_link = f"https://osu.ppy.sh/community/matches/{match_id}" if match_id else None

    # Upsert match
    upsert_match(
        match_id,
        event=event, stage=stage, match_name=name,
        team_a=team_a, team_b=team_b,
        score_a=red_score, score_b=blue_score, result=result,
        match_link=match_link, start_time=start_time, end_time=end_time,
        source="osu_api",
    )

    # Upsert players
    for uid_str, uname in users.items():
        try:
            uid = int(uid_str)
        except (ValueError, TypeError):
            continue
        upsert_player(uid, username=uname, source="osu_api")

    # Upsert games + scores
    game_rows = []
    score_rows = []
    for g in (payload.get("games") or []):
        gid = g.get("game_id")
        if gid is None:
            continue
        game_rows.append({
            "match_id": match_id, "game_id": gid,
            "beatmap_id": g.get("beatmap_id"),
            "beatmap_title": g.get("beatmap_title"),
            "beatmap_version": g.get("beatmap_version"),
            "star_rating": g.get("star_rating"),
            "slot": None,  # mappool slot resolved later
            "mods": g.get("mods"),
            "mode": g.get("mode"),
            "scoring_type": g.get("scoring_type"),
            "team_type": g.get("team_type"),
            "winning_team": g.get("winning_team"),
            "red_total": g.get("red_total"),
            "blue_total": g.get("blue_total"),
            "start_time": g.get("start_time"),
            "end_time": g.get("end_time"),
        })
        for s in (g.get("scores") or []):
            uid = s.get("user_id")
            if uid is None:
                continue
            team = s.get("team")
            tc = team_a if team == "red" else (team_b if team == "blue" else None)
            score_rows.append({
                "match_id": match_id, "game_id": gid, "user_id": uid,
                "username": s.get("username"),
                "score": s.get("score"), "accuracy": s.get("accuracy"),
                "max_combo": s.get("max_combo"),
                "count_300": s.get("count_300"), "count_100": s.get("count_100"),
                "count_50": s.get("count_50"), "count_miss": s.get("count_miss"),
                "mods": s.get("mods"), "team": team, "team_code": tc,
                "passed": s.get("passed"), "slot": s.get("slot"),
            })

    games_written = upsert_games(game_rows)
    scores_written = upsert_scores(score_rows)

    # Source link
    if match_id:
        upsert_source_link("match", str(match_id), "osu_api", str(match_id), match_link)

    return {"match_id": match_id or 0, "games": games_written, "scores": scores_written}
