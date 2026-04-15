from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
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


def _ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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
    normalized = {
        "player": _clean_text(match.get("player")),
        "opponent": _clean_text(match.get("opponent")),
        "event": _clean_text(match.get("event")),
        "stage": _clean_text(match.get("stage")),
        "source": _clean_text(match.get("source")) or "manual",
        "source_type": _clean_text(match.get("source_type")) or source_type or _infer_source_type(source_file),
        "source_file": _clean_text(match.get("source_file")) or source_file,
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
        "match_id": _clean_text(match.get("match_id")),
        "import_batch": import_batch or _clean_text(match.get("import_batch")) or _utc_now_iso(),
    }

    required_fields = ["player", "event", "source", "mod", "slot"]
    missing = [field for field in required_fields if not normalized[field]]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)} | row={match}")

    normalized["fingerprint"] = _clean_text(match.get("fingerprint")) or _build_fingerprint(normalized)
    return normalized


def get_connection() -> sqlite3.Connection:
    _ensure_data_dir()
    connection = sqlite3.connect(DB_PATH)
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


def init_db() -> None:
    with get_connection() as connection:
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

        connection.execute(CREATE_TEAMS_TABLE_SQL)

        connection.commit()


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
    WHERE lower(trim(player)) = lower(trim(?))
    ORDER BY date DESC, id DESC
    """
    with get_connection() as connection:
        rows = connection.execute(query, (username,)).fetchall()
    return [dict(row) for row in rows]


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
    values = [
        (
            _clean_text(row.get("player")),
            _clean_text(row.get("player_team")),
            _clean_text(row.get("event")),
            _clean_text(row.get("stage")),
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
    with get_connection() as connection:
        connection.executemany(UPSERT_PLAYER_SCORE_SQL, values)
        connection.commit()
    return len(values)


def fetch_player_scores(username: str) -> list[dict[str, Any]]:
    """Return all per-round Performance Score rows for one player, newest
    rounds first (best-effort: by id desc, since these CSVs have no date)."""
    init_db()
    query = """
    SELECT *
    FROM player_scores
    WHERE lower(trim(player)) = lower(trim(?))
    ORDER BY id DESC
    """
    with get_connection() as connection:
        rows = connection.execute(query, (username,)).fetchall()
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
    event, stage, source, team, team_code, opponent_team,
    team_score, opponent_score, result, match_link, match_index,
    date, import_batch, fingerprint
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fingerprint) DO UPDATE SET
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
        fingerprint = _build_tournament_match_fingerprint(row)
        values.append(
            (
                _clean_text(row.get("event")),
                _clean_text(row.get("stage")),
                _clean_text(row.get("source")) or "manual",
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
) -> int:
    """Fill in opponent_team / match_link / date on an existing
    tournament_matches row keyed by (event, stage, team_code, match_index).

    If no row exists for that key but team_score+opponent_score are provided,
    insert a new one. Used by the manual match metadata importer.
    Only fills NULLs via COALESCE, so re-runs are safe.
    """
    init_db()
    import_batch = _utc_now_iso()

    with get_connection() as connection:
        update_sql = """
        UPDATE tournament_matches
        SET
            opponent_team = COALESCE(?, opponent_team),
            match_link    = COALESCE(?, match_link),
            date          = COALESCE(?, date),
            import_batch  = ?
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
                import_batch,
                event, event,
                stage, stage,
                team_code, team_code,
                match_index, match_index,
            ),
        )
        updated = cursor.rowcount

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
    WITH player_teams AS (
        SELECT DISTINCT player_team
        FROM matches
        WHERE lower(trim(player)) = lower(trim(?))
          AND player_team IS NOT NULL
          AND TRIM(player_team) <> ''
    )
    SELECT
        tm.*,
        own.team_name AS team_name,
        opp.team_name AS opponent_team_name
    FROM tournament_matches tm
    LEFT JOIN teams own ON own.team_code = tm.team_code
    LEFT JOIN teams opp ON opp.team_code = tm.opponent_team
    WHERE tm.team_code IN (SELECT player_team FROM player_teams)
       OR tm.team       IN (SELECT player_team FROM player_teams)
    ORDER BY tm.date DESC, tm.id DESC
    LIMIT ?
    """
    with get_connection() as connection:
        rows = connection.execute(query, (username, limit)).fetchall()
    return [dict(row) for row in rows]


def export_all_matches_to_json(output_path: str | Path) -> Path:
    rows = fetch_all_matches()
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return output
