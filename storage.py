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
    player_team TEXT,
    opponent_team TEXT,
    match_id TEXT,
    import_batch TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_matches_player ON matches(player);",
    "CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(date);",
    "CREATE INDEX IF NOT EXISTS idx_matches_slot ON matches(slot);",
    "CREATE INDEX IF NOT EXISTS idx_matches_source ON matches(source);",
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
    player_team,
    opponent_team,
    match_id,
    import_batch,
    fingerprint
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    star_rating = excluded.star_rating,
    beatmap_id = excluded.beatmap_id,
    map_name = excluded.map_name,
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


def init_db() -> None:
    with get_connection() as connection:
        connection.execute(CREATE_MATCHES_TABLE_SQL)
        for statement in CREATE_INDEXES_SQL:
            connection.execute(statement)
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


def export_all_matches_to_json(output_path: str | Path) -> Path:
    rows = fetch_all_matches()
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return output
