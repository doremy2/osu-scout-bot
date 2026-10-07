from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage import DB_PATH  # noqa: E402


DISCOVERY_PATH = PROJECT_ROOT / "data" / "reports" / "stage_tournament_discovery.json"
STAGE_DETAIL_CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "stage" / "tournaments"
HISTORY_JSON_PATH = PROJECT_ROOT / "data" / "stage_player_tournament_history.json"
STAGE_RPC_TOURNAMENT_GET = "https://otr.stagec.net/rpc/tournaments/get"

CREATE_STAGE_PLAYER_STATS_SQL = """
CREATE TABLE IF NOT EXISTS stage_player_tournament_stats (
    tournament_id INTEGER NOT NULL,
    tournament_name TEXT NOT NULL,
    abbreviation TEXT,
    year INTEGER,
    forum_url TEXT,
    stage_url TEXT,
    rank_range_lower_bound INTEGER,
    ruleset INTEGER,
    lobby_size INTEGER,
    verification_status INTEGER,
    start_date TEXT,
    end_date TEXT,
    player_id INTEGER NOT NULL,
    osu_id INTEGER,
    username TEXT NOT NULL,
    country TEXT,
    average_rating_delta REAL,
    average_match_cost REAL,
    average_score REAL,
    average_placement REAL,
    average_accuracy REAL,
    matches_played INTEGER,
    matches_won INTEGER,
    matches_lost INTEGER,
    games_played INTEGER,
    games_won INTEGER,
    games_lost INTEGER,
    match_win_rate REAL,
    rating_before REAL,
    rating_after REAL,
    source TEXT NOT NULL,
    source_url TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (tournament_id, player_id)
);
"""

CREATE_STAGE_PLAYER_STATS_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_stage_player_stats_username ON stage_player_tournament_stats(username);",
    "CREATE INDEX IF NOT EXISTS idx_stage_player_stats_osu_id ON stage_player_tournament_stats(osu_id);",
    "CREATE INDEX IF NOT EXISTS idx_stage_player_stats_end_date ON stage_player_tournament_stats(end_date);",
    "CREATE INDEX IF NOT EXISTS idx_stage_player_stats_tournament ON stage_player_tournament_stats(tournament_name);",
]


def _safe_console(value: Any) -> str:
    text = str(value)
    try:
        text.encode(sys.stdout.encoding or "utf-8")
        return text
    except UnicodeEncodeError:
        return text.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(sys.stdout.encoding or "utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clean_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return text[:10] if len(text) >= 10 else text


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _stage_id(row: dict[str, Any]) -> int | None:
    raw = ((row.get("metadata_json") or {}).get("stage") or {}).get("raw") or {}
    return _to_int(raw.get("id"))


def _load_discovery_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _post_stage_rpc(tournament_id: int, timeout: float = 30.0) -> dict[str, Any]:
    body = json.dumps({"json": {"id": tournament_id}}).encode("utf-8")
    request = Request(
        STAGE_RPC_TOURNAMENT_GET,
        data=body,
        headers={
            "content-type": "application/json",
            "origin": "https://otr.stagec.net",
            "referer": f"https://otr.stagec.net/tournaments/{tournament_id}",
            "user-agent": "Mozilla/5.0 (osu-scout stage sync)",
        },
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    data = payload.get("json")
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected Stage payload for tournament {tournament_id}")
    return data


def _load_or_fetch_tournament(tournament_id: int, *, refresh: bool = False, sleep_seconds: float = 0.05) -> dict[str, Any]:
    STAGE_DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = STAGE_DETAIL_CACHE_DIR / f"{tournament_id}.json"
    if cache_path.exists() and not refresh:
        return json.loads(cache_path.read_text(encoding="utf-8"))
    data = _post_stage_rpc(tournament_id)
    cache_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    return data


def _load_cached_tournament(tournament_id: int) -> dict[str, Any] | None:
    cache_path = STAGE_DETAIL_CACHE_DIR / f"{tournament_id}.json"
    if not cache_path.exists():
        return None
    return json.loads(cache_path.read_text(encoding="utf-8"))


def _iter_stat_rows(tournament: dict[str, Any], source_row: dict[str, Any], fetched_at: str) -> list[dict[str, Any]]:
    tournament_id = _to_int(tournament.get("id"))
    if tournament_id is None:
        return []
    year = _to_int(source_row.get("year"))
    stage_url = source_row.get("stage_url") or source_row.get("source_url") or f"https://otr.stagec.net/tournaments/{tournament_id}"
    rows: list[dict[str, Any]] = []
    for stat in tournament.get("playerTournamentStats") or []:
        if not isinstance(stat, dict):
            continue
        player = stat.get("player") if isinstance(stat.get("player"), dict) else {}
        username = player.get("username")
        player_id = _to_int(stat.get("playerId") or player.get("id"))
        if not username or player_id is None:
            continue
        rows.append(
            {
                "tournament_id": tournament_id,
                "tournament_name": tournament.get("name") or source_row.get("tournament_name") or f"Stage tournament {tournament_id}",
                "abbreviation": tournament.get("abbreviation"),
                "year": year,
                "forum_url": tournament.get("forumUrl") or source_row.get("forum_url"),
                "stage_url": stage_url,
                "rank_range_lower_bound": _to_int(tournament.get("rankRangeLowerBound")),
                "ruleset": _to_int(tournament.get("ruleset")),
                "lobby_size": _to_int(tournament.get("lobbySize")),
                "verification_status": _to_int(tournament.get("verificationStatus")),
                "start_date": _clean_date(tournament.get("startTime") or source_row.get("start_date")),
                "end_date": _clean_date(tournament.get("endTime") or source_row.get("end_date")),
                "player_id": player_id,
                "osu_id": _to_int(player.get("osuId")),
                "username": str(username),
                "country": player.get("country"),
                "average_rating_delta": _to_float(stat.get("averageRatingDelta")),
                "average_match_cost": _to_float(stat.get("averageMatchCost")),
                "average_score": _to_float(stat.get("averageScore")),
                "average_placement": _to_float(stat.get("averagePlacement")),
                "average_accuracy": _to_float(stat.get("averageAccuracy")),
                "matches_played": _to_int(stat.get("matchesPlayed")),
                "matches_won": _to_int(stat.get("matchesWon")),
                "matches_lost": _to_int(stat.get("matchesLost")),
                "games_played": _to_int(stat.get("gamesPlayed")),
                "games_won": _to_int(stat.get("gamesWon")),
                "games_lost": _to_int(stat.get("gamesLost")),
                "match_win_rate": _to_float(stat.get("matchWinRate")),
                "rating_before": _to_float(stat.get("ratingBefore")),
                "rating_after": _to_float(stat.get("ratingAfter")),
                "source": "stage",
                "source_url": stage_url,
                "fetched_at": fetched_at,
            }
        )
    return rows


def _write_rows(db_path: Path, rows: list[dict[str, Any]]) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(CREATE_STAGE_PLAYER_STATS_SQL)
        for statement in CREATE_STAGE_PLAYER_STATS_INDEXES:
            connection.execute(statement)
        if rows:
            columns = list(rows[0].keys())
            placeholders = ", ".join("?" for _ in columns)
            column_sql = ", ".join(columns)
            update_sql = ", ".join(f"{column}=excluded.{column}" for column in columns if column not in {"tournament_id", "player_id"})
            connection.executemany(
                f"""
                INSERT INTO stage_player_tournament_stats ({column_sql})
                VALUES ({placeholders})
                ON CONFLICT(tournament_id, player_id) DO UPDATE SET {update_sql}
                """,
                [[row.get(column) for column in columns] for row in rows],
            )
        connection.commit()
    finally:
        connection.close()


def _write_history_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": _utc_now(),
        "source": "stage",
        "rows": rows,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _read_all_history_rows(db_path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute(CREATE_STAGE_PLAYER_STATS_SQL)
        rows = connection.execute(
            """
            SELECT *
            FROM stage_player_tournament_stats
            ORDER BY COALESCE(end_date, start_date, '') DESC, tournament_name, username
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync o!TR Stage player tournament stats into SQLite.")
    parser.add_argument("--discovery-json", type=Path, default=DISCOVERY_PATH)
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--years", nargs="*", type=int, default=[2026, 2025, 2024, 2023, 2022])
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--cached-only", action="store_true", help="Only ingest already cached Stage tournament detail JSON.")
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--min-verification", type=int, default=4)
    parser.add_argument("--history-json", type=Path, default=HISTORY_JSON_PATH)
    parser.add_argument(
        "--tournament-ids",
        nargs="*",
        type=int,
        default=None,
        help="Limit sync to specific Stage tournament IDs.",
    )
    args = parser.parse_args()

    keep_years = set(args.years or [])
    keep_tournament_ids = set(args.tournament_ids or [])
    candidates = []
    for row in _load_discovery_rows(args.discovery_json):
        if keep_years and row.get("year") not in keep_years:
            continue
        if row.get("game_mode") != "osu":
            continue
        raw = ((row.get("metadata_json") or {}).get("stage") or {}).get("raw") or {}
        if _to_int(raw.get("verificationStatus")) is not None and _to_int(raw.get("verificationStatus")) < args.min_verification:
            continue
        tid = _stage_id(row)
        if tid is None:
            continue
        if keep_tournament_ids and tid not in keep_tournament_ids:
            continue
        candidates.append(row)

    candidates.sort(key=lambda row: (str(row.get("end_date") or row.get("start_date") or ""), int(row.get("year") or 0)), reverse=True)
    if args.limit:
        candidates = candidates[: args.limit]

    fetched_at = _utc_now()
    rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for index, source_row in enumerate(candidates, start=1):
        tournament_id = _stage_id(source_row)
        if tournament_id is None:
            continue
        try:
            if args.cached_only:
                tournament = _load_cached_tournament(tournament_id)
                if tournament is None:
                    continue
            else:
                tournament = _load_or_fetch_tournament(tournament_id, refresh=args.refresh, sleep_seconds=args.sleep)
            rows.extend(_iter_stat_rows(tournament, source_row, fetched_at))
            print(
                _safe_console(
                    f"[{index}/{len(candidates)}] {tournament_id} {source_row.get('tournament_name')} -> {len(tournament.get('playerTournamentStats') or [])} players"
                )
            )
        except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            failures.append({"tournament_id": tournament_id, "name": source_row.get("tournament_name"), "error": str(exc)})
            print(_safe_console(f"[{index}/{len(candidates)}] {tournament_id} failed: {exc}"))

    _write_rows(args.db_path, rows)
    history_rows = _read_all_history_rows(args.db_path)
    _write_history_json(args.history_json, history_rows)

    print(f"Stage tournaments checked: {len(candidates)}")
    print(f"Stage player stat rows written: {len(rows)}")
    print(f"Stage player history rows exported: {len(history_rows)}")
    print(f"Failures: {len(failures)}")
    if failures:
        reports_dir = PROJECT_ROOT / "data" / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "stage_player_stats_failures.json").write_text(
            json.dumps(failures, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return 0 if rows else 1


if __name__ == "__main__":
    raise SystemExit(main())
