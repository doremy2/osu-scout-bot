from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import timezone, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.manual_map_metadata import apply_manual_map_metadata
from importers.manual_match_metadata import apply_manual_matches
from importers.owc_wiki import fetch_owc_2025_bracket
from storage import (
    DB_PATH,
    canonicalize_stage,
    upsert_tournament_events,
    upsert_tournament_map_pool,
    upsert_tournament_stages,
)

METADATA_DIR = PROJECT_ROOT / "data" / "metadata" / "owc_2025"
REPORT_DIR = PROJECT_ROOT / "data" / "reports"
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "owc_2025"
MATCHES_CSV = METADATA_DIR / "matches.csv"
MAPS_CSV = METADATA_DIR / "maps.csv"
BEATMAP_CACHE_PATH = CACHE_DIR / "beatmap_lookup_cache.json"
REPORT_JSON_PATH = REPORT_DIR / "owc_2025_metadata_validation.json"
REPORT_MD_PATH = REPORT_DIR / "owc_2025_metadata_validation.md"

STAGE_ORDER = {
    "Qualifiers": 0,
    "Group Stage": 1,
    "Round of 16": 2,
    "Quarterfinals": 3,
    "Semifinals": 4,
    "Finals": 5,
    "Grand Finals": 6,
}

MOD_ORDER = {
    "NM": 0,
    "HD": 1,
    "HR": 2,
    "DT": 3,
    "FM": 4,
    "TB": 5,
}

OSU_OAUTH_TOKEN_URL = "https://osu.ppy.sh/oauth/token"
OSU_API_BASE = "https://osu.ppy.sh/api/v2"


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> int | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _to_float(value: Any) -> float | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _slot_sort_key(slot: str) -> tuple[int, int, str]:
    prefix = "".join(ch for ch in slot if ch.isalpha()).upper()
    digits = "".join(ch for ch in slot if ch.isdigit())
    return (MOD_ORDER.get(prefix, 99), int(digits) if digits else 999, slot)


def _stage_sort_key(stage: str | None) -> tuple[int, str]:
    normalized = canonicalize_stage(stage)
    return (STAGE_ORDER.get(normalized, 999), normalized or "")


def _normalize_team_code(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    return text.upper().replace(":", "").strip() or None


def _normalize_slot(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    return text.upper().replace(" ", "")


def _load_env() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
        return
    except ImportError:
        pass

    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _parse_existing_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            if not any((value or "").strip() for value in raw.values()):
                continue
            rows.append({key: _clean_text(value) for key, value in raw.items()})
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _load_team_codes() -> set[str]:
    team_path = METADATA_DIR / "teams.csv"
    return {
        _normalize_team_code(row.get("team_code")) or ""
        for row in _parse_existing_csv(team_path)
        if _normalize_team_code(row.get("team_code"))
    }


def _query_canonical_match_rows(db_path: str) -> list[dict[str, Any]]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            WITH grouped AS (
                SELECT
                    stage,
                    match_id,
                    red_team_code,
                    blue_team_code,
                    MIN(COALESCE(start_time, end_time)) AS start_time,
                    MAX(COALESCE(end_time, start_time)) AS end_time,
                    SUM(CASE WHEN winning_team = 'red' THEN 1 ELSE 0 END) AS red_wins,
                    SUM(CASE WHEN winning_team = 'blue' THEN 1 ELSE 0 END) AS blue_wins
                FROM match_games
                WHERE event = 'OWC 2025'
                  AND red_team_code IS NOT NULL
                  AND blue_team_code IS NOT NULL
                GROUP BY stage, match_id, red_team_code, blue_team_code
            )
            SELECT *
            FROM grouped
            ORDER BY
                CASE stage
                    WHEN 'Group Stage' THEN 1
                    WHEN 'Round of 16' THEN 2
                    WHEN 'Quarterfinals' THEN 3
                    WHEN 'Semifinals' THEN 4
                    WHEN 'Finals' THEN 5
                    WHEN 'Grand Finals' THEN 6
                    ELSE 999
                END,
                start_time,
                match_id
            """
        ).fetchall()
    finally:
        connection.close()

    counters: dict[tuple[str, str], int] = defaultdict(int)
    output: list[dict[str, Any]] = []
    for row in rows:
        stage = canonicalize_stage(row["stage"])
        match_date = _clean_text(row["start_time"] or row["end_time"])
        date_value = match_date[:10] if match_date else None
        match_link = f"https://osu.ppy.sh/community/matches/{row['match_id']}"

        for team_code, opponent_code, team_score, opponent_score in (
            (
                _normalize_team_code(row["red_team_code"]),
                _normalize_team_code(row["blue_team_code"]),
                _to_int(row["red_wins"]),
                _to_int(row["blue_wins"]),
            ),
            (
                _normalize_team_code(row["blue_team_code"]),
                _normalize_team_code(row["red_team_code"]),
                _to_int(row["blue_wins"]),
                _to_int(row["red_wins"]),
            ),
        ):
            if not team_code or not opponent_code:
                continue
            match_index = counters[(stage or "", team_code)]
            counters[(stage or "", team_code)] += 1
            result = "draw"
            if (team_score or 0) > (opponent_score or 0):
                result = "win"
            elif (team_score or 0) < (opponent_score or 0):
                result = "loss"

            output.append(
                {
                    "event": "OWC 2025",
                    "stage": stage,
                    "match_index": match_index,
                    "team_code": team_code,
                    "opponent_team_code": opponent_code,
                    "team_score": team_score,
                    "opponent_score": opponent_score,
                    "result": result,
                    "match_link": match_link,
                    "date": date_value,
                    "match_id": row["match_id"],
                    "match_start_time": _clean_text(row["start_time"]),
                    "match_end_time": _clean_text(row["end_time"]),
                }
            )
    return output


def _validate_match_rows(
    existing_rows: list[dict[str, Any]],
    canonical_rows: list[dict[str, Any]],
    *,
    team_codes: set[str],
) -> dict[str, Any]:
    existing_by_key: dict[tuple[str, str, int, str], dict[str, Any]] = {}
    duplicate_keys: list[dict[str, Any]] = []
    invalid_dates: list[dict[str, Any]] = []
    unknown_codes: list[dict[str, Any]] = []

    for row in existing_rows:
        stage = canonicalize_stage(row.get("stage"))
        match_index = _to_int(row.get("match_index")) or 0
        team_code = _normalize_team_code(row.get("team_code"))
        opponent_code = _normalize_team_code(row.get("opponent_team_code"))
        key = ("OWC 2025", stage or "", match_index, team_code or "")
        if key in existing_by_key:
            duplicate_keys.append({"key": key, "row": row})
        else:
            existing_by_key[key] = row

        date_value = _clean_text(row.get("date"))
        if date_value:
            try:
                datetime.strptime(date_value, "%Y-%m-%d")
            except ValueError:
                invalid_dates.append({"key": key, "date": date_value})

        for code_key, code_value in (("team_code", team_code), ("opponent_team_code", opponent_code)):
            if code_value and code_value not in team_codes:
                unknown_codes.append({"key": key, "field": code_key, "code": code_value})

    canonical_by_key = {
        (row["event"], row["stage"] or "", int(row["match_index"]), row["team_code"] or ""): row
        for row in canonical_rows
    }

    missing_from_csv = []
    changed_rows = []
    for key, canonical_row in canonical_by_key.items():
        existing_row = existing_by_key.get(key)
        if existing_row is None:
            missing_from_csv.append(
                {
                    "key": key,
                    "canonical": canonical_row,
                }
            )
            continue
        differences = {}
        comparisons = {
            "opponent_team_code": _normalize_team_code(existing_row.get("opponent_team_code")),
            "team_score": _to_int(existing_row.get("team_score")),
            "opponent_score": _to_int(existing_row.get("opponent_score")),
            "result": (_clean_text(existing_row.get("result")) or "").lower() or None,
            "match_link": _clean_text(existing_row.get("match_link")),
            "date": _clean_text(existing_row.get("date")),
        }
        for field, existing_value in comparisons.items():
            canonical_value = canonical_row.get(field)
            if existing_value != canonical_value:
                differences[field] = {"existing": existing_value, "canonical": canonical_value}
        if differences:
            changed_rows.append({"key": key, "differences": differences})

    extra_rows = [
        {"key": key, "row": row}
        for key, row in existing_by_key.items()
        if key not in canonical_by_key
    ]

    return {
        "existing_row_count": len(existing_rows),
        "canonical_row_count": len(canonical_rows),
        "duplicate_key_count": len(duplicate_keys),
        "blank_match_link_count": sum(1 for row in existing_rows if not _clean_text(row.get("match_link"))),
        "blank_date_count": sum(1 for row in existing_rows if not _clean_text(row.get("date"))),
        "invalid_date_count": len(invalid_dates),
        "unknown_team_code_count": len(unknown_codes),
        "missing_from_csv_count": len(missing_from_csv),
        "extra_row_count": len(extra_rows),
        "changed_row_count": len(changed_rows),
        "duplicate_keys": duplicate_keys[:20],
        "invalid_dates": invalid_dates[:20],
        "unknown_team_codes": unknown_codes[:20],
        "missing_from_csv": missing_from_csv[:20],
        "extra_rows": extra_rows[:20],
        "changed_rows": changed_rows[:40],
    }


def _query_observed_pool_rows(db_path: str) -> list[dict[str, Any]]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            SELECT
                stage,
                slot,
                COUNT(*) AS row_count,
                COUNT(DISTINCT map_name) AS name_variants,
                MIN(map_name) AS map_name
            FROM matches
            WHERE event = 'OWC 2025'
              AND stage IS NOT NULL
              AND slot IS NOT NULL
            GROUP BY stage, slot
            ORDER BY
                CASE stage
                    WHEN 'Qualifiers' THEN 0
                    WHEN 'Group Stage' THEN 1
                    WHEN 'Round of 16' THEN 2
                    WHEN 'Quarterfinals' THEN 3
                    WHEN 'Semifinals' THEN 4
                    WHEN 'Finals' THEN 5
                    WHEN 'Grand Finals' THEN 6
                    ELSE 999
                END,
                slot
            """
        ).fetchall()
    finally:
        connection.close()
    return [dict(row) for row in rows]


def _load_beatmap_cache() -> dict[str, dict[str, Any]]:
    if not BEATMAP_CACHE_PATH.exists():
        return {}
    try:
        return json.loads(BEATMAP_CACHE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_beatmap_cache(cache: dict[str, dict[str, Any]]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    BEATMAP_CACHE_PATH.write_text(
        json.dumps(cache, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )


class OsuBeatmapClient:
    def __init__(self, client_id: str, client_secret: str) -> None:
        if not client_id or not client_secret:
            raise RuntimeError("OSU_CLIENT_ID and OSU_CLIENT_SECRET must be set for beatmap enrichment.")
        self._client_id = client_id
        self._client_secret = client_secret
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    @classmethod
    def from_env(cls) -> "OsuBeatmapClient":
        _load_env()
        return cls(
            client_id=os.getenv("OSU_CLIENT_ID", ""),
            client_secret=os.getenv("OSU_CLIENT_SECRET", ""),
        )

    def _ensure_token(self) -> None:
        if self._token and time.time() < self._token_expires_at - 30:
            return
        request = Request(
            OSU_OAUTH_TOKEN_URL,
            data=json.dumps(
                {
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "grant_type": "client_credentials",
                    "scope": "public",
                }
            ).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))

    def _headers(self) -> dict[str, str]:
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "User-Agent": "osu-scout/1.0",
        }

    def get_beatmap(self, beatmap_id: int) -> dict[str, Any] | None:
        request = Request(
            f"{OSU_API_BASE}/beatmaps/{quote(str(beatmap_id))}",
            headers=self._headers(),
        )
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return {
            "beatmap_id": _to_int(payload.get("id")),
            "difficulty_name": _clean_text(payload.get("version")),
            "star_rating": _to_float(payload.get("difficulty_rating")),
            "beatmap_title": _clean_text((payload.get("beatmapset") or {}).get("title")),
        }


def _build_map_rows(
    db_path: str,
    *,
    skip_star_ratings: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    observed_rows = _query_observed_pool_rows(db_path)
    observed_by_key = {
        (canonicalize_stage(row["stage"]) or "", _normalize_slot(row["slot"]) or ""): row
        for row in observed_rows
    }

    bracket = fetch_owc_2025_bracket()
    wiki_pool_rows = []
    for entry in bracket.mappool:
        wiki_pool_rows.append(
            {
                "event": "OWC 2025",
                "stage": canonicalize_stage(entry.stage),
                "slot": _normalize_slot(entry.slot),
                "map_name": _clean_text(entry.map_name),
                "difficulty_name": _clean_text(entry.difficulty_name),
                "beatmap_id": _to_int(entry.beatmap_id),
            }
        )

    wiki_by_key = {
        (row["stage"] or "", row["slot"] or ""): row
        for row in wiki_pool_rows
        if row["stage"] and row["slot"]
    }

    existing_rows = _parse_existing_csv(MAPS_CSV)
    existing_by_key = {
        (
            canonicalize_stage(row.get("stage")) or "",
            _normalize_slot(row.get("slot")) or "",
        ): row
        for row in existing_rows
        if row.get("stage") and row.get("slot")
    }

    beatmap_cache = _load_beatmap_cache()
    client: OsuBeatmapClient | None = None
    if not skip_star_ratings:
        try:
            client = OsuBeatmapClient.from_env()
        except RuntimeError:
            client = None

    all_keys = sorted(
        set(observed_by_key) | set(wiki_by_key),
        key=lambda item: (_stage_sort_key(item[0]), _slot_sort_key(item[1])),
    )

    rows: list[dict[str, Any]] = []
    missing_wiki_slots: list[dict[str, Any]] = []
    missing_observed_slots: list[dict[str, Any]] = []
    name_mismatches: list[dict[str, Any]] = []
    broken_references: list[dict[str, Any]] = []

    for key in all_keys:
        stage, slot = key
        observed = observed_by_key.get(key)
        wiki_row = wiki_by_key.get(key)
        existing = existing_by_key.get(key)

        if wiki_row is None and observed is not None:
            missing_wiki_slots.append({"stage": stage, "slot": slot, "observed": observed})
        if observed is None and wiki_row is not None:
            missing_observed_slots.append({"stage": stage, "slot": slot, "wiki": wiki_row})

        observed_name = _clean_text((observed or {}).get("map_name"))
        wiki_name = _clean_text((wiki_row or {}).get("map_name"))
        if observed_name and wiki_name and observed_name != wiki_name:
            name_mismatches.append(
                {
                    "stage": stage,
                    "slot": slot,
                    "observed_map_name": observed_name,
                    "wiki_map_name": wiki_name,
                }
            )

        beatmap_id = _to_int((wiki_row or {}).get("beatmap_id")) or _to_int((existing or {}).get("beatmap_id"))
        difficulty_name = _clean_text((wiki_row or {}).get("difficulty_name")) or _clean_text((existing or {}).get("difficulty_name"))
        star_rating = _to_float((existing or {}).get("star_rating"))

        cache_key = str(beatmap_id) if beatmap_id is not None else None
        if cache_key and star_rating is None:
            cached_row = beatmap_cache.get(cache_key) or {}
            star_rating = _to_float(cached_row.get("star_rating"))
            difficulty_name = difficulty_name or _clean_text(cached_row.get("difficulty_name"))

        if beatmap_id is not None and star_rating is None and client is not None:
            payload = client.get_beatmap(beatmap_id)
            if payload:
                beatmap_cache[str(beatmap_id)] = payload
                star_rating = _to_float(payload.get("star_rating"))
                difficulty_name = difficulty_name or _clean_text(payload.get("difficulty_name"))

        map_name = observed_name or wiki_name or _clean_text((existing or {}).get("map_name"))
        row = {
            "event": "OWC 2025",
            "stage": stage,
            "slot": slot,
            "map_name": map_name,
            "difficulty_name": difficulty_name,
            "beatmap_id": beatmap_id,
            "star_rating": round(star_rating, 2) if star_rating is not None else None,
        }
        if row["beatmap_id"] is None or row["map_name"] is None:
            broken_references.append(row)
        rows.append(row)

    _save_beatmap_cache(beatmap_cache)

    report = {
        "existing_row_count": len(existing_rows),
        "observed_pool_row_count": len(observed_rows),
        "wiki_pool_row_count": len(wiki_pool_rows),
        "generated_row_count": len(rows),
        "missing_wiki_slot_count": len(missing_wiki_slots),
        "missing_observed_slot_count": len(missing_observed_slots),
        "name_mismatch_count": len(name_mismatches),
        "missing_beatmap_id_count": sum(1 for row in rows if row["beatmap_id"] is None),
        "missing_star_rating_count": sum(1 for row in rows if row["star_rating"] is None),
        "broken_reference_count": len(broken_references),
        "missing_wiki_slots": missing_wiki_slots[:40],
        "missing_observed_slots": missing_observed_slots[:40],
        "name_mismatches": name_mismatches[:40],
        "broken_references": broken_references[:40],
    }
    return rows, report


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# OWC 2025 Metadata Validation",
        "",
        "## Match Metadata",
        f"- Existing CSV rows: {report['matches']['existing_row_count']}",
        f"- Canonical DB/API rows: {report['matches']['canonical_row_count']}",
        f"- Missing rows in CSV: {report['matches']['missing_from_csv_count']}",
        f"- Changed rows vs canonical truth: {report['matches']['changed_row_count']}",
        f"- Blank match links: {report['matches']['blank_match_link_count']}",
        f"- Blank dates: {report['matches']['blank_date_count']}",
        "",
        "## Map Metadata",
        f"- Existing CSV rows: {report['maps']['existing_row_count']}",
        f"- Observed pool rows in matches table: {report['maps']['observed_pool_row_count']}",
        f"- Wiki pool rows: {report['maps']['wiki_pool_row_count']}",
        f"- Generated map rows: {report['maps']['generated_row_count']}",
        f"- Missing beatmap IDs: {report['maps']['missing_beatmap_id_count']}",
        f"- Missing star ratings: {report['maps']['missing_star_rating_count']}",
        f"- Broken references: {report['maps']['broken_reference_count']}",
        "",
    ]

    if report["matches"]["changed_rows"]:
        lines.append("## Sample Match Fixes")
        for item in report["matches"]["changed_rows"][:10]:
            key = item["key"]
            lines.append(f"- {key[1]} | {key[3]} | match_index={key[2]} -> {json.dumps(item['differences'], ensure_ascii=True)}")
        lines.append("")

    if report["maps"]["name_mismatches"]:
        lines.append("## Sample Map Name Mismatches")
        for item in report["maps"]["name_mismatches"][:10]:
            lines.append(
                f"- {item['stage']} {item['slot']}: observed `{item['observed_map_name']}` vs wiki `{item['wiki_map_name']}`"
            )
        lines.append("")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild and validate OWC 2025 metadata files.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="SQLite DB path.")
    parser.add_argument("--write", action="store_true", help="Write refreshed matches.csv and maps.csv.")
    parser.add_argument("--apply-db", action="store_true", help="Replay refreshed metadata back into SQLite after writing.")
    parser.add_argument("--skip-star-ratings", action="store_true", help="Skip osu! beatmap API lookups for star ratings.")
    args = parser.parse_args()

    team_codes = _load_team_codes()
    canonical_match_rows = _query_canonical_match_rows(args.db_path)
    existing_match_rows = _parse_existing_csv(MATCHES_CSV)
    match_report = _validate_match_rows(existing_match_rows, canonical_match_rows, team_codes=team_codes)

    clean_match_rows = [
        {
            "event": row["event"],
            "stage": row["stage"],
            "match_index": row["match_index"],
            "team_code": row["team_code"],
            "opponent_team_code": row["opponent_team_code"],
            "team_score": row["team_score"],
            "opponent_score": row["opponent_score"],
            "result": row["result"],
            "match_link": row["match_link"],
            "date": row["date"],
        }
        for row in canonical_match_rows
    ]

    clean_map_rows, map_report = _build_map_rows(
        args.db_path,
        skip_star_ratings=args.skip_star_ratings,
    )

    report = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "matches": match_report,
        "maps": map_report,
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_JSON_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")
    REPORT_MD_PATH.write_text(_render_markdown(report), encoding="utf-8")

    if args.write:
        _write_csv(
            MATCHES_CSV,
            clean_match_rows,
            [
                "event",
                "stage",
                "match_index",
                "team_code",
                "opponent_team_code",
                "team_score",
                "opponent_score",
                "result",
                "match_link",
                "date",
            ],
        )
        _write_csv(
            MAPS_CSV,
            clean_map_rows,
            [
                "event",
                "stage",
                "slot",
                "map_name",
                "difficulty_name",
                "beatmap_id",
                "star_rating",
            ],
        )

    if args.apply_db:
        match_stats = apply_manual_matches(clean_match_rows)
        map_stats = apply_manual_map_metadata(clean_map_rows)
        metadata_event_stats = upsert_tournament_events(
            [
                {
                    "event": "OWC 2025",
                    "display_name": "OWC 2025",
                    "short_name": "OWC 2025",
                    "tier": "world_cup",
                    "start_date": min((row["date"] for row in clean_match_rows if row.get("date")), default=None),
                    "end_date": max((row["date"] for row in clean_match_rows if row.get("date")), default=None),
                    "source": "OWC_WIKI",
                    "source_type": "html",
                    "source_file": str(MATCHES_CSV),
                    "source_url": "https://osu.ppy.sh/wiki/en/Tournaments/OWC/2025",
                    "metadata": {
                        "match_csv": str(MATCHES_CSV),
                        "map_csv": str(MAPS_CSV),
                    },
                }
            ]
        )
        metadata_stage_stats = upsert_tournament_stages(
            [
                {
                    "event": "OWC 2025",
                    "stage": stage,
                    "stage_order": STAGE_ORDER.get(stage),
                    "stage_type": "tournament_stage",
                    "starts_at": None,
                    "ends_at": None,
                    "source": "OWC_WIKI",
                    "source_type": "html",
                    "source_file": str(MATCHES_CSV),
                    "source_url": "https://osu.ppy.sh/wiki/en/Tournaments/OWC/2025",
                }
                for stage in sorted({row["stage"] for row in clean_map_rows}, key=_stage_sort_key)
            ]
        )
        metadata_map_stats = upsert_tournament_map_pool(
            [
                {
                    **row,
                    "source": "OWC_WIKI",
                    "source_type": "html",
                    "source_file": str(MAPS_CSV),
                    "source_url": "https://osu.ppy.sh/wiki/en/Tournaments/OWC/2025",
                }
                for row in clean_map_rows
            ]
        )
        print("DB apply stats:")
        print(
            json.dumps(
                {
                    "matches": match_stats,
                    "maps": map_stats,
                    "metadata": {
                        "events_written": metadata_event_stats,
                        "stages_written": metadata_stage_stats,
                        "mappool_written": metadata_map_stats,
                    },
                },
                indent=2,
            )
        )

    print(f"Wrote validation report: {REPORT_JSON_PATH}")
    print(f"Wrote validation summary: {REPORT_MD_PATH}")
    print(json.dumps({
        "match_rows": len(clean_match_rows),
        "map_rows": len(clean_map_rows),
        "match_changes": match_report["changed_row_count"],
        "match_missing_rows": match_report["missing_from_csv_count"],
        "map_missing_beatmap_ids": map_report["missing_beatmap_id_count"],
        "map_missing_star_ratings": map_report["missing_star_rating_count"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
