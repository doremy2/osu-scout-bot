"""Reusable tournament package importer for non-OWC / multi-event sources.

Package shape:

{
  "source": {
    "name": "ROMAI",
    "type": "json",
    "url": "https://example.com/source"
  },
  "events": [
    {
      "event": "ROMAI Week 5",
      "display_name": "ROMAI Week 5",
      "tier": "weekly"
    }
  ],
  "stages": [
    {
      "event": "ROMAI Week 5",
      "stage": "Week 5",
      "stage_order": 1,
      "stage_type": "ladder"
    }
  ],
  "players": [
    {
      "event": "ROMAI Week 5",
      "player": "example",
      "user_id": 123
    }
  ],
  "mappool": [
    {
      "event": "ROMAI Week 5",
      "stage": "Week 5",
      "slot": "NM1",
      "beatmap_id": 123456
    }
  ],
  "matches": [
    {
      "event": "ROMAI Week 5",
      "stage": "Week 5",
      "team": "example",
      "opponent_team": "other",
      "team_score": 5,
      "opponent_score": 3,
      "date": "2026-04-20"
    }
  ],
  "map_scores": [
    {
      "player": "example",
      "opponent": "other",
      "event": "ROMAI Week 5",
      "stage": "Week 5",
      "date": "2026-04-20",
      "slot": "NM1",
      "mod": "NM",
      "score": 512345,
      "accuracy": 97.32,
      "result": "win"
    }
  ]
}

The importer writes:
  - tournament_events
  - tournament_stages
  - tournament_players
  - tournament_map_pool
  - tournament_matches
  - matches (map-level score rows)
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from storage import (
    canonicalize_stage,
    insert_matches,
    insert_tournament_matches,
    upsert_tournament_events,
    upsert_tournament_map_pool,
    upsert_tournament_players,
    upsert_tournament_stages,
)


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


def _load_package(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return {"map_scores": payload}
    if not isinstance(payload, dict):
        raise ValueError(f"Unsupported package payload: {type(payload)!r}")
    return payload


def _normalize_package_meta(payload: dict[str, Any], package_path: str | Path) -> dict[str, Any]:
    package_status = (_clean_text(payload.get("package_status")) or "unlabeled").lower()
    raw_production_safe = payload.get("production_safe")
    if raw_production_safe is None:
        production_safe = package_status in {"verified", "production", "production_safe"}
    else:
        production_safe = bool(raw_production_safe)
    package_name = _clean_text(payload.get("package_name")) or _clean_text(payload.get("event"))
    package_id = _clean_text(payload.get("package_id"))
    if package_id is None:
        stem = Path(package_path).stem
        package_id = stem.replace(" ", "-").lower()
    return {
        "package_id": package_id,
        "package_name": package_name or package_id,
        "package_status": package_status,
        "production_safe": production_safe,
        "notes": payload.get("notes") or [],
    }


def _source_defaults(
    payload: dict[str, Any],
    package_path: str | Path,
    package_meta: dict[str, Any],
) -> dict[str, Any]:
    source = payload.get("source") or {}
    if not isinstance(source, dict):
        source = {}
    source_type = _clean_text(source.get("type")) or "json"
    status = _clean_text(package_meta.get("package_status"))
    if status and status not in source_type.casefold():
        source_type = f"{source_type}_{status}"
    return {
        "source": _clean_text(source.get("name")) or _clean_text(payload.get("source")) or "manual",
        "source_type": source_type,
        "source_file": str(Path(package_path)),
        "source_url": _clean_text(source.get("url")),
    }


def _apply_defaults(rows: list[dict[str, Any]], defaults: dict[str, Any]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        normalized = dict(defaults)
        normalized.update({key: value for key, value in row.items() if value is not None})
        output.append(normalized)
    return output


def _coerce_event_rows(
    payload: dict[str, Any],
    defaults: dict[str, Any],
    map_scores: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    events = payload.get("events")
    if isinstance(events, list):
        return _apply_defaults([row for row in events if isinstance(row, dict)], defaults)

    single_event = payload.get("event")
    if isinstance(single_event, dict):
        return _apply_defaults([single_event], defaults)
    if isinstance(single_event, str):
        return _apply_defaults([{"event": single_event, "display_name": single_event}], defaults)
    inferred = []
    seen: set[str] = set()
    for row in map_scores:
        event = _clean_text(row.get("event"))
        if event is None or event.casefold() in seen:
            continue
        seen.add(event.casefold())
        inferred.append({"event": event, "display_name": event})
    return _apply_defaults(inferred, defaults)


def _coerce_stage_rows(
    payload: dict[str, Any],
    defaults: dict[str, Any],
    map_scores: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    stages = payload.get("stages") or []
    rows = [row for row in stages if isinstance(row, dict)]
    if rows:
        return _apply_defaults(rows, defaults)

    inferred = []
    seen: set[tuple[str, str]] = set()
    for row in map_scores:
        event = _clean_text(row.get("event"))
        stage = canonicalize_stage(_clean_text(row.get("stage"))) or _clean_text(row.get("stage"))
        if event is None or stage is None:
            continue
        key = (event.casefold(), stage.casefold())
        if key in seen:
            continue
        seen.add(key)
        inferred.append({"event": event, "stage": stage})
    return _apply_defaults(inferred, defaults)


def _coerce_player_rows(
    payload: dict[str, Any],
    defaults: dict[str, Any],
    map_scores: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    players = payload.get("players") or []
    rows = [row for row in players if isinstance(row, dict)]
    if rows:
        return _apply_defaults(rows, defaults)

    inferred: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in map_scores:
        event = _clean_text(row.get("event"))
        player = _clean_text(row.get("player"))
        if event is None or player is None:
            continue
        key = (event, player, _clean_text(row.get("player_team")) or "")
        inferred.setdefault(
            key,
            {
                "event": event,
                "player": player,
                "team_code": _clean_text(row.get("player_team")),
                "user_id": _to_int(row.get("user_id")),
            },
        )
    return _apply_defaults(list(inferred.values()), defaults)


def _coerce_mappool_rows(payload: dict[str, Any], defaults: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("mappool") or []
    normalized_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized = dict(row)
        stage = canonicalize_stage(_clean_text(normalized.get("stage")))
        slot = _clean_text(normalized.get("slot"))
        if stage is None or slot is None:
            continue
        normalized["stage"] = stage
        normalized["slot"] = slot.upper()
        normalized_rows.append(normalized)
    return _apply_defaults(normalized_rows, defaults)


def _coerce_map_scores(payload: dict[str, Any], defaults: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("map_scores") or payload.get("matches") or []
    output = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        player = _clean_text(row.get("player"))
        event = _clean_text(row.get("event"))
        slot = _clean_text(row.get("slot"))
        mod = _clean_text(row.get("mod"))
        if player is None or event is None or slot is None:
            continue
        normalized = {
            "player": player,
            "opponent": _clean_text(row.get("opponent")),
            "event": event,
            "stage": canonicalize_stage(_clean_text(row.get("stage"))) or _clean_text(row.get("stage")),
            "source": _clean_text(row.get("source")) or defaults["source"],
            "source_type": _clean_text(row.get("source_type")) or defaults["source_type"],
            "source_file": _clean_text(row.get("source_file")) or defaults["source_file"],
            "source_url": _clean_text(row.get("source_url")) or defaults.get("source_url"),
            "date": _clean_text(row.get("date")),
            "mod": mod or "".join(ch for ch in slot.upper() if ch.isalpha()) or "NM",
            "slot": slot.upper(),
            "score": _to_int(row.get("score")),
            "accuracy": _to_float(row.get("accuracy")),
            "result": (_clean_text(row.get("result")) or "unknown").lower(),
            "star_rating": _to_float(row.get("star_rating")),
            "beatmap_id": _to_int(row.get("beatmap_id")),
            "map_name": _clean_text(row.get("map_name")),
            "difficulty_name": _clean_text(row.get("difficulty_name")),
            "player_team": _clean_text(row.get("player_team")),
            "opponent_team": _clean_text(row.get("opponent_team")),
            "match_id": _clean_text(row.get("match_id")),
            "user_id": _to_int(row.get("user_id")),
        }
        output.append(normalized)
    return output


def _derive_match_rows_from_map_scores(
    map_scores: list[dict[str, Any]],
    defaults: dict[str, Any],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in map_scores:
        event = _clean_text(row.get("event"))
        player = _clean_text(row.get("player"))
        opponent = _clean_text(row.get("opponent") or row.get("opponent_team"))
        if event is None or player is None or opponent is None:
            continue
        stage = canonicalize_stage(_clean_text(row.get("stage"))) or _clean_text(row.get("stage"))
        key = (
            event,
            stage or "",
            player,
            opponent,
            _clean_text(row.get("date")) or "",
            _clean_text(row.get("match_id")) or "",
        )
        bucket = grouped.setdefault(
            key,
            {
                "event": event,
                "stage": stage,
                "team": player,
                "team_code": _clean_text(row.get("player_team")),
                "opponent_team": opponent,
                "match_link": _clean_text(row.get("match_link")),
                "match_id": _clean_text(row.get("match_id")),
                "date": _clean_text(row.get("date")),
                "team_score": 0,
                "opponent_score": 0,
                "source": defaults["source"],
                "source_type": defaults.get("source_type"),
                "source_file": defaults.get("source_file"),
                "source_url": defaults.get("source_url"),
            },
        )
        result = (_clean_text(row.get("result")) or "").lower()
        if result == "win":
            bucket["team_score"] += 1
        elif result == "loss":
            bucket["opponent_score"] += 1

    grouped_rows = sorted(
        grouped.values(),
        key=lambda row: (
            row.get("event") or "",
            canonicalize_stage(row.get("stage")) or "",
            row.get("date") or "",
            row.get("team") or "",
            row.get("opponent_team") or "",
        ),
    )
    counters: dict[tuple[str, str], int] = defaultdict(int)
    output = []
    for row in grouped_rows:
        stage = canonicalize_stage(_clean_text(row.get("stage"))) or _clean_text(row.get("stage"))
        team_key = (row["event"], row.get("team") or "")
        match_index = counters[team_key]
        counters[team_key] += 1
        result = "draw"
        if row["team_score"] > row["opponent_score"]:
            result = "win"
        elif row["team_score"] < row["opponent_score"]:
            result = "loss"
        output.append(
            {
                "event": row["event"],
                "stage": stage,
                "team": row.get("team"),
                "team_code": row.get("team_code"),
                "opponent_team": row.get("opponent_team"),
                "team_score": row.get("team_score"),
                "opponent_score": row.get("opponent_score"),
                "result": result,
                "match_link": row.get("match_link"),
                "match_index": match_index,
                "date": row.get("date"),
                "source": defaults["source"],
                "source_type": row.get("source_type") or defaults.get("source_type"),
                "source_file": row.get("source_file") or defaults.get("source_file"),
                "source_url": row.get("source_url") or defaults.get("source_url"),
            }
        )
    return output


def _coerce_match_rows(
    payload: dict[str, Any],
    defaults: dict[str, Any],
    map_scores: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    explicit_rows = payload.get("matches") or []
    if explicit_rows and all(isinstance(row, dict) and row.get("team") for row in explicit_rows):
        normalized = []
        for row in explicit_rows:
            normalized.append(
                {
                    "event": _clean_text(row.get("event")),
                    "stage": canonicalize_stage(_clean_text(row.get("stage"))) or _clean_text(row.get("stage")),
                    "team": _clean_text(row.get("team")),
                    "team_code": _clean_text(row.get("team_code")),
                    "opponent_team": _clean_text(row.get("opponent_team")),
                    "team_score": _to_int(row.get("team_score")),
                    "opponent_score": _to_int(row.get("opponent_score")),
                    "result": (_clean_text(row.get("result")) or "").lower() or None,
                    "match_link": _clean_text(row.get("match_link")),
                    "match_index": _to_int(row.get("match_index")),
                    "date": _clean_text(row.get("date")),
                    "source": _clean_text(row.get("source")) or defaults["source"],
                    "source_type": _clean_text(row.get("source_type")) or defaults.get("source_type"),
                    "source_file": _clean_text(row.get("source_file")) or defaults.get("source_file"),
                    "source_url": _clean_text(row.get("source_url")) or defaults.get("source_url"),
                }
            )
        return normalized
    return _derive_match_rows_from_map_scores(map_scores, defaults)


def parse_tournament_package(path: str | Path) -> dict[str, Any]:
    package_path = Path(path)
    payload = _load_package(package_path)
    package_meta = _normalize_package_meta(payload, package_path)
    defaults = _source_defaults(payload, package_path, package_meta)

    map_scores = _coerce_map_scores(payload, defaults)
    package = {
        "package_meta": package_meta,
        "defaults": defaults,
        "events": _coerce_event_rows(payload, defaults, map_scores),
        "stages": _coerce_stage_rows(payload, defaults, map_scores),
        "players": _coerce_player_rows(payload, defaults, map_scores),
        "mappool": _coerce_mappool_rows(payload, defaults),
        "matches": _coerce_match_rows(payload, defaults, map_scores),
        "map_scores": map_scores,
    }
    return package


def summarize_tournament_package(package: dict[str, Any]) -> dict[str, Any]:
    package_meta = package.get("package_meta") or {}
    return {
        "package_id": package_meta.get("package_id"),
        "package_name": package_meta.get("package_name"),
        "package_status": package_meta.get("package_status"),
        "production_safe": bool(package_meta.get("production_safe")),
        "events": len(package.get("events") or []),
        "stages": len(package.get("stages") or []),
        "players": len(package.get("players") or []),
        "mappool": len(package.get("mappool") or []),
        "matches": len(package.get("matches") or []),
        "map_scores": len(package.get("map_scores") or []),
    }


def apply_tournament_package(
    package: dict[str, Any],
    *,
    allow_non_production: bool = False,
) -> dict[str, int]:
    package_meta = package.get("package_meta") or {}
    if not allow_non_production and not package_meta.get("production_safe"):
        raise ValueError(
            "Refusing to import a non-production-safe package without "
            "allow_non_production=True"
        )

    defaults = package.get("defaults") or {}
    source_file = _clean_text(defaults.get("source_file"))
    source_type = _clean_text(defaults.get("source_type")) or "json"

    event_rows = package.get("events") or []
    stage_rows = package.get("stages") or []
    player_rows = package.get("players") or []
    mappool_rows = package.get("mappool") or []
    match_rows = package.get("matches") or []
    map_score_rows = package.get("map_scores") or []

    stats = {
        "events_written": upsert_tournament_events(event_rows),
        "stages_written": upsert_tournament_stages(stage_rows),
        "players_written": upsert_tournament_players(player_rows),
        "mappool_written": upsert_tournament_map_pool(mappool_rows),
        "match_rows_written": insert_tournament_matches(match_rows) if match_rows else 0,
        "map_score_rows_written": insert_matches(
            map_score_rows,
            source_file=source_file,
            source_type=source_type,
        ) if map_score_rows else 0,
    }
    return stats
