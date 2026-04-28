from __future__ import annotations

import sqlite3
from dataclasses import asdict
from datetime import date, datetime
from typing import Any, Iterable

from models import EventInput, PlayerInput
from osu_profile_enrichment import (
    DEFAULT_PROFILE_CACHE_TTL_HOURS,
    enrich_players_with_osu_profiles,
)
from storage import DB_PATH, canonicalize_stage

STAGE_TIER_WEIGHTS = {
    "Qualifiers": 0.80,
    "Group Stage": 0.95,
    "Round of 32": 1.00,
    "Lower Round 1": 1.00,
    "Lower Round 2": 1.00,
    "Lower Round 3": 1.00,
    "Round of 16": 1.05,
    "Quarterfinals": 1.18,
    "Semifinals": 1.30,
    "Finals": 1.42,
    "Grand Finals": 1.55,
}

TOURNAMENT_TIER_WEIGHTS = {
    "world_cup": 1.35,
    "premier": 1.15,
    "major": 1.00,
    "minor": 0.85,
    "community": 0.75,
}


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    return float(text)


def _normalize_player_key(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    normalized = "".join(ch for ch in text.casefold() if ch.isalnum())
    return normalized or text.casefold()


def _normalize_team_key(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    normalized = " ".join(text.casefold().split())
    return normalized or text.casefold()


def _parse_reference_date(reference_date: date | str | None) -> date:
    if isinstance(reference_date, date):
        return reference_date
    if isinstance(reference_date, str) and reference_date.strip():
        return datetime.fromisoformat(reference_date.strip()).date()
    return date.today()


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = _clean_text(value)
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_linear(values: dict[Any, float]) -> dict[Any, float]:
    if not values:
        return {}
    min_value = min(values.values())
    max_value = max(values.values())
    if min_value == max_value:
        return {key: 50.0 for key in values}
    return {
        key: max(0.0, min(100.0, 100.0 * ((value - min_value) / (max_value - min_value))))
        for key, value in values.items()
    }


def _load_alias_map(connection: sqlite3.Connection) -> dict[str, str]:
    rows = connection.execute(
        """
        SELECT alias, canonical_name
        FROM player_aliases
        """
    ).fetchall()
    alias_map: dict[str, str] = {}
    for row in rows:
        alias_key = _normalize_player_key(row["alias"])
        canonical_name = _clean_text(row["canonical_name"])
        if alias_key and canonical_name:
            alias_map[alias_key] = canonical_name
            canonical_key = _normalize_player_key(canonical_name)
            if canonical_key:
                alias_map[canonical_key] = canonical_name
    return alias_map


def _canonical_player_name(name: str | None, alias_map: dict[str, str]) -> str | None:
    cleaned = _clean_text(name)
    if cleaned is None:
        return None
    return alias_map.get(_normalize_player_key(cleaned) or "", cleaned)


def _event_matches_filter(values: Iterable[str] | None) -> set[str]:
    if not values:
        return set()
    return {value for value in (_clean_text(value) for value in values) if value}


def _load_player_score_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            player,
            event,
            stage,
            player_team,
            pscore,
            avg_score,
            avg_accuracy
        FROM player_scores
        WHERE event IS NOT NULL
          AND player IS NOT NULL
        """
    ).fetchall()


def _load_match_stage_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        WITH game_average AS (
            SELECT
                match_id,
                game_id,
                AVG(score) AS avg_score
            FROM match_scores
            WHERE score IS NOT NULL
            GROUP BY match_id, game_id
        )
        SELECT
            mg.event,
            mg.stage,
            ms.username,
            ms.team_code,
            COUNT(*) AS map_total,
            SUM(
                CASE
                    WHEN lower(ms.team) = lower(mg.winning_team) THEN 1
                    ELSE 0
                END
            ) AS map_wins,
            AVG(ms.score) AS avg_score,
            AVG(ms.accuracy) AS avg_accuracy,
            AVG(
                CASE
                    WHEN ga.avg_score > 0 THEN CAST(ms.score AS REAL) / ga.avg_score
                    ELSE NULL
                END
            ) AS performance_ratio,
            MAX(COALESCE(mg.end_time, mg.start_time)) AS last_played_at
        FROM match_scores ms
        JOIN match_games mg
          ON ms.match_id = mg.match_id
         AND ms.game_id = mg.game_id
        JOIN game_average ga
          ON ga.match_id = ms.match_id
         AND ga.game_id = ms.game_id
        WHERE mg.event IS NOT NULL
          AND ms.username IS NOT NULL
        GROUP BY
            mg.event,
            mg.stage,
            ms.user_id,
            ms.username,
            ms.team_code
        """
    ).fetchall()


def _load_legacy_match_stage_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            event,
            stage,
            player AS username,
            player_team AS team_code,
            COUNT(*) AS map_total,
            SUM(CASE WHEN lower(result) = 'win' THEN 1 ELSE 0 END) AS map_wins,
            AVG(score) AS avg_score,
            AVG(accuracy) AS avg_accuracy,
            MAX(date) AS last_played_at
        FROM matches
        WHERE event IS NOT NULL
          AND player IS NOT NULL
        GROUP BY
            event,
            stage,
            lower(trim(player)),
            player,
            player_team
        """
    ).fetchall()


def _load_team_opponents(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        WITH match_meta AS (
            SELECT
                match_id,
                event,
                stage,
                red_team_code,
                blue_team_code,
                MAX(COALESCE(end_time, start_time)) AS last_played_at
            FROM match_games
            WHERE event IS NOT NULL
            GROUP BY
                match_id,
                event,
                stage,
                red_team_code,
                blue_team_code
        ),
        team_matches AS (
            SELECT
                event,
                stage,
                red_team_code AS team_code,
                blue_team_code AS opponent_team,
                last_played_at
            FROM match_meta
            WHERE red_team_code IS NOT NULL
              AND blue_team_code IS NOT NULL
            UNION ALL
            SELECT
                event,
                stage,
                blue_team_code AS team_code,
                red_team_code AS opponent_team,
                last_played_at
            FROM match_meta
            WHERE red_team_code IS NOT NULL
              AND blue_team_code IS NOT NULL
        )
        SELECT
            event,
            stage,
            team_code,
            opponent_team,
            COUNT(*) AS matches_played,
            MAX(last_played_at) AS last_played_at
        FROM team_matches
        GROUP BY event, stage, team_code, opponent_team
        """
    ).fetchall()


def _load_tournament_team_opponents(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            event,
            stage,
            team_code,
            team,
            opponent_team,
            COUNT(*) AS matches_played,
            MAX(date) AS last_played_at
        FROM tournament_matches
        WHERE event IS NOT NULL
          AND stage IS NOT NULL
          AND (team_code IS NOT NULL OR team IS NOT NULL)
          AND opponent_team IS NOT NULL
        GROUP BY
            event,
            stage,
            team_code,
            team,
            opponent_team
        """
    ).fetchall()


def _load_legacy_team_opponents(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            event,
            stage,
            player_team AS team_code,
            opponent_team,
            COUNT(
                DISTINCT COALESCE(
                    match_id,
                    COALESCE(date, '') || '|' || COALESCE(player_team, '') || '|' || COALESCE(opponent_team, '') || '|' || COALESCE(stage, '')
                )
            ) AS matches_played,
            MAX(date) AS last_played_at
        FROM matches
        WHERE event IS NOT NULL
          AND player_team IS NOT NULL
          AND opponent_team IS NOT NULL
        GROUP BY
            event,
            stage,
            player_team,
            opponent_team
        """
    ).fetchall()


def _load_team_stage_results(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            event,
            stage,
            team_code,
            team,
            AVG(
                CASE
                    WHEN lower(result) = 'win' THEN 100.0
                    WHEN lower(result) = 'draw' THEN 50.0
                    WHEN lower(result) = 'loss' THEN 0.0
                    ELSE NULL
                END
            ) AS result_score,
            COUNT(*) AS matches_played,
            MAX(date) AS last_played_at
        FROM tournament_matches
        WHERE event IS NOT NULL
          AND stage IS NOT NULL
          AND (team_code IS NOT NULL OR team IS NOT NULL)
          AND result IS NOT NULL
        GROUP BY
            event,
            stage,
            team_code,
            team
        """
    ).fetchall()


def _load_tournament_tiers(connection: sqlite3.Connection) -> dict[str, str]:
    try:
        rows = connection.execute(
            """
            SELECT event, tier
            FROM tournament_events
            WHERE event IS NOT NULL
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    return {
        _clean_text(row["event"]): (_clean_text(row["tier"]) or "major")
        for row in rows
        if _clean_text(row["event"])
    }


def _tournament_tier_weight(tier: str | None) -> float:
    return TOURNAMENT_TIER_WEIGHTS.get((tier or "").casefold(), 1.0)


def _load_tournament_metadata(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """Load player_count and match_count per tournament for size/field weighting."""
    meta: dict[str, dict[str, Any]] = {}
    try:
        rows = connection.execute(
            """
            SELECT
                te.event,
                te.tier,
                (SELECT COUNT(DISTINCT tp.player) FROM tournament_players tp WHERE tp.event = te.event) AS player_count,
                (SELECT COUNT(*) FROM tournament_matches tm WHERE tm.event = te.event) AS match_count,
                (SELECT COUNT(DISTINCT m.player) FROM matches m WHERE m.event = te.event) AS active_player_count
            FROM tournament_events te
            WHERE te.event IS NOT NULL
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return meta
    for row in rows:
        event = _clean_text(row["event"])
        if event:
            meta[event] = {
                "player_count": int(row["player_count"] or 0),
                "match_count": int(row["match_count"] or 0),
                "active_player_count": int(row["active_player_count"] or 0),
                "tier": _clean_text(row["tier"]) or "major",
            }
    return meta


def _tournament_size_score(player_count: int) -> float:
    """Scale event weight by tournament size.

    size_score = clamp(log(player_count) / log(256), 0.75, 1.10)
    A 16-player invitational gets ~0.75, a 256-player open gets 1.0,
    a 330-player world cup gets ~1.05.
    """
    import math
    if player_count <= 1:
        return 0.75
    raw = math.log(player_count) / math.log(256)
    return max(0.75, min(1.10, raw))


def _field_strength_score(tier: str | None, player_count: int) -> float:
    """Estimate field strength from tier and size.

    Open-rank stacked events get higher weight; small restricted
    events get lower weight.
    """
    tier_lower = (tier or "").casefold()
    # Base score by tier
    if tier_lower == "world_cup":
        base = 1.20
    elif tier_lower == "premier":
        base = 1.10
    elif tier_lower == "major":
        base = 1.00
    elif tier_lower == "minor":
        base = 0.85
    else:
        base = 0.90
    # Small field penalty
    if player_count < 32:
        base *= 0.90
    elif player_count < 64:
        base *= 0.95
    return max(0.80, min(1.25, base))


def _clamp_score(value: float | None, minimum: float = 0.0, maximum: float = 100.0) -> float | None:
    if value is None:
        return None
    return max(minimum, min(maximum, float(value)))


def _normalize_stage_importance(stage_weight: float) -> float:
    """Convert stage multiplier into a 0-100 importance score."""
    min_weight = min(STAGE_TIER_WEIGHTS.values())
    max_weight = max(STAGE_TIER_WEIGHTS.values())
    if max_weight == min_weight:
        return 50.0
    return _clamp_score(100.0 * ((stage_weight - min_weight) / (max_weight - min_weight))) or 0.0


def _derive_impact_score(
    *,
    match_cost: float | None,
    win_rate: float | None,
    placement_percentile: float | None,
    strength_of_schedule: float | None,
    stage_tier_weight: float,
) -> float | None:
    """Reward performance that contributes to winning instead of raw score farming."""
    parts = {
        "map_performance": _clamp_score(match_cost),
        "map_wins": _clamp_score(win_rate),
        "match_result_proxy": _clamp_score(placement_percentile),
        "stage_importance": _normalize_stage_importance(stage_tier_weight),
        "opponent_strength": _clamp_score(strength_of_schedule),
    }
    weights = {
        "map_performance": 0.35,
        "map_wins": 0.25,
        "match_result_proxy": 0.20,
        "stage_importance": 0.10,
        "opponent_strength": 0.10,
    }
    total = 0.0
    total_weight = 0.0
    for key, weight in weights.items():
        value = parts[key]
        if value is None:
            continue
        total += value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return _clamp_score(total / total_weight)


def _raw_stage_metric(bucket: dict[str, Any]) -> float | None:
    raw_metric = _to_float(bucket.get("pscore"))
    if raw_metric is None:
        raw_metric = _to_float(bucket.get("performance_ratio"))
    if raw_metric is None:
        raw_metric = _to_float(bucket.get("avg_score"))
    return raw_metric


def build_power_ranking_inputs_from_db(
    *,
    db_path: str = str(DB_PATH),
    event_filters: Iterable[str] | None = None,
    reference_date: date | str | None = None,
    include_undated_stages: bool = False,
    enrich_osu_profiles: bool = True,
    profile_cache_ttl_hours: float | None = DEFAULT_PROFILE_CACHE_TTL_HOURS,
) -> tuple[list[PlayerInput], list[EventInput]]:
    reference_day = _parse_reference_date(reference_date)
    event_filter_set = _event_matches_filter(event_filters)

    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        alias_map = _load_alias_map(connection)
        tournament_tiers = _load_tournament_tiers(connection)
        tournament_meta = _load_tournament_metadata(connection)
        player_stage: dict[tuple[str, str, str, str | None], dict[str, Any]] = {}

        for row in _load_match_stage_rows(connection):
            event = _clean_text(row["event"])
            stage = canonicalize_stage(_clean_text(row["stage"]))
            if not event or not stage:
                continue
            if event_filter_set and event not in event_filter_set:
                continue
            canonical_name = _canonical_player_name(row["username"], alias_map)
            if canonical_name is None:
                continue
            team_code = _clean_text(row["team_code"])
            key = (canonical_name, event, stage, team_code)
            bucket = player_stage.setdefault(
                key,
                {
                    "username": canonical_name,
                    "event": event,
                    "stage": stage,
                    "team_code": team_code,
                    "pscore": None,
                    "avg_score": _to_float(row["avg_score"]),
                    "avg_accuracy": _to_float(row["avg_accuracy"]),
                    "performance_ratio": _to_float(row["performance_ratio"]),
                    "map_total": int(row["map_total"] or 0),
                    "map_wins": int(row["map_wins"] or 0),
                    "last_played_at": _clean_text(row["last_played_at"]),
                    "has_match_detail": True,
                },
            )
            bucket["map_total"] = int(bucket["map_total"] or 0) + int(row["map_total"] or 0)
            bucket["map_wins"] = int(bucket["map_wins"] or 0) + int(row["map_wins"] or 0)
            if bucket.get("last_played_at") is None or (
                row["last_played_at"] and str(row["last_played_at"]) > str(bucket["last_played_at"])
            ):
                bucket["last_played_at"] = _clean_text(row["last_played_at"])
            if bucket.get("performance_ratio") is None:
                bucket["performance_ratio"] = _to_float(row["performance_ratio"])
            bucket["has_match_detail"] = True

        for row in _load_legacy_match_stage_rows(connection):
            event = _clean_text(row["event"])
            stage = canonicalize_stage(_clean_text(row["stage"]))
            if not event or not stage:
                continue
            if event_filter_set and event not in event_filter_set:
                continue
            canonical_name = _canonical_player_name(row["username"], alias_map)
            if canonical_name is None:
                continue
            team_code = _clean_text(row["team_code"])
            key = (canonical_name, event, stage, team_code)
            bucket = player_stage.setdefault(
                key,
                {
                    "username": canonical_name,
                    "event": event,
                    "stage": stage,
                    "team_code": team_code,
                    "pscore": None,
                    "avg_score": _to_float(row["avg_score"]),
                    "avg_accuracy": _to_float(row["avg_accuracy"]),
                    "performance_ratio": None,
                    "map_total": 0,
                    "map_wins": 0,
                    "last_played_at": _clean_text(row["last_played_at"]),
                    "has_match_detail": False,
                },
            )
            if bucket.get("has_match_detail"):
                continue
            bucket["map_total"] = int(bucket["map_total"] or 0) + int(row["map_total"] or 0)
            bucket["map_wins"] = int(bucket["map_wins"] or 0) + int(row["map_wins"] or 0)
            if bucket.get("avg_score") is None:
                bucket["avg_score"] = _to_float(row["avg_score"])
            if bucket.get("avg_accuracy") is None:
                bucket["avg_accuracy"] = _to_float(row["avg_accuracy"])
            if bucket.get("last_played_at") is None or (
                row["last_played_at"] and str(row["last_played_at"]) > str(bucket["last_played_at"])
            ):
                bucket["last_played_at"] = _clean_text(row["last_played_at"])

        for row in _load_player_score_rows(connection):
            event = _clean_text(row["event"])
            stage = canonicalize_stage(_clean_text(row["stage"]))
            if not event or not stage:
                continue
            if event_filter_set and event not in event_filter_set:
                continue
            canonical_name = _canonical_player_name(row["player"], alias_map)
            if canonical_name is None:
                continue
            team_code = _clean_text(row["player_team"])
            key = (canonical_name, event, stage, team_code)
            bucket = player_stage.setdefault(
                key,
                {
                    "username": canonical_name,
                    "event": event,
                    "stage": stage,
                    "team_code": team_code,
                    "pscore": None,
                    "avg_score": _to_float(row["avg_score"]),
                    "avg_accuracy": _to_float(row["avg_accuracy"]),
                    "performance_ratio": None,
                    "map_total": 0,
                    "map_wins": 0,
                    "last_played_at": None,
                    "has_match_detail": False,
                },
            )
            if bucket.get("pscore") is None:
                bucket["pscore"] = _to_float(row["pscore"])
            if bucket.get("avg_score") is None:
                bucket["avg_score"] = _to_float(row["avg_score"])
            if bucket.get("avg_accuracy") is None:
                bucket["avg_accuracy"] = _to_float(row["avg_accuracy"])

        if not player_stage:
            return [], []

        raw_match_cost_by_stage: dict[tuple[str, str], dict[tuple[str, str, str | None], float]] = {}
        for key, bucket in player_stage.items():
            raw_metric = _raw_stage_metric(bucket)
            if raw_metric is None:
                continue
            _, event, stage, _ = key
            stage_key = (event, stage)
            raw_match_cost_by_stage.setdefault(stage_key, {})[key] = raw_metric

        normalized_match_cost: dict[tuple[str, str, str | None], float] = {}
        for stage_values in raw_match_cost_by_stage.values():
            normalized_match_cost.update(_normalize_linear(stage_values))

        team_stage_raw: dict[tuple[str, str], dict[str, list[float]]] = {}
        for key, bucket in player_stage.items():
            team_code = _clean_text(bucket.get("team_code"))
            team_lookup = _normalize_team_key(team_code)
            if not team_lookup:
                continue
            raw_metric = _raw_stage_metric(bucket)
            if raw_metric is None:
                continue
            _, event, stage, _ = key
            team_stage_raw.setdefault((event, stage), {}).setdefault(team_lookup, []).append(raw_metric)

        team_stage_strength: dict[tuple[str, str, str], float] = {}
        for stage_key, team_values in team_stage_raw.items():
            averages = {
                team_code: sum(values) / len(values)
                for team_code, values in team_values.items()
                if values
            }
            for team_code, score in _normalize_linear(averages).items():
                event, stage = stage_key
                team_stage_strength[(event, stage, team_code)] = score

        team_stage_result: dict[tuple[str, str, str], float] = {}
        for row in _load_team_stage_results(connection):
            event = _clean_text(row["event"])
            stage = canonicalize_stage(_clean_text(row["stage"]))
            if not event or not stage:
                continue
            if event_filter_set and event not in event_filter_set:
                continue
            result_score = _clamp_score(_to_float(row["result_score"]))
            if result_score is None:
                continue
            for team_value in (row["team_code"], row["team"]):
                team_lookup = _normalize_team_key(team_value)
                if team_lookup:
                    team_stage_result[(event, stage, team_lookup)] = result_score

        opponent_strengths: dict[tuple[str, str, str], tuple[float, float]] = {}
        stage_last_played: dict[tuple[str, str, str], str | None] = {}
        detail_opponent_keys: set[tuple[str, str, str]] = set()
        for row in _load_team_opponents(connection):
            event = _clean_text(row["event"])
            stage = canonicalize_stage(_clean_text(row["stage"]))
            team_code = _clean_text(row["team_code"])
            opponent_team = _clean_text(row["opponent_team"])
            team_lookup = _normalize_team_key(team_code)
            opponent_lookup = _normalize_team_key(opponent_team)
            if not event or not stage or not team_lookup or not opponent_lookup:
                continue
            if event_filter_set and event not in event_filter_set:
                continue
            strength = team_stage_strength.get((event, stage, opponent_lookup))
            if strength is None:
                continue
            weight = int(row["matches_played"] or 0)
            key = (event, stage, team_lookup)
            detail_opponent_keys.add(key)
            total, total_weight = opponent_strengths.get(key, (0.0, 0.0))
            opponent_strengths[key] = (total + (strength * weight), total_weight + weight)
            last_played = _clean_text(row["last_played_at"])
            if key not in stage_last_played or (last_played and str(last_played) > str(stage_last_played[key])):
                stage_last_played[key] = last_played

        for row in _load_tournament_team_opponents(connection):
            event = _clean_text(row["event"])
            stage = canonicalize_stage(_clean_text(row["stage"]))
            team_lookup = _normalize_team_key(row["team_code"]) or _normalize_team_key(row["team"])
            opponent_lookup = _normalize_team_key(row["opponent_team"])
            if not event or not stage or not team_lookup or not opponent_lookup:
                continue
            if event_filter_set and event not in event_filter_set:
                continue
            key = (event, stage, team_lookup)
            if key in detail_opponent_keys:
                continue
            strength = team_stage_strength.get((event, stage, opponent_lookup))
            if strength is None:
                continue
            weight = int(row["matches_played"] or 0)
            total, total_weight = opponent_strengths.get(key, (0.0, 0.0))
            opponent_strengths[key] = (total + (strength * weight), total_weight + weight)
            last_played = _clean_text(row["last_played_at"])
            if key not in stage_last_played or (last_played and str(last_played) > str(stage_last_played[key])):
                stage_last_played[key] = last_played

        for row in _load_legacy_team_opponents(connection):
            event = _clean_text(row["event"])
            stage = canonicalize_stage(_clean_text(row["stage"]))
            team_code = _clean_text(row["team_code"])
            opponent_team = _clean_text(row["opponent_team"])
            team_lookup = _normalize_team_key(team_code)
            opponent_lookup = _normalize_team_key(opponent_team)
            if not event or not stage or not team_lookup or not opponent_lookup:
                continue
            if event_filter_set and event not in event_filter_set:
                continue
            key = (event, stage, team_lookup)
            if key in detail_opponent_keys:
                continue
            strength = team_stage_strength.get((event, stage, opponent_lookup))
            if strength is None:
                continue
            weight = int(row["matches_played"] or 0)
            total, total_weight = opponent_strengths.get(key, (0.0, 0.0))
            opponent_strengths[key] = (total + (strength * weight), total_weight + weight)
            last_played = _clean_text(row["last_played_at"])
            if key not in stage_last_played or (last_played and str(last_played) > str(stage_last_played[key])):
                stage_last_played[key] = last_played

        events: list[EventInput] = []
        player_event_names_last_12m: dict[str, set[str]] = {}
        player_min_days: dict[str, float] = {}

        for key, bucket in player_stage.items():
            username, event, stage, team_code = key
            last_played_at = _clean_text(bucket.get("last_played_at")) or stage_last_played.get((event, stage, team_code or ""))
            last_played_dt = _parse_iso_datetime(last_played_at)
            if last_played_dt is None and not include_undated_stages:
                continue

            days_since_event = None
            event_date = None
            if last_played_dt is not None:
                event_date = last_played_dt.date().isoformat()
                days_since_event = float((reference_day - last_played_dt.date()).days)

            map_total = int(bucket.get("map_total") or 0)
            map_wins = int(bucket.get("map_wins") or 0)
            win_rate = (100.0 * map_wins / map_total) if map_total > 0 else None

            match_cost = normalized_match_cost.get(key)
            team_lookup = _normalize_team_key(team_code)
            placement_percentile = (
                team_stage_result.get((event, stage, team_lookup))
                if team_lookup
                else None
            )
            placement_result_source = "match_result" if placement_percentile is not None else "team_strength_fallback"
            if placement_percentile is None:
                placement_percentile = (
                    team_stage_strength.get((event, stage, team_lookup))
                    if team_lookup
                    else None
                )
            if placement_percentile is None:
                placement_percentile = match_cost
                placement_result_source = "match_cost_fallback"
            schedule_raw = opponent_strengths.get((event, stage, team_lookup or ""))
            if schedule_raw:
                total, total_weight = schedule_raw
                strength_of_schedule = total / total_weight if total_weight else None
            else:
                strength_of_schedule = None

            stage_tier_weight = STAGE_TIER_WEIGHTS.get(stage, 1.0)
            impact_score = _derive_impact_score(
                match_cost=match_cost,
                win_rate=win_rate,
                placement_percentile=placement_percentile,
                strength_of_schedule=strength_of_schedule,
                stage_tier_weight=stage_tier_weight,
            )
            tournament_tier = tournament_tiers.get(event)
            tournament_tier_weight = _tournament_tier_weight(tournament_tier)
            tmeta = tournament_meta.get(event, {})
            t_player_count = tmeta.get("player_count", 0) or tmeta.get("active_player_count", 0)
            size_score = _tournament_size_score(t_player_count)
            field_score = _field_strength_score(tournament_tier, t_player_count)
            # event_weight = prestige * stage * size * field_strength, clamped [0.70, 1.60]
            raw_event_weight = tournament_tier_weight * size_score * field_score
            event_tier_weight = stage_tier_weight * max(0.70, min(1.60, raw_event_weight))
            event_name = f"{event} - {stage}"

            events.append(
                EventInput(
                    username=username,
                    event_name=event_name,
                    event_date=event_date,
                    days_since_event=days_since_event,
                    impact_score=impact_score,
                    match_cost=match_cost,
                    win_rate=win_rate,
                    placement_percentile=placement_percentile,
                    strength_of_schedule=strength_of_schedule,
                    event_tier_weight=event_tier_weight,
                    metadata={
                        "event": event,
                        "stage": stage,
                        "stage_tier_weight": stage_tier_weight,
                        "impact_score_inputs": {
                            "map_performance": match_cost,
                            "map_wins": win_rate,
                            "placement_result": placement_percentile,
                            "placement_result_source": placement_result_source,
                            "stage_importance": _normalize_stage_importance(stage_tier_weight),
                            "opponent_strength": strength_of_schedule,
                        },
                        "tournament_tier": tournament_tier,
                        "tournament_tier_weight": tournament_tier_weight,
                        "tournament_size_score": round(size_score, 3),
                        "field_strength_score": round(field_score, 3),
                        "tournament_player_count": t_player_count,
                        "team_code": team_code,
                        "raw_pscore": bucket.get("pscore"),
                        "performance_ratio": bucket.get("performance_ratio"),
                        "map_total": map_total,
                        "map_wins": map_wins,
                    },
                )
            )

            if days_since_event is not None and days_since_event <= 365:
                player_event_names_last_12m.setdefault(username, set()).add(event)
                current_min = player_min_days.get(username)
                player_min_days[username] = (
                    days_since_event
                    if current_min is None
                    else min(current_min, days_since_event)
                )

        # Pre-load user_id and country_code from tournament_players so that
        # the leaderboard export has working avatars and country flags even
        # when osu\! profile enrichment is skipped.
        _player_identity: dict[str, dict[str, Any]] = {}
        for tp_row in connection.execute(
            """
            SELECT player, user_id, country_code, team_code
            FROM tournament_players
            WHERE user_id IS NOT NULL
            ORDER BY rowid DESC
            """
        ).fetchall():
            tp_name = _clean_text(tp_row["player"])
            if tp_name is None:
                continue
            canonical = _canonical_player_name(tp_name, alias_map)
            if canonical is None:
                continue
            key = canonical.casefold()
            team_code = _clean_text(tp_row["team_code"])
            country_code = _clean_text(tp_row["country_code"])
            if country_code is None and team_code and len(team_code) == 2 and team_code.isalpha():
                country_code = team_code.upper()
            if key not in _player_identity:
                _player_identity[key] = {
                    "user_id": tp_row["user_id"],
                    "country_code": country_code,
                }
            else:
                # Fill in country_code if missing from a previous row
                if _player_identity[key].get("country_code") is None:
                    cc = country_code
                    if cc:
                        _player_identity[key]["country_code"] = cc

        players: list[PlayerInput] = []
        seen_players: set[str] = set()
        for event_row in events:
            username = event_row.username
            if username.casefold() in seen_players:
                continue
            seen_players.add(username.casefold())
            identity = _player_identity.get(username.casefold(), {})
            players.append(
                PlayerInput(
                    username=username,
                    user_id=identity.get("user_id"),
                    country_code=identity.get("country_code"),
                    elitebotix_rating=None,
                    skill_issue_rating=None,
                    bancho_rank=None,
                    lazer_rank=None,
                    tournaments_played_last_12m=len(player_event_names_last_12m.get(username, set())),
                    days_since_last_event=player_min_days.get(username),
                    metadata={
                        "source": "sqlite_pipeline",
                    },
                )
            )

        players.sort(key=lambda player: player.username.casefold())
        events.sort(key=lambda event: (event.username.casefold(), event.event_name or ""))
        if enrich_osu_profiles:
            players, events, _ = enrich_players_with_osu_profiles(
                players,
                events,
                cache_ttl_hours=profile_cache_ttl_hours,
                db_path=db_path,
            )
        return players, events
    finally:
        connection.close()


def player_inputs_to_rows(players: Iterable[PlayerInput]) -> list[dict[str, Any]]:
    return [asdict(player) for player in players]


def event_inputs_to_rows(events: Iterable[EventInput]) -> list[dict[str, Any]]:
    return [asdict(event) for event in events]
