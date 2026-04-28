from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any

from config import DEFAULT_CONFIG, PowerRankingConfig
from models import EventInput, PlayerInput, PlayerRankingResult
from osu_profile_enrichment import DEFAULT_PROFILE_CACHE_TTL_HOURS
from ranking_pipeline import (
    build_power_ranking_inputs_from_db,
    event_inputs_to_rows,
    player_inputs_to_rows,
)
from scoring import rank_players


def _load_rows(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path)
    if source.suffix.lower() == ".json":
        payload = json.loads(source.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("players", "events", "rows"):
                if isinstance(payload.get(key), list):
                    return payload[key]
        raise ValueError(f"Unsupported JSON structure in {source}")

    if source.suffix.lower() == ".csv":
        with source.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    raise ValueError(f"Unsupported file format: {source}")


def load_players(path: str | Path) -> list[PlayerInput]:
    return [PlayerInput.from_dict(row) for row in _load_rows(path)]


def load_events(path: str | Path) -> list[EventInput]:
    return [EventInput.from_dict(row) for row in _load_rows(path)]


def _format_value(value: Any, *, decimals: int = 2) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return f"{value:.{decimals}f}"
    return str(value)


def _render_table(results: list[PlayerRankingResult]) -> str:
    headers = [
        ("#", 4),
        ("username", 18),
        ("final", 8),
        ("EB", 8),
        ("SI", 8),
        ("bancho", 8),
        ("lazer", 8),
        ("form", 8),
        ("cons", 8),
        ("rel", 7),
        ("act", 7),
        ("prov", 6),
    ]
    header_line = " ".join(label.ljust(width) for label, width in headers)
    separator = "-" * len(header_line)
    lines = [header_line, separator]
    for index, result in enumerate(results, start=1):
        row = [
            str(index),
            result.username,
            _format_value(result.final_power_score),
            _format_value(result.elitebotix_score),
            _format_value(result.skill_issue_score),
            _format_value(result.bancho_score),
            _format_value(result.lazer_score),
            _format_value(result.recent_tournament_form),
            _format_value(result.consistency_score),
            _format_value(result.reliability_multiplier, decimals=3),
            _format_value(result.activity_multiplier, decimals=3),
            "yes" if result.provisional else "no",
        ]
        lines.append(
            " ".join(
                str(value)[:width].ljust(width)
                for value, (_, width) in zip(row, headers)
            )
        )
    return "\n".join(lines)


def _results_to_output(
    results: list[PlayerRankingResult],
    *,
    include_debug: bool = False,
) -> list[dict[str, Any]]:
    return [result.to_output_dict(include_debug=include_debug) for result in results]


def _leaderboard_tier(rank: int, total_players: int) -> str:
    if total_players <= 0:
        return "Tier 3"
    tier_1_cutoff = max(1, math.ceil(total_players * 0.05))
    tier_2_cutoff = max(tier_1_cutoff, math.ceil(total_players * 0.20))
    if rank <= tier_1_cutoff:
        return "Tier 1"
    if rank <= tier_2_cutoff:
        return "Tier 2"
    return "Tier 3"


def _activity_status(days_since_last_event: float | None) -> str:
    if days_since_last_event is None:
        return "unknown"
    if days_since_last_event <= 60:
        return "active"
    if days_since_last_event <= 180:
        return "recent"
    if days_since_last_event <= 365:
        return "inactive"
    return "dormant"


def _event_sort_key(event: EventInput) -> tuple[float, str]:
    if event.days_since_event is None:
        return (float("inf"), event.event_name or "")
    return (float(event.days_since_event), event.event_name or "")


TEAM_SIZE_SPECIFIC_CUP_EVENTS = {"FDC 2025", "4WC 2025", "3WC 2025"}


def _event_base_name(event: EventInput) -> str:
    event_name = event.metadata.get("event") or event.event_name or ""
    if " - " in event_name:
        return event_name.split(" - ", 1)[0]
    return event_name


def _event_performance_value(event: EventInput) -> float:
    return (
        0.30 * (event.impact_score or 0.0)
        + 0.25 * (event.win_rate or 0.0)
        + 0.25 * (event.placement_percentile or 0.0)
        + 0.15 * (event.strength_of_schedule or 0.0)
        + 0.05 * (event.match_cost or 0.0)
    )


def _event_contribution(event: EventInput) -> float:
    days_since_event = event.days_since_event
    recency_weight = math.exp(-(days_since_event or 0.0) / 120.0) if days_since_event is not None else 0.0
    return max(0.0, (event.event_tier_weight or 0.0) * recency_weight * _event_performance_value(event))


def _load_previous_ranks(path: str | Path | None) -> dict[str, int]:
    if path is None:
        return {}
    source = Path(path)
    if not source.exists():
        return {}
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, list):
        return {}
    ranks: dict[str, int] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        username = row.get("username")
        rank = row.get("rank")
        if username is None or rank is None:
            continue
        try:
            ranks[str(username).casefold()] = int(rank)
        except (TypeError, ValueError):
            continue
    return ranks


def _confidence_payload(
    *,
    result: PlayerRankingResult,
    rank: int,
    player_events: list[EventInput],
    previous_ranks: dict[str, int],
) -> dict[str, Any]:
    contributions_by_event: dict[str, float] = defaultdict(float)
    total_contribution = 0.0
    for event in player_events:
        contribution = _event_contribution(event)
        if contribution <= 0:
            continue
        event_name = _event_base_name(event)
        contributions_by_event[event_name] += contribution
        total_contribution += contribution

    unique_tournaments = {name for name in contributions_by_event if name}
    dominant_event = None
    dominant_share = 0.0
    if contributions_by_event and total_contribution > 0:
        dominant_event, dominant_value = max(contributions_by_event.items(), key=lambda item: item[1])
        dominant_share = dominant_value / total_contribution

    team_wc_contribution = sum(
        value for event_name, value in contributions_by_event.items()
        if event_name in TEAM_SIZE_SPECIFIC_CUP_EVENTS
    )
    team_wc_share = team_wc_contribution / total_contribution if total_contribution > 0 else 0.0

    previous_rank = previous_ranks.get(result.username.casefold())
    rank_jump = previous_rank - rank if previous_rank is not None else None

    warning_flags: list[str] = []
    unique_count = len(unique_tournaments)
    if unique_count < 3:
        warning_flags.append("low_sample")
    if dominant_share > 0.70 or unique_count == 1:
        warning_flags.append("one_event")
    if team_wc_share > 0.50:
        warning_flags.append("team_wc_heavy")
    if rank_jump is not None and rank_jump > 50:
        warning_flags.append("unstable")
    if (
        (result.final_power_score or 0.0) >= 50.0
        and (unique_count < 3 or (result.consistency_score or 0.0) <= 35.0)
    ):
        warning_flags.append("needs_formula_review")
    if "unstable" in warning_flags or "one_event" in warning_flags or "team_wc_heavy" in warning_flags:
        warning_flags.append("needs_formula_review")

    deduped_flags = list(dict.fromkeys(warning_flags))
    confidence_label = "low" if deduped_flags else "high"
    if confidence_label == "high" and unique_count == 3:
        confidence_label = "medium"

    return {
        "unique_tournaments_count": unique_count,
        "dominant_event": dominant_event,
        "dominant_event_score_share": round(dominant_share, 4),
        "team_world_cup_score_share": round(team_wc_share, 4),
        "previous_rank": previous_rank,
        "rank_jump": rank_jump,
        "confidence_label": confidence_label,
        "warning_flags": deduped_flags,
    }


def _top_recent_events(events: list[EventInput], *, limit: int = 5) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for event in sorted(events, key=_event_sort_key)[:limit]:
        rows.append(
            {
                "event_name": event.event_name,
                "event": event.metadata.get("event"),
                "stage": event.metadata.get("stage"),
                "event_date": event.event_date,
                "days_since_event": event.days_since_event,
                "match_cost": event.match_cost,
                "impact_score": event.impact_score,
                "win_rate": event.win_rate,
                "placement_percentile": event.placement_percentile,
                "strength_of_schedule": event.strength_of_schedule,
                "event_tier_weight": event.event_tier_weight,
                "map_total": event.metadata.get("map_total"),
                "map_wins": event.metadata.get("map_wins"),
            }
        )
    return rows


def _build_leaderboard_output(
    results: list[PlayerRankingResult],
    *,
    players: list[PlayerInput],
    events: list[EventInput],
    aliases_by_player: dict[str, list[str]] | None = None,
    previous_ranks: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    aliases_by_player = aliases_by_player or {}
    previous_ranks = previous_ranks or {}
    players_by_name = {player.username.casefold(): player for player in players}
    events_by_name: dict[str, list[EventInput]] = defaultdict(list)
    for event in events:
        events_by_name[event.username.casefold()].append(event)

    total_players = len(results)
    rows: list[dict[str, Any]] = []
    for rank, result in enumerate(results, start=1):
        player = players_by_name.get(result.username.casefold())
        country = player.country_code if player else None
        player_events = events_by_name.get(result.username.casefold(), [])
        confidence = _confidence_payload(
            result=result,
            rank=rank,
            player_events=player_events,
            previous_ranks=previous_ranks,
        )
        rows.append(
            {
                "rank": rank,
                "username": result.username,
                "profile_username": player.profile_username if player else result.username,
                "aliases": aliases_by_player.get(result.username.casefold(), []),
                "user_id": player.user_id if player else None,
                "country": country,
                "country_rank": player.country_rank if player else None,
                "bancho_rank": player.bancho_rank if player else None,
                "pp": player.pp if player else None,
                "tier": _leaderboard_tier(rank, total_players),
                "tier_percentile": round(100.0 * rank / total_players, 2) if total_players else None,
                "final_power_score": result.final_power_score,
                "recent_tournament_form": result.recent_tournament_form,
                "consistency_score": result.consistency_score,
                "reliability_multiplier": result.reliability_multiplier,
                "activity_multiplier": result.activity_multiplier,
                "tournaments_played_last_12m": result.tournaments_played_last_12m,
                **confidence,
                "provisional": result.provisional,
                "provisional_basis": "fewer_than_3_unique_tournaments_last_12m",
                "activity_status": _activity_status(result.days_since_last_event),
                "days_since_last_event": result.days_since_last_event,
                "top_recent_events": _top_recent_events(events_by_name.get(result.username.casefold(), [])),
                "explanation": result.explanation,
            }
        )
    return rows


def _load_aliases_by_player(db_path: str | Path | None) -> dict[str, list[str]]:
    if db_path is None:
        return {}
    aliases: dict[str, set[str]] = defaultdict(set)
    try:
        connection = sqlite3.connect(db_path)
        rows = connection.execute(
            """
            SELECT alias, canonical_name
            FROM player_aliases
            WHERE alias IS NOT NULL
              AND canonical_name IS NOT NULL
            """
        ).fetchall()
    except sqlite3.Error:
        return {}
    finally:
        try:
            connection.close()
        except UnboundLocalError:
            pass

    for alias, canonical_name in rows:
        alias_text = str(alias).strip()
        canonical_text = str(canonical_name).strip()
        if not alias_text or not canonical_text:
            continue
        if alias_text.casefold() == canonical_text.casefold():
            continue
        aliases[canonical_text.casefold()].add(alias_text)
    return {key: sorted(values, key=str.casefold) for key, values in aliases.items()}


def _write_csv(path: str | Path, rows: list[dict[str, Any]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return
    flattened_rows = []
    for row in rows:
        normalized = dict(row)
        normalized["debug"] = json.dumps(normalized.get("debug", {}), ensure_ascii=True)
        flattened_rows.append(normalized)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(flattened_rows[0].keys()))
        writer.writeheader()
        writer.writerows(flattened_rows)


def _write_json(path: str | Path, payload: Any) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_config(args: argparse.Namespace) -> PowerRankingConfig:
    return replace(
        DEFAULT_CONFIG,
        elitebotix_weight=args.elitebotix_weight,
        skill_issue_weight=args.skill_issue_weight,
        bancho_weight=args.bancho_weight,
        lazer_weight=args.lazer_weight,
        recent_form_weight=args.recent_form_weight,
        consistency_weight=args.consistency_weight,
        consistency_penalty_scale=args.consistency_penalty_scale,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build osu! tournament power rankings.")
    parser.add_argument("--players", default="example_players.json", help="Path to players JSON/CSV.")
    parser.add_argument("--events", default="example_events.json", help="Path to events JSON/CSV.")
    parser.add_argument("--from-db", action="store_true", help="Build player/event inputs from SQLite instead of JSON/CSV files.")
    parser.add_argument("--db-path", default="data/osu_scout.db", help="SQLite DB path for --from-db mode.")
    parser.add_argument("--event-filter", action="append", default=None, help="Repeatable event filter for --from-db mode, e.g. --event-filter \"OWC 2025\".")
    parser.add_argument("--include-undated-stages", action="store_true", help="Keep stage rows even when no match-detail timestamp exists.")
    parser.add_argument("--skip-osu-enrichment", action="store_true", help="Disable Bancho profile enrichment in --from-db mode.")
    parser.add_argument("--profile-cache-ttl-hours", type=float, default=DEFAULT_PROFILE_CACHE_TTL_HOURS, help="Maximum cache age in hours for osu! profile enrichment.")
    parser.add_argument("--players-out", default=None, help="Optional JSON export path for generated player inputs.")
    parser.add_argument("--events-out", default=None, help="Optional JSON export path for generated event inputs.")
    parser.add_argument("--reference-date", default=None, help="ISO date used for recency calculations.")
    parser.add_argument("--format", choices=("table", "json"), default="table", help="stdout output format.")
    parser.add_argument("--csv-out", default=None, help="Optional CSV export path.")
    parser.add_argument("--leaderboard-out", default=None, help="Optional website-ready leaderboard JSON export path.")
    parser.add_argument("--previous-leaderboard", default=None, help="Optional previous leaderboard JSON used only for movement/confidence flags.")
    parser.add_argument("--debug", action="store_true", help="Include debug payloads in JSON/CSV output.")

    parser.add_argument("--elitebotix-weight", type=float, default=DEFAULT_CONFIG.elitebotix_weight)
    parser.add_argument("--skill-issue-weight", type=float, default=DEFAULT_CONFIG.skill_issue_weight)
    parser.add_argument("--bancho-weight", type=float, default=DEFAULT_CONFIG.bancho_weight)
    parser.add_argument("--lazer-weight", type=float, default=DEFAULT_CONFIG.lazer_weight)
    parser.add_argument("--recent-form-weight", type=float, default=DEFAULT_CONFIG.recent_form_weight)
    parser.add_argument("--consistency-weight", type=float, default=DEFAULT_CONFIG.consistency_weight)
    parser.add_argument(
        "--consistency-penalty-scale",
        type=float,
        default=DEFAULT_CONFIG.consistency_penalty_scale,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = _build_config(args)
    if args.from_db:
        players, events = build_power_ranking_inputs_from_db(
            db_path=args.db_path,
            event_filters=args.event_filter,
            reference_date=args.reference_date,
            include_undated_stages=args.include_undated_stages,
            enrich_osu_profiles=not args.skip_osu_enrichment,
            profile_cache_ttl_hours=args.profile_cache_ttl_hours,
        )
    else:
        players = load_players(args.players)
        events = load_events(args.events)

    if args.players_out:
        _write_json(args.players_out, player_inputs_to_rows(players))
    if args.events_out:
        _write_json(args.events_out, event_inputs_to_rows(events))

    results = rank_players(
        players,
        events,
        config=config,
        reference_date=args.reference_date,
    )

    if args.format == "json":
        print(json.dumps(_results_to_output(results, include_debug=args.debug), indent=2))
    else:
        print(_render_table(results))
        if args.debug:
            print()
            for result in results:
                print(f"- {result.username}: {result.explanation}")

    if args.csv_out:
        _write_csv(
            args.csv_out,
            _results_to_output(results, include_debug=args.debug),
        )
    if args.leaderboard_out:
        _write_json(
            args.leaderboard_out,
            _build_leaderboard_output(
                results,
                players=players,
                events=events,
                aliases_by_player=_load_aliases_by_player(args.db_path if args.from_db else None),
                previous_ranks=_load_previous_ranks(args.previous_leaderboard),
            ),
        )


if __name__ == "__main__":
    main()
