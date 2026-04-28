from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Iterable

from config import DEFAULT_CONFIG, PowerRankingConfig, RatingRange
from models import EventInput, PlayerInput, PlayerRankingResult


def clamp(value: float | None, minimum: float = 0.0, maximum: float = 100.0) -> float | None:
    if value is None:
        return None
    return max(minimum, min(maximum, float(value)))


def safe_weighted_average(values: dict[str, float | None], weights: dict[str, float]) -> float | None:
    weighted_sum = 0.0
    total_weight = 0.0
    for key, weight in weights.items():
        value = values.get(key)
        if value is None or weight <= 0:
            continue
        weighted_sum += float(value) * float(weight)
        total_weight += float(weight)
    if total_weight <= 0:
        return None
    return weighted_sum / total_weight


def _with_default(value: float | None, default: float | None) -> float | None:
    if value is not None:
        return value
    return default


def normalize_log_rank(rank: int | None, rank_cutoff: int) -> float | None:
    if rank is None or rank <= 0 or rank_cutoff <= 1:
        return None
    if rank <= 1:
        return 100.0
    raw_score = 100.0 * (1.0 - (math.log(rank) / math.log(rank_cutoff)))
    return clamp(raw_score)


def normalize_linear_rating(value: float | None, min_value: float | None, max_value: float | None) -> float | None:
    if value is None:
        return None
    if min_value is None or max_value is None:
        return None
    if max_value == min_value:
        return 50.0
    raw_score = 100.0 * ((float(value) - float(min_value)) / (float(max_value) - float(min_value)))
    return clamp(raw_score)


def _parse_reference_date(reference_date: date | str | None) -> date:
    if isinstance(reference_date, date):
        return reference_date
    if isinstance(reference_date, str) and reference_date.strip():
        return datetime.fromisoformat(reference_date.strip()).date()
    return date.today()


def _resolve_days_since_event(event: EventInput, reference_date: date) -> float | None:
    if event.days_since_event is not None:
        return max(0.0, float(event.days_since_event))
    if event.event_date:
        event_day = datetime.fromisoformat(event.event_date).date()
        return max(0.0, float((reference_date - event_day).days))
    return None


def _derive_range(
    players: Iterable[PlayerInput],
    attribute_name: str,
    configured_range: RatingRange,
) -> tuple[float | None, float | None]:
    values = [
        float(getattr(player, attribute_name))
        for player in players
        if getattr(player, attribute_name) is not None
    ]
    dynamic_min = min(values) if values else None
    dynamic_max = max(values) if values else None
    min_value = configured_range.min_value if configured_range.min_value is not None else dynamic_min
    max_value = configured_range.max_value if configured_range.max_value is not None else dynamic_max
    return min_value, max_value


def build_rating_bounds(
    players: Iterable[PlayerInput],
    config: PowerRankingConfig = DEFAULT_CONFIG,
) -> dict[str, tuple[float | None, float | None]]:
    players = list(players)
    return {
        "elitebotix_rating": _derive_range(
            players,
            "elitebotix_rating",
            config.rating_ranges["elitebotix_rating"],
        ),
        "skill_issue_rating": _derive_range(
            players,
            "skill_issue_rating",
            config.rating_ranges["skill_issue_rating"],
        ),
    }


def score_event(
    event: EventInput,
    *,
    config: PowerRankingConfig = DEFAULT_CONFIG,
    reference_date: date | str | None = None,
) -> dict[str, Any] | None:
    reference_day = _parse_reference_date(reference_date)
    days_since_event = _resolve_days_since_event(event, reference_day)
    if days_since_event is None:
        days_since_event = 0.0

    component_scores = {
        "impact_score": clamp(event.impact_score),
        "match_cost": clamp(event.match_cost),
        "win_rate": clamp(event.win_rate),
        "placement_percentile": clamp(event.placement_percentile),
        "strength_of_schedule": clamp(event.strength_of_schedule),
    }
    base_performance = safe_weighted_average(
        component_scores,
        config.event_component_weights(),
    )
    if base_performance is None:
        return None

    tier_weight = max(0.0, float(event.event_tier_weight or 0.0))
    recency_weight = math.exp(-days_since_event / config.event_recency_decay_days)
    combined_weight = tier_weight * recency_weight
    event_score = combined_weight * base_performance

    return {
        "event_name": event.event_name or f"{event.username}-event",
        "days_since_event": round(days_since_event, 2),
        "recency_weight": round(recency_weight, 4),
        "event_tier_weight": round(tier_weight, 4),
        "combined_weight": round(combined_weight, 4),
        "base_performance": round(base_performance, 2),
        "event_score": round(event_score, 2),
        "components": component_scores,
    }


def _event_tournament_key(event_name: str | None) -> str:
    text = event_name or ""
    if " - " in text:
        return text.split(" - ", 1)[0]
    return text or "unknown"


def _compute_tournament_level_consistency(
    scored_events: list[dict[str, Any]],
    *,
    config: PowerRankingConfig,
) -> float:
    tournament_buckets: dict[str, tuple[float, float]] = {}
    for scored in scored_events:
        tournament = _event_tournament_key(scored.get("event_name"))
        performance = float(scored["base_performance"])
        weight = max(0.0, float(scored.get("combined_weight") or 0.0))
        if weight <= 0:
            weight = 1.0
        total, total_weight = tournament_buckets.get(tournament, (0.0, 0.0))
        tournament_buckets[tournament] = (
            total + performance * weight,
            total_weight + weight,
        )

    tournament_performances = [
        total / total_weight
        for total, total_weight in tournament_buckets.values()
        if total_weight > 0
    ]
    if len(tournament_performances) >= config.minimum_consistency_events:
        performance_stdev = statistics.pstdev(tournament_performances)
        return clamp(100.0 - (performance_stdev * config.consistency_penalty_scale)) or 0.0
    if len(tournament_performances) == 1:
        return config.default_consistency_score
    return 0.0


def aggregate_recent_tournament_form(
    events: Iterable[EventInput],
    *,
    config: PowerRankingConfig = DEFAULT_CONFIG,
    reference_date: date | str | None = None,
) -> tuple[float, float, list[dict[str, Any]]]:
    scored_events: list[dict[str, Any]] = []
    weighted_score_sum = 0.0
    total_weight = 0.0

    for event in events:
        scored = score_event(event, config=config, reference_date=reference_date)
        if scored is None:
            continue
        scored_events.append(scored)
        weighted_score_sum += float(scored["event_score"])
        total_weight += float(scored["combined_weight"])

    if total_weight > 0:
        recent_form = clamp(weighted_score_sum / total_weight) or 0.0
    else:
        recent_form = 0.0

    consistency_score = _compute_tournament_level_consistency(
        scored_events,
        config=config,
    )

    return round(recent_form, 2), round(consistency_score, 2), scored_events


def compute_reliability_multiplier(
    tournaments_played_last_12m: int,
    *,
    config: PowerRankingConfig = DEFAULT_CONFIG,
) -> float:
    progress = min(1.0, max(0.0, tournaments_played_last_12m / config.reliability_target_tournaments))
    return round(config.reliability_base + (config.reliability_bonus * progress), 4)


def compute_activity_multiplier(
    days_since_last_event: float | None,
    *,
    config: PowerRankingConfig = DEFAULT_CONFIG,
) -> float:
    if days_since_last_event is None:
        return round(config.activity_base, 4)
    days = max(0.0, float(days_since_last_event))
    return round(config.activity_base + (config.activity_bonus * math.exp(-days / config.activity_decay_days)), 4)


def _derive_tournaments_played(events: Iterable[EventInput], reference_date: date) -> int:
    count = 0
    for event in events:
        days_since_event = _resolve_days_since_event(event, reference_date)
        if days_since_event is None or days_since_event > 365:
            continue
        count += 1
    return count


def _derive_days_since_last_event(events: Iterable[EventInput], reference_date: date) -> float | None:
    values = [
        _resolve_days_since_event(event, reference_date)
        for event in events
    ]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return min(values)


def _build_explanation(
    username: str,
    component_scores: dict[str, float | None],
    reliability_multiplier: float,
    activity_multiplier: float,
    final_power_score: float,
    provisional: bool,
) -> str:
    ranked_components = sorted(
        (
            (name, score)
            for name, score in component_scores.items()
            if score is not None
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    top_parts = ", ".join(
        f"{name.replace('_', ' ')} {score:.1f}"
        for name, score in ranked_components[:3]
    ) or "no major components"
    provisional_text = " provisional" if provisional else ""
    return (
        f"{username} scores {final_power_score:.2f} with {top_parts}; "
        f"reliability {reliability_multiplier:.3f}, activity {activity_multiplier:.3f},{provisional_text}".strip(",")
    )


def compute_player_ranking(
    player: PlayerInput,
    player_events: Iterable[EventInput],
    *,
    rating_bounds: dict[str, tuple[float | None, float | None]],
    config: PowerRankingConfig = DEFAULT_CONFIG,
    reference_date: date | str | None = None,
) -> PlayerRankingResult:
    reference_day = _parse_reference_date(reference_date)
    events = list(player_events)

    elitebotix_score = normalize_linear_rating(
        player.elitebotix_rating,
        *rating_bounds["elitebotix_rating"],
    )
    skill_issue_score = normalize_linear_rating(
        player.skill_issue_rating,
        *rating_bounds["skill_issue_rating"],
    )
    bancho_score = normalize_log_rank(player.bancho_rank, config.bancho_rank_cutoff)
    lazer_score = normalize_log_rank(player.lazer_rank, config.lazer_rank_cutoff)

    recent_form, computed_consistency, event_debug = aggregate_recent_tournament_form(
        events,
        config=config,
        reference_date=reference_day,
    )
    consistency_score = clamp(player.consistency_metric)
    if consistency_score is None:
        consistency_score = computed_consistency

    tournaments_played_last_12m = (
        player.tournaments_played_last_12m
        if player.tournaments_played_last_12m is not None
        else _derive_tournaments_played(events, reference_day)
    )
    days_since_last_event = (
        player.days_since_last_event
        if player.days_since_last_event is not None
        else _derive_days_since_last_event(events, reference_day)
    )

    component_scores = {
        "elitebotix_score": elitebotix_score,
        "skill_issue_score": skill_issue_score,
        "bancho_score": bancho_score,
        "lazer_score": lazer_score,
        "recent_tournament_form": recent_form,
        "consistency_score": consistency_score,
    }
    weighted_component_scores = {
        "elitebotix_score": _with_default(elitebotix_score, config.missing_elitebotix_score_default),
        "skill_issue_score": _with_default(skill_issue_score, config.missing_skill_issue_score_default),
        "bancho_score": _with_default(bancho_score, config.missing_bancho_score_default),
        "lazer_score": _with_default(lazer_score, config.missing_lazer_score_default),
        "recent_tournament_form": recent_form,
        "consistency_score": consistency_score,
    }
    base_power_score = safe_weighted_average(weighted_component_scores, config.component_weights()) or 0.0
    reliability_multiplier = compute_reliability_multiplier(
        tournaments_played_last_12m,
        config=config,
    )
    activity_multiplier = compute_activity_multiplier(
        days_since_last_event,
        config=config,
    )
    final_power_score = round(base_power_score * reliability_multiplier * activity_multiplier, 2)
    provisional = tournaments_played_last_12m < config.provisional_tournament_threshold

    debug = {
        "raw_inputs": {
            "elitebotix_rating": player.elitebotix_rating,
            "skill_issue_rating": player.skill_issue_rating,
            "bancho_rank": player.bancho_rank,
            "lazer_rank": player.lazer_rank,
            "tournaments_played_last_12m": tournaments_played_last_12m,
            "days_since_last_event": days_since_last_event,
        },
        "rating_bounds": rating_bounds,
        "event_breakdown": event_debug,
        "skillset_subscores": player.skillset_subscores,
        "weighted_component_scores": weighted_component_scores,
    }

    return PlayerRankingResult(
        username=player.username,
        elitebotix_score=round(elitebotix_score, 2) if elitebotix_score is not None else None,
        skill_issue_score=round(skill_issue_score, 2) if skill_issue_score is not None else None,
        bancho_score=round(bancho_score, 2) if bancho_score is not None else None,
        lazer_score=round(lazer_score, 2) if lazer_score is not None else None,
        recent_tournament_form=round(recent_form, 2),
        consistency_score=round(consistency_score, 2),
        reliability_multiplier=reliability_multiplier,
        activity_multiplier=activity_multiplier,
        final_power_score=final_power_score,
        provisional=provisional,
        base_power_score=round(base_power_score, 2),
        tournaments_played_last_12m=tournaments_played_last_12m,
        days_since_last_event=round(days_since_last_event, 2) if days_since_last_event is not None else None,
        explanation=_build_explanation(
            player.username,
            component_scores,
            reliability_multiplier,
            activity_multiplier,
            final_power_score,
            provisional,
        ),
        debug=debug,
    )


def rank_players(
    players: Iterable[PlayerInput],
    events: Iterable[EventInput],
    *,
    config: PowerRankingConfig = DEFAULT_CONFIG,
    reference_date: date | str | None = None,
) -> list[PlayerRankingResult]:
    players = list(players)
    grouped_events: dict[str, list[EventInput]] = defaultdict(list)
    for event in events:
        grouped_events[event.username.casefold()].append(event)

    rating_bounds = build_rating_bounds(players, config=config)
    results = [
        compute_player_ranking(
            player,
            grouped_events.get(player.username.casefold(), []),
            rating_bounds=rating_bounds,
            config=config,
            reference_date=reference_date,
        )
        for player in players
    ]
    return sorted(
        results,
        key=lambda result: (
            result.final_power_score,
            result.base_power_score,
            result.username.casefold(),
        ),
        reverse=True,
    )
