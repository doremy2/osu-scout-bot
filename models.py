"""Normalized domain models for osu! scout.

Top section: canonical data models for the new ingestion pipeline.
Bottom section: legacy ranking/power-score models (kept for backward compat).

Hierarchy (new):
    Player
    Tournament
      └── TournamentEntry (player × tournament placement)
    Match (one BO-N series between two players/teams)
      └── Game (one beatmap play inside a match)
          └── PlayerGameScore (one player's line on one game)
    RatingSnapshot (external rating at a point in time)
    SourceLink (provenance for any record)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


# =====================================================================
#  NEW normalized models — ingestion pipeline
# =====================================================================

@dataclass
class Player:
    """Canonical player identity."""
    user_id: int | None = None
    username: str | None = None
    aliases: list[str] = field(default_factory=list)
    country_code: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass
class Tournament:
    """A tournament / event (OWC 2025, ROMAI #47, etc.)."""
    tournament_id: str | None = None       # slug or external id
    name: str = ""
    abbreviation: str | None = None
    year: int | None = None
    format: str | None = None              # "4v4", "1v1"
    team_size: int | None = None
    tier: str | None = None                # "premier", "major", "minor", "community"
    start_date: str | None = None
    end_date: str | None = None
    source: str | None = None
    source_url: str | None = None


@dataclass
class TournamentEntry:
    """One player/team's participation in a tournament."""
    tournament_id: str | None = None
    player_id: int | None = None
    team_name: str | None = None
    team_code: str | None = None
    seed: int | None = None
    placement: int | None = None
    placement_text: str | None = None
    source: str | None = None


@dataclass
class Match:
    """One BO-N series between two players/teams."""
    match_id: int | None = None
    tournament_id: str | None = None
    event: str | None = None
    stage: str | None = None
    match_name: str | None = None
    team_a: str | None = None
    team_b: str | None = None
    score_a: int | None = None
    score_b: int | None = None
    result: str | None = None              # "team_a" / "team_b" / "draw"
    match_link: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    source: str | None = None
    source_url: str | None = None


@dataclass
class Game:
    """One beatmap played inside a match."""
    game_id: int | None = None
    match_id: int | None = None
    beatmap_id: int | None = None
    beatmap_title: str | None = None
    beatmap_version: str | None = None
    star_rating: float | None = None
    slot: str | None = None                # mappool slot: "NM1", "HD2"
    mods: list[str] = field(default_factory=list)
    mode: str | None = None
    scoring_type: str | None = None
    team_type: str | None = None
    winning_team: str | None = None
    red_total: int | None = None
    blue_total: int | None = None
    start_time: str | None = None
    end_time: str | None = None


@dataclass
class PlayerGameScore:
    """One player's performance on one game/map."""
    match_id: int | None = None
    game_id: int | None = None
    user_id: int | None = None
    username: str | None = None
    score: int | None = None
    accuracy: float | None = None
    max_combo: int | None = None
    count_300: int | None = None
    count_100: int | None = None
    count_50: int | None = None
    count_miss: int | None = None
    mods: list[str] = field(default_factory=list)
    team: str | None = None
    team_code: str | None = None
    passed: bool = True
    slot: int | None = None


@dataclass
class RatingSnapshot:
    """A point-in-time rating from an external system."""
    user_id: int | None = None
    username: str | None = None
    source: str = ""
    rating_type: str = ""
    value: float | None = None
    display_value: str | None = None
    rank: int | None = None
    peak_value: float | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    fetched_at: str | None = None


@dataclass
class SourceLink:
    """Provenance record: where a piece of data came from."""
    record_type: str = ""
    record_id: str | None = None
    source: str = ""
    source_id: str | None = None
    source_url: str | None = None
    imported_at: str | None = None
    updated_at: str | None = None


# =====================================================================
#  LEGACY ranking / power-score models (kept for backward compat)
# =====================================================================


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


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    return int(float(text))


@dataclass(slots=True)
class PlayerInput:
    username: str
    user_id: int | None = None
    profile_username: str | None = None
    pp: float | None = None
    country_rank: int | None = None
    country_code: str | None = None
    elitebotix_rating: float | None = None
    skill_issue_rating: float | None = None
    bancho_rank: int | None = None
    lazer_rank: int | None = None
    tournaments_played_last_12m: int | None = None
    days_since_last_event: float | None = None
    consistency_metric: float | None = None
    skillset_subscores: dict[str, float] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "PlayerInput":
        reserved = {
            "username",
            "user_id",
            "profile_username",
            "pp",
            "country_rank",
            "country_code",
            "elitebotix_rating",
            "skill_issue_rating",
            "bancho_rank",
            "lazer_rank",
            "tournaments_played_last_12m",
            "days_since_last_event",
            "consistency_metric",
            "skillset_subscores",
        }
        username = _clean_text(row.get("username"))
        if username is None:
            raise ValueError(f"Player row is missing username: {row}")

        raw_skillsets = row.get("skillset_subscores") or {}
        skillset_subscores: dict[str, float] = {}
        if isinstance(raw_skillsets, dict):
            for key, value in raw_skillsets.items():
                numeric = _to_float(value)
                if numeric is None:
                    continue
                skillset_subscores[str(key)] = numeric

        metadata = {
            key: value
            for key, value in row.items()
            if key not in reserved
        }

        return cls(
            username=username,
            user_id=_to_int(row.get("user_id")),
            profile_username=_clean_text(row.get("profile_username")),
            pp=_to_float(row.get("pp")),
            country_rank=_to_int(row.get("country_rank")),
            country_code=_clean_text(row.get("country_code")),
            elitebotix_rating=_to_float(row.get("elitebotix_rating")),
            skill_issue_rating=_to_float(row.get("skill_issue_rating")),
            bancho_rank=_to_int(row.get("bancho_rank")),
            lazer_rank=_to_int(row.get("lazer_rank")),
            tournaments_played_last_12m=_to_int(row.get("tournaments_played_last_12m")),
            days_since_last_event=_to_float(row.get("days_since_last_event")),
            consistency_metric=_to_float(row.get("consistency_metric")),
            skillset_subscores=skillset_subscores,
            metadata=metadata,
        )


@dataclass(slots=True)
class EventInput:
    username: str
    event_name: str | None = None
    event_date: str | None = None
    days_since_event: float | None = None
    impact_score: float | None = None
    match_cost: float | None = None
    win_rate: float | None = None
    placement_percentile: float | None = None
    strength_of_schedule: float | None = None
    event_tier_weight: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> "EventInput":
        reserved = {
            "username",
            "event_name",
            "event_date",
            "days_since_event",
            "impact_score",
            "match_cost",
            "win_rate",
            "placement_percentile",
            "strength_of_schedule",
            "event_tier_weight",
        }
        username = _clean_text(row.get("username"))
        if username is None:
            raise ValueError(f"Event row is missing username: {row}")

        metadata = {
            key: value
            for key, value in row.items()
            if key not in reserved
        }

        return cls(
            username=username,
            event_name=_clean_text(row.get("event_name")),
            event_date=_clean_text(row.get("event_date")),
            days_since_event=_to_float(row.get("days_since_event")),
            impact_score=_to_float(row.get("impact_score")),
            match_cost=_to_float(row.get("match_cost")),
            win_rate=_to_float(row.get("win_rate")),
            placement_percentile=_to_float(row.get("placement_percentile")),
            strength_of_schedule=_to_float(row.get("strength_of_schedule")),
            event_tier_weight=_to_float(row.get("event_tier_weight")) or 1.0,
            metadata=metadata,
        )


@dataclass(slots=True)
class PlayerRankingResult:
    username: str
    elitebotix_score: float | None
    skill_issue_score: float | None
    bancho_score: float | None
    lazer_score: float | None
    recent_tournament_form: float
    consistency_score: float
    reliability_multiplier: float
    activity_multiplier: float
    final_power_score: float
    provisional: bool
    base_power_score: float
    tournaments_played_last_12m: int
    days_since_last_event: float | None
    explanation: str
    debug: dict[str, Any] = field(default_factory=dict)

    def to_output_dict(self, *, include_debug: bool = False) -> dict[str, Any]:
        payload = asdict(self)
        if not include_debug:
            payload.pop("debug", None)
        return payload
