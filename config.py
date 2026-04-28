from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class RatingRange:
    min_value: float | None = None
    max_value: float | None = None


@dataclass(slots=True)
class PowerRankingConfig:
    elitebotix_weight: float = 0.32
    skill_issue_weight: float = 0.32
    bancho_weight: float = 0.08
    lazer_weight: float = 0.03
    recent_form_weight: float = 0.20
    consistency_weight: float = 0.05

    bancho_rank_cutoff: int = 100_000
    lazer_rank_cutoff: int = 100_000

    event_impact_weight: float = 0.30
    event_win_rate_weight: float = 0.25
    event_placement_weight: float = 0.25
    event_strength_of_schedule_weight: float = 0.15
    event_match_cost_weight: float = 0.05

    event_recency_decay_days: float = 180.0
    activity_decay_days: float = 180.0

    reliability_base: float = 0.85
    reliability_bonus: float = 0.15
    reliability_target_tournaments: float = 8.0

    activity_base: float = 0.90
    activity_bonus: float = 0.10

    consistency_penalty_scale: float = 1.75
    default_consistency_score: float = 25.0
    minimum_consistency_events: int = 2
    provisional_tournament_threshold: int = 3
    missing_elitebotix_score_default: float | None = 50.0
    missing_skill_issue_score_default: float | None = 50.0
    missing_bancho_score_default: float | None = 50.0
    missing_lazer_score_default: float | None = 50.0

    rating_ranges: dict[str, RatingRange] = field(
        default_factory=lambda: {
            "elitebotix_rating": RatingRange(),
            "skill_issue_rating": RatingRange(),
        }
    )

    def component_weights(self) -> dict[str, float]:
        return {
            "elitebotix_score": self.elitebotix_weight,
            "skill_issue_score": self.skill_issue_weight,
            "bancho_score": self.bancho_weight,
            "lazer_score": self.lazer_weight,
            "recent_tournament_form": self.recent_form_weight,
            "consistency_score": self.consistency_weight,
        }

    def event_component_weights(self) -> dict[str, float]:
        return {
            "impact_score": self.event_impact_weight,
            "match_cost": self.event_match_cost_weight,
            "win_rate": self.event_win_rate_weight,
            "placement_percentile": self.event_placement_weight,
            "strength_of_schedule": self.event_strength_of_schedule_weight,
        }


DEFAULT_CONFIG = PowerRankingConfig()
