"""Tournament formats. The format is stored on the tournament row; everything format-specific
(teams? tabs? how a lobby maps to sides?) is looked up here, so a new format (2v2, FFA, ...)
is one more entry instead of a rewrite."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TournamentFormat:
    key: str
    label: str
    has_teams: bool        # tournament_teams / memberships / team leaderboard exist
    description: str


FORMATS: dict[str, TournamentFormat] = {f.key: f for f in (
    TournamentFormat("1v1", "1v1", False, "Individual players; matches are Player A vs Player B."),
    TournamentFormat("team", "Team", True, "Teams or countries; matches are Team A vs Team B."),
)}
DEFAULT_FORMAT = "1v1"


def get_format(key: str | None) -> TournamentFormat:
    return FORMATS.get(key or DEFAULT_FORMAT) or FORMATS[DEFAULT_FORMAT]


def validate_format(key: str | None) -> str:
    if key not in FORMATS:
        raise ValueError(f"Unknown tournament format {key!r}; expected one of {', '.join(FORMATS)}")
    return key
