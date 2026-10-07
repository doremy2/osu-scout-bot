"""Source-agnostic data objects. Importers produce these; the repository stores them."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class MatchLink:
    """An osu! multiplayer lobby discovered in some source."""
    osu_match_id: int
    round_raw: str | None = None      # text hint, e.g. "Quarterfinals" or the sheet tab name
    context: str | None = None        # the row / line the link was found in


@dataclass
class PlayerInfo:
    user_id: int
    username: str
    country_code: str | None = None
    country_name: str | None = None
    avatar_url: str | None = None


@dataclass
class BeatmapInfo:
    beatmap_id: int
    beatmapset_id: int | None = None
    artist: str | None = None
    title: str | None = None
    version: str | None = None
    star_rating: float | None = None
    mode: str | None = None


@dataclass
class ParsedScore:
    user_id: int
    score: int
    accuracy: float | None
    max_combo: int | None
    count_300: int | None
    count_100: int | None
    count_50: int | None
    count_miss: int | None
    mods: list[str]
    team: str | None
    slot: int | None
    passed: bool


@dataclass
class ParsedGame:
    osu_game_id: int
    order_index: int
    beatmap_id: int | None
    mods: list[str]
    scoring_type: str | None
    team_type: str | None
    start_time: str | None
    end_time: str | None
    scores: list[ParsedScore] = field(default_factory=list)


@dataclass
class ParsedMatch:
    osu_match_id: int
    name: str | None
    start_time: str | None
    end_time: str | None
    team_red: str | None
    team_blue: str | None
    games: list[ParsedGame]
    players: list[PlayerInfo]
    beatmaps: list[BeatmapInfo]
