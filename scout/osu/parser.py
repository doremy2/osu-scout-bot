"""osu! API match payload -> ParsedMatch.

Tolerant of both score formats the API has used:
  legacy:  score, mods ["HD"], statistics {count_300, count_100, count_50, count_miss}
  lazer:   total_score / legacy_total_score, mods [{"acronym": "HD"}], statistics {great, ok, meh, miss}
"""
from __future__ import annotations

import re

from ..models import BeatmapInfo, ParsedGame, ParsedMatch, ParsedScore, PlayerInfo

# "VRSO: (Team A) vs (Team B)"  /  "VRSO: Team A vs Team B"
_TITLE_RE = re.compile(r"^\s*([^:]+?)\s*:\s*\(?(.+?)\)?\s+vs\.?\s+\(?(.+?)\)?\s*$", re.IGNORECASE)


def parse_title(name: str | None) -> tuple[str | None, str | None, str | None]:
    """Returns (acronym, red, blue). osu! convention: first team is red."""
    if not name:
        return None, None, None
    m = _TITLE_RE.match(name)
    if not m:
        return None, None, None
    return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()


def _mods(raw) -> list[str]:
    out = []
    for m in raw or []:
        if isinstance(m, dict):
            m = m.get("acronym")
        if m:
            out.append(str(m).upper())
    return out


def _stat(stats: dict, *keys) -> int | None:
    for k in keys:
        if stats.get(k) is not None:
            return int(stats[k])
    return None


def _parse_score(s: dict) -> ParsedScore:
    stats = s.get("statistics") or {}
    m = s.get("match") or {}
    score = s.get("score")
    if score is None:
        score = s.get("legacy_total_score") or s.get("total_score") or 0
    passed = m.get("pass", s.get("passed", True))
    return ParsedScore(
        user_id=int(s["user_id"]),
        score=int(score),
        accuracy=float(s["accuracy"]) if s.get("accuracy") is not None else None,
        max_combo=s.get("max_combo"),
        count_300=_stat(stats, "count_300", "great"),
        count_100=_stat(stats, "count_100", "ok"),
        count_50=_stat(stats, "count_50", "meh"),
        count_miss=_stat(stats, "count_miss", "miss"),
        mods=_mods(s.get("mods")),
        team=(m.get("team") or None),
        slot=m.get("slot"),
        passed=bool(passed) if passed is not None else True,
    )


def parse_match(payload: dict) -> ParsedMatch:
    match = payload.get("match") or {}
    _, red, blue = parse_title(match.get("name"))

    games: list[ParsedGame] = []
    beatmaps: dict[int, BeatmapInfo] = {}
    for ev in payload.get("events", []):
        g = ev.get("game")
        if not g:
            continue
        bm = g.get("beatmap") or {}
        bset = bm.get("beatmapset") or {}
        beatmap_id = g.get("beatmap_id") or bm.get("id")
        if beatmap_id and beatmap_id not in beatmaps:
            beatmaps[beatmap_id] = BeatmapInfo(
                beatmap_id=int(beatmap_id),
                beatmapset_id=bm.get("beatmapset_id") or bset.get("id"),
                artist=bset.get("artist"),
                title=bset.get("title"),
                version=bm.get("version"),
                star_rating=bm.get("difficulty_rating"),
                mode=bm.get("mode") or g.get("mode"),
            )
        games.append(ParsedGame(
            osu_game_id=int(g["id"]),
            order_index=len(games),
            beatmap_id=int(beatmap_id) if beatmap_id else None,
            mods=_mods(g.get("mods")),
            scoring_type=g.get("scoring_type"),
            team_type=g.get("team_type"),
            start_time=g.get("start_time"),
            end_time=g.get("end_time"),
            scores=[_parse_score(s) for s in g.get("scores") or []],
        ))

    players = [
        PlayerInfo(
            user_id=int(u["id"]),
            username=u.get("username") or str(u["id"]),
            country_code=u.get("country_code"),
            country_name=(u.get("country") or {}).get("name"),
            avatar_url=u.get("avatar_url"),
        )
        for u in payload.get("users", [])
    ]
    return ParsedMatch(
        osu_match_id=int(match.get("id")),
        name=match.get("name"),
        start_time=match.get("start_time"),
        end_time=match.get("end_time"),
        team_red=red,
        team_blue=blue,
        games=games,
        players=players,
        beatmaps=list(beatmaps.values()),
    )
