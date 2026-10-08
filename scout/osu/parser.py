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


def parse_room(payload: dict) -> ParsedMatch:
    """A lazer multiplayer room (see OsuClient.get_room_full) -> the same ParsedMatch a stable match produces.

    Differences from stable: the games are the room's played playlist items, scores use the *standardised* total score
    (what the room history shows), mods carry lazer-only acronyms (DA, ...), and scores have no team colour."""
    room = payload.get("room") or {}
    by_item = {str(k): v for k, v in (payload.get("scores") or {}).items()}
    _, red, blue = parse_title(room.get("name"))

    games: list[ParsedGame] = []
    beatmaps: dict[int, BeatmapInfo] = {}
    users: dict[int, PlayerInfo] = {}
    played = [i for i in room.get("playlist", []) if by_item.get(str(i["id"]))]
    for item in sorted(played, key=lambda i: (i.get("played_at") or "", i["id"])):
        scs = by_item[str(item["id"])]
        bm = item.get("beatmap") or {}
        bset = bm.get("beatmapset") or {}
        beatmap_id = item.get("beatmap_id") or bm.get("id")
        if beatmap_id and beatmap_id not in beatmaps:
            beatmaps[beatmap_id] = BeatmapInfo(
                beatmap_id=int(beatmap_id), beatmapset_id=bm.get("beatmapset_id") or bset.get("id"),
                artist=bset.get("artist"), title=bset.get("title"), version=bm.get("version"),
                star_rating=bm.get("difficulty_rating"), mode=bm.get("mode"))
        parsed_scores = []
        for s in scs:
            u = s.get("user") or {}
            if s["user_id"] not in users:
                users[s["user_id"]] = PlayerInfo(
                    user_id=int(s["user_id"]), username=u.get("username") or str(s["user_id"]),
                    country_code=u.get("country_code"), country_name=(u.get("country") or {}).get("name"),
                    avatar_url=u.get("avatar_url"))
            st = s.get("statistics") or {}
            total = s.get("total_score")
            parsed_scores.append(ParsedScore(
                user_id=int(s["user_id"]), score=int(total if total is not None else s.get("classic_total_score") or 0),
                accuracy=float(s["accuracy"]) if s.get("accuracy") is not None else None, max_combo=s.get("max_combo"),
                count_300=_stat(st, "great"), count_100=_stat(st, "ok"), count_50=_stat(st, "meh"),
                count_miss=_stat(st, "miss"), mods=_mods(s.get("mods")), team=None, slot=None,
                passed=bool(s.get("passed", True))))
        started = [s.get("started_at") for s in scs if s.get("started_at")]
        ended = [s.get("ended_at") for s in scs if s.get("ended_at")]
        games.append(ParsedGame(
            osu_game_id=int(item["id"]), order_index=len(games),
            beatmap_id=int(beatmap_id) if beatmap_id else None, mods=_mods(item.get("required_mods")),
            scoring_type="standardised", team_type="head-to-head",
            start_time=min(started) if started else item.get("played_at"),
            end_time=max(ended) if ended else item.get("played_at"), scores=parsed_scores))

    return ParsedMatch(
        osu_match_id=int(room.get("id")), name=room.get("name"), start_time=room.get("starts_at"),
        end_time=room.get("ends_at"), team_red=red, team_blue=blue, games=games,
        players=list(users.values()), beatmaps=list(beatmaps.values()))


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
