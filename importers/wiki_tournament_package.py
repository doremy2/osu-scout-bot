"""Build reusable tournament-package JSON from osu! wiki pages + osu! API."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from importers.osu_api import OsuApiClient, OsuApiError, parse_match_id, parse_room_id
from importers.owc_wiki import (
    _TABLE_CELL_RE,
    _TABLE_ROW_RE,
    _extract_mappool_from_section,
    _get,
    _strip_tags,
)
from storage import canonicalize_stage


_MATCH_URL_RE = re.compile(
    r"https?://osu\.ppy\.sh/community/matches/(?P<id>\d+)",
    re.IGNORECASE,
)
_ROOM_URL_RE = re.compile(
    r"https?://osu\.ppy\.sh/multiplayer/rooms/(?P<id>\d+)",
    re.IGNORECASE,
)
_USER_URL_RE = re.compile(
    r"https?://osu\.ppy\.sh/users/(?P<id>\d+)",
    re.IGNORECASE,
)
_HEADING_RE = re.compile(
    r"<h(?P<level>[23])[^>]*>(?P<title>.*?)</h(?P=level)>(?P<body>.*?)(?=<h[23]\b|\Z)",
    re.DOTALL | re.IGNORECASE,
)
_TABLE_RE = re.compile(r"<table\b[^>]*>.*?</table>", re.DOTALL | re.IGNORECASE)
_DATE_TOKEN_RE = re.compile(
    r"(?P<date>(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+\d{1,2}\s+[A-Za-z]+\s+\d{4})|(?P<table><table\b[^>]*>.*?</table>)",
    re.DOTALL | re.IGNORECASE,
)
_VS_NAME_RE = re.compile(
    r"\(([^()]+)\)\s*(?:vs\.?|v\.?)\s*\(([^()]+)\)",
    re.IGNORECASE,
)
_ISO_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_STAGE_ABBREVIATIONS = {
    "qf": "Quarterfinals",
    "quarterfinal": "Quarterfinals",
    "quarterfinals": "Quarterfinals",
    "sf": "Semifinals",
    "semifinal": "Semifinals",
    "semifinals": "Semifinals",
    "gf": "Grand Finals",
    "grand final": "Grand Finals",
    "grand finals": "Grand Finals",
    "ro16": "Round of 16",
    "round of 16": "Round of 16",
    "ro32": "Round of 32",
    "round of 32": "Round of 32",
    "f": "Finals",
    "final": "Finals",
    "finals": "Finals",
}
_DAY_FORMAT = "%A, %d %B %Y"
_CANONICAL_STAGE_ORDER = {
    "Qualifiers": 0,
    "Group Stage": 1,
    "Round of 32": 2,
    "Round of 16": 3,
    "Quarterfinals": 4,
    "Semifinals": 5,
    "Finals": 6,
    "Grand Finals": 7,
}


@dataclass(frozen=True)
class WikiTournamentPackageConfig:
    slug: str
    event: str
    wiki_url: str
    short_name: str
    tier: str
    team_size: int
    event_format: str
    notes: tuple[str, ...] = ()
    package_status: str = "verified"
    production_safe: bool = True
    api_request_delay: float = 0.1


@dataclass
class HeadingBlock:
    level: int
    title: str
    body: str
    parent_title: str | None = None


RECENT_TOURNAMENT_CONFIGS: dict[str, WikiTournamentPackageConfig] = {
    "lga_2025": WikiTournamentPackageConfig(
        slug="lga_2025",
        event="LGA 2025",
        wiki_url="https://osu.ppy.sh/wiki/en/Tournaments/LGA/2025",
        short_name="LGA 2025",
        tier="premier",
        team_size=1,
        event_format="1v1",
        notes=(
            "Official osu! wiki page with lazer room links.",
            "Week-based mappool stages are kept as metadata stages; played maps are linked by beatmap_id.",
        ),
    ),
    "resc_2025": WikiTournamentPackageConfig(
        slug="resc_2025",
        event="RESC 2025",
        wiki_url="https://osu.ppy.sh/wiki/en/Tournaments/RESC/2025",
        short_name="RESC 2025",
        tier="major",
        team_size=3,
        event_format="3v3",
        notes=("Community match links come from the official osu! wiki tournament page.",),
    ),
    "oit_2025": WikiTournamentPackageConfig(
        slug="oit_2025",
        event="OIT 2025",
        wiki_url="https://osu.ppy.sh/wiki/en/Tournaments/OIT/2025",
        short_name="OIT 2025",
        tier="major",
        team_size=1,
        event_format="1v1",
        notes=("Week-labelled finals sections are collapsed into canonical match stages where possible.",),
    ),
    "fdc_2025": WikiTournamentPackageConfig(
        slug="fdc_2025",
        event="FDC 2025",
        wiki_url="https://osu.ppy.sh/wiki/en/Tournaments/FDC/2025",
        short_name="FDC 2025",
        tier="major",
        team_size=2,
        event_format="2v2",
        notes=("Standard osu! 2v2 event from the official osu! wiki tournament page.",),
    ),
    "4wc_2025": WikiTournamentPackageConfig(
        slug="4wc_2025",
        event="4WC 2025",
        wiki_url="https://osu.ppy.sh/wiki/en/Tournaments/4WC/2025",
        short_name="4WC 2025",
        tier="major",
        team_size=4,
        event_format="4v4",
        notes=("Standard osu! 4v4 event from the official osu! wiki tournament page.",),
    ),
    "3wc_2025": WikiTournamentPackageConfig(
        slug="3wc_2025",
        event="3WC 2025",
        wiki_url="https://osu.ppy.sh/wiki/en/Tournaments/3WC/2025",
        short_name="3WC 2025",
        tier="major",
        team_size=3,
        event_format="3v3",
        notes=("Standard osu! 3v3 event from the official osu! wiki tournament page.",),
    ),
}


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


def _normalize_key(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    normalized = "".join(ch for ch in text.casefold() if ch.isalnum())
    return normalized or text.casefold()


def _iter_heading_blocks(page_html: str) -> list[HeadingBlock]:
    blocks: list[HeadingBlock] = []
    current_h2: str | None = None
    for match in _HEADING_RE.finditer(page_html):
        level = int(match.group("level"))
        title = _strip_tags(match.group("title"))
        body = match.group("body") or ""
        if level == 2:
            current_h2 = title
            blocks.append(HeadingBlock(level=level, title=title, body=body))
        else:
            blocks.append(HeadingBlock(level=level, title=title, body=body, parent_title=current_h2))
    return blocks


def _find_h2_block(blocks: list[HeadingBlock], title_fragment: str) -> HeadingBlock | None:
    fragment = title_fragment.casefold()
    for block in blocks:
        if block.level == 2 and fragment in block.title.casefold():
            return block
    return None


def _find_h3_blocks(blocks: list[HeadingBlock], parent_fragment: str) -> list[HeadingBlock]:
    fragment = parent_fragment.casefold()
    return [
        block
        for block in blocks
        if block.level == 3 and block.parent_title and fragment in block.parent_title.casefold()
    ]


def _parse_iso_date_token(value: str | None) -> str | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None
    cleaned = cleaned.rstrip(":")
    try:
        return datetime.strptime(cleaned, _DAY_FORMAT).date().isoformat()
    except ValueError:
        return None


def _iter_dated_tables(section_html: str) -> list[tuple[str | None, str]]:
    current_date: str | None = None
    tables: list[tuple[str | None, str]] = []
    for match in _DATE_TOKEN_RE.finditer(section_html):
        date_token = match.group("date")
        table_html = match.group("table")
        if date_token:
            current_date = _parse_iso_date_token(_strip_tags(date_token))
        elif table_html:
            tables.append((current_date, table_html))
    if not tables:
        for table_match in _TABLE_RE.finditer(section_html):
            tables.append((current_date, table_match.group(0)))
    return tables


def _parse_table(table_html: str) -> tuple[list[str], list[tuple[list[str], list[str]]]]:
    rows: list[tuple[list[str], list[str]]] = []
    for row_match in _TABLE_ROW_RE.finditer(table_html):
        cells = [cell.group("body") for cell in _TABLE_CELL_RE.finditer(row_match.group("body"))]
        if not cells:
            continue
        rows.append((cells, [_strip_tags(cell) for cell in cells]))
    if not rows:
        return [], []
    return rows[0][1], rows[1:]


def _extract_schedule_dates(blocks: list[HeadingBlock]) -> tuple[str | None, str | None]:
    schedule_block = _find_h2_block(blocks, "Tournament schedule")
    if schedule_block is None:
        return None, None
    dates = sorted(set(_ISO_DATE_RE.findall(schedule_block.body)))
    if not dates:
        return None, None
    return dates[0], dates[-1]


def _clean_stage_label(value: str | None) -> str | None:
    cleaned = _clean_text(value)
    if cleaned is None:
        return None
    return re.sub(r"\s+", " ", cleaned).strip() or None


def _canonical_match_stage(section_title: str | None, row_label: str | None = None) -> str | None:
    section_stage = canonicalize_stage(section_title) or _clean_stage_label(section_title)
    label = _clean_stage_label(row_label)
    if label is None:
        return section_stage

    normalized = re.sub(r"[\s\-_]+", " ", label.casefold()).strip()
    if normalized in _STAGE_ABBREVIATIONS:
        return _STAGE_ABBREVIATIONS[normalized]
    if normalized in {"grand final", "grand finals"}:
        return "Grand Finals"
    if normalized in {"lower", "upper", "winners", "losers"}:
        return section_stage
    if re.fullmatch(r"(?:lr|wr|lb|ub)\d+", normalized):
        return label.upper().replace(" ", "")
    if normalized in {"wf", "winners final", "winners finals"}:
        return "Finals"
    if normalized in {"lf", "losers final", "losers finals"}:
        return "Finals"
    return canonicalize_stage(label) or label


def _extract_pair_names(value: str | None) -> tuple[str | None, str | None]:
    text = _clean_text(value)
    if text is None:
        return None, None
    match = _VS_NAME_RE.search(text)
    if match is None:
        return None, None
    return _clean_text(match.group(1)), _clean_text(match.group(2))


def _extract_link_info(row_html: str) -> tuple[str | None, str | None, str | None]:
    room_match = _ROOM_URL_RE.search(row_html)
    if room_match:
        room_id = room_match.group("id")
        return "room", room_id, f"https://osu.ppy.sh/multiplayer/rooms/{room_id}"
    match_match = _MATCH_URL_RE.search(row_html)
    if match_match:
        match_id = match_match.group("id")
        return "match", match_id, f"https://osu.ppy.sh/community/matches/{match_id}"
    return None, None, None


def _extract_user_ids(row_html: str) -> list[int]:
    user_ids: list[int] = []
    for match in _USER_URL_RE.finditer(row_html):
        user_id = int(match.group("id"))
        if user_id not in user_ids:
            user_ids.append(user_id)
    return user_ids


def _derive_participants(header: list[str], row_texts: list[str]) -> tuple[str | None, str | None]:
    ignored: set[int] = set()
    for index, heading in enumerate(header):
        lowered = heading.casefold()
        if any(token in lowered for token in ("stage", "bracket", "match link", "vod")):
            ignored.add(index)

    candidates: list[str] = []
    for index, value in enumerate(row_texts):
        text = _clean_text(value)
        if text is None or index in ignored:
            continue
        if text.startswith("#") or re.fullmatch(r"\d+", text):
            continue
        candidates.append(text)

    if len(candidates) >= 2:
        return candidates[0], candidates[-1]
    return None, None


def _derive_score_pair(row_texts: list[str]) -> tuple[int | None, int | None]:
    numbers = [_to_int(value) for value in row_texts if _to_int(value) is not None]
    if len(numbers) >= 2:
        return numbers[0], numbers[1]
    return None, None


def _extract_match_rows(
    *,
    config: WikiTournamentPackageConfig,
    blocks: list[HeadingBlock],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen_links: set[str] = set()
    for block in _find_h3_blocks(blocks, "Match results"):
        section_stage_title = _clean_stage_label(block.title)
        for date_iso, table_html in _iter_dated_tables(block.body):
            header, data_rows = _parse_table(table_html)
            if not data_rows:
                continue
            stage_label_index = None
            bracket_label_index = None
            for index, heading in enumerate(header):
                lowered = heading.casefold()
                if "stage" in lowered:
                    stage_label_index = index
                elif "bracket" in lowered:
                    bracket_label_index = index

            for raw_cells, row_texts in data_rows:
                row_html = " ".join(raw_cells)
                link_kind, link_id, match_link = _extract_link_info(row_html)
                if match_link is None or match_link in seen_links:
                    continue
                seen_links.add(match_link)

                row_stage_label = None
                if stage_label_index is not None and stage_label_index < len(row_texts):
                    row_stage_label = row_texts[stage_label_index]
                elif bracket_label_index is not None and bracket_label_index < len(row_texts):
                    row_stage_label = row_texts[bracket_label_index]

                team_a, team_b = _derive_participants(header, row_texts)
                score_a, score_b = _derive_score_pair(row_texts)
                user_ids = _extract_user_ids(row_html)
                results.append(
                    {
                        "event": config.event,
                        "stage": _canonical_match_stage(section_stage_title, row_stage_label),
                        "date": date_iso,
                        "team_a": team_a,
                        "team_b": team_b,
                        "score_a": score_a,
                        "score_b": score_b,
                        "match_link": match_link,
                        "match_id": link_id,
                        "match_kind": link_kind,
                        "user_ids": user_ids,
                        "metadata": {
                            "wiki_section": section_stage_title,
                            "wiki_row_stage": _clean_text(row_stage_label),
                            "wiki_url": config.wiki_url,
                            "link_kind": link_kind,
                        },
                    }
                )
    return results


def _extract_mappool_rows(
    *,
    config: WikiTournamentPackageConfig,
    blocks: list[HeadingBlock],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for block in _find_h3_blocks(blocks, "Mappools"):
        stage_title = _clean_stage_label(block.title)
        if stage_title is None:
            continue
        for entry in _extract_mappool_from_section(stage_title, block.body):
            key = (entry.stage, entry.slot)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            rows.append(
                {
                    "event": config.event,
                    "stage": entry.stage,
                    "slot": entry.slot,
                    "mod": "".join(ch for ch in entry.slot if ch.isalpha()).upper() or None,
                    "map_name": entry.map_name,
                    "difficulty_name": entry.difficulty_name,
                    "beatmap_id": entry.beatmap_id,
                    "star_rating": entry.star_rating,
                    "metadata": {
                        "wiki_stage_title": stage_title,
                        "wiki_url": config.wiki_url,
                    },
                }
            )
    return rows


def _build_mappool_lookup(
    mappool_rows: list[dict[str, Any]],
) -> tuple[dict[tuple[str, int], dict[str, Any]], dict[int, dict[str, Any]], set[int]]:
    by_stage: dict[tuple[str, int], dict[str, Any]] = {}
    global_entries: dict[int, dict[str, Any]] = {}
    beatmap_ids: set[int] = set()
    for row in mappool_rows:
        stage = _clean_stage_label(row.get("stage"))
        beatmap_id = _to_int(row.get("beatmap_id"))
        if stage is not None and beatmap_id is not None:
            by_stage[(stage, beatmap_id)] = row
        if beatmap_id is None:
            continue
        beatmap_ids.add(beatmap_id)
        global_entries.setdefault(beatmap_id, row)
    return by_stage, global_entries, beatmap_ids


def _infer_slot_from_mods(mods: list[str], index: int) -> str:
    prefixes = [str(mod).upper() for mod in mods if str(mod).strip()]
    prefix = None
    for candidate in prefixes:
        if candidate in {"NM", "HD", "HR", "DT", "FM", "TB", "LM", "FL", "OG"}:
            prefix = candidate
            break
    if prefix is None:
        prefix = "NM"
    return "TB" if prefix == "TB" else f"{prefix}{index}"


def _resolve_slot(
    *,
    stage: str | None,
    beatmap_id: int | None,
    by_stage_lookup: dict[tuple[str, int], dict[str, Any]],
    global_lookup: dict[int, dict[str, Any]],
    fallback_mods: list[str],
    fallback_index: int,
) -> tuple[str, str, dict[str, Any]]:
    if stage is not None and beatmap_id is not None:
        stage_row = by_stage_lookup.get((stage, beatmap_id))
        if stage_row:
            slot = _clean_text(stage_row.get("slot")) or _infer_slot_from_mods(fallback_mods, fallback_index)
            mod = _clean_text(stage_row.get("mod")) or "".join(ch for ch in slot if ch.isalpha()).upper() or "NM"
            return slot, mod, {"slot_inferred": False, "matched_mappool_stage": stage}
    if beatmap_id is not None:
        row = global_lookup.get(beatmap_id)
        if row:
            slot = _clean_text(row.get("slot")) or _infer_slot_from_mods(fallback_mods, fallback_index)
            mod = _clean_text(row.get("mod")) or "".join(ch for ch in slot if ch.isalpha()).upper() or "NM"
            return slot, mod, {"slot_inferred": False, "matched_mappool_stage": _clean_text(row.get("stage"))}
    slot = _infer_slot_from_mods(fallback_mods, fallback_index)
    mod = "".join(ch for ch in slot if ch.isalpha()).upper() or "NM"
    return slot, mod, {"slot_inferred": True}


def _remember_player(
    players: dict[tuple[str, int | None], dict[str, Any]],
    *,
    config: WikiTournamentPackageConfig,
    player: str,
    user_id: int | None,
    team_code: str | None,
    metadata: dict[str, Any] | None = None,
) -> None:
    key = (player, user_id)
    row = players.setdefault(
        key,
        {
            "event": config.event,
            "player": player,
            "user_id": user_id,
            "team_code": team_code,
            "metadata": {
                "sources": [],
            },
        },
    )
    if team_code and not row.get("team_code"):
        row["team_code"] = team_code
    if metadata:
        sources = row["metadata"].setdefault("sources", [])
        if metadata not in sources:
            sources.append(metadata)


def _best_score_per_user(scores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[int, dict[str, Any]] = {}
    for score in scores:
        user_id = _to_int(score.get("user_id"))
        if user_id is None:
            continue
        current = best.get(user_id)
        total = _to_int(score.get("total_score")) or 0
        if current is None or total > (_to_int(current.get("total_score")) or 0):
            best[user_id] = score
    return list(best.values())


def _choose_room_competitors(
    room_payload: dict[str, Any],
    wiki_row: dict[str, Any],
    room_scores: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    room_name_a, room_name_b = _extract_pair_names(room_payload.get("name"))
    desired_names = {
        key
        for key in [
            _normalize_key(room_name_a),
            _normalize_key(wiki_row.get("team_a")),
            _normalize_key(room_name_b),
            _normalize_key(wiki_row.get("team_b")),
        ]
        if key
    }
    desired_user_ids = {int(user_id) for user_id in (wiki_row.get("user_ids") or [])}
    shortlisted = []
    for score in room_scores:
        user = score.get("user") or {}
        username = _clean_text(user.get("username"))
        user_id = _to_int(score.get("user_id")) or _to_int(user.get("id"))
        if username and _normalize_key(username) in desired_names:
            shortlisted.append(score)
            continue
        if user_id is not None and user_id in desired_user_ids:
            shortlisted.append(score)
    if shortlisted:
        return _best_score_per_user(shortlisted)
    return _best_score_per_user(room_scores)[:2]


def _community_match_to_package_rows(
    *,
    config: WikiTournamentPackageConfig,
    wiki_row: dict[str, Any],
    match_payload: Any,
    by_stage_lookup: dict[tuple[str, int], dict[str, Any]],
    global_lookup: dict[int, dict[str, Any]],
    event_mappool_ids: set[int],
    players: dict[tuple[str, int | None], dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    red_name, blue_name = _extract_pair_names(match_payload.name)
    red_name = red_name or _clean_text(wiki_row.get("team_a")) or "Red"
    blue_name = blue_name or _clean_text(wiki_row.get("team_b")) or "Blue"
    stage = _clean_stage_label(wiki_row.get("stage"))
    map_scores: list[dict[str, Any]] = []
    skipped_beatmaps = 0
    red_wins = 0
    blue_wins = 0

    for game_index, game in enumerate(match_payload.games, start=1):
        beatmap_id = _to_int(game.beatmap_id)
        if event_mappool_ids and beatmap_id is None:
            skipped_beatmaps += 1
            continue
        if event_mappool_ids and beatmap_id is not None and beatmap_id not in event_mappool_ids:
            skipped_beatmaps += 1
            continue
        slot, mod, slot_meta = _resolve_slot(
            stage=stage,
            beatmap_id=beatmap_id,
            by_stage_lookup=by_stage_lookup,
            global_lookup=global_lookup,
            fallback_mods=list(game.mods or []),
            fallback_index=game_index,
        )
        winner = (game.winning_team or "").lower()
        if winner == "red":
            red_wins += 1
        elif winner == "blue":
            blue_wins += 1

        for score in game.scores:
            if (score.team or "").lower() not in {"red", "blue"}:
                continue
            username = _clean_text(score.username) or _clean_text(match_payload.users.get(score.user_id))
            if username is None:
                continue
            team_name = red_name if (score.team or "").lower() == "red" else blue_name
            opponent_name = blue_name if team_name == red_name else red_name
            result = "unknown"
            if winner in {"red", "blue"}:
                result = "win" if winner == (score.team or "").lower() else "loss"
            played_at = _clean_text(game.end_time) or _clean_text(game.start_time) or _clean_text(wiki_row.get("date"))
            map_scores.append(
                {
                    "player": username,
                    "opponent": opponent_name,
                    "event": config.event,
                    "stage": stage,
                    "date": (played_at or "")[:10] or _clean_text(wiki_row.get("date")),
                    "mod": mod,
                    "slot": slot,
                    "score": score.score,
                    "accuracy": score.accuracy * 100.0 if score.accuracy is not None and score.accuracy <= 1 else score.accuracy,
                    "result": result,
                    "star_rating": game.star_rating,
                    "beatmap_id": beatmap_id,
                    "map_name": _clean_text(game.beatmap_title),
                    "difficulty_name": _clean_text(game.beatmap_version),
                    "player_team": team_name,
                    "opponent_team": opponent_name,
                    "match_id": str(match_payload.match_id),
                    "user_id": score.user_id,
                    "quality": "partial" if slot_meta.get("slot_inferred") else "verified",
                    "inferred_fields": ["slot"] if slot_meta.get("slot_inferred") else [],
                }
            )
            _remember_player(
                players,
                config=config,
                player=username,
                user_id=score.user_id,
                team_code=team_name if config.team_size > 1 else username,
                metadata={"type": "match_api", "match_link": wiki_row.get("match_link")},
            )

    actual_red = red_wins
    actual_blue = blue_wins
    official_red = _to_int(wiki_row.get("score_a")) if wiki_row.get("score_a") is not None else actual_red
    official_blue = _to_int(wiki_row.get("score_b")) if wiki_row.get("score_b") is not None else actual_blue

    match_rows = [
        {
            "event": config.event,
            "stage": stage,
            "team": red_name,
            "team_code": red_name if config.team_size > 1 else red_name,
            "opponent_team": blue_name,
            "team_score": official_red,
            "opponent_score": official_blue,
            "result": "win" if official_red > official_blue else "loss" if official_red < official_blue else "draw",
            "match_link": wiki_row.get("match_link"),
            "date": _clean_text(wiki_row.get("date")) or (match_payload.start_time or "")[:10],
            "source": "osu_wiki+osu_api",
            "source_type": "wiki_package_verified",
            "source_url": config.wiki_url,
        },
        {
            "event": config.event,
            "stage": stage,
            "team": blue_name,
            "team_code": blue_name if config.team_size > 1 else blue_name,
            "opponent_team": red_name,
            "team_score": official_blue,
            "opponent_score": official_red,
            "result": "win" if official_blue > official_red else "loss" if official_blue < official_red else "draw",
            "match_link": wiki_row.get("match_link"),
            "date": _clean_text(wiki_row.get("date")) or (match_payload.start_time or "")[:10],
            "source": "osu_wiki+osu_api",
            "source_type": "wiki_package_verified",
            "source_url": config.wiki_url,
        },
    ]
    return map_scores, {
        "matches": match_rows,
        "validation": {
            "match_link": wiki_row.get("match_link"),
            "match_kind": "match",
            "derived_score": [actual_red, actual_blue],
            "official_score": [official_red, official_blue],
            "score_mismatch": [actual_red, actual_blue] != [official_red, official_blue],
            "skipped_beatmaps": skipped_beatmaps,
            "map_rows": len(map_scores),
        },
    }


def _room_to_package_rows(
    *,
    config: WikiTournamentPackageConfig,
    wiki_row: dict[str, Any],
    room_payload: dict[str, Any],
    room_leaderboard: list[dict[str, Any]],
    client: OsuApiClient,
    by_stage_lookup: dict[tuple[str, int], dict[str, Any]],
    global_lookup: dict[int, dict[str, Any]],
    event_mappool_ids: set[int],
    players: dict[tuple[str, int | None], dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    room_id = parse_room_id(wiki_row.get("match_link"))
    if room_id is None:
        raise OsuApiError(f"Cannot parse room from {wiki_row.get('match_link')!r}")
    stage = _clean_stage_label(wiki_row.get("stage"))
    room_name_a, room_name_b = _extract_pair_names(room_payload.get("name"))
    left_name = room_name_a or _clean_text(wiki_row.get("team_a"))
    right_name = room_name_b or _clean_text(wiki_row.get("team_b"))
    wins_by_name: dict[str, int] = {name: 0 for name in [left_name, right_name] if name}
    map_scores: list[dict[str, Any]] = []
    skipped_beatmaps = 0

    for playlist_index, playlist_item in enumerate(room_payload.get("playlist") or [], start=1):
        if _clean_text(playlist_item.get("played_at")) is None:
            continue
        beatmap = playlist_item.get("beatmap") or {}
        beatmapset = beatmap.get("beatmapset") or {}
        beatmap_id = _to_int(playlist_item.get("beatmap_id")) or _to_int(beatmap.get("id"))
        if event_mappool_ids and beatmap_id is None:
            skipped_beatmaps += 1
            continue
        if event_mappool_ids and beatmap_id is not None and beatmap_id not in event_mappool_ids:
            skipped_beatmaps += 1
            continue
        raw_scores = client.get_room_playlist_scores(room_id, playlist_item.get("id"))
        if not raw_scores:
            continue
        chosen_scores = _choose_room_competitors(room_payload, wiki_row, raw_scores)
        if len(chosen_scores) < 2:
            continue
        chosen_scores = sorted(
            chosen_scores,
            key=lambda row: _to_int(row.get("total_score")) or 0,
            reverse=True,
        )[:2]
        score_by_name: dict[str, dict[str, Any]] = {}
        for raw_score in chosen_scores:
            user = raw_score.get("user") or {}
            username = _clean_text(user.get("username"))
            if username:
                score_by_name[username] = raw_score
        ordered_names = [name for name in [left_name, right_name] if name in score_by_name]
        if len(ordered_names) < 2:
            ordered_names = list(score_by_name.keys())[:2]
        if len(ordered_names) < 2:
            continue
        left_total = _to_int(score_by_name[ordered_names[0]].get("total_score")) or 0
        right_total = _to_int(score_by_name[ordered_names[1]].get("total_score")) or 0
        winner_name = ordered_names[0] if left_total >= right_total else ordered_names[1]
        wins_by_name[winner_name] = wins_by_name.get(winner_name, 0) + 1

        slot, mod, slot_meta = _resolve_slot(
            stage=stage,
            beatmap_id=beatmap_id,
            by_stage_lookup=by_stage_lookup,
            global_lookup=global_lookup,
            fallback_mods=[mod_row.get("acronym") for mod_row in (playlist_item.get("required_mods") or [])],
            fallback_index=playlist_index,
        )

        for player_name, opponent_name in [
            (ordered_names[0], ordered_names[1]),
            (ordered_names[1], ordered_names[0]),
        ]:
            raw_score = score_by_name[player_name]
            user = raw_score.get("user") or {}
            user_id = _to_int(raw_score.get("user_id")) or _to_int(user.get("id"))
            ended_at = _clean_text(raw_score.get("ended_at")) or _clean_text(playlist_item.get("played_at"))
            map_scores.append(
                {
                    "player": player_name,
                    "opponent": opponent_name,
                    "event": config.event,
                    "stage": stage,
                    "date": (ended_at or "")[:10] or _clean_text(wiki_row.get("date")),
                    "mod": mod,
                    "slot": slot,
                    "score": _to_int(raw_score.get("total_score")),
                    "accuracy": (_to_float(raw_score.get("accuracy")) or 0.0) * 100.0,
                    "result": "win" if player_name == winner_name else "loss",
                    "star_rating": _to_float(beatmap.get("difficulty_rating")),
                    "beatmap_id": beatmap_id,
                    "map_name": (_clean_text(beatmapset.get("artist")) and _clean_text(beatmapset.get("title")) and f"{_clean_text(beatmapset.get('artist'))} - {_clean_text(beatmapset.get('title'))}") or _clean_text(beatmapset.get("title")),
                    "difficulty_name": _clean_text(beatmap.get("version")),
                    "player_team": player_name,
                    "opponent_team": opponent_name,
                    "match_id": f"room:{room_id}",
                    "user_id": user_id,
                    "quality": "partial" if slot_meta.get("slot_inferred") else "verified",
                    "inferred_fields": ["slot"] if slot_meta.get("slot_inferred") else [],
                }
            )
            _remember_player(
                players,
                config=config,
                player=player_name,
                user_id=user_id,
                team_code=player_name,
                metadata={"type": "room_api", "match_link": wiki_row.get("match_link")},
            )

    official_left = _to_int(wiki_row.get("score_a"))
    official_right = _to_int(wiki_row.get("score_b"))
    derived_left = wins_by_name.get(left_name or "", 0)
    derived_right = wins_by_name.get(right_name or "", 0)
    team_score_left = official_left if official_left is not None else derived_left
    team_score_right = official_right if official_right is not None else derived_right

    match_rows = [
        {
            "event": config.event,
            "stage": stage,
            "team": left_name,
            "team_code": left_name,
            "opponent_team": right_name,
            "team_score": team_score_left,
            "opponent_score": team_score_right,
            "result": "win" if team_score_left > team_score_right else "loss" if team_score_left < team_score_right else "draw",
            "match_link": wiki_row.get("match_link"),
            "date": _clean_text(wiki_row.get("date")) or (_clean_text(room_payload.get("starts_at")) or "")[:10],
            "source": "osu_wiki+osu_api",
            "source_type": "wiki_package_verified",
            "source_url": config.wiki_url,
        },
        {
            "event": config.event,
            "stage": stage,
            "team": right_name,
            "team_code": right_name,
            "opponent_team": left_name,
            "team_score": team_score_right,
            "opponent_score": team_score_left,
            "result": "win" if team_score_right > team_score_left else "loss" if team_score_right < team_score_left else "draw",
            "match_link": wiki_row.get("match_link"),
            "date": _clean_text(wiki_row.get("date")) or (_clean_text(room_payload.get("starts_at")) or "")[:10],
            "source": "osu_wiki+osu_api",
            "source_type": "wiki_package_verified",
            "source_url": config.wiki_url,
        },
    ]
    return map_scores, {
        "matches": match_rows,
        "validation": {
            "match_link": wiki_row.get("match_link"),
            "match_kind": "room",
            "derived_score": [derived_left, derived_right],
            "official_score": [team_score_left, team_score_right],
            "score_mismatch": [derived_left, derived_right] != [team_score_left, team_score_right],
            "skipped_beatmaps": skipped_beatmaps,
            "map_rows": len(map_scores),
        },
    }


def _assign_match_indexes(match_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counters: dict[tuple[str, str | None, str], int] = {}
    sorted_rows = sorted(
        match_rows,
        key=lambda row: (
            _clean_text(row.get("event")) or "",
            _CANONICAL_STAGE_ORDER.get(_clean_text(row.get("stage")) or "", 99),
            _clean_text(row.get("stage")) or "",
            _clean_text(row.get("date")) or "",
            _clean_text(row.get("team")) or "",
            _clean_text(row.get("match_link")) or "",
        ),
    )
    for row in sorted_rows:
        key = (
            _clean_text(row.get("event")) or "",
            _clean_text(row.get("stage")),
            _clean_text(row.get("team")) or "",
        )
        row["match_index"] = counters.get(key, 0)
        counters[key] = row["match_index"] + 1
    return sorted_rows


def _infer_top_placements(match_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grand_final_rows = [row for row in match_rows if _clean_text(row.get("stage")) == "Grand Finals"]
    if not grand_final_rows:
        return []
    grand_final_rows = sorted(grand_final_rows, key=lambda row: (_clean_text(row.get("date")) or "", _clean_text(row.get("match_link")) or ""))
    final = grand_final_rows[-1]
    team_score = _to_int(final.get("team_score")) or 0
    opponent_score = _to_int(final.get("opponent_score")) or 0
    first = _clean_text(final.get("team")) if team_score > opponent_score else _clean_text(final.get("opponent_team"))
    second = _clean_text(final.get("opponent_team")) if first == _clean_text(final.get("team")) else _clean_text(final.get("team"))

    third = None
    finals_rows = [row for row in match_rows if _clean_text(row.get("stage")) == "Finals"]
    if finals_rows:
        finals_rows = sorted(finals_rows, key=lambda row: (_clean_text(row.get("date")) or "", _clean_text(row.get("match_link")) or ""))
        lower_final = finals_rows[-1]
        team_score = _to_int(lower_final.get("team_score")) or 0
        opponent_score = _to_int(lower_final.get("opponent_score")) or 0
        third = _clean_text(lower_final.get("team")) if team_score < opponent_score else _clean_text(lower_final.get("opponent_team"))

    placements = []
    if first:
        placements.append({"placement": 1, "name": first})
    if second:
        placements.append({"placement": 2, "name": second})
    if third:
        placements.append({"placement": 3, "name": third})
    return placements


def _build_stage_rows(
    *,
    config: WikiTournamentPackageConfig,
    match_rows: list[dict[str, Any]],
    mappool_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    ordered: list[str] = []
    seen: set[str] = set()
    for row in match_rows + mappool_rows:
        stage = _clean_stage_label(row.get("stage"))
        if stage is None or stage in seen:
            continue
        seen.add(stage)
        ordered.append(stage)

    rows: list[dict[str, Any]] = []
    for index, stage in enumerate(
        sorted(ordered, key=lambda value: (_CANONICAL_STAGE_ORDER.get(value, 99), ordered.index(value))),
        start=1,
    ):
        dates = [_clean_text(row.get("date")) for row in match_rows if _clean_stage_label(row.get("stage")) == stage and _clean_text(row.get("date"))]
        rows.append(
            {
                "event": config.event,
                "stage": stage,
                "stage_order": _CANONICAL_STAGE_ORDER.get(stage, index),
                "stage_type": "bracket" if canonicalize_stage(stage) else "custom",
                "starts_at": min(dates) if dates else None,
                "ends_at": max(dates) if dates else None,
                "metadata": {"wiki_url": config.wiki_url},
            }
        )
    return rows


def validate_package_payload(
    *,
    event: str,
    matches: list[dict[str, Any]],
    map_scores: list[dict[str, Any]],
    players: list[dict[str, Any]],
    mappool: list[dict[str, Any]],
    placements: dict[str, Any] | None = None,
    validation_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    player_names = {_normalize_key(row.get("player")) for row in players if _normalize_key(row.get("player"))}
    mappool_ids = {_to_int(row.get("beatmap_id")) for row in mappool if _to_int(row.get("beatmap_id")) is not None}

    link_counts: dict[str, int] = {}
    team_link_counts: dict[tuple[str, str], int] = {}
    for row in matches:
        link = _clean_text(row.get("match_link"))
        team = _clean_text(row.get("team"))
        if link:
            link_counts[link] = link_counts.get(link, 0) + 1
        if link and team:
            key = (link, team.casefold())
            team_link_counts[key] = team_link_counts.get(key, 0) + 1
    duplicate_match_links = sum(max(0, count - 2) for count in link_counts.values())
    duplicate_match_links += sum(max(0, count - 1) for count in team_link_counts.values())

    broken_player_refs = 0
    broken_map_refs = 0
    missing_timestamps = 0
    slot_inferred = 0
    for row in map_scores:
        if _clean_text(row.get("date")) is None:
            missing_timestamps += 1
        if _normalize_key(row.get("player")) not in player_names:
            broken_player_refs += 1
        beatmap_id = _to_int(row.get("beatmap_id"))
        if beatmap_id is None or (mappool_ids and beatmap_id not in mappool_ids):
            broken_map_refs += 1
        if row.get("quality") == "partial":
            slot_inferred += 1

    match_missing_dates = sum(1 for row in matches if _clean_text(row.get("date")) is None)
    api_errors = [row for row in validation_rows if row.get("error")]
    score_mismatches = [row for row in validation_rows if row.get("score_mismatch")]
    skipped_beatmaps = sum(_to_int(row.get("skipped_beatmaps")) or 0 for row in validation_rows)
    placement_records = len(placements or {})
    missing_player_placements = sum(
        1
        for row in players
        if row.get("placement") is None and row.get("placement_percentile") is None
    )
    return {
        "event": event,
        "matches": len(matches),
        "map_scores": len(map_scores),
        "players": len(players),
        "mappool": len(mappool),
        "placement_records": placement_records,
        "missing_player_placements": missing_player_placements,
        "missing_timestamps": missing_timestamps + match_missing_dates,
        "duplicate_match_links": duplicate_match_links,
        "broken_player_references": broken_player_refs,
        "broken_map_references": broken_map_refs,
        "slot_inferred_rows": slot_inferred,
        "api_errors": len(api_errors),
        "score_mismatches": len(score_mismatches),
        "skipped_beatmaps": skipped_beatmaps,
        "details": {
            "api_errors": api_errors,
            "score_mismatches": score_mismatches,
        },
    }


def build_wiki_tournament_package(
    config: WikiTournamentPackageConfig,
    *,
    client: OsuApiClient,
) -> tuple[dict[str, Any], dict[str, Any]]:
    original_delay = getattr(client, "_request_delay", None)
    if original_delay is not None:
        client._request_delay = config.api_request_delay
    try:
        page_html = _get(config.wiki_url)
        blocks = _iter_heading_blocks(page_html)
        start_date, end_date = _extract_schedule_dates(blocks)
        mappool_rows = _extract_mappool_rows(config=config, blocks=blocks)
        by_stage_lookup, global_lookup, event_mappool_ids = _build_mappool_lookup(mappool_rows)
        wiki_match_rows = _extract_match_rows(config=config, blocks=blocks)

        players: dict[tuple[str, int | None], dict[str, Any]] = {}
        map_scores: list[dict[str, Any]] = []
        match_rows: list[dict[str, Any]] = []
        validation_rows: list[dict[str, Any]] = []

        for wiki_row in wiki_match_rows:
            match_link = _clean_text(wiki_row.get("match_link"))
            if match_link is None:
                continue
            try:
                if wiki_row.get("match_kind") == "room":
                    room_payload = client.get_room(match_link)
                    if room_payload is None:
                        validation_rows.append({"match_link": match_link, "match_kind": "room", "error": "room_not_found"})
                        continue
                    room_leaderboard = client.get_room_leaderboard(match_link)
                    extracted_rows, meta = _room_to_package_rows(
                        config=config,
                        wiki_row=wiki_row,
                        room_payload=room_payload,
                        room_leaderboard=room_leaderboard,
                        client=client,
                        by_stage_lookup=by_stage_lookup,
                        global_lookup=global_lookup,
                        event_mappool_ids=event_mappool_ids,
                        players=players,
                    )
                else:
                    match_id = parse_match_id(match_link)
                    if match_id is None:
                        validation_rows.append({"match_link": match_link, "match_kind": "match", "error": "invalid_match_link"})
                        continue
                    match_payload = client.get_match(match_id)
                    if match_payload is None:
                        validation_rows.append({"match_link": match_link, "match_kind": "match", "error": "match_not_found"})
                        continue
                    extracted_rows, meta = _community_match_to_package_rows(
                        config=config,
                        wiki_row=wiki_row,
                        match_payload=match_payload,
                        by_stage_lookup=by_stage_lookup,
                        global_lookup=global_lookup,
                        event_mappool_ids=event_mappool_ids,
                        players=players,
                    )
            except OsuApiError as exc:
                validation_rows.append({"match_link": match_link, "match_kind": _clean_text(wiki_row.get("match_kind")), "error": str(exc)})
                continue

            map_scores.extend(extracted_rows)
            match_rows.extend(meta["matches"])
            validation_rows.append(meta["validation"])

        match_rows = _assign_match_indexes(match_rows)
        placements = _infer_top_placements(match_rows)
        report = validate_package_payload(
            event=config.event,
            matches=match_rows,
            map_scores=map_scores,
            players=list(players.values()),
            mappool=mappool_rows,
            placements=placements,
            validation_rows=validation_rows,
        )
        package_status = config.package_status
        production_safe = config.production_safe
        if report["missing_timestamps"] or report["duplicate_match_links"] or report["broken_map_references"]:
            package_status = "partial"
            production_safe = False

        package = {
            "package_id": config.slug,
            "package_name": config.event,
            "package_status": package_status,
            "production_safe": production_safe,
            "notes": list(config.notes),
            "source": {
                "name": "osu_wiki+osu_api",
                "type": "wiki_package",
                "url": config.wiki_url,
            },
            "events": [
                {
                    "event": config.event,
                    "display_name": config.event,
                    "short_name": config.short_name,
                    "tier": config.tier,
                    "start_date": start_date,
                    "end_date": end_date,
                    "metadata": {
                        "wiki_url": config.wiki_url,
                        "event_format": config.event_format,
                        "team_size": config.team_size,
                        "placements": placements,
                        "placements_inferred": bool(placements),
                        "validation": report,
                    },
                }
            ],
            "stages": _build_stage_rows(config=config, match_rows=match_rows, mappool_rows=mappool_rows),
            "players": sorted(players.values(), key=lambda row: (_normalize_key(row.get("player")) or "", _to_int(row.get("user_id")) or 0)),
            "mappool": mappool_rows,
            "matches": match_rows,
            "map_scores": map_scores,
        }
        return package, report
    finally:
        if original_delay is not None:
            client._request_delay = original_delay


def render_validation_report_markdown(reports: list[dict[str, Any]]) -> str:
    lines = ["# Tournament Package Validation", ""]
    for report in reports:
        lines.extend(
            [
                f"## {report['event']}",
                "",
                f"- Matches: {report['matches']}",
                f"- Map scores: {report['map_scores']}",
                f"- Players: {report['players']}",
                f"- Mappool rows: {report['mappool']}",
                f"- Inferred placement records: {report.get('placement_records', 0)}",
                f"- Players missing explicit placements: {report.get('missing_player_placements', 0)}",
                f"- Missing timestamps: {report['missing_timestamps']}",
                f"- Duplicate match links: {report['duplicate_match_links']}",
                f"- Broken player references: {report['broken_player_references']}",
                f"- Broken map references: {report['broken_map_references']}",
                f"- Slot-inferred rows: {report['slot_inferred_rows']}",
                f"- API errors: {report['api_errors']}",
                f"- Score mismatches: {report['score_mismatches']}",
                f"- Skipped non-mappool beatmaps: {report['skipped_beatmaps']}",
                "",
            ]
        )
        mismatches = report.get("details", {}).get("score_mismatches") or []
        if mismatches:
            lines.append("Score mismatches:")
            for row in mismatches[:10]:
                lines.append(f"- {row.get('match_link')}: derived {row.get('derived_score')} vs official {row.get('official_score')}")
            lines.append("")
        api_errors = report.get("details", {}).get("api_errors") or []
        if api_errors:
            lines.append("API errors:")
            for row in api_errors[:10]:
                lines.append(f"- {row.get('match_link')}: {row.get('error')}")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_package_and_report(
    *,
    package: dict[str, Any],
    report: dict[str, Any],
    package_dir: str | Path,
    report_dir: str | Path,
) -> tuple[Path, Path, Path]:
    package_root = Path(package_dir)
    report_root = Path(report_dir)
    status_dir = package_root / (package.get("package_status") or "unlabeled")
    status_dir.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    package_path = status_dir / f"{package['package_id']}.json"
    report_json_path = report_root / f"{package['package_id']}_validation.json"
    report_md_path = report_root / f"{package['package_id']}_validation.md"

    package_path.write_text(json.dumps(package, indent=2, ensure_ascii=True), encoding="utf-8")
    report_json_path.write_text(json.dumps(report, indent=2, ensure_ascii=True), encoding="utf-8")
    report_md_path.write_text(render_validation_report_markdown([report]), encoding="utf-8")
    return package_path, report_json_path, report_md_path
