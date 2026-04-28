from __future__ import annotations

import hashlib
import html
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from importers.historical_tournament_index import (
    BRACKET_RE,
    DISCORD_RE,
    LAZER_ROOM_RE,
    MATCH_LINK_RE,
    SPREADSHEET_RE,
)


STAGE_TOURNAMENTS_URL = "https://otr.stagec.net/tournaments"
STAGE_BASE_URL = "https://otr.stagec.net"
USER_AGENT = "osu-scout/1.0 (stage-discovery-index)"

FORMAT_RE = re.compile(r"\b([1-8])\s*(?:v|vs\.?|versus)\s*([1-8])\b", re.IGNORECASE)
YEAR_RE = re.compile(r"\b(20(?:2[0-6]|1[9]))\b")
OSU_MODE_RE = re.compile(r"\b(?:osu|std|standard|osu!standard|osu!\s*std|o!std)\b", re.IGNORECASE)
NON_OSU_MODE_RE = re.compile(
    r"\b(?:taiko|catch|ctb|mania|4k|7k|mixed|multimode|multi[-\s]?mode|all\s*modes?)\b",
    re.IGNORECASE,
)

NAME_KEYS = ("tournament_name", "name", "title", "displayName", "display_name", "shortName")
URL_KEYS = ("url", "href", "link", "stage_url", "tournamentUrl", "tournament_url")
START_KEYS = ("start_date", "startDate", "startTime", "start", "starts_at", "startsAt", "beginDate", "begin_date")
END_KEYS = ("end_date", "endDate", "endTime", "end", "ends_at", "endsAt", "finishDate", "finish_date")
MODE_KEYS = ("mode", "game_mode", "gameMode", "ruleset", "rulesetName")
FORMAT_KEYS = ("format", "team_size", "teamSize", "team_size_name", "teamFormat", "lobbySize")
PLAYER_COUNT_KEYS = ("player_count", "playerCount", "playersCount", "participantCount", "participants")
MATCH_COUNT_KEYS = ("match_count", "matchCount", "matchesCount", "totalMatches")
VERIFIED_KEYS = ("verified_ratio", "verifiedRatio", "verified_rate", "verificationRate")
VERIFIED_COUNT_KEYS = ("verified_count", "verifiedCount", "verifiedMatches", "verified_matches")


@dataclass(slots=True)
class StageTournament:
    tournament_name: str
    url: str
    year: int
    start_date: str | None = None
    end_date: str | None = None
    format: str | None = None
    game_mode: str = "unknown"
    player_count: int | None = None
    match_count: int | None = None
    verified_ratio: float | None = None
    stage_url: str | None = None
    raw: dict[str, Any] | None = None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_url(url: str | None, *, base_url: str = STAGE_BASE_URL) -> str | None:
    if not url:
        return None
    cleaned = html.unescape(str(url)).strip()
    if not cleaned:
        return None
    if cleaned.startswith("/"):
        cleaned = urljoin(base_url, cleaned)
    parsed = urlparse(cleaned)
    return parsed._replace(fragment="").geturl().rstrip("/")


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"<[^>]+>", "", html.unescape(str(value)))
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def fetch_stage_html(url: str = STAGE_TOURNAMENTS_URL, *, timeout: float = 30.0) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def _first_present(payload: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    lowered = {str(key).casefold(): key for key in payload}
    for key in keys:
        actual = lowered.get(key.casefold())
        if actual is not None and payload[actual] not in (None, ""):
            return payload[actual]
    return None


def _to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return int(value)
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"\d[\d,]*", text)
    return int(match.group(0).replace(",", "")) if match else None


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None
    number = float(match.group(0))
    if "%" in text:
        return number / 100.0
    return number


def _parse_date(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    cleaned = text.replace("Z", "+00:00")
    if " " in cleaned and "T" not in cleaned:
        cleaned = cleaned.replace(" ", "T", 1)
    try:
        return datetime.fromisoformat(cleaned).date().isoformat()
    except ValueError:
        pass
    match = re.search(r"\b(20\d{2})[-/](\d{1,2})[-/](\d{1,2})\b", text)
    if match:
        year, month, day = (int(part) for part in match.groups())
        return f"{year:04d}-{month:02d}-{day:02d}"
    return None


def _infer_year(*values: Any) -> int | None:
    for value in values:
        text = clean_text(value)
        if not text:
            continue
        date_value = _parse_date(text)
        if date_value:
            return int(date_value[:4])
        match = YEAR_RE.search(text)
        if match:
            return int(match.group(1))
    return None


def _normalize_mode(value: Any, haystack: str = "") -> str:
    if isinstance(value, int):
        return {0: "osu", 1: "taiko", 2: "catch", 3: "mania", 4: "mania", 5: "mania"}.get(value, "unknown")
    numeric = _to_int(value)
    if numeric is not None and str(value).strip() == str(numeric):
        return {0: "osu", 1: "taiko", 2: "catch", 3: "mania", 4: "mania", 5: "mania"}.get(numeric, "unknown")
    text = f"{clean_text(value) or ''} {haystack}"
    lowered = text.casefold()
    if NON_OSU_MODE_RE.search(text):
        return "mixed" if "mixed" in lowered or "multi" in lowered else (
            "taiko" if "taiko" in lowered else "catch" if "catch" in lowered or "ctb" in lowered else "mania"
        )
    if OSU_MODE_RE.search(text):
        return "osu"
    return "unknown"


def _infer_format(value: Any, haystack: str = "") -> str | None:
    text = f"{clean_text(value) or ''} {haystack}"
    match = FORMAT_RE.search(text)
    if match:
        return f"{match.group(1)}v{match.group(2)}"
    numeric = _to_int(value)
    if numeric and 1 <= numeric <= 8:
        return f"{numeric}v{numeric}"
    return None


def _stable_key(name: str, year: int, stage_url: str) -> str:
    raw = f"stage|{year}|{name.casefold()}|{stage_url.casefold()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _find_json_script_payloads(html_text: str) -> list[Any]:
    payloads: list[Any] = []
    next_match = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html_text,
        re.IGNORECASE | re.DOTALL,
    )
    if next_match:
        try:
            payloads.append(json.loads(html.unescape(next_match.group(1))))
        except json.JSONDecodeError:
            pass

    for match in re.finditer(r"<script[^>]*>(.*?)</script>", html_text, re.IGNORECASE | re.DOTALL):
        script = html.unescape(match.group(1))
        if "tournament" not in script.casefold():
            continue
        for object_match in re.finditer(r"(\{[^{}]*(?:tournament|Tournament)[^{}]*\})", script):
            try:
                payloads.append(json.loads(object_match.group(1)))
            except json.JSONDecodeError:
                continue
    return payloads


def _walk_json(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_json(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_json(child)


def _looks_like_tournament_object(value: dict[str, Any]) -> bool:
    if not any(key in value for key in NAME_KEYS):
        return False
    signal_keys = set(URL_KEYS + START_KEYS + END_KEYS + MODE_KEYS + FORMAT_KEYS + PLAYER_COUNT_KEYS + MATCH_COUNT_KEYS)
    return any(key in value for key in signal_keys)


def _count_verified_matches(value: Any) -> tuple[int, int]:
    total = 0
    verified = 0
    for node in _walk_json(value):
        if not isinstance(node, dict):
            continue
        if any(key.casefold() in {"matchid", "match_id", "lobbyid", "roomid"} for key in node):
            total += 1
            text = json.dumps(node, ensure_ascii=False).casefold()
            if '"verified": true' in text or '"isverified": true' in text or '"status": "verified"' in text:
                verified += 1
    return verified, total


def _extract_links(raw: dict[str, Any]) -> dict[str, list[str]]:
    text = json.dumps(raw, ensure_ascii=False)
    return {
        "spreadsheets": sorted(set(SPREADSHEET_RE.findall(text))),
        "brackets": sorted(set(BRACKET_RE.findall(text))),
        "discords": sorted(set(DISCORD_RE.findall(text))),
        "match_links": sorted(set(f"https://osu.ppy.sh/community/matches/{mid}" for mid in MATCH_LINK_RE.findall(text))),
        "lazer_rooms": sorted(set(f"https://osu.ppy.sh/multiplayer/rooms/{rid}" for rid in LAZER_ROOM_RE.findall(text))),
    }


def _tournament_from_object(raw: dict[str, Any]) -> StageTournament | None:
    name = clean_text(_first_present(raw, NAME_KEYS))
    if not name:
        return None
    raw_url = normalize_url(_first_present(raw, URL_KEYS))
    identifier = _first_present(raw, ("id", "slug", "tournament_id", "tournamentId"))
    if raw_url is None and identifier is not None:
        raw_url = normalize_url(f"/tournaments/{identifier}")
    if raw_url is None:
        raw_url = STAGE_TOURNAMENTS_URL

    start_date = _parse_date(_first_present(raw, START_KEYS))
    end_date = _parse_date(_first_present(raw, END_KEYS))
    year = _infer_year(start_date, end_date, name, raw_url)
    if year is None:
        return None

    haystack = f"{name} {raw_url} {json.dumps(raw, ensure_ascii=False)[:4000]}"
    match_count = _to_int(_first_present(raw, MATCH_COUNT_KEYS))
    player_count = _to_int(_first_present(raw, PLAYER_COUNT_KEYS))
    verified_ratio = _to_float(_first_present(raw, VERIFIED_KEYS))
    verified_count = _to_int(_first_present(raw, VERIFIED_COUNT_KEYS))
    verification_status = _to_int(raw.get("verificationStatus"))
    inferred_verified, inferred_matches = _count_verified_matches(raw)
    if match_count is None and inferred_matches > 0:
        match_count = inferred_matches
    if verified_ratio is None and verified_count is not None and match_count:
        verified_ratio = verified_count / match_count
    if verified_ratio is None and inferred_matches > 0:
        verified_ratio = inferred_verified / inferred_matches
    if verified_ratio is None and verification_status is not None:
        # Stage verificationStatus 4 is verified in exported rows; 3 is a
        # rejected/partial state in the same payloads.
        verified_ratio = 1.0 if verification_status >= 4 else 0.0
    if verified_ratio is not None and verified_ratio > 1:
        verified_ratio = verified_ratio / 100.0

    return StageTournament(
        tournament_name=name,
        url=raw_url,
        year=year,
        start_date=start_date,
        end_date=end_date,
        format=_infer_format(_first_present(raw, FORMAT_KEYS), haystack),
        game_mode=_normalize_mode(_first_present(raw, MODE_KEYS), haystack),
        player_count=player_count,
        match_count=match_count,
        verified_ratio=max(0.0, min(1.0, verified_ratio)) if verified_ratio is not None else None,
        stage_url=raw_url,
        raw=raw,
    )


def parse_stage_payload(payload: Any) -> list[StageTournament]:
    if isinstance(payload, dict) and isinstance(payload.get("log"), dict):
        return parse_stage_har(payload)

    tournaments: list[StageTournament] = []
    seen: set[str] = set()
    for node in _walk_json(payload):
        if not _looks_like_tournament_object(node):
            continue
        tournament = _tournament_from_object(node)
        if tournament is None:
            continue
        key = f"{tournament.year}|{tournament.tournament_name.casefold()}|{tournament.url.casefold()}"
        if key in seen:
            continue
        seen.add(key)
        tournaments.append(tournament)
    return tournaments


def parse_stage_har(payload: dict[str, Any]) -> list[StageTournament]:
    """Extract Stage tournament payloads from a browser-exported HAR file."""
    tournaments: list[StageTournament] = []
    entries = payload.get("log", {}).get("entries", [])
    if not isinstance(entries, list):
        return tournaments
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        request_url = str(entry.get("request", {}).get("url") or "")
        response = entry.get("response") if isinstance(entry.get("response"), dict) else {}
        content = response.get("content") if isinstance(response.get("content"), dict) else {}
        text = content.get("text")
        if not text or "tournament" not in f"{request_url} {text}".casefold():
            continue
        mime_type = str(content.get("mimeType") or "")
        if "json" in mime_type or text.strip().startswith(("{", "[")):
            try:
                tournaments.extend(parse_stage_payload(json.loads(text)))
            except json.JSONDecodeError:
                continue
        elif "html" in mime_type or "<html" in text[:500].casefold():
            tournaments.extend(parse_stage_html(text))
    return tournaments


def parse_stage_html(html_text: str) -> list[StageTournament]:
    tournaments: list[StageTournament] = []
    for payload in _find_json_script_payloads(html_text):
        tournaments.extend(parse_stage_payload(payload))
    if tournaments:
        return tournaments

    # Conservative HTML fallback: index visible tournament links only.
    seen: set[str] = set()
    for href, label in re.findall(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text, re.DOTALL | re.IGNORECASE):
        name = clean_text(label)
        url = normalize_url(href)
        if not name or not url or "/tournament" not in urlparse(url).path.casefold():
            continue
        year = _infer_year(name, url)
        if year is None:
            continue
        key = f"{year}|{name.casefold()}|{url.casefold()}"
        if key in seen:
            continue
        seen.add(key)
        tournaments.append(
            StageTournament(
                tournament_name=name,
                url=url,
                year=year,
                game_mode=_normalize_mode(None, f"{name} {url}"),
                stage_url=url,
                raw={"html_fallback": True, "href": href, "label": name},
            )
        )
    return tournaments


def load_stage_source(*, cache_path: str | Path | None = None, url: str = STAGE_TOURNAMENTS_URL) -> tuple[list[StageTournament], dict[str, Any]]:
    source_meta: dict[str, Any] = {"source_url": url}
    if cache_path:
        path = Path(cache_path)
        raw_text = path.read_text(encoding="utf-8")
        source_meta["cache_path"] = str(path)
        if path.suffix.casefold() in {".json", ".har"}:
            payload = json.loads(raw_text)
            return parse_stage_payload(payload), source_meta
        return parse_stage_html(raw_text), source_meta
    html_text = fetch_stage_html(url)
    return parse_stage_html(html_text), source_meta


def canonical_name(value: str | None) -> str:
    text = clean_text(value) or ""
    text = text.casefold().replace("osu!", "osu")
    text = re.sub(r"\b(?:registrations?|open|std|standard)\b", " ", text)
    return "".join(ch for ch in text if ch.isalnum())


def cross_reference_stage_tournaments(
    tournaments: list[StageTournament],
    existing_sources: Iterable[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    by_year: dict[int, list[dict[str, Any]]] = {}
    for source in existing_sources:
        try:
            year = int(source.get("year"))
        except (TypeError, ValueError):
            continue
        by_year.setdefault(year, []).append(source)

    matches: dict[str, dict[str, Any]] = {}
    for tournament in tournaments:
        target = canonical_name(tournament.tournament_name)
        if not target:
            continue
        best: dict[str, Any] | None = None
        best_score = 0
        for candidate in by_year.get(tournament.year, []):
            candidate_name = canonical_name(candidate.get("tournament_name"))
            if not candidate_name:
                continue
            score = 0
            if target == candidate_name:
                score = 100
            elif target in candidate_name or candidate_name in target:
                score = 70
            elif target[:8] and target[:8] in candidate_name:
                score = 45
            if score > best_score:
                best_score = score
                best = candidate
        if best and best_score >= 45:
            matches[f"{tournament.year}|{tournament.tournament_name.casefold()}|{tournament.url.casefold()}"] = best
    return matches


def classify_stage_tournament(tournament: StageTournament, linked_source: dict[str, Any] | None) -> str:
    if tournament.game_mode != "osu":
        return "ignore"
    has_external = bool(
        linked_source
        and (
            linked_source.get("forum_url")
            or linked_source.get("wiki_url")
            or linked_source.get("spreadsheet_url")
            or linked_source.get("bracket_url")
            or linked_source.get("linked_match_urls")
            or linked_source.get("lazer_room_urls")
        )
    )
    if not has_external and tournament.raw:
        has_external = bool(
            tournament.raw.get("forumUrl")
            or tournament.raw.get("wikiUrl")
            or tournament.raw.get("spreadsheetUrl")
            or tournament.raw.get("bracketUrl")
        )
    verified_ratio = tournament.verified_ratio or 0.0
    match_count = tournament.match_count or 0
    if has_external and match_count >= 20 and verified_ratio >= 0.80:
        return "production_safe"
    if has_external and match_count >= 8 and verified_ratio >= 0.50:
        return "likely_importable"
    if match_count > 0:
        return "stage_only"
    if has_external or tournament.start_date or tournament.end_date:
        return "partial"
    return "ignore"


def _quality_for_classification(classification: str) -> str:
    return {
        "production_safe": "verified",
        "likely_importable": "high",
        "stage_only": "partial",
        "partial": "partial",
        "ignore": "low",
    }.get(classification, "partial")


def _priority_score(tournament: StageTournament, classification: str, linked_source: dict[str, Any] | None) -> int:
    score = {
        "production_safe": 1000,
        "likely_importable": 750,
        "stage_only": 450,
        "partial": 200,
        "ignore": 0,
    }.get(classification, 0)
    score += min(tournament.match_count or 0, 200) * 3
    score += min(tournament.player_count or 0, 512) // 8
    score += int((tournament.verified_ratio or 0.0) * 200)
    if linked_source and linked_source.get("forum_url"):
        score += 120
    if linked_source and linked_source.get("wiki_url"):
        score += 100
    if linked_source and linked_source.get("spreadsheet_url"):
        score += 80
    if linked_source and linked_source.get("bracket_url"):
        score += 80
    return score


def _merge_list_field(primary: Any, secondary: Any) -> list[str]:
    values: list[str] = []
    for source in (primary, secondary):
        if not source:
            continue
        if isinstance(source, str):
            try:
                parsed = json.loads(source)
            except json.JSONDecodeError:
                parsed = [source]
        else:
            parsed = source
        if isinstance(parsed, list):
            values.extend(str(item) for item in parsed if item)
    seen: set[str] = set()
    result: list[str] = []
    for item in values:
        key = item.casefold()
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def stage_tournaments_to_source_rows(
    tournaments: list[StageTournament],
    *,
    existing_sources: Iterable[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    linked_by_key = cross_reference_stage_tournaments(tournaments, existing_sources)
    rows: list[dict[str, Any]] = []
    now = utc_now_iso()
    for tournament in tournaments:
        stage_key = f"{tournament.year}|{tournament.tournament_name.casefold()}|{tournament.url.casefold()}"
        linked = linked_by_key.get(stage_key)
        links = _extract_links(tournament.raw or {})
        classification = classify_stage_tournament(tournament, linked)
        metadata = {
            "stage": {
                "source": "o!TR Stage",
                "not_sole_source_of_truth": True,
                "raw": tournament.raw,
                "extracted_links": links,
            },
            "linked_source_key": linked.get("tournament_key") if linked else None,
            "linked_source_name": linked.get("tournament_name") if linked else None,
            "has_forum_cross_reference": bool(linked and linked.get("forum_url")),
            "has_wiki_cross_reference": bool(linked and linked.get("wiki_url")),
        }
        match_links = _merge_list_field(links["match_links"], linked.get("linked_match_urls") if linked else None)
        room_links = _merge_list_field(links["lazer_rooms"], linked.get("lazer_room_urls") if linked else None)
        rows.append(
            {
                "tournament_key": _stable_key(tournament.tournament_name, tournament.year, tournament.url),
                "tournament_name": tournament.tournament_name,
                "year": tournament.year,
                "source_url": tournament.url,
                "forum_url": normalize_url((tournament.raw or {}).get("forumUrl")) or (linked.get("forum_url") if linked else None),
                "wiki_url": linked.get("wiki_url") if linked else None,
                "spreadsheet_url": (links["spreadsheets"] or [linked.get("spreadsheet_url") if linked else None])[0],
                "bracket_url": (links["brackets"] or [linked.get("bracket_url") if linked else None])[0],
                "discord_url": (links["discords"] or [linked.get("discord_url") if linked else None])[0],
                "created_at": tournament.start_date,
                "last_post_at": tournament.end_date,
                "rank_range": linked.get("rank_range") if linked else None,
                "team_size": str((tournament.raw or {}).get("lobbySize")) if (tournament.raw or {}).get("lobbySize") else (linked.get("team_size") if linked else None),
                "format": tournament.format or (linked.get("format") if linked else None),
                "status": "discovered",
                "last_checked_at": now,
                "data_quality": _quality_for_classification(classification),
                "notes": f"Stage discovery classification={classification}; Stage is enrichment only, not sole source of truth.",
                "source": "stage",
                "source_type": "stage_tournament_index",
                "linked_match_urls": match_links,
                "lazer_room_urls": room_links,
                "linked_source_key": linked.get("tournament_key") if linked else None,
                "priority_score": _priority_score(tournament, classification, linked),
                "start_date": tournament.start_date,
                "end_date": tournament.end_date,
                "game_mode": tournament.game_mode,
                "player_count": tournament.player_count,
                "match_count": tournament.match_count,
                "verified_ratio": tournament.verified_ratio,
                "stage_url": tournament.stage_url or tournament.url,
                "classification": classification,
                "metadata_json": metadata,
                "discovered_at": now,
            }
        )
    return rows


def build_stage_import_queue(rows: Iterable[dict[str, Any]], *, limit: int = 50) -> list[dict[str, Any]]:
    candidates = [
        row for row in rows
        if row.get("classification") in {"production_safe", "likely_importable", "stage_only"}
        and row.get("game_mode") == "osu"
    ]

    def date_sort_value(row: dict[str, Any]) -> str:
        return str(row.get("end_date") or row.get("start_date") or "")

    candidates.sort(
        key=lambda row: (
            {"production_safe": 0, "likely_importable": 1, "stage_only": 2}.get(str(row.get("classification")), 9),
            -float(row.get("verified_ratio") or 0.0),
            -int(row.get("match_count") or 0),
            -int(row.get("player_count") or 0),
            date_sort_value(row),
        )
    )
    queue: list[dict[str, Any]] = []
    for index, row in enumerate(candidates[:limit], start=1):
        queue.append(
            {
                "queue_rank": index,
                "import_status": "manual_approval_required",
                "tournament_key": row.get("tournament_key"),
                "tournament_name": row.get("tournament_name"),
                "year": row.get("year"),
                "classification": row.get("classification"),
                "data_quality": row.get("data_quality"),
                "game_mode": row.get("game_mode"),
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "format": row.get("format"),
                "player_count": row.get("player_count"),
                "match_count": row.get("match_count"),
                "verified_ratio": row.get("verified_ratio"),
                "stage_url": row.get("stage_url"),
                "forum_url": row.get("forum_url"),
                "wiki_url": row.get("wiki_url"),
                "spreadsheet_url": row.get("spreadsheet_url"),
                "bracket_url": row.get("bracket_url"),
                "match_link_count": len(row.get("linked_match_urls") or []),
                "lazer_room_count": len(row.get("lazer_room_urls") or []),
                "priority_score": row.get("priority_score") or 0,
                "notes": row.get("notes"),
            }
        )
    return queue
