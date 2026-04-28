from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from storage import (
    fetch_cached_external_ratings,
    resolve_player_identity,
    upsert_external_ratings,
)

ROMAI_SOURCE = "romai"
ELITEBOTIX_SOURCE = "elitebotix_duel"
SKILLISSUE_SOURCE = "skillissue"

RATING_SOURCES = (
    ROMAI_SOURCE,
    ELITEBOTIX_SOURCE,
    SKILLISSUE_SOURCE,
)

MODE_ORDER = {
    "1v1": 0,
    "2v2": 1,
    "3v3": 2,
    "4v4": 3,
}


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _format_number(value: float | None, *, decimals: int = 0, suffix: str = "") -> str | None:
    if value is None:
        return None
    rounded = round(value, decimals)
    if decimals <= 0:
        return f"{int(round(rounded))}{suffix}"
    return f"{rounded:.{decimals}f}{suffix}"


def _mode_sort_key(mode: str) -> tuple[int, str]:
    return MODE_ORDER.get(mode, 99), mode


def _format_mode_map(values: dict[str, float], *, prefix: str | None = None) -> str:
    parts = []
    for mode, value in sorted(values.items(), key=lambda item: _mode_sort_key(item[0])):
        formatted = _format_number(value, decimals=0)
        if formatted is None:
            continue
        parts.append(f"{mode} {formatted}")
    if not parts:
        return "N/A"
    if prefix:
        return f"{prefix}: " + " | ".join(parts)
    return " | ".join(parts)


def _iter_mode_maps(node: Any) -> list[dict[str, float]]:
    maps: list[dict[str, float]] = []
    if isinstance(node, dict):
        extracted: dict[str, float] = {}
        for key, value in node.items():
            numeric = _coerce_float(value)
            if numeric is None:
                continue
            text_key = str(key)
            if text_key in MODE_ORDER:
                extracted[text_key] = numeric
        if extracted:
            maps.append(extracted)
        for value in node.values():
            maps.extend(_iter_mode_maps(value))
    elif isinstance(node, list):
        for value in node:
            maps.extend(_iter_mode_maps(value))
    return maps


def _get_casefold(mapping: dict[str, Any], *names: str) -> Any:
    lowered = {str(key).casefold(): value for key, value in mapping.items()}
    for name in names:
        if name.casefold() in lowered:
            return lowered[name.casefold()]
    return None


def _http_timeout() -> float:
    return _coerce_float(os.getenv("EXTERNAL_RATINGS_TIMEOUT_SECONDS")) or 4.0


def _cache_ttl_hours() -> float:
    return _coerce_float(os.getenv("EXTERNAL_RATINGS_CACHE_TTL_HOURS")) or 12.0


def _romai_base_url() -> str:
    return (os.getenv("ROMAI_BASE_URL") or "https://rom-ai-site.vercel.app").rstrip("/")


def _skillissue_base_url() -> str:
    return (os.getenv("SKILLISSUE_BASE_URL") or "").strip().rstrip("/")


def _skillissue_template() -> str:
    return (os.getenv("SKILLISSUE_API_URL_TEMPLATE") or "").strip()


def _elitebotix_template() -> str:
    return (os.getenv("ELITEBOTIX_API_URL_TEMPLATE") or "").strip()


def _candidate_names(identity: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for name in [
        identity.get("input"),
        identity.get("canonical_name"),
        *(identity.get("names") or []),
    ]:
        text = _clean_text(name)
        if text is None:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def _request_json(url: str) -> dict[str, Any] | list[Any] | None:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "osu-scout/1.0",
        },
    )
    try:
        with urlopen(request, timeout=_http_timeout()) as response:
            status_code = getattr(response, "status", None) or response.getcode()
            if status_code == 404:
                return None
            payload_bytes = response.read()
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise

    payload_text = payload_bytes.decode("utf-8").strip()
    if not payload_text:
        return None
    payload = json.loads(payload_text)
    if isinstance(payload, (dict, list)):
        return payload
    return None


def _cache_rows_for_result(
    *,
    source: str,
    identity: dict[str, Any],
    result: dict[str, Any],
) -> list[dict[str, Any]]:
    if result.get("status") not in {"ok", "not_found"}:
        return []

    alias_names = _candidate_names(identity)
    for extra_name in [result.get("canonical_name"), result.get("lookup_name")]:
        text = _clean_text(extra_name)
        if text and text.casefold() not in {name.casefold() for name in alias_names}:
            alias_names.append(text)

    payload = result.get("payload_json")
    if payload is not None and not isinstance(payload, str):
        payload = json.dumps(payload, ensure_ascii=True)

    rows = []
    for lookup_name in alias_names:
        rows.append(
            {
                "source": source,
                "lookup_name": lookup_name,
                "canonical_name": result.get("canonical_name") or identity.get("canonical_name"),
                "user_id": result.get("user_id"),
                "display_value": result.get("display_value"),
                "payload_json": payload,
                "status": result.get("status"),
                "fetched_at": result.get("fetched_at"),
            }
        )
    return rows


def _fetch_romai(identity: dict[str, Any]) -> dict[str, Any]:
    saw_response = False
    saw_request_error = False
    for candidate in _candidate_names(identity):
        try:
            payload = _request_json(f"{_romai_base_url()}/api/player/{quote(candidate)}")
        except (HTTPError, URLError, OSError, ValueError):
            saw_request_error = True
            continue
        saw_response = True
        if not isinstance(payload, dict):
            continue

        current_modes = {
            mode: value
            for mode, value in (_get_casefold(payload, "elo") or {}).items()
            if mode in MODE_ORDER and (_coerce_float(value) or 0) > 0
        } if isinstance(_get_casefold(payload, "elo"), dict) else {}

        peak_modes: dict[str, float] = {}
        for mode_map in _iter_mode_maps(_get_casefold(payload, "peak")):
            for mode, value in mode_map.items():
                peak_modes[mode] = max(peak_modes.get(mode, 0.0), value)
        for mode_map in _iter_mode_maps(_get_casefold(payload, "seasons")):
            for mode, value in mode_map.items():
                peak_modes[mode] = max(peak_modes.get(mode, 0.0), value)

        if current_modes:
            display_value = _format_mode_map(current_modes)
        elif peak_modes:
            display_value = _format_mode_map(peak_modes, prefix="Peak")
        else:
            display_value = "Unrated"

        return {
            "source": ROMAI_SOURCE,
            "lookup_name": candidate,
            "canonical_name": _get_casefold(payload, "osuUserName", "username", "name") or candidate,
            "user_id": _get_casefold(payload, "osuUserId", "userId"),
            "display_value": display_value,
            "payload_json": payload,
            "status": "ok",
        }

    return {
        "source": ROMAI_SOURCE,
        "canonical_name": identity.get("canonical_name"),
        "user_id": (identity.get("user_ids") or [None])[0],
        "display_value": "N/A",
        "payload_json": None,
        "status": "error" if saw_request_error and not saw_response else "not_found",
    }


def _extract_named_rating(payload: dict[str, Any]) -> str | None:
    rating_block = _get_casefold(payload, "rating")
    if isinstance(rating_block, dict):
        name = _clean_text(_get_casefold(rating_block, "name", "tier", "league"))
        value = _coerce_float(_get_casefold(rating_block, "value", "rating", "total", "sr"))
        if name and value is not None:
            return f"{name} ({_format_number(value, decimals=1)})"
        if name:
            return name
        if value is not None:
            return _format_number(value, decimals=1)

    total_value = _coerce_float(
        _get_casefold(
            payload,
            "totalRating",
            "total_rating",
            "duelRating",
            "duel_rating",
            "rating",
            "value",
        )
    )
    tier_name = _clean_text(_get_casefold(payload, "league", "tier", "rank", "name"))
    if tier_name and total_value is not None:
        payload_keys = {str(key).casefold() for key in payload.keys()}
        suffix = "*" if "rating" in payload_keys else ""
        return f"{tier_name} ({_format_number(total_value, decimals=2, suffix=suffix)})"
    if total_value is not None:
        return _format_number(total_value, decimals=2)
    return None


def _fetch_skillissue(identity: dict[str, Any]) -> dict[str, Any]:
    base_url = _skillissue_base_url()
    template = _skillissue_template()
    if not base_url and not template:
        return {
            "source": SKILLISSUE_SOURCE,
            "display_value": "N/A",
            "status": "unavailable",
        }

    requests_to_try: list[tuple[str, str | None, int | None]] = []
    user_ids = [int(user_id) for user_id in identity.get("user_ids") or []]
    for user_id in user_ids:
        if template:
            url = template.format(username="", user_id=user_id)
        else:
            url = f"{base_url}/integrations/spreadsheets/player_rating?userId={user_id}"
        requests_to_try.append((url, None, user_id))
    for candidate in _candidate_names(identity):
        if template:
            url = template.format(username=quote(candidate), user_id="")
        else:
            url = f"{base_url}/integrations/spreadsheets/player_ratings?username={quote(candidate)}"
        requests_to_try.append((url, candidate, None))

    saw_response = False
    saw_request_error = False
    for url, candidate_name, forced_user_id in requests_to_try:
        try:
            payload = _request_json(url)
        except (HTTPError, URLError, OSError, ValueError):
            saw_request_error = True
            continue
        saw_response = True
        if not isinstance(payload, dict):
            continue

        display_value = _extract_named_rating(payload)
        if not display_value:
            continue

        return {
            "source": SKILLISSUE_SOURCE,
            "lookup_name": candidate_name,
            "canonical_name": _get_casefold(payload, "activeUsername", "username", "name") or identity.get("canonical_name"),
            "user_id": _get_casefold(payload, "playerId", "userId") or forced_user_id,
            "display_value": display_value,
            "payload_json": payload,
            "status": "ok",
        }

    return {
        "source": SKILLISSUE_SOURCE,
        "canonical_name": identity.get("canonical_name"),
        "user_id": user_ids[0] if user_ids else None,
        "display_value": "N/A",
        "payload_json": None,
        "status": "error" if saw_request_error and not saw_response else "not_found",
    }


def _fetch_elitebotix(identity: dict[str, Any]) -> dict[str, Any]:
    template = _elitebotix_template()
    if not template:
        return {
            "source": ELITEBOTIX_SOURCE,
            "display_value": "N/A",
            "status": "unavailable",
        }

    user_ids = [int(user_id) for user_id in identity.get("user_ids") or []]
    requests_to_try: list[tuple[str, str | None, int | None]] = []
    for user_id in user_ids:
        requests_to_try.append((template.format(username="", user_id=user_id), None, user_id))
    for candidate in _candidate_names(identity):
        requests_to_try.append((template.format(username=quote(candidate), user_id=""), candidate, None))

    saw_response = False
    saw_request_error = False
    for url, candidate_name, forced_user_id in requests_to_try:
        try:
            payload = _request_json(url)
        except (HTTPError, URLError, OSError, ValueError):
            saw_request_error = True
            continue
        saw_response = True
        if not isinstance(payload, dict):
            continue

        display_value = _extract_named_rating(payload)
        if not display_value:
            continue

        return {
            "source": ELITEBOTIX_SOURCE,
            "lookup_name": candidate_name,
            "canonical_name": _get_casefold(payload, "username", "name") or identity.get("canonical_name"),
            "user_id": _get_casefold(payload, "userId", "osuUserId") or forced_user_id,
            "display_value": display_value,
            "payload_json": payload,
            "status": "ok",
        }

    return {
        "source": ELITEBOTIX_SOURCE,
        "canonical_name": identity.get("canonical_name"),
        "user_id": user_ids[0] if user_ids else None,
        "display_value": "N/A",
        "payload_json": None,
        "status": "error" if saw_request_error and not saw_response else "not_found",
    }


FETCHERS = {
    ROMAI_SOURCE: _fetch_romai,
    ELITEBOTIX_SOURCE: _fetch_elitebotix,
    SKILLISSUE_SOURCE: _fetch_skillissue,
}


def get_external_ratings(username: str) -> dict[str, str]:
    identity = resolve_player_identity(username)
    cached_rows = fetch_cached_external_ratings(
        username,
        sources=RATING_SOURCES,
        max_age_hours=_cache_ttl_hours(),
    )

    ratings = {
        ROMAI_SOURCE: "N/A",
        ELITEBOTIX_SOURCE: "N/A",
        SKILLISSUE_SOURCE: "N/A",
    }
    for source, row in cached_rows.items():
        ratings[source] = row.get("display_value") or "N/A"

    missing_sources = [source for source in RATING_SOURCES if source not in cached_rows]
    if not missing_sources:
        return {
            "romai": ratings[ROMAI_SOURCE],
            "elitebotix_duel": ratings[ELITEBOTIX_SOURCE],
            "skillissue": ratings[SKILLISSUE_SOURCE],
        }

    cache_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(len(missing_sources), len(FETCHERS))) as executor:
        future_map = {
            executor.submit(FETCHERS[source], identity): source
            for source in missing_sources
        }
        for future in as_completed(future_map):
            source = future_map[future]
            try:
                result = future.result()
            except Exception:
                continue
            if not isinstance(result, dict):
                continue
            ratings[source] = _clean_text(result.get("display_value")) or "N/A"
            cache_rows.extend(_cache_rows_for_result(
                source=source, identity=identity, result=result,
            ))

    if cache_rows:
        try:
            from storage import upsert_external_ratings
            upsert_external_ratings(cache_rows)
        except Exception:
            pass

    return {
        "romai": ratings[ROMAI_SOURCE],
        "elitebotix_duel": ratings[ELITEBOTIX_SOURCE],
        "skillissue": ratings[SKILLISSUE_SOURCE],
    }