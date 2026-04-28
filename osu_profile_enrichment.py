from __future__ import annotations

import json
import os
import time
from dataclasses import replace
from datetime import timezone, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from models import EventInput, PlayerInput
from storage import (
    fetch_cached_osu_user_profile,
    get_connection,
    resolve_player_identity,
    upsert_osu_user_profiles,
)

OSU_OAUTH_TOKEN_URL = "https://osu.ppy.sh/oauth/token"
OSU_API_BASE = "https://osu.ppy.sh/api/v2"
DEFAULT_PROFILE_CACHE_TTL_HOURS = 24.0 * 7.0
DEFAULT_PROFILE_REQUEST_DELAY_SECONDS = 0.05


class OsuProfileError(RuntimeError):
    pass


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


def _dedupe_names(names: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for name in names:
        text = _clean_text(name)
        if text is None:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
    return output


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        env_path = Path(".env")
        if not env_path.exists():
            return
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class OsuProfileClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        request_delay: float = DEFAULT_PROFILE_REQUEST_DELAY_SECONDS,
    ) -> None:
        if not client_id or not client_secret:
            raise OsuProfileError("OSU_CLIENT_ID and OSU_CLIENT_SECRET must be set.")
        self._client_id = client_id
        self._client_secret = client_secret
        self._request_delay = request_delay
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    @classmethod
    def from_env(cls) -> "OsuProfileClient":
        _load_env()
        return cls(
            client_id=os.getenv("OSU_CLIENT_ID", ""),
            client_secret=os.getenv("OSU_CLIENT_SECRET", ""),
        )

    def _ensure_token(self) -> None:
        if self._token and time.time() < self._token_expires_at - 30:
            return
        request = Request(
            OSU_OAUTH_TOKEN_URL,
            data=json.dumps(
                {
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "grant_type": "client_credentials",
                    "scope": "public",
                }
            ).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))

    def _headers(self) -> dict[str, str]:
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
            "User-Agent": "osu-scout/1.0",
        }

    def get_user_profile(
        self,
        identifier: int | str,
        *,
        key: str | None = None,
        mode: str = "osu",
    ) -> dict[str, Any] | None:
        url = f"{OSU_API_BASE}/users/{quote(str(identifier))}/{mode}"
        if key:
            url += "?" + urlencode({"key": key})
        request = Request(url, headers=self._headers())
        try:
            with urlopen(request, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 404:
                return None
            raise OsuProfileError(f"osu! user request failed: {exc.code}") from exc
        except URLError as exc:
            raise OsuProfileError(f"osu! user request failed: {exc}") from exc

        time.sleep(self._request_delay)
        if not isinstance(payload, dict):
            return None
        return payload


def _extract_lazer_rank(payload: dict[str, Any]) -> int | None:
    # Official user payloads we probed expose the canonical osu rank and PP,
    # but no documented, consistently separate lazer-only rank field.
    # Keep this hook so we can adopt one later without reshaping callers.
    for candidate in (
        payload.get("statistics_lazer"),
        payload.get("lazer_statistics"),
        payload.get("statistics_rulesets_lazer"),
    ):
        if isinstance(candidate, dict):
            rank = _to_int(candidate.get("global_rank"))
            if rank is not None:
                return rank
    return None


def _normalize_profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    statistics = payload.get("statistics") or {}
    if not statistics and isinstance(payload.get("statistics_rulesets"), dict):
        statistics = (payload.get("statistics_rulesets") or {}).get("osu") or {}
    return {
        "user_id": _to_int(payload.get("id")),
        "profile_username": _clean_text(payload.get("username")),
        "country_code": _clean_text(payload.get("country_code")),
        "bancho_rank": _to_int(statistics.get("global_rank")),
        "pp": _to_float(statistics.get("pp")),
        "country_rank": _to_int(statistics.get("country_rank")),
        "lazer_rank": _extract_lazer_rank(payload),
        "payload_json": payload,
        "status": "ok",
        "fetched_at": _utc_now_iso(),
    }


def _build_cache_rows(identity: dict[str, Any], result: dict[str, Any]) -> list[dict[str, Any]]:
    alias_names = _dedupe_names(
        [
            identity.get("input"),
            identity.get("canonical_name"),
            *(identity.get("names") or []),
            result.get("profile_username"),
        ]
    )
    return [
        {
            "lookup_name": lookup_name,
            "user_id": result.get("user_id"),
            "profile_username": result.get("profile_username"),
            "country_code": result.get("country_code"),
            "bancho_rank": result.get("bancho_rank"),
            "pp": result.get("pp"),
            "country_rank": result.get("country_rank"),
            "lazer_rank": result.get("lazer_rank"),
            "payload_json": result.get("payload_json"),
            "status": result.get("status"),
            "fetched_at": result.get("fetched_at"),
        }
        for lookup_name in alias_names
    ]


def _fetch_profile_for_identity(
    client: OsuProfileClient,
    identity: dict[str, Any],
) -> dict[str, Any]:
    user_ids = [int(user_id) for user_id in identity.get("user_ids") or []]
    candidate_names = _dedupe_names(
        [
            identity.get("input"),
            identity.get("canonical_name"),
            *(identity.get("names") or []),
        ]
    )

    for user_id in user_ids:
        payload = client.get_user_profile(user_id)
        if payload:
            return _normalize_profile_payload(payload)

    for candidate_name in candidate_names:
        payload = client.get_user_profile(candidate_name, key="username")
        if payload:
            return _normalize_profile_payload(payload)

    return {
        "user_id": user_ids[0] if user_ids else None,
        "profile_username": None,
        "country_code": None,
        "bancho_rank": None,
        "pp": None,
        "country_rank": None,
        "lazer_rank": None,
        "payload_json": None,
        "status": "not_found",
        "fetched_at": _utc_now_iso(),
    }


def _merge_players(existing: PlayerInput, incoming: PlayerInput) -> PlayerInput:
    metadata = dict(existing.metadata)
    metadata.update(incoming.metadata)
    skillsets = dict(existing.skillset_subscores)
    skillsets.update(incoming.skillset_subscores)
    return PlayerInput(
        username=existing.username,
        user_id=incoming.user_id or existing.user_id,
        profile_username=incoming.profile_username or existing.profile_username,
        pp=incoming.pp if incoming.pp is not None else existing.pp,
        country_rank=incoming.country_rank if incoming.country_rank is not None else existing.country_rank,
        country_code=incoming.country_code or existing.country_code,
        elitebotix_rating=incoming.elitebotix_rating if incoming.elitebotix_rating is not None else existing.elitebotix_rating,
        skill_issue_rating=incoming.skill_issue_rating if incoming.skill_issue_rating is not None else existing.skill_issue_rating,
        bancho_rank=incoming.bancho_rank if incoming.bancho_rank is not None else existing.bancho_rank,
        lazer_rank=incoming.lazer_rank if incoming.lazer_rank is not None else existing.lazer_rank,
        tournaments_played_last_12m=max(
            existing.tournaments_played_last_12m or 0,
            incoming.tournaments_played_last_12m or 0,
        ) or None,
        days_since_last_event=min(
            value
            for value in [existing.days_since_last_event, incoming.days_since_last_event]
            if value is not None
        ) if any(value is not None for value in [existing.days_since_last_event, incoming.days_since_last_event]) else None,
        consistency_metric=incoming.consistency_metric if incoming.consistency_metric is not None else existing.consistency_metric,
        skillset_subscores=skillsets,
        metadata=metadata,
    )


def _dedupe_players(players: Iterable[PlayerInput]) -> list[PlayerInput]:
    merged: dict[str, PlayerInput] = {}
    for player in players:
        key = player.username.casefold()
        if key not in merged:
            merged[key] = player
        else:
            merged[key] = _merge_players(merged[key], player)
    return sorted(merged.values(), key=lambda player: player.username.casefold())


def _dedupe_events(events: Iterable[EventInput]) -> list[EventInput]:
    seen: set[tuple[Any, ...]] = set()
    output: list[EventInput] = []
    for event in events:
        key = (
            event.username.casefold(),
            event.event_name,
            event.event_date,
            event.days_since_event,
            event.impact_score,
            event.match_cost,
            event.win_rate,
            event.placement_percentile,
            event.strength_of_schedule,
            event.event_tier_weight,
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(event)
    return sorted(output, key=lambda event: (event.username.casefold(), event.event_name or ""))


def enrich_players_with_osu_profiles(
    players: list[PlayerInput],
    events: list[EventInput],
    *,
    cache_ttl_hours: float | None = DEFAULT_PROFILE_CACHE_TTL_HOURS,
    db_path: str | Path | None = None,
) -> tuple[list[PlayerInput], list[EventInput], dict[str, int]]:
    if not players:
        return players, events, {"cached": 0, "fetched": 0, "not_found": 0, "errors": 0, "skipped": 0}

    stats = {"cached": 0, "fetched": 0, "not_found": 0, "errors": 0, "skipped": 0}
    try:
        client = OsuProfileClient.from_env()
    except OsuProfileError:
        client = None
        stats["skipped"] = len(players)

    rename_map: dict[str, str] = {}
    enriched_players: list[PlayerInput] = []

    with get_connection(db_path) as connection:
        for player in players:
            identity = resolve_player_identity(player.username, connection=connection)
            cached = fetch_cached_osu_user_profile(
                names=identity.get("names"),
                user_ids=identity.get("user_ids"),
                max_age_hours=cache_ttl_hours,
                db_path=db_path,
            )

            profile = cached
            if profile is not None:
                stats["cached"] += 1
            elif client is not None:
                try:
                    fetched = _fetch_profile_for_identity(client, identity)
                except OsuProfileError:
                    stats["errors"] += 1
                    fetched = None
                if fetched is not None:
                    if fetched.get("status") == "ok":
                        stats["fetched"] += 1
                    elif fetched.get("status") == "not_found":
                        stats["not_found"] += 1
                    rows = _build_cache_rows(identity, fetched)
                    if rows:
                        upsert_osu_user_profiles(rows, db_path=db_path)
                    profile = fetched

            metadata = dict(player.metadata)
            metadata["osu_profile_status"] = (profile or {}).get("status") or metadata.get("osu_profile_status") or "unavailable"
            metadata["lazer_rank_source"] = "official_api_not_available"

            profile_username = _clean_text((profile or {}).get("profile_username")) or player.profile_username
            output_username = profile_username or player.username
            if output_username.casefold() != player.username.casefold() or output_username != player.username:
                metadata["original_username"] = player.username
            rename_map[player.username.casefold()] = output_username

            enriched_players.append(
                PlayerInput(
                    username=output_username,
                    user_id=_to_int((profile or {}).get("user_id")) or player.user_id or (identity.get("user_ids") or [None])[0],
                    profile_username=profile_username or output_username,
                    pp=_to_float((profile or {}).get("pp")) if (profile or {}).get("pp") is not None else player.pp,
                    country_rank=_to_int((profile or {}).get("country_rank")) if (profile or {}).get("country_rank") is not None else player.country_rank,
                    country_code=_clean_text((profile or {}).get("country_code")) or player.country_code,
                    elitebotix_rating=player.elitebotix_rating,
                    skill_issue_rating=player.skill_issue_rating,
                    bancho_rank=_to_int((profile or {}).get("bancho_rank")) if (profile or {}).get("bancho_rank") is not None else player.bancho_rank,
                    lazer_rank=_to_int((profile or {}).get("lazer_rank")) if (profile or {}).get("lazer_rank") is not None else player.lazer_rank,
                    tournaments_played_last_12m=player.tournaments_played_last_12m,
                    days_since_last_event=player.days_since_last_event,
                    consistency_metric=player.consistency_metric,
                    skillset_subscores=dict(player.skillset_subscores),
                    metadata=metadata,
                )
            )

    renamed_events = [
        replace(
            event,
            username=rename_map.get(event.username.casefold(), event.username),
        )
        for event in events
    ]
    return _dedupe_players(enriched_players), _dedupe_events(renamed_events), stats
