"""osu! API v2 client + name-based beatmap enrichment.

This module is a METADATA enrichment source, not a tournament data source.
It takes existing rows in the SQLite DB that have a `map_name` but no
`beatmap_id` / `star_rating`, looks them up against the osu! API by name,
and writes the resolved metadata back.

Why name-based: the OWC CSV format we currently import has no beatmap ID
and no beatmap URL, only a human-readable map name. Once a future source
gives us real beatmap IDs we should prefer those (`get_beatmap(id)`) and
fall back to name search only when needed.

Usage:
    from importers.osu_api import OsuApiClient, enrich_database

    client = OsuApiClient.from_env()
    stats = enrich_database(client, dry_run=False)
    print(stats)

Environment variables:
    OSU_CLIENT_ID
    OSU_CLIENT_SECRET

Get these from https://osu.ppy.sh/home/account/edit (OAuth section).
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Any

import requests

from storage import fetch_unenriched_map_keys, update_enrichment_for_map

OSU_OAUTH_TOKEN_URL = "https://osu.ppy.sh/oauth/token"
OSU_API_BASE = "https://osu.ppy.sh/api/v2"

# Polite default — osu! API allows ~1000 req/min but we want to be a good
# citizen and we have very few unique maps per tournament.
DEFAULT_REQUEST_DELAY_SECONDS = 1.0


@dataclass
class BeatmapMatch:
    """Normalized result from a beatmap search."""
    beatmap_id: int
    beatmapset_id: int
    title: str
    artist: str
    version: str          # difficulty name
    star_rating: float
    creator: str | None = None


class OsuApiError(RuntimeError):
    pass


class OsuApiClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        request_delay: float = DEFAULT_REQUEST_DELAY_SECONDS,
    ) -> None:
        if not client_id or not client_secret:
            raise OsuApiError(
                "OSU_CLIENT_ID and OSU_CLIENT_SECRET must be set. "
                "Get them at https://osu.ppy.sh/home/account/edit"
            )
        self._client_id = client_id
        self._client_secret = client_secret
        self._token: str | None = None
        self._token_expires_at: float = 0.0
        self._request_delay = request_delay
        self._session = requests.Session()

    # ---------- factory ----------

    @classmethod
    def from_env(cls) -> "OsuApiClient":
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        return cls(
            client_id=os.getenv("OSU_CLIENT_ID", ""),
            client_secret=os.getenv("OSU_CLIENT_SECRET", ""),
        )

    # ---------- auth ----------

    def _ensure_token(self) -> None:
        if self._token and time.time() < self._token_expires_at - 30:
            return

        response = self._session.post(
            OSU_OAUTH_TOKEN_URL,
            json={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "grant_type": "client_credentials",
                "scope": "public",
            },
            timeout=15,
        )
        if response.status_code != 200:
            raise OsuApiError(
                f"OAuth token request failed: {response.status_code} {response.text}"
            )
        payload = response.json()
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))

    def _auth_headers(self) -> dict[str, str]:
        self._ensure_token()
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json",
        }

    # ---------- API calls ----------

    def get_beatmap(self, beatmap_id: int) -> BeatmapMatch | None:
        """Fetch a single beatmap by its osu! beatmap_id (preferred path
        once a source gives us IDs)."""
        url = f"{OSU_API_BASE}/beatmaps/{beatmap_id}"
        response = self._session.get(url, headers=self._auth_headers(), timeout=15)
        time.sleep(self._request_delay)

        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise OsuApiError(f"GET beatmap {beatmap_id} -> {response.status_code}")

        data = response.json()
        beatmapset = data.get("beatmapset", {}) or {}
        return BeatmapMatch(
            beatmap_id=int(data["id"]),
            beatmapset_id=int(data.get("beatmapset_id") or beatmapset.get("id") or 0),
            title=str(beatmapset.get("title") or ""),
            artist=str(beatmapset.get("artist") or ""),
            version=str(data.get("version") or ""),
            star_rating=float(data.get("difficulty_rating") or 0.0),
            creator=beatmapset.get("creator"),
        )

    def search_beatmap_by_name(self, map_name: str) -> BeatmapMatch | None:
        """Resolve a free-form 'Artist - Title [Difficulty]' style string
        to a real beatmap via /beatmapsets/search.

        Strategy:
          1. Parse the input into (artist?, title?, difficulty?) using a
             permissive regex. OWC slot titles are usually 'Artist - Title'
             without a difficulty bracket.
          2. Search by the cleanest query string we can build.
          3. From returned beatmapsets, pick the best beatmap, scored by:
             - exact title match
             - exact artist match (if known)
             - exact difficulty/version match (if known)
             - osu!standard mode preferred
        Returns None if no plausible match.
        """
        artist, title, difficulty = _parse_map_name(map_name)
        if not title and not artist:
            return None

        query = " ".join(filter(None, [artist, title])).strip()
        if not query:
            return None

        url = f"{OSU_API_BASE}/beatmapsets/search"
        response = self._session.get(
            url,
            headers=self._auth_headers(),
            params={"q": query, "m": 0},  # m=0 = osu!standard
            timeout=20,
        )
        time.sleep(self._request_delay)

        if response.status_code != 200:
            raise OsuApiError(
                f"Beatmap search '{query}' -> {response.status_code} {response.text[:200]}"
            )

        payload = response.json() or {}
        beatmapsets = payload.get("beatmapsets") or []
        if not beatmapsets:
            return None

        return _pick_best_beatmap(beatmapsets, artist, title, difficulty)

    def close(self) -> None:
        self._session.close()


# ---------- parsing / matching helpers ----------

# Permissive: 'Artist - Title [Difficulty]' or 'Artist - Title'
_MAP_NAME_RE = re.compile(
    r"""^\s*
        (?P<artist>[^-\[]+?)
        \s*-\s*
        (?P<title>[^\[]+?)
        \s*
        (?:\[(?P<diff>[^\]]+)\])?
        \s*$
    """,
    re.VERBOSE,
)


def _parse_map_name(map_name: str) -> tuple[str | None, str | None, str | None]:
    if not map_name:
        return None, None, None

    text = map_name.strip()
    # Some OWC slot strings include the slot prefix like 'NM1 Artist - Title'.
    text = re.sub(r"^(NM|HD|HR|DT|FM|EZ|HT|TB)\d*\s+", "", text, flags=re.IGNORECASE)

    match = _MAP_NAME_RE.match(text)
    if not match:
        # Fall back: treat the whole thing as a title.
        return None, text, None

    return (
        (match.group("artist") or "").strip() or None,
        (match.group("title") or "").strip() or None,
        (match.group("diff") or "").strip() or None,
    )


def _norm(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _pick_best_beatmap(
    beatmapsets: list[dict[str, Any]],
    artist: str | None,
    title: str | None,
    difficulty: str | None,
) -> BeatmapMatch | None:
    target_artist = _norm(artist)
    target_title = _norm(title)
    target_diff = _norm(difficulty)

    best: tuple[int, BeatmapMatch] | None = None

    for bset in beatmapsets:
        bset_title = _norm(bset.get("title"))
        bset_artist = _norm(bset.get("artist"))

        title_score = 0
        if target_title and bset_title == target_title:
            title_score = 100
        elif target_title and target_title in bset_title:
            title_score = 60
        elif target_title and bset_title in target_title:
            title_score = 40

        artist_score = 0
        if target_artist and bset_artist == target_artist:
            artist_score = 50
        elif target_artist and target_artist in bset_artist:
            artist_score = 25

        for bm in bset.get("beatmaps") or []:
            if int(bm.get("mode_int", 0)) != 0:
                continue  # osu!standard only

            version = _norm(bm.get("version"))
            diff_score = 0
            if target_diff and version == target_diff:
                diff_score = 40
            elif target_diff and target_diff in version:
                diff_score = 20

            total = title_score + artist_score + diff_score
            if total <= 0:
                continue

            candidate = BeatmapMatch(
                beatmap_id=int(bm["id"]),
                beatmapset_id=int(bset.get("id") or bm.get("beatmapset_id") or 0),
                title=str(bset.get("title") or ""),
                artist=str(bset.get("artist") or ""),
                version=str(bm.get("version") or ""),
                star_rating=float(bm.get("difficulty_rating") or 0.0),
                creator=bset.get("creator"),
            )
            if best is None or total > best[0]:
                best = (total, candidate)

    if best is None:
        return None

    # Require at least a moderate confidence to avoid wild mismatches.
    score, match = best
    if score < 60:
        return None
    return match


# ---------- enrichment driver ----------

def enrich_database(
    client: OsuApiClient,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    verbose: bool = True,
) -> dict[str, int]:
    """Walk every (event, stage, slot, map_name) group that's still missing
    metadata, resolve it via the osu! API, and write back beatmap_id +
    star_rating to all matching rows.

    Returns a small stats dict so the caller can print a summary.
    """
    keys = fetch_unenriched_map_keys()
    if limit is not None:
        keys = keys[:limit]

    stats = {
        "groups_seen": len(keys),
        "groups_resolved": 0,
        "groups_missed": 0,
        "rows_updated": 0,
    }

    # Cache per unique map_name so identical titles across slots/stages
    # don't double-call the API.
    name_cache: dict[str, BeatmapMatch | None] = {}

    for key in keys:
        map_name = key.get("map_name") or ""
        cache_key = _norm(map_name)

        if cache_key in name_cache:
            match = name_cache[cache_key]
        else:
            try:
                match = client.search_beatmap_by_name(map_name)
            except OsuApiError as exc:
                if verbose:
                    print(f"  ! API error on '{map_name}': {exc}")
                match = None
            name_cache[cache_key] = match

        if match is None:
            stats["groups_missed"] += 1
            if verbose:
                print(f"  - no match: {map_name}")
            continue

        stats["groups_resolved"] += 1
        if verbose:
            print(
                f"  + {map_name!r} -> id={match.beatmap_id} "
                f"sr={match.star_rating:.2f} [{match.version}]"
            )

        if dry_run:
            continue

        updated = update_enrichment_for_map(
            event=key.get("event"),
            stage=key.get("stage"),
            slot=key.get("slot"),
            map_name=map_name,
            beatmap_id=match.beatmap_id,
            star_rating=match.star_rating,
        )
        stats["rows_updated"] += updated

    return stats
