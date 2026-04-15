"""osu! API v2 client + name-based beatmap enrichment + multiplayer match fetch.

This module has two distinct roles:

  1. METADATA enrichment source (original role)
     It takes existing rows in the SQLite DB that have a `map_name` but no
     `beatmap_id` / `star_rating`, looks them up against the osu! API by
     name, and writes the resolved metadata back.

  2. TOURNAMENT MATCH-DETAIL source (new, OWC 2025 layered ingestion)
     Given an osu! multiplayer match id (from a match link like
     https://osu.ppy.sh/community/matches/123456789), call /matches/{id}
     and return a fully-structured `Match` with every map played,
     per-player scores, winning team, acc, mods, etc.

     This is what unlocks real Map WR, Match WR, per-map opponent info,
     and canonical beatmap IDs (with star ratings) without touching a
     single CSV. Consumers: importers/owc_wiki.py -> scripts/sync_owc_2025.py.

Why name-based beatmap search still exists: the OWC leaderboard CSV format
has no beatmap ID, only a human-readable map name. Once the match-detail
path is wired in end to end, it supplants name-based search entirely for
tournament data; name search stays as a fallback for stray CSV rows.

Usage:
    from importers.osu_api import OsuApiClient, enrich_database, parse_match_id

    client = OsuApiClient.from_env()

    # Metadata enrichment (existing behavior)
    stats = enrich_database(client, dry_run=False)

    # Multiplayer match fetch (new)
    match = client.get_match(123456789)
    print(match.team_scores, len(match.games))

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


@dataclass
class MatchScore:
    """One player's score on one map in a multiplayer match."""
    user_id: int
    username: str | None
    score: int
    accuracy: float
    max_combo: int
    mods: list[str]
    team: str | None      # 'red' / 'blue' / None for headtohead
    passed: bool
    slot: int | None      # lobby slot, not mappool slot


@dataclass
class MatchGame:
    """One map played within a multiplayer match."""
    game_id: int
    beatmap_id: int | None
    beatmap_title: str | None
    beatmap_version: str | None
    star_rating: float | None
    mode: str | None
    scoring_type: str | None
    team_type: str | None
    mods: list[str]
    start_time: str | None
    end_time: str | None
    scores: list[MatchScore]
    winning_team: str | None      # 'red' / 'blue' / None
    red_total: int
    blue_total: int


@dataclass
class Match:
    """Full multiplayer match (BO9/BO11/BO13) from /matches/{id}."""
    match_id: int
    name: str | None              # lobby name, e.g. 'OWC2025: (Poland) vs (United States)'
    start_time: str | None
    end_time: str | None
    games: list[MatchGame]
    red_score: int                # total maps won by red (derived)
    blue_score: int               # total maps won by blue (derived)
    users: dict[int, str]         # user_id -> username, for all seen players


class OsuApiError(RuntimeError):
    pass


# ---------- match-link helpers ----------

_MATCH_LINK_RE = re.compile(
    r"(?:https?://)?osu\.ppy\.sh/community/matches/(?P<id>\d+)",
    re.IGNORECASE,
)


def parse_match_id(value: str | int | None) -> int | None:
    """Accept a bare int, a match URL, or None; return the match_id."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    m = _MATCH_LINK_RE.search(text)
    if m:
        return int(m.group("id"))
    return None


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

    # ---------- multiplayer match fetch ----------

    def get_match(self, match_id: int | str) -> Match | None:
        """Fetch a full multiplayer match from /matches/{id}.

        The /matches endpoint is paginated (`events` array). Each page
        returns up to ~100 events. A BO13 grand final with warmups +
        chat + bans easily exceeds that, so we walk forward using the
        `events[-1].id` as the `after` cursor until no new events come
        back.

        Returns a `Match` with every `game` event flattened into
        `match.games`, plus a username lookup. Returns None on 404.
        """
        parsed_id = parse_match_id(match_id)
        if parsed_id is None:
            raise OsuApiError(f"Cannot parse match id from: {match_id!r}")

        url = f"{OSU_API_BASE}/matches/{parsed_id}"

        users: dict[int, str] = {}
        games: list[MatchGame] = []
        match_name: str | None = None
        start_time: str | None = None
        end_time: str | None = None

        after: int | None = None
        safety = 50  # hard cap to avoid runaway pagination

        while safety > 0:
            safety -= 1
            params: dict[str, Any] = {"limit": 100}
            if after is not None:
                params["after"] = after

            response = self._session.get(
                url,
                headers=self._auth_headers(),
                params=params,
                timeout=20,
            )
            time.sleep(self._request_delay)

            if response.status_code == 404:
                return None
            if response.status_code != 200:
                raise OsuApiError(
                    f"GET match {parsed_id} -> {response.status_code} {response.text[:200]}"
                )

            payload = response.json() or {}

            meta = payload.get("match") or {}
            if match_name is None:
                match_name = meta.get("name")
            if start_time is None:
                start_time = meta.get("start_time")
            # end_time keeps getting refreshed as the match closes
            if meta.get("end_time"):
                end_time = meta.get("end_time")

            for u in payload.get("users") or []:
                uid = u.get("id")
                if uid is not None:
                    users[int(uid)] = u.get("username")

            events = payload.get("events") or []
            if not events:
                break

            for ev in events:
                if ev.get("detail", {}).get("type") == "other" and ev.get("game"):
                    game_obj = _build_match_game(ev["game"])
                    if game_obj is not None:
                        games.append(game_obj)

            last_event_id = events[-1].get("id")
            if last_event_id is None or (after is not None and last_event_id <= after):
                break
            after = int(last_event_id)

            # Stop if we've caught up to the end of the event list.
            if len(events) < 100:
                break

        red_score = sum(1 for g in games if g.winning_team == "red")
        blue_score = sum(1 for g in games if g.winning_team == "blue")

        return Match(
            match_id=parsed_id,
            name=match_name,
            start_time=start_time,
            end_time=end_time,
            games=games,
            red_score=red_score,
            blue_score=blue_score,
            users=users,
        )

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


# ---------- match parsing helpers ----------

def _build_match_game(raw: dict[str, Any]) -> MatchGame | None:
    """Convert a raw `game` block from /matches/{id} into a MatchGame."""
    try:
        game_id = int(raw.get("id"))
    except (TypeError, ValueError):
        return None

    beatmap = raw.get("beatmap") or {}
    beatmapset = beatmap.get("beatmapset") or {}

    scores: list[MatchScore] = []
    red_total = 0
    blue_total = 0

    for s in raw.get("scores") or []:
        team = (s.get("match") or {}).get("team")
        if team in ("", "none"):
            team = None
        passed = bool(s.get("passed", False))
        try:
            user_id = int(s.get("user_id") or 0)
        except (TypeError, ValueError):
            user_id = 0
        try:
            score_val = int(s.get("score") or 0)
        except (TypeError, ValueError):
            score_val = 0
        try:
            max_combo = int(s.get("max_combo") or 0)
        except (TypeError, ValueError):
            max_combo = 0

        accuracy = float(s.get("accuracy") or 0.0)
        if accuracy <= 1.0:
            # API returns 0.0-1.0; convert to percent for consistency
            accuracy *= 100.0

        mods_raw = s.get("mods") or []
        mods = [str(m) for m in mods_raw]

        scores.append(
            MatchScore(
                user_id=user_id,
                username=None,  # filled in at the Match layer via users map
                score=score_val,
                accuracy=accuracy,
                max_combo=max_combo,
                mods=mods,
                team=team,
                passed=passed,
                slot=(s.get("match") or {}).get("slot"),
            )
        )

        if passed and team == "red":
            red_total += score_val
        elif passed and team == "blue":
            blue_total += score_val

    if red_total > blue_total:
        winning_team = "red"
    elif blue_total > red_total:
        winning_team = "blue"
    else:
        winning_team = None

    return MatchGame(
        game_id=game_id,
        beatmap_id=int(beatmap.get("id")) if beatmap.get("id") is not None else None,
        beatmap_title=beatmapset.get("title"),
        beatmap_version=beatmap.get("version"),
        star_rating=float(beatmap.get("difficulty_rating")) if beatmap.get("difficulty_rating") is not None else None,
        mode=beatmap.get("mode"),
        scoring_type=raw.get("scoring_type"),
        team_type=raw.get("team_type"),
        mods=[str(m) for m in (raw.get("mods") or [])],
        start_time=raw.get("start_time"),
        end_time=raw.get("end_time"),
        scores=scores,
        winning_team=winning_team,
        red_total=red_total,
        blue_total=blue_total,
    )


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
