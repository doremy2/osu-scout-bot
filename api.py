"""osu! scout — JSON API server.

Exposes player scouting data as REST endpoints for the future website
dashboard.  Runs alongside the Discord bot as a separate process.

Usage:
    python api.py                   # dev server on :8000
    uvicorn api:app --port 8000     # production
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from storage import (
    backfill_match_history_from_legacy,
    fetch_recent_match_history,
    fetch_discovered_tournaments,
    fetch_tournament_sources,
    init_db,
)

LEADERBOARD_PATH = Path(__file__).resolve().parent / "data" / "leaderboard_multi_event.json"

LEADERBOARD_FIELDS = (
    "rank",
    "username",
    "user_id",
    "avatar_url",
    "aliases",
    "country_code",
    "country_name",
    "country_flag_url",
    "tier",
    "final_power_score",
    "recent_tournament_form",
    "consistency_score",
    "reliability_multiplier",
    "activity_multiplier",
    "unique_tournaments_count",
    "dominant_event",
    "dominant_event_score_share",
    "team_world_cup_score_share",
    "previous_rank",
    "rank_jump",
    "confidence_label",
    "warning_flags",
    "provisional",
    "top_recent_events",
    "explanation",
)

LEADERBOARD_SORT_FIELDS = {
    "rank",
    "final_power_score",
    "recent_tournament_form",
    "consistency_score",
    "reliability_multiplier",
    "activity_multiplier",
    "tournaments_played_last_12m",
}

app = FastAPI(
    title="osu! scout API",
    version="0.1.0",
    description="Tournament scouting data for osu! players",
)

# Allow cross-origin for future web dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    """Initialize DB and backfill match history on server start."""
    init_db()
    count = await asyncio.to_thread(backfill_match_history_from_legacy)
    if count:
        print(f"[api] Backfilled {count} rows into match_history")


def _load_leaderboard() -> list[dict[str, Any]]:
    if not LEADERBOARD_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail=f"Leaderboard export not found: {LEADERBOARD_PATH}",
        )
    try:
        payload = json.loads(LEADERBOARD_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=503, detail="Leaderboard export is invalid JSON") from exc
    if not isinstance(payload, list):
        raise HTTPException(status_code=503, detail="Leaderboard export must be a JSON list")
    return [row for row in payload if isinstance(row, dict)]


def _matches_username(row: dict[str, Any], username: str) -> bool:
    target = username.casefold()
    names = [row.get("username"), row.get("profile_username"), *(row.get("aliases") or [])]
    return any(str(name).casefold() == target for name in names if name)


def _public_leaderboard_row(row: dict[str, Any]) -> dict[str, Any]:
    user_id = row.get("user_id")
    country_code = row.get("country") or row.get("country_code")
    payload = {
        "rank": row.get("rank"),
        "username": row.get("username"),
        "user_id": user_id,
        "avatar_url": f"https://a.ppy.sh/{user_id}" if user_id else None,
        "aliases": row.get("aliases") or [],
        "country_code": country_code,
        "country_name": country_code,
        "country_flag_url": f"https://flagcdn.com/w40/{str(country_code).lower()}.png" if country_code else None,
        "tier": row.get("tier"),
        "final_power_score": row.get("final_power_score"),
        "recent_tournament_form": row.get("recent_tournament_form"),
        "consistency_score": row.get("consistency_score"),
        "reliability_multiplier": row.get("reliability_multiplier"),
        "activity_multiplier": row.get("activity_multiplier"),
        "unique_tournaments_count": row.get("unique_tournaments_count"),
        "dominant_event": row.get("dominant_event"),
        "dominant_event_score_share": row.get("dominant_event_score_share"),
        "team_world_cup_score_share": row.get("team_world_cup_score_share"),
        "previous_rank": row.get("previous_rank"),
        "rank_jump": row.get("rank_jump"),
        "confidence_label": row.get("confidence_label"),
        "warning_flags": row.get("warning_flags") or [],
        "provisional": row.get("provisional"),
        "top_recent_events": row.get("top_recent_events") or [],
        "explanation": row.get("explanation"),
    }
    return {field: payload.get(field) for field in LEADERBOARD_FIELDS}


def _public_recent_match(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "match_date": row.get("match_date"),
        "tournament_name": row.get("tournament_name"),
        "stage": row.get("stage"),
        "team_name": row.get("team_name"),
        "opponent_name": row.get("opponent_name"),
        "opponent_team_name": row.get("opponent_team_name"),
        "result": row.get("result"),
        "player_score": row.get("player_score"),
        "opponent_score": row.get("opponent_score"),
        "match_link": row.get("match_link"),
        "match_id": row.get("match_id"),
        "source": row.get("source"),
        "data_quality": row.get("data_quality"),
    }


def _score_breakdown(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "final_power_score": row.get("final_power_score"),
        "base_power_score": row.get("base_power_score"),
        "elitebotix_score": row.get("elitebotix_score"),
        "skill_issue_score": row.get("skill_issue_score"),
        "bancho_score": row.get("bancho_score"),
        "lazer_score": row.get("lazer_score"),
        "recent_tournament_form": row.get("recent_tournament_form"),
        "consistency_score": row.get("consistency_score"),
        "reliability_multiplier": row.get("reliability_multiplier"),
        "activity_multiplier": row.get("activity_multiplier"),
        "tournaments_played_last_12m": row.get("tournaments_played_last_12m"),
        "unique_tournaments_count": row.get("unique_tournaments_count"),
        "dominant_event": row.get("dominant_event"),
        "dominant_event_score_share": row.get("dominant_event_score_share"),
        "team_world_cup_score_share": row.get("team_world_cup_score_share"),
        "previous_rank": row.get("previous_rank"),
        "rank_jump": row.get("rank_jump"),
        "confidence_label": row.get("confidence_label"),
        "warning_flags": row.get("warning_flags") or [],
        "days_since_last_event": row.get("days_since_last_event"),
        "activity_status": row.get("activity_status"),
        "provisional": row.get("provisional"),
        "provisional_basis": row.get("provisional_basis"),
        "bancho_rank": row.get("bancho_rank"),
        "country_rank": row.get("country_rank"),
        "pp": row.get("pp"),
    }


def _sort_leaderboard(
    rows: list[dict[str, Any]],
    *,
    sort_by: str,
    order: str,
) -> list[dict[str, Any]]:
    if sort_by not in LEADERBOARD_SORT_FIELDS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported sort_by '{sort_by}'. Supported: {sorted(LEADERBOARD_SORT_FIELDS)}",
        )
    reverse = order.casefold() == "desc"
    if order.casefold() not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")
    return sorted(
        rows,
        key=lambda row: (
            row.get(sort_by) is None,
            row.get(sort_by) if row.get(sort_by) is not None else 0,
            row.get("rank") or 0,
        ),
        reverse=reverse,
    )


# --- Leaderboard endpoints -------------------------------------------------

@app.get("/leaderboard")
async def leaderboard(
    tier: str | None = Query(default=None, description="Filter by tier, e.g. Tier 1"),
    country: str | None = Query(default=None, description="Filter by country code, e.g. PL"),
    provisional: bool | None = Query(default=None, description="Filter provisional status"),
    limit: int = Query(default=100, ge=1, le=10_000),
    offset: int = Query(default=0, ge=0),
    sort_by: str = Query(default="rank", description="rank, final_power_score, recent_tournament_form, consistency_score, reliability_multiplier, activity_multiplier, tournaments_played_last_12m"),
    order: str = Query(default="asc", description="asc or desc"),
) -> list[dict[str, Any]]:
    """Return the website-ready tournament power leaderboard."""
    rows = _load_leaderboard()
    if tier:
        rows = [row for row in rows if str(row.get("tier", "")).casefold() == tier.casefold()]
    if country:
        rows = [
            row
            for row in rows
            if str(row.get("country") or row.get("country_code") or "").casefold() == country.casefold()
        ]
    if provisional is not None:
        rows = [row for row in rows if bool(row.get("provisional")) is provisional]
    rows = _sort_leaderboard(rows, sort_by=sort_by, order=order)
    return [_public_leaderboard_row(row) for row in rows[offset:offset + limit]]


# ─── Player endpoints ──────────────────────────────────────────

@app.get("/player/{username}/power")
async def player_power(
    username: str,
    recent_match_limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, Any]:
    """Return one player's leaderboard rank, score breakdown, and recent context."""
    leaderboard_rows = _load_leaderboard()
    row = next(
        (candidate for candidate in leaderboard_rows if _matches_username(candidate, username)),
        None,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"No leaderboard data found for {username}")

    # Try file-based match data first (works even if DB is malformed)
    recent_matches = await asyncio.to_thread(
        _file_based_recent_matches,
        row.get("username") or username,
        recent_match_limit,
    )
    # Fall back to DB if file-based returns nothing
    if not recent_matches:
        try:
            recent_matches = await asyncio.to_thread(
                fetch_recent_match_history,
                row.get("username") or username,
                limit=recent_match_limit,
            )
        except Exception:
            recent_matches = []

    return {
        "username": row.get("username"),
        "profile_username": row.get("profile_username"),
        "user_id": row.get("user_id"),
        "avatar_url": f"https://a.ppy.sh/{row.get('user_id')}" if row.get("user_id") else None,
        "aliases": row.get("aliases") or [],
        "rank": row.get("rank"),
        "tier": row.get("tier"),
        "country_code": row.get("country") or row.get("country_code"),
        "country_flag_url": (
            f"https://flagcdn.com/w40/{str(row.get('country') or row.get('country_code')).lower()}.png"
            if (row.get("country") or row.get("country_code"))
            else None
        ),
        "score_breakdown": _score_breakdown(row),
        "recent_tournament_events": row.get("top_recent_events") or [],
        "recent_matches": [_public_recent_match(match) for match in recent_matches],
        "explanation": row.get("explanation"),
    }


@app.get("/player/{username}/recent-matches")
async def player_recent_matches(
    username: str,
    limit: int = Query(default=20, ge=1, le=100),
    quality: str | None = Query(default=None, description="Filter by data_quality: verified, partial, inferred, sample"),
) -> list[dict[str, Any]]:
    """Return recent match history for a player.

    Response fields per match:
    - match_date, tournament_name, stage
    - team_name, opponent_name, opponent_team_name
    - result (win/loss/draw/unknown), player_score, opponent_score
    - match_link, match_id
    - source, data_quality
    """
    rows = await asyncio.to_thread(
        fetch_recent_match_history,
        username,
        limit=limit,
        data_quality=quality,
    )

    # Clean up internal fields before returning
    cleaned = []
    for row in rows:
        entry = {
            "match_date": row.get("match_date"),
            "tournament_name": row.get("tournament_name"),
            "stage": row.get("stage"),
            "team_name": row.get("team_name"),
            "opponent_name": row.get("opponent_name"),
            "opponent_team_name": row.get("opponent_team_name"),
            "result": row.get("result"),
            "player_score": row.get("player_score"),
            "opponent_score": row.get("opponent_score"),
            "match_link": row.get("match_link"),
            "match_id": row.get("match_id"),
            "source": row.get("source"),
            "data_quality": row.get("data_quality"),
        }
        cleaned.append(entry)

    return cleaned


@app.get("/player/{username}/summary")
async def player_summary(username: str) -> dict[str, Any]:
    """Return overall scouting summary for a player."""
    from analysis import get_overall_summary

    summary = await asyncio.to_thread(get_overall_summary, username)
    if summary is None:
        raise HTTPException(status_code=404, detail=f"No data found for {username}")

    # Return a JSON-safe subset (skip raw slot dicts that may not serialize cleanly)
    return {
        "username": username,
        "total_maps_played": summary.get("total_maps_played"),
        "map_winrate": summary.get("map_winrate"),
        "match_winrate": summary.get("match_winrate"),
        "avg_performance_score": summary.get("avg_performance_score"),
        "strengths": summary.get("strengths"),
        "weaknesses": summary.get("weaknesses"),
        "recent_maps": summary.get("recent_maps", [])[:10],
        "recent_match_history": summary.get("recent_match_history", [])[:10],
    }


# ─── Tournament discovery endpoints ────────────────────────────

@app.get("/tournaments")
async def list_tournaments(
    limit: int = Query(default=50, ge=1, le=500),
    status: str | None = Query(default=None),
    game_mode: str | None = Query(default=None),
) -> list[dict[str, Any]]:
    """Return discovered tournaments from Forum 55 scraping."""
    rows = await asyncio.to_thread(
        fetch_discovered_tournaments,
        limit=limit,
        status=status,
    )
    if game_mode:
        rows = [r for r in rows if (r.get("game_mode") or "").lower() == game_mode.lower()]
    return rows


# ─── Health check ───────────────────────────────────────────────

@app.get("/tournament-sources")
async def list_tournament_sources(
    year: int | None = Query(default=None, ge=2020, le=2026),
    status: str | None = Query(default=None),
    data_quality: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=10_000),
) -> list[dict[str, Any]]:
    """Return historical tournament source index rows."""
    return await asyncio.to_thread(
        fetch_tournament_sources,
        year=year,
        status=status,
        data_quality=data_quality,
        limit=limit,
    )



# ─── File-based tournament discovery endpoints ───────────────────

def _load_tournament_catalog() -> list[dict]:
    """Merge verified packages + stage discovery + year discovery files into one catalog.

    This reads from JSON/report files only — no SQLite dependency.
    """
    import hashlib
    data_dir = Path(__file__).resolve().parent / "data"

    # 1. Imported tournaments from verified packages
    imported: dict[str, dict] = {}
    pkg_dir = data_dir / "packages" / "verified"
    if pkg_dir.exists():
        for f in sorted(pkg_dir.glob("*.json")):
            try:
                pkg = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                continue
            slug = f.stem
            source = pkg.get("source") or {}
            events = pkg.get("events") or [{}]
            event = events[0] if events else {}
            imported[slug] = {
                "slug": slug,
                "name": _pretty_tournament_name(slug),
                "year": _year_from_slug(slug),
                "game_mode": "osu",
                "format": None,
                "rank_range": None,
                "team_size": None,
                "start_date": None,
                "end_date": None,
                "stage_url": None,
                "forum_url": None,
                "wiki_url": source.get("url"),
                "source_url": source.get("url"),
                "player_count": len(pkg.get("players") or []),
                "match_count": len(pkg.get("matches") or []),
                "map_score_count": len(pkg.get("map_scores") or []),
                "classification": "imported",
                "import_status": "imported",
                "tier": event.get("tier"),
                "data_quality": "verified",
            }

    # 2. Stage discovery
    stage_file = data_dir / "reports" / "stage_tournament_discovery.json"
    discovered: list[dict] = []
    seen_names: set[str] = set()
    for slug in imported:
        seen_names.add(slug.lower())
    if stage_file.exists():
        try:
            stage = json.loads(stage_file.read_text(encoding="utf-8"))
            for row in (stage.get("rows") or []):
                name = row.get("tournament_name", "")
                norm = name.lower().replace(" ", "_").replace("!", "")
                # Skip if already imported
                if any(norm.startswith(s) or s.startswith(norm) for s in seen_names):
                    continue
                slug = hashlib.sha1(name.encode()).hexdigest()[:12]
                meta_raw = (row.get("metadata_json") or {}).get("stage", {}).get("raw", {})
                rank_lower = meta_raw.get("rankRangeLowerBound")
                rank_range_str = None
                if rank_lower and rank_lower > 0:
                    rank_range_str = f"#{rank_lower}+"
                discovered.append({
                    "slug": slug,
                    "name": name,
                    "year": row.get("year"),
                    "game_mode": row.get("game_mode") or "osu",
                    "format": row.get("format"),
                    "rank_range": rank_range_str or row.get("rank_range"),
                    "team_size": row.get("team_size"),
                    "start_date": row.get("start_date"),
                    "end_date": row.get("end_date"),
                    "stage_url": row.get("stage_url") or row.get("source_url"),
                    "forum_url": row.get("forum_url"),
                    "wiki_url": row.get("wiki_url"),
                    "source_url": row.get("source_url"),
                    "player_count": row.get("player_count"),
                    "match_count": row.get("match_count"),
                    "map_score_count": None,
                    "classification": row.get("classification") or "partial",
                    "import_status": "discovered",
                    "tier": None,
                    "data_quality": row.get("data_quality") or "partial",
                })
                seen_names.add(norm)
        except Exception:
            pass

    # 3. Year-specific discovery files (2024, 2025)
    for year in (2024, 2025):
        year_file = data_dir / "reports" / f"tournament_sources_{year}_discovery.json"
        if not year_file.exists():
            continue
        try:
            yd = json.loads(year_file.read_text(encoding="utf-8"))
            for row in (yd.get("rows") or []):
                name = row.get("tournament_name", "")
                norm = name.lower().replace(" ", "_").replace("!", "")
                if any(norm.startswith(s) or s.startswith(norm) for s in seen_names):
                    continue
                slug = hashlib.sha1(name.encode()).hexdigest()[:12]
                status = row.get("status", "discovered")
                classification = "imported" if status == "imported" else (
                    "production_safe" if row.get("production_candidate") else
                    row.get("data_quality") or "partial"
                )
                discovered.append({
                    "slug": slug,
                    "name": name,
                    "year": row.get("year") or year,
                    "game_mode": row.get("game_mode") or "unknown",
                    "format": row.get("format"),
                    "rank_range": row.get("rank_range"),
                    "team_size": row.get("team_size"),
                    "start_date": None,
                    "end_date": None,
                    "stage_url": None,
                    "forum_url": row.get("forum_url"),
                    "wiki_url": row.get("wiki_url"),
                    "source_url": row.get("source_url"),
                    "player_count": None,
                    "match_count": row.get("match_or_room_links"),
                    "map_score_count": None,
                    "classification": classification,
                    "import_status": "imported" if status == "imported" else "discovered",
                    "tier": None,
                    "data_quality": row.get("data_quality") or "partial",
                })
                seen_names.add(norm)
        except Exception:
            pass

    return list(imported.values()) + discovered


_PRETTY_NAMES: dict[str, str] = {
    "owc_2025": "osu! World Cup 2025",
    "3wc_2025": "3 Digit World Cup 2025",
    "4wc_2025": "4 Digit World Cup 2025",
    "fdc_2025": "French Draft Cup 2025",
    "lga_2025": "Liveplay Global Arena 2025",
    "oit_2025": "osu! Invitational Tournament 2025",
    "resc_2025": "Resurrection Cup 2025",
}


def _pretty_tournament_name(slug: str) -> str:
    return _PRETTY_NAMES.get(slug, slug.replace("_", " ").title())


def _year_from_slug(slug: str) -> int:
    parts = slug.split("_")
    for p in reversed(parts):
        if p.isdigit() and len(p) == 4:
            return int(p)
    return 2025



# ─── File-based match data (bypasses malformed DB) ────────────────

_PACKAGE_MATCH_CACHE: dict[str, Any] | None = None


def _load_package_match_data() -> dict[str, Any]:
    """Load and index all match + score data from verified packages.

    Returns a dict with:
        player_teams:  {username_lower: [{event, team_code, user_id}, ...]}
        matches:       [{event, stage, team, opponent_team, ...}, ...]
        player_scores: {username_lower: [{event, stage, score, acc, ...}, ...]}
    """
    global _PACKAGE_MATCH_CACHE
    if _PACKAGE_MATCH_CACHE is not None:
        return _PACKAGE_MATCH_CACHE

    pkg_dir = Path(__file__).resolve().parent / "data" / "packages" / "verified"
    player_teams: dict[str, list[dict]] = {}      # username_lower -> team entries
    all_matches: list[dict] = []
    player_scores: dict[str, list[dict]] = {}      # username_lower -> score entries
    user_id_to_names: dict[int, set[str]] = {}     # user_id -> set of usernames

    if not pkg_dir.exists():
        _PACKAGE_MATCH_CACHE = {"player_teams": {}, "matches": [], "player_scores": {}}
        return _PACKAGE_MATCH_CACHE

    for f in sorted(pkg_dir.glob("*.json")):
        try:
            pkg = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue

        event_name = _pretty_tournament_name(f.stem)

        # Index players
        for p in (pkg.get("players") or []):
            name = p.get("player", "")
            uid = p.get("user_id")
            low = name.lower()
            entry = {"event": event_name, "team_code": p.get("team_code"), "user_id": uid}
            player_teams.setdefault(low, []).append(entry)
            if uid:
                user_id_to_names.setdefault(uid, set()).add(low)

        # Index matches
        for m in (pkg.get("matches") or []):
            match_entry = {**m}
            if not match_entry.get("event"):
                match_entry["event"] = event_name
            all_matches.append(match_entry)

        # Index map scores
        for s in (pkg.get("map_scores") or []):
            name = s.get("player", "")
            low = name.lower()
            score_entry = {**s}
            if not score_entry.get("event"):
                score_entry["event"] = event_name
            player_scores.setdefault(low, []).append(score_entry)

    _PACKAGE_MATCH_CACHE = {
        "player_teams": player_teams,
        "matches": all_matches,
        "player_scores": player_scores,
        "user_id_to_names": user_id_to_names,
    }
    return _PACKAGE_MATCH_CACHE


def _resolve_player_names(username: str) -> set[str]:
    """Find all name variants for a player across packages."""
    data = _load_package_match_data()
    low = username.lower()
    names = {low}

    # Check if this username appears in any package
    for entry in data["player_teams"].get(low, []):
        uid = entry.get("user_id")
        if uid and uid in data["user_id_to_names"]:
            names.update(data["user_id_to_names"][uid])

    # Also check leaderboard aliases
    try:
        lb_rows = _load_leaderboard()
        for row in lb_rows:
            if str(row.get("username", "")).lower() in names:
                for alias in (row.get("aliases") or []):
                    names.add(alias.lower())
                break
            for alias in (row.get("aliases") or []):
                if alias.lower() in names:
                    for a2 in (row.get("aliases") or []):
                        names.add(a2.lower())
                    names.add(str(row.get("username", "")).lower())
                    break
    except Exception:
        pass

    return names


def _file_based_recent_matches(username: str, limit: int = 20) -> list[dict[str, Any]]:
    """Build recent match list from verified packages — no DB needed."""
    data = _load_package_match_data()
    names = _resolve_player_names(username)

    # Build team mapping from BOTH players array and map_scores player_team field.
    # For OWC the players array team_code works (e.g. "PL").
    # For team tournaments like FDC, the map_scores player_team is the real team name.
    teams_by_event: dict[str, set[str]] = {}  # event -> set of team names/codes
    for name in names:
        for entry in data["player_teams"].get(name, []):
            evt = entry.get("event", "")
            tc = entry.get("team_code", "")
            if evt and tc:
                teams_by_event.setdefault(evt, set()).add(tc)
        # Also check map_scores for player_team
        for score in data["player_scores"].get(name, []):
            evt = score.get("event", "")
            pt = score.get("player_team", "")
            if evt and pt:
                teams_by_event.setdefault(evt, set()).add(pt)

    if not teams_by_event:
        return []

    # Find matches where the player's team participated
    result_matches: list[dict] = []
    seen: set[str] = set()

    for match in data["matches"]:
        evt = match.get("event", "")
        if evt not in teams_by_event:
            continue
        team = match.get("team_code") or match.get("team", "")
        if team not in teams_by_event[evt]:
            continue

        # Dedup by match_link or (event+stage+opponent+date)
        key = match.get("match_link") or f"{evt}|{match.get('stage')}|{match.get('opponent_team')}|{match.get('date')}"
        if key in seen:
            continue
        seen.add(key)

        opp_team = match.get("opponent_team", "")
        opp_name = _team_to_country_name(opp_team) if len(opp_team) == 2 else opp_team

        result_matches.append({
            "match_date": match.get("date"),
            "tournament_name": evt,
            "stage": match.get("stage"),
            "team_name": _team_to_country_name(team) if len(team) == 2 else team,
            "opponent_name": opp_name,
            "opponent_team_name": opp_name,
            "result": match.get("result"),
            "player_score": match.get("team_score"),
            "opponent_score": match.get("opponent_score"),
            "match_link": match.get("match_link"),
            "match_id": None,
            "source": match.get("source", "verified_package"),
            "data_quality": "verified",
        })

    # Sort by date descending
    result_matches.sort(key=lambda r: r.get("match_date") or "", reverse=True)
    return result_matches[:limit]


def _team_to_country_name(code: str) -> str:
    """Convert 2-letter team/country code to country name."""
    try:
        import pycountry
        c = pycountry.countries.get(alpha_2=code.upper())
        return c.name if c else code
    except Exception:
        # Fallback for common osu! WC codes
        _COMMON = {
            "US": "United States", "KR": "South Korea", "JP": "Japan",
            "PL": "Poland", "AU": "Australia", "DE": "Germany",
            "GB": "United Kingdom", "CA": "Canada", "RU": "Russia",
            "BR": "Brazil", "FR": "France", "CN": "China",
            "TW": "Taiwan", "PH": "Philippines", "ID": "Indonesia",
            "CL": "Chile", "SE": "Sweden", "FI": "Finland",
            "RO": "Romania", "HK": "Hong Kong", "MY": "Malaysia",
            "SG": "Singapore", "TH": "Thailand", "NL": "Netherlands",
            "NO": "Norway", "DK": "Denmark", "MX": "Mexico",
            "AR": "Argentina", "ES": "Spain", "IT": "Italy",
            "PT": "Portugal", "CZ": "Czech Republic", "VN": "Vietnam",
            "UA": "Ukraine", "AT": "Austria", "BE": "Belgium",
            "NZ": "New Zealand", "PE": "Peru", "CO": "Colombia",
            "KZ": "Kazakhstan", "EE": "Estonia", "LV": "Latvia",
            "LT": "Lithuania", "SK": "Slovakia", "HU": "Hungary",
            "IL": "Israel", "SA": "Saudi Arabia", "AE": "UAE",
            "TR": "Turkey", "GR": "Greece", "BG": "Bulgaria",
            "IE": "Ireland", "UY": "Uruguay", "CH": "Switzerland",
        }
        return _COMMON.get(code.upper(), code)


def _file_based_player_tournament_stats(username: str) -> list[dict[str, Any]]:
    """Get per-tournament stats for a player from verified packages."""
    data = _load_package_match_data()
    names = _resolve_player_names(username)

    # Gather all scores across name variants
    all_scores: list[dict] = []
    for name in names:
        all_scores.extend(data["player_scores"].get(name, []))

    if not all_scores:
        return []

    # Group by event+stage
    from collections import defaultdict
    by_event: dict[str, list[dict]] = defaultdict(list)
    for s in all_scores:
        evt = s.get("event", "Unknown")
        by_event[evt].append(s)

    result = []
    for evt, scores in by_event.items():
        stages = set(s.get("stage", "") for s in scores)
        dates = [s.get("date") for s in scores if s.get("date")]
        wins = sum(1 for s in scores if s.get("result") == "win")
        total = len(scores)
        avg_acc = sum(s.get("accuracy", 0) for s in scores) / total if total else 0
        avg_score = sum(s.get("score", 0) for s in scores) / total if total else 0

        result.append({
            "tournament": evt,
            "maps_played": total,
            "map_wins": wins,
            "map_losses": total - wins,
            "win_rate": round(wins / total * 100, 1) if total else 0,
            "avg_accuracy": round(avg_acc, 2),
            "avg_score": round(avg_score),
            "stages": sorted(stages),
            "first_date": min(dates) if dates else None,
            "last_date": max(dates) if dates else None,
        })

    result.sort(key=lambda r: r.get("last_date") or "", reverse=True)
    return result

@app.get("/tournaments/catalog")
async def tournament_catalog(
    year: int | None = Query(default=None, ge=2020, le=2030),
    game_mode: str | None = Query(default=None),
    classification: str | None = Query(default=None),
    import_status: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
) -> dict[str, Any]:
    """Return merged tournament catalog from JSON files — no DB dependency."""
    rows = await asyncio.to_thread(_load_tournament_catalog)
    if year:
        rows = [r for r in rows if r.get("year") == year]
    if game_mode:
        rows = [r for r in rows if (r.get("game_mode") or "").lower() == game_mode.lower()]
    if classification:
        rows = [r for r in rows if r.get("classification") == classification]
    if import_status:
        rows = [r for r in rows if r.get("import_status") == import_status]
    # Sort: imported first, then by year desc, then name
    rows.sort(key=lambda r: (
        0 if r.get("import_status") == "imported" else 1,
        -(r.get("year") or 0),
        r.get("name", ""),
    ))
    total = len(rows)
    rows = rows[:limit]
    # Count stats
    imported_count = sum(1 for r in rows if r.get("import_status") == "imported")
    discovered_count = total - imported_count
    return {
        "total": total,
        "imported_count": imported_count,
        "discovered_count": discovered_count,
        "rows": rows,
    }


@app.get("/tournaments/catalog/{slug}")
async def tournament_detail(slug: str) -> dict[str, Any]:
    """Return a single tournament by slug."""
    rows = await asyncio.to_thread(_load_tournament_catalog)
    match = next((r for r in rows if r.get("slug") == slug), None)
    if not match:
        raise HTTPException(status_code=404, detail=f"Tournament not found: {slug}")
    return match




@app.get("/player/{username}/tournament-stats")
async def player_tournament_stats(username: str) -> list[dict[str, Any]]:
    """Return per-tournament statistics for a player from verified packages."""
    return await asyncio.to_thread(_file_based_player_tournament_stats, username)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
