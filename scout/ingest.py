"""Ingestion pipeline: source -> links -> osu! API -> parsed rows -> SQLite.

Each step is separately re-runnable:
  discover()  scan a source, register lobbies as 'pending'
  fetch()     download pending lobbies (raw JSON cached), parse + store
  reparse()   rebuild games/scores from the raw cache (no API calls) after parser changes
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Callable, Protocol

from . import classify, teams
from .db import repo
from .osu import MatchNotFound, OsuApiError, parse_match, parse_room
from .sources import discover_all

log = logging.getLogger("scout.ingest")


class MatchFetcher(Protocol):
    def get_match_full(self, match_id: int) -> dict: ...


def discover(conn: sqlite3.Connection, tournament_id: int, location: str,
             kind: str | None = None, round_override: str | None = None) -> tuple[int, int]:
    """Returns (links found, new matches)."""
    kind, links, slots = discover_all(location, kind)
    repo.save_pool_slots(conn, tournament_id, slots)
    new = repo.add_match_links(conn, tournament_id, links, round_override=round_override)
    repo.record_source(conn, tournament_id, kind, location, len(links))
    return len(links), new


def fetch(conn: sqlite3.Connection, tournament_id: int, client: MatchFetcher,
          retry_failed: bool = False, use_cache: bool = True,
          progress: Callable[[str], None] = print,
          on_match: Callable[[int, int, dict[str, int]], None] | None = None) -> dict[str, int]:
    """on_match(done, total, counts) fires after every lobby - the web importer uses it for progress."""
    todo = repo.pending_matches(conn, tournament_id, retry_failed=retry_failed)
    counts = {"imported": 0, "failed": 0, "not_found": 0}
    if on_match:
        on_match(0, len(todo), dict(counts))
    for i, m in enumerate(todo, 1):
        status, detail = fetch_one(conn, m, client, use_cache=use_cache)
        counts[status] += 1
        progress(f"[{i}/{len(todo)}] {m['osu_match_id']}: {detail}")
        if on_match:
            on_match(i, len(todo), dict(counts))
    finalize(conn, tournament_id)
    return counts


def fetch_one(conn: sqlite3.Connection, m, client: MatchFetcher, use_cache: bool = True) -> tuple[str, str]:
    """Import one pending lobby. Returns ("imported" | "not_found" | "failed", human-readable detail).
    The raw API payload is cached for later re-parsing, except on the hosted database where it would be dead weight."""
    mid = m["osu_match_id"]
    room = (m["kind"] if "kind" in m.keys() else "match") == "room"     # lazer room vs stable match
    cache_key = -mid if room else mid            # one cache for both: room ids never collide with match ids
    try:
        payload = repo.load_raw(conn, cache_key) if use_cache else None
        if payload is None:
            payload = client.get_room_full(mid) if room else client.get_match_full(mid)
            if not getattr(conn, "is_remote", False):
                repo.cache_raw(conn, cache_key, payload)
        parsed = parse_room(payload) if room else parse_match(payload)
        repo.save_parsed_match(conn, m["id"], parsed)
        repo.mark_match(conn, m["id"], "imported")
        return "imported", f"{parsed.name} — {len(parsed.games)} games"
    except MatchNotFound:
        repo.mark_match(conn, m["id"], "not_found", "osu! API returned 404 (deleted or wrong id)")
        return "not_found", "not found"
    except (OsuApiError, KeyError, ValueError, TypeError) as e:
        repo.mark_match(conn, m["id"], "failed", str(e)[:500])
        return "failed", f"FAILED {e}"


def reparse(conn: sqlite3.Connection, tournament_id: int) -> int:
    rows = conn.execute(
        "SELECT id, osu_match_id, kind FROM tournament_matches WHERE tournament_id = ? AND status = 'imported'",
        (tournament_id,),
    ).fetchall()
    for m in rows:
        room = m["kind"] == "room"
        payload = repo.load_raw(conn, -m["osu_match_id"] if room else m["osu_match_id"])
        if payload:
            repo.save_parsed_match(conn, m["id"], parse_room(payload) if room else parse_match(payload))
    finalize(conn, tournament_id)
    return len(rows)


def finalize(conn: sqlite3.Connection, tournament_id: int) -> dict[str, int]:
    repo.refresh_tournament_dates(conn, tournament_id)
    stats = classify.classify_tournament(conn, tournament_id)
    stats.update(teams.derive_teams(conn, tournament_id))
    return stats
