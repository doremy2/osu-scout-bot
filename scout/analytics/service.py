"""Read-side service the web API calls: cached analyses + tournament listing.

Rebuilding a report takes a fraction of a second, but pages hit it constantly, so one
Analysis per tournament is cached and rebuilt only when the underlying rows change.
"""
from __future__ import annotations

import sqlite3
import threading

from ..formats import get_format
from .report import Analysis, build_analysis

_cache: dict[str, tuple[tuple, Analysis]] = {}
_lock = threading.Lock()


def _token(conn: sqlite3.Connection, tournament_id: int) -> tuple:
    m = conn.execute("SELECT COUNT(*), COALESCE(MAX(fetched_at), ''), COALESCE(SUM(LENGTH(COALESCE(round, ''))), 0) "
                     "FROM tournament_matches WHERE tournament_id = ? AND status = 'imported'", (tournament_id,)).fetchone()
    g = conn.execute("SELECT COUNT(*), COALESCE(SUM(g.excluded), 0) FROM match_games g JOIN tournament_matches m "
                     "ON m.id = g.match_id WHERE m.tournament_id = ?", (tournament_id,)).fetchone()
    t = conn.execute("SELECT format, name, acronym FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
    tm = conn.execute("SELECT COUNT(*) FROM team_memberships WHERE tournament_id = ?", (tournament_id,)).fetchone()[0]
    pool = conn.execute("SELECT COUNT(*) FROM mappool WHERE tournament_id = ?", (tournament_id,)).fetchone()[0]
    return (*tuple(m), *tuple(g), *tuple(t), tm, pool)


def get_analysis(conn: sqlite3.Connection, slug: str) -> Analysis:
    row = conn.execute("SELECT id FROM tournaments WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        raise KeyError(slug)
    token = _token(conn, row["id"])
    with _lock:
        hit = _cache.get(slug)
        if hit and hit[0] == token:
            return hit[1]
    an = build_analysis(conn, slug)
    with _lock:
        _cache[slug] = (token, an)
    return an


def invalidate(slug: str | None = None) -> None:
    with _lock:
        if slug:
            _cache.pop(slug, None)
        else:
            _cache.clear()


def list_tournaments(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """SELECT t.id, t.slug, t.name, t.acronym, t.format, t.start_date, t.end_date,
                  (SELECT COUNT(*) FROM tournament_matches m WHERE m.tournament_id = t.id AND m.status = 'imported') AS matches,
                  (SELECT COUNT(*) FROM tournament_matches m WHERE m.tournament_id = t.id AND m.status IN ('pending','failed')) AS pending,
                  (SELECT COUNT(*) FROM tournament_players p WHERE p.tournament_id = t.id) AS players,
                  (SELECT COUNT(*) FROM tournament_teams x WHERE x.tournament_id = t.id) AS teams
           FROM tournaments t ORDER BY COALESCE(t.end_date, t.created_at) DESC, t.id DESC""").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["format_label"] = get_format(d["format"]).label
        d["has_teams"] = get_format(d["format"]).has_teams
        out.append(d)
    return out
