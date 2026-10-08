"""Repository functions: the only place that writes tournament data."""
from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
import zlib

from ..formats import DEFAULT_FORMAT, validate_format
from .batch import Statement, run_batch
from ..models import MatchLink, ParsedMatch
from ..rounds import normalize_round


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()  # "Türkiye" -> "Turkiye"
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# --- tournaments ----------------------------------------------------------
def upsert_tournament(conn: sqlite3.Connection, slug: str, name: str | None = None,
                      acronym: str | None = None, warmups: int | None = None,
                      fmt: str | None = None) -> int:
    if fmt is not None:
        validate_format(fmt)
    row = conn.execute("SELECT id FROM tournaments WHERE slug = ?", (slug,)).fetchone()
    if row:
        if name or acronym or warmups is not None or fmt:
            conn.execute(
                "UPDATE tournaments SET name = COALESCE(?, name), acronym = COALESCE(?, acronym), "
                "warmups = COALESCE(?, warmups), format = COALESCE(?, format) WHERE id = ?",
                (name, acronym, warmups, fmt, row["id"]),
            )
            conn.commit()
        return row["id"]
    cur = conn.execute(
        "INSERT INTO tournaments (slug, name, acronym, warmups, format) VALUES (?, ?, ?, ?, ?)",
        (slug, name or slug, acronym, warmups or 0, fmt or DEFAULT_FORMAT),
    )
    conn.commit()
    return cur.lastrowid


_SHEET_ID = re.compile(r"/spreadsheets/d/([A-Za-z0-9_-]+)")


def sheet_id(url: str | None) -> str | None:
    m = _SHEET_ID.search(url or "")
    return m.group(1) if m else None


def tournament_uses_sheet(conn: sqlite3.Connection, slug: str, url: str) -> bool:
    """True if this tournament was imported from the same Google spreadsheet (different tab/gid links count as the same)."""
    want = sheet_id(url)
    t = get_tournament(conn, slug)
    if not want or t is None:
        return False
    return any(sheet_id(r[0]) == want for r in conn.execute(
        "SELECT location FROM tournament_sources WHERE tournament_id = ?", (t["id"],)))


def delete_tournament(conn: sqlite3.Connection, slug: str) -> bool:
    """Remove a tournament and everything that hangs off it (explicitly, so it works with or without FK enforcement)."""
    t = get_tournament(conn, slug)
    if t is None:
        return False
    tid = t["id"]
    m = "(SELECT id FROM tournament_matches WHERE tournament_id = ?)"
    g = f"(SELECT id FROM match_games WHERE match_id IN {m})"
    run_batch(conn, [
        (f"DELETE FROM game_scores WHERE game_id IN {g}", (tid,)),
        (f"DELETE FROM match_games WHERE match_id IN {m}", (tid,)),
        ("UPDATE tournament_matches SET team_red_id = NULL, team_blue_id = NULL WHERE tournament_id = ?", (tid,)),
        ("DELETE FROM team_memberships WHERE tournament_id = ?", (tid,)),
        ("DELETE FROM tournament_teams WHERE tournament_id = ?", (tid,)),
        ("DELETE FROM tournament_players WHERE tournament_id = ?", (tid,)),
        ("DELETE FROM tournament_sources WHERE tournament_id = ?", (tid,)),
        ("DELETE FROM mappool WHERE tournament_id = ?", (tid,)),
        ("DELETE FROM pool_slots WHERE tournament_id = ?", (tid,)),
        ("DELETE FROM tournament_matches WHERE tournament_id = ?", (tid,)),
        ("DELETE FROM tournaments WHERE id = ?", (tid,)),
    ])
    return True


def get_tournament(conn: sqlite3.Connection, slug: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM tournaments WHERE slug = ?", (slug,)).fetchone()


def refresh_tournament_dates(conn: sqlite3.Connection, tournament_id: int) -> None:
    conn.execute(
        """UPDATE tournaments SET
             start_date = COALESCE(start_date, (SELECT date(MIN(start_time)) FROM tournament_matches WHERE tournament_id = ?)),
             end_date   = (SELECT date(MAX(COALESCE(end_time, start_time))) FROM tournament_matches WHERE tournament_id = ?)
           WHERE id = ?""",
        (tournament_id, tournament_id, tournament_id),
    )
    conn.commit()


# --- discovery ------------------------------------------------------------
def record_source(conn: sqlite3.Connection, tournament_id: int, kind: str, location: str, links_found: int) -> None:
    conn.execute(
        """INSERT INTO tournament_sources (tournament_id, kind, location, links_found) VALUES (?, ?, ?, ?)
           ON CONFLICT(tournament_id, location) DO UPDATE SET scanned_at = datetime('now'), links_found = excluded.links_found""",
        (tournament_id, kind, location, links_found),
    )
    conn.commit()


def add_match_links(conn: sqlite3.Connection, tournament_id: int, links: list[MatchLink],
                    round_override: str | None = None) -> int:
    """Insert newly discovered lobbies as 'pending'. Existing rows keep their status,
    but get a round if they didn't have one. Returns number of new matches."""
    known = {r[0] for r in conn.execute(
        "SELECT osu_match_id FROM tournament_matches WHERE tournament_id = ?", (tournament_id,))}
    stmts: list[Statement] = []
    new = 0
    for link in links:
        raw = round_override or link.round_raw
        code = normalize_round(raw)
        if link.osu_match_id not in known:
            new += 1
            known.add(link.osu_match_id)
        stmts.append((
            """INSERT INTO tournament_matches (tournament_id, osu_match_id, round, round_raw, source_context)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(tournament_id, osu_match_id) DO UPDATE SET
                 round = COALESCE(tournament_matches.round, excluded.round),
                 round_raw = COALESCE(tournament_matches.round_raw, excluded.round_raw)""",
            (tournament_id, link.osu_match_id, code, raw, link.context),
        ))
    run_batch(conn, stmts)
    return new


def set_match_round(conn: sqlite3.Connection, tournament_id: int, osu_match_id: int, round_text: str) -> None:
    conn.execute(
        "UPDATE tournament_matches SET round = ?, round_raw = ? WHERE tournament_id = ? AND osu_match_id = ?",
        (normalize_round(round_text) or round_text, round_text, tournament_id, osu_match_id),
    )
    conn.commit()


def pending_matches(conn: sqlite3.Connection, tournament_id: int, retry_failed: bool = False) -> list[sqlite3.Row]:
    statuses = ("pending", "failed") if retry_failed else ("pending",)
    q = f"SELECT * FROM tournament_matches WHERE tournament_id = ? AND status IN ({','.join('?' * len(statuses))}) ORDER BY osu_match_id"
    return conn.execute(q, (tournament_id, *statuses)).fetchall()


def mark_match(conn: sqlite3.Connection, match_row_id: int, status: str, error: str | None = None) -> None:
    conn.execute(
        "UPDATE tournament_matches SET status = ?, error = ?, fetched_at = datetime('now') WHERE id = ?",
        (status, error, match_row_id),
    )
    conn.commit()


# --- raw cache ------------------------------------------------------------
def cache_raw(conn: sqlite3.Connection, osu_match_id: int, payload: dict) -> None:
    blob = zlib.compress(json.dumps(payload, separators=(",", ":")).encode())
    conn.execute(
        "INSERT INTO raw_match_cache (osu_match_id, payload) VALUES (?, ?) "
        "ON CONFLICT(osu_match_id) DO UPDATE SET payload = excluded.payload, fetched_at = datetime('now')",
        (osu_match_id, blob),
    )


def load_raw(conn: sqlite3.Connection, osu_match_id: int) -> dict | None:
    row = conn.execute("SELECT payload FROM raw_match_cache WHERE osu_match_id = ?", (osu_match_id,)).fetchone()
    return json.loads(zlib.decompress(row["payload"])) if row else None


# --- parsed match ---------------------------------------------------------
def save_parsed_match(conn: sqlite3.Connection, match_row_id: int, pm: ParsedMatch) -> None:
    """Replace this lobby's games/scores with the parsed version (idempotent), as one batch."""
    st: list[Statement] = []
    for p in pm.players:
        st.append((
            """INSERT INTO players (user_id, username, country_code, country_name, avatar_url) VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET username = excluded.username,
                 country_code = COALESCE(excluded.country_code, players.country_code),
                 country_name = COALESCE(excluded.country_name, players.country_name),
                 avatar_url = COALESCE(excluded.avatar_url, players.avatar_url),
                 updated_at = datetime('now')""",
            (p.user_id, p.username, p.country_code, p.country_name, p.avatar_url)))
    for b in pm.beatmaps:
        st.append((
            """INSERT INTO beatmaps (beatmap_id, beatmapset_id, artist, title, version, star_rating, mode)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(beatmap_id) DO UPDATE SET
                 beatmapset_id = COALESCE(excluded.beatmapset_id, beatmaps.beatmapset_id),
                 artist = COALESCE(excluded.artist, beatmaps.artist),
                 title = COALESCE(excluded.title, beatmaps.title),
                 version = COALESCE(excluded.version, beatmaps.version),
                 star_rating = COALESCE(excluded.star_rating, beatmaps.star_rating),
                 mode = COALESCE(excluded.mode, beatmaps.mode)""",
            (b.beatmap_id, b.beatmapset_id, b.artist, b.title, b.version, b.star_rating, b.mode)))
    st.append(("UPDATE tournament_matches SET name = ?, team_red = ?, team_blue = ?, start_time = ?, end_time = ? WHERE id = ?",
               (pm.name, pm.team_red, pm.team_blue, pm.start_time, pm.end_time, match_row_id)))
    st.append(("DELETE FROM match_games WHERE match_id = ?", (match_row_id,)))
    known_users = {p.user_id for p in pm.players}
    for g in pm.games:
        st.append((
            """INSERT INTO match_games (match_id, osu_game_id, order_index, beatmap_id, mods, scoring_type,
                                        team_type, start_time, end_time)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (match_row_id, g.osu_game_id, g.order_index, g.beatmap_id, ",".join(g.mods),
             g.scoring_type, g.team_type, g.start_time, g.end_time)))
        for s in g.scores:
            if s.user_id not in known_users:  # restricted/deleted users can be missing from `users`
                st.append(("INSERT OR IGNORE INTO players (user_id, username) VALUES (?, ?)", (s.user_id, f"user {s.user_id}")))
            st.append((
                """INSERT OR REPLACE INTO game_scores (game_id, user_id, score, accuracy, max_combo, count_300,
                     count_100, count_50, count_miss, mods, team, slot, passed)
                   VALUES ((SELECT id FROM match_games WHERE match_id = ? AND osu_game_id = ?),
                           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (match_row_id, g.osu_game_id, s.user_id, s.score, s.accuracy, s.max_combo, s.count_300, s.count_100,
                 s.count_50, s.count_miss, ",".join(s.mods), s.team, s.slot, int(s.passed))))
    run_batch(conn, st)


# --- pool slots (display order) -----------------------------------------------
def save_pool_slots(conn: sqlite3.Connection, tournament_id: int, slots: list[dict]) -> int:
    """Replace the tournament's slot list. Kept only if the sheet actually yielded some."""
    if not slots:
        return 0
    conn.execute("DELETE FROM pool_slots WHERE tournament_id = ?", (tournament_id,))
    conn.executemany(
        "INSERT OR REPLACE INTO pool_slots (tournament_id, round, slot, beatmap_id, beatmapset_id, position, label, star_rating) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(tournament_id, s["round"], s["slot"], s["beatmap_id"], s.get("beatmapset_id"), s["position"], s.get("label"),
          s.get("star_rating")) for s in slots],
    )
    conn.commit()
    return len(slots)


# --- mappool --------------------------------------------------------------
def load_mappool(conn: sqlite3.Connection, tournament_id: int, rows: list[dict]) -> int:
    """rows: dicts with beatmap_id, slot and optional round."""
    n = 0
    for r in rows:
        bid = str(r.get("beatmap_id") or "").strip()
        m = re.search(r"(\d+)\s*$", bid)  # accepts plain ids or beatmap URLs
        if not m or not r.get("slot"):
            continue
        rnd = r.get("round") or ""
        rnd = normalize_round(rnd) or rnd
        conn.execute(
            "INSERT OR REPLACE INTO mappool (tournament_id, round, beatmap_id, slot) VALUES (?, ?, ?, ?)",
            (tournament_id, rnd, int(m.group(1)), r["slot"].strip().upper()),
        )
        n += 1
    conn.commit()
    return n
