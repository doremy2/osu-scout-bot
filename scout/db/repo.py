"""Repository functions: the only place that writes tournament data."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import unicodedata
import zlib
from datetime import datetime, timedelta, timezone

from ..clients import DEFAULT_CLIENT, validate_client
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
                      fmt: str | None = None, client: str | None = None) -> int:
    if fmt is not None:
        validate_format(fmt)
    if client is not None:
        validate_client(client)
    row = conn.execute("SELECT id FROM tournaments WHERE slug = ?", (slug,)).fetchone()
    if row:
        if name or acronym or warmups is not None or fmt or client:
            conn.execute(
                "UPDATE tournaments SET name = COALESCE(?, name), acronym = COALESCE(?, acronym), "
                "warmups = COALESCE(?, warmups), format = COALESCE(?, format), client = COALESCE(?, client) WHERE id = ?",
                (name, acronym, warmups, fmt, client, row["id"]),
            )
            conn.commit()
        return row["id"]
    cur = conn.execute(
        "INSERT INTO tournaments (slug, name, acronym, warmups, format, client) VALUES (?, ?, ?, ?, ?, ?)",
        (slug, name or slug, acronym, warmups or 0, fmt or DEFAULT_FORMAT, client or DEFAULT_CLIENT),
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
            """INSERT INTO tournament_matches (tournament_id, osu_match_id, round, round_raw, source_context, kind)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tournament_id, osu_match_id) DO UPDATE SET
                 round = COALESCE(tournament_matches.round, excluded.round),
                 round_raw = COALESCE(tournament_matches.round_raw, excluded.round_raw)""",
            (tournament_id, link.osu_match_id, code, raw, link.context, link.kind),
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
        "UPDATE tournament_matches SET status = ?, error = ?, fetched_at = datetime('now'), "
        "attempts = attempts + CASE WHEN ? = 'imported' THEN 0 ELSE 1 END WHERE id = ?",
        (status, error, status, match_row_id),
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


# --- automatic updating ----------------------------------------------------------
UTC = timezone.utc
RETRY_FAILED_AFTER = timedelta(minutes=30)       # a failed fetch is tried again after this long...
RETRY_NOT_FOUND_AFTER = timedelta(hours=6)       # ...a 404 later still (the sheet may list a lobby before it exists)
MAX_ATTEMPTS = 5                                 # then it is left alone until someone resets it
LIVE_LOBBY_WINDOW = timedelta(hours=6)           # a lobby that started this recently may still be running
LIVE_LOBBY_REFRESH = timedelta(minutes=10)


def utcnow() -> datetime:
    return datetime.now(UTC)


def stamp(when: datetime | None = None) -> str:
    """The timestamp format the database uses (UTC, same as datetime('now'))."""
    return (when or utcnow()).astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def parse_stamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        d = datetime.fromisoformat(value.strip().replace(" ", "T"))
    except ValueError:
        return None
    return d.replace(tzinfo=UTC) if d.tzinfo is None else d.astimezone(UTC)


def iso(value: str | None) -> str | None:
    d = parse_stamp(value)
    return d.strftime("%Y-%m-%dT%H:%M:%SZ") if d else None


def links_hash(links: list[MatchLink], slots: list[dict] | None = None) -> str:
    """Fingerprint of what a source currently says (which lobbies, in which round, which pool). Cheap change detection."""
    rows = sorted((l.kind, l.osu_match_id, l.round_raw or "") for l in links)
    pool = sorted((s.get("round") or "", s.get("slot") or "", s.get("beatmap_id") or 0, s.get("beatmapset_id") or 0)
                  for s in (slots or []))
    return hashlib.sha256(json.dumps([rows, pool], separators=(",", ":")).encode()).hexdigest()[:24]


def known_match_ids(conn: sqlite3.Connection, tournament_id: int) -> set[int]:
    """Lobby ids the tournament already holds, whatever their status (this is what the unique key is on)."""
    return {r[0] for r in conn.execute("SELECT osu_match_id FROM tournament_matches WHERE tournament_id = ?", (tournament_id,))}


def set_source(conn: sqlite3.Connection, tournament_id: int, url: str, source_type: str, overwrite: bool = False) -> None:
    """Remember where a tournament came from. Tracking is switched on the first time a source is recorded; after that
    it is left as the owner set it."""
    row = conn.execute("SELECT source_url FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
    if row is None:
        return
    if row["source_url"] is None:
        conn.execute("UPDATE tournaments SET source_url = ?, source_type = ?, auto_update = ? WHERE id = ?",
                     (url, source_type, 1 if source_type == "google_sheet" else 0, tournament_id))
    elif overwrite and row["source_url"] != url:
        conn.execute("UPDATE tournaments SET source_url = ?, source_type = ? WHERE id = ?", (url, source_type, tournament_id))
    conn.commit()


def set_auto_update(conn: sqlite3.Connection, slug: str, enabled: bool, source_url: str | None = None) -> bool:
    t = get_tournament(conn, slug)
    if t is None:
        return False
    if source_url:
        conn.execute("UPDATE tournaments SET source_url = ?, source_type = ?, check_failures = 0 WHERE id = ?",
                     (source_url, "google_sheet", t["id"]))
    conn.execute("UPDATE tournaments SET auto_update = ? WHERE id = ?", (1 if enabled else 0, t["id"]))
    conn.commit()
    return True


def record_check(conn: sqlite3.Connection, tournament_id: int, *, source_hash: str | None = None, new: int = 0,
                 error: str | None = None, now: datetime | None = None) -> None:
    """Book-keep one look at the source. A failed look backs off (check_failures); a look that found new lobbies
    marks the tournament as changed (that keeps it on the fast schedule)."""
    when = stamp(now)
    if error is not None:
        conn.execute("UPDATE tournaments SET last_checked_at = ?, last_check_error = ?, check_failures = check_failures + 1 "
                     "WHERE id = ?", (when, error[:300], tournament_id))
    else:
        conn.execute(
            """UPDATE tournaments SET last_checked_at = ?, last_check_error = NULL, check_failures = 0,
                      last_source_hash = COALESCE(?, last_source_hash),
                      last_changed_at = CASE WHEN ? > 0 THEN ? ELSE last_changed_at END,
                      last_new_matches = CASE WHEN ? > 0 THEN ? ELSE last_new_matches END
               WHERE id = ?""",
            (when, source_hash, new, when, new, new, tournament_id))
    conn.commit()


def requeue_retryable(conn: sqlite3.Connection, tournament_id: int, now: datetime | None = None) -> int:
    """Failed fetches and 404s get another chance after a cool-down (bounded by MAX_ATTEMPTS)."""
    now = now or utcnow()
    n = 0
    for status, wait in (("failed", RETRY_FAILED_AFTER), ("not_found", RETRY_NOT_FOUND_AFTER)):
        cur = conn.execute(
            "UPDATE tournament_matches SET status = 'pending' WHERE tournament_id = ? AND status = ? AND attempts < ? "
            "AND (fetched_at IS NULL OR fetched_at <= ?)", (tournament_id, status, MAX_ATTEMPTS, stamp(now - wait)))
        n += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    conn.commit()
    return n


def reopen_live_matches(conn: sqlite3.Connection, tournament_id: int, now: datetime | None = None) -> int:
    """A stable lobby with no end time that started a few hours ago is probably still being played: queue it for a fresh
    download (dropping its cached copy) so games played since the last look are picked up. Rooms are left alone
    (a lazer room costs a dozen API calls)."""
    now = now or utcnow()
    rows = conn.execute(
        "SELECT id, osu_match_id FROM tournament_matches WHERE tournament_id = ? AND status = 'imported' AND kind = 'match' "
        "AND end_time IS NULL AND start_time IS NOT NULL AND replace(replace(start_time, 'T', ' '), 'Z', '') >= ? "
        "AND (fetched_at IS NULL OR fetched_at <= ?)",
        (tournament_id, stamp(now - LIVE_LOBBY_WINDOW), stamp(now - LIVE_LOBBY_REFRESH))).fetchall()
    for r in rows:
        conn.execute("DELETE FROM raw_match_cache WHERE osu_match_id = ?", (r["osu_match_id"],))
        conn.execute("UPDATE tournament_matches SET status = 'pending' WHERE id = ?", (r["id"],))
    conn.commit()
    return len(rows)
