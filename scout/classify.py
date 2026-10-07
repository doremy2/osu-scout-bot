"""Decide, for every stored game, its mod bucket and whether analytics should use it.

Runs after import and can be re-run at any time (e.g. after loading a mappool),
because it only reads/writes columns on match_games.
"""
from __future__ import annotations

import re
import sqlite3

# Mods that don't change what a map "is" for pool purposes.
NEUTRAL_MODS = {"NF", "SD", "PF", "TD", "SO", "MR", "CL", "RX0"}
_SLOT_PREFIX = re.compile(r"^([A-Za-z]+)")


def bucket_from_slot(slot: str) -> str:
    m = _SLOT_PREFIX.match(slot.strip())
    return m.group(1).upper() if m else slot.upper()


def bucket_from_mods(mods: set[str]) -> str:
    mods = {m for m in mods if m not in NEUTRAL_MODS}
    if mods & {"DT", "NC"}:
        return "DT"
    if "HT" in mods or "DC" in mods:
        return "HT"
    if "EZ" in mods:
        return "EZ"
    if "FL" in mods:
        return "FL"
    if "HR" in mods:
        return "HR"
    if "HD" in mods:
        return "HD"
    return "NM"


def infer_bucket(game_mods: list[str], score_mods: list[list[str]]) -> str:
    """Without a mappool: if players ran different mods (or added mods on top of the
    lobby's), it was a FreeMod pick; otherwise the shared mods decide."""
    base = {m for m in game_mods if m not in NEUTRAL_MODS}
    player_sets = [frozenset(m for m in sm if m not in NEUTRAL_MODS) for sm in score_mods]
    if len(set(player_sets)) > 1 or any(ps - base for ps in player_sets):
        return "FM"
    return bucket_from_mods(base)


def _superseded_games(conn: sqlite3.Connection, games: list[sqlite3.Row]) -> set[int]:
    """Game ids that were genuinely remade inside one lobby.

    A replay is the *same beatmap played again straight away by the same players* (abort / lag /
    referee remake). So a game only counts as replayed when
      - it is in the same lobby (`games` is one lobby's games),
      - the next game(s) in that lobby are on the same beatmap with overlapping players, and
      - one of those later attempts actually completed (end_time, >= 2 scores).
    The same beatmap appearing elsewhere in the tournament - another match, another qualifier
    lobby, or the same lobby playing its whole pool a second time (other maps in between) - is
    NOT a replay; those are independent performances.
    """
    info = []
    for g in games:
        users = {r[0] for r in conn.execute("SELECT user_id FROM game_scores WHERE game_id = ?", (g["id"],))}
        info.append((g, users))

    out: set[int] = set()
    group: list[tuple[sqlite3.Row, set[int]]] = []

    def flush() -> None:
        if len(group) < 2:
            return
        good = [i for i, (g, users) in enumerate(group) if g["end_time"] and len(users) >= 2]
        if good:
            out.update(g["id"] for g, _ in group[: good[-1]])  # everything before the last completed attempt

    for g, users in info:
        if group:
            prev, prev_users = group[-1]
            same_map = g["beatmap_id"] is not None and g["beatmap_id"] == prev["beatmap_id"]
            same_people = not users or not prev_users or bool(users & prev_users)
            if not (same_map and same_people):
                flush()
                group = []
        group.append((g, users))
    flush()
    return out


def classify_tournament(conn: sqlite3.Connection, tournament_id: int) -> dict[str, int]:
    t = conn.execute("SELECT warmups FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
    warmups = t["warmups"] if t else 0

    pool: dict[tuple[str, int], str] = {
        (r["round"], r["beatmap_id"]): r["slot"]
        for r in conn.execute("SELECT round, beatmap_id, slot FROM mappool WHERE tournament_id = ?", (tournament_id,))
    }
    has_pool = bool(pool)

    stats = {"games": 0, "used": 0, "excluded": 0}
    matches = conn.execute(
        "SELECT id, round FROM tournament_matches WHERE tournament_id = ? AND status = 'imported'",
        (tournament_id,),
    ).fetchall()
    for match in matches:
        games = conn.execute(
            "SELECT id, beatmap_id, mods, end_time, order_index FROM match_games WHERE match_id = ? ORDER BY order_index",
            (match["id"],),
        ).fetchall()
        superseded = _superseded_games(conn, games)

        for g in games:
            stats["games"] += 1
            score_rows = conn.execute("SELECT mods FROM game_scores WHERE game_id = ?", (g["id"],)).fetchall()
            game_mods = [m for m in g["mods"].split(",") if m]
            slot = None
            if has_pool and g["beatmap_id"]:
                slot = pool.get((match["round"] or "", g["beatmap_id"])) or pool.get(("", g["beatmap_id"]))

            reason, warm = None, 0
            if len(score_rows) < 2:
                reason = "fewer than 2 scores"
            elif not g["end_time"]:
                reason = "aborted"
            elif g["id"] in superseded:
                reason = "replayed later"
            elif has_pool and slot is None:
                reason, warm = "not in mappool (warmup?)", 1
            elif not has_pool and g["order_index"] < warmups:
                reason, warm = f"first {warmups} games treated as warmups", 1

            if slot:
                bucket = bucket_from_slot(slot)
            else:
                bucket = infer_bucket(game_mods, [[m for m in r["mods"].split(",") if m] for r in score_rows])

            conn.execute(
                "UPDATE match_games SET mod_bucket = ?, pool_slot = ?, is_warmup = ?, excluded = ?, exclude_reason = ? WHERE id = ?",
                (bucket, slot, warm, 1 if reason else 0, reason, g["id"]),
            )
            stats["excluded" if reason else "used"] += 1
    conn.commit()
    return stats
