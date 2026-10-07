"""Derive tournament-scoped teams + memberships from imported lobbies.

Nothing here is stored on the global player row: the same osu! player can represent
different teams in different tournaments. Runs inside `ingest.finalize`, so it is
re-computed whenever matches are (re)imported or reclassified.

Sources of membership, strongest first:
  1. versus matches: the lobby title names the red/blue team, scores carry the colour
  2. a lobby labelled "Lobby <Team>" (qualifier lobbies of team tournaments)
  3. country: unaffiliated players join the team that is (mostly) their country
"""
from __future__ import annotations

import re
import sqlite3
from collections import Counter, defaultdict

from .db.batch import run_batch
from .db.repo import slugify
from .formats import get_format

_LOBBY_LABEL = re.compile(r"^\s*lobby\s+(.+?)\s*$", re.IGNORECASE)


def _plurality(counter: Counter):
    return counter.most_common(1)[0][0] if counter else None


def derive_teams(conn: sqlite3.Connection, tournament_id: int) -> dict[str, int]:
    t = conn.execute("SELECT format FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
    fmt = get_format(t["format"] if t else None)

    # reset derived rows (matches reference teams, so detach first)
    conn.execute("UPDATE tournament_matches SET team_red_id = NULL, team_blue_id = NULL WHERE tournament_id = ?",
                 (tournament_id,))
    conn.execute("DELETE FROM team_memberships WHERE tournament_id = ?", (tournament_id,))
    conn.execute("DELETE FROM tournament_teams WHERE tournament_id = ?", (tournament_id,))
    conn.execute("DELETE FROM tournament_players WHERE tournament_id = ?", (tournament_id,))

    conn.execute(
        """INSERT INTO tournament_players (tournament_id, user_id)
           SELECT DISTINCT m.tournament_id, s.user_id
           FROM game_scores s JOIN match_games g ON g.id = s.game_id JOIN tournament_matches m ON m.id = g.match_id
           WHERE m.tournament_id = ? AND m.status = 'imported' AND g.excluded = 0""",
        (tournament_id,),
    )
    if not fmt.has_teams:
        conn.commit()
        return {"teams": 0, "players": conn.execute(
            "SELECT COUNT(*) FROM tournament_players WHERE tournament_id = ?", (tournament_id,)).fetchone()[0]}

    country = {r["user_id"]: r["country_code"] for r in conn.execute("SELECT user_id, country_code FROM players")}
    display: dict[str, str] = {}                      # team key -> display name
    votes: dict[int, Counter] = defaultdict(Counter)  # user -> Counter(team key) from versus matches
    lobby_votes: dict[int, Counter] = defaultdict(Counter)
    match_teams: dict[int, tuple[str | None, str | None]] = {}

    def key_for(name: str) -> str:
        k = slugify(name) or name.casefold()
        display.setdefault(k, name.strip())
        return k

    matches = conn.execute(
        "SELECT id, round, team_red, team_blue FROM tournament_matches WHERE tournament_id = ? AND status = 'imported'",
        (tournament_id,),
    ).fetchall()
    for m in matches:
        rows = conn.execute(
            """SELECT s.user_id, s.team, g.team_type FROM game_scores s JOIN match_games g ON g.id = s.game_id
               WHERE g.match_id = ? AND g.excluded = 0""", (m["id"],)).fetchall()
        if not rows or not (m["team_red"] and m["team_blue"]):
            continue
        label = _LOBBY_LABEL.match(m["team_blue"])
        if m["round"] == "Q" or label:
            if label:   # "Qualifiers vs Lobby Egypt": the lobby belongs to one team
                k = key_for(label.group(1))
                for uid in {r["user_id"] for r in rows}:
                    lobby_votes[uid][k] += 1
            continue
        kr, kb = key_for(m["team_red"]), key_for(m["team_blue"])
        match_teams[m["id"]] = (kr, kb)
        for r in rows:
            if r["team_type"] and "team" in r["team_type"] and r["team"] in ("red", "blue"):
                votes[r["user_id"]][kr if r["team"] == "red" else kb] += 1

    member_key: dict[int, tuple[str, str]] = {}
    for uid, c in votes.items():
        member_key[uid] = (_plurality(c), "match")
    for uid, c in lobby_votes.items():
        if uid not in member_key:
            member_key[uid] = (_plurality(c), "lobby")

    # country fallback for players with no team yet
    team_country: dict[str, str | None] = {}
    by_team: dict[str, Counter] = defaultdict(Counter)
    for uid, (k, _) in member_key.items():
        if country.get(uid):
            by_team[k][country[uid]] += 1
    for k, c in by_team.items():
        team_country[k] = _plurality(c)
    country_to_team = {cc: k for k, cc in team_country.items() if cc}

    players = [r["user_id"] for r in conn.execute(
        "SELECT user_id FROM tournament_players WHERE tournament_id = ?", (tournament_id,))]
    names = {r["country_code"]: r["country_name"] for r in conn.execute(
        "SELECT country_code, country_name FROM players WHERE country_name IS NOT NULL")}
    for uid in players:
        if uid in member_key or not country.get(uid):
            continue
        cc = country[uid]
        k = country_to_team.get(cc)
        if k is None:
            k = key_for(names.get(cc) or cc)
            country_to_team[cc] = k
            team_country[k] = cc
        member_key[uid] = (k, "country")

    team_keys = sorted({k for k, _ in member_key.values()} | {k for pair in match_teams.values() for k in pair})
    run_batch(conn, [
        ("INSERT INTO tournament_teams (tournament_id, slug, name, country_code) VALUES (?, ?, ?, ?)",
         (tournament_id, k, display[k], team_country.get(k)))
        for k in team_keys
    ])
    team_id: dict[str, int] = {r["slug"]: r["id"] for r in conn.execute(
        "SELECT id, slug FROM tournament_teams WHERE tournament_id = ?", (tournament_id,))}
    in_tournament = set(players)
    writes = [
        ("INSERT INTO team_memberships (tournament_id, team_id, user_id, source) VALUES (?, ?, ?, ?)",
         (tournament_id, team_id[k], uid, source))
        for uid, (k, source) in member_key.items() if uid in in_tournament
    ]
    writes += [
        ("UPDATE tournament_matches SET team_red_id = ?, team_blue_id = ? WHERE id = ?", (team_id[kr], team_id[kb], mid))
        for mid, (kr, kb) in match_teams.items()
    ]
    run_batch(conn, writes)
    return {"teams": len(team_id), "players": len(players)}
