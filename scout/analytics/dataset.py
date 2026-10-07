"""Load the analysable slice of a tournament (included games only) into plain objects.

Analytics never touches SQL beyond this file, so a future web backend or a
different DB can reuse ratings/awards unchanged.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass
class ScoreRow:
    game_id: int
    match_id: int
    osu_match_id: int
    round: str | None
    order_index: int
    bucket: str
    pool_slot: str | None
    beatmap_id: int | None
    scoring_type: str | None
    team_type: str | None
    user_id: int
    score: int
    accuracy: float | None
    max_combo: int | None
    count_miss: int | None
    team: str | None
    passed: bool

    @property
    def side(self) -> str:
        """Who this score plays for: team colour in team-vs, the player themself otherwise."""
        if self.team_type and "team" in self.team_type and self.team in ("red", "blue"):
            return self.team
        return f"u{self.user_id}"

    @property
    def metric(self) -> float:
        """The number the lobby actually competed on."""
        if self.scoring_type == "accuracy" and self.accuracy is not None:
            return self.accuracy
        if self.scoring_type == "combo" and self.max_combo is not None:
            return float(self.max_combo)
        return float(self.score)


@dataclass
class Dataset:
    tournament: dict
    scores: list[ScoreRow]
    players: dict[int, dict]
    matches: dict[int, dict]      # match row id -> match info (only matches with counted games)
    beatmaps: dict[int, dict]
    teams: dict[int, dict]        # tournament_teams.id -> row (empty for formats without teams)
    member_team: dict[int, int]   # user_id -> team id, scoped to this tournament
    empty_matches: int = 0        # imported lobbies with no counted game (abandoned / forfeited)


def load_dataset(conn: sqlite3.Connection, tournament_id: int) -> Dataset:
    t = dict(conn.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone())
    rows = conn.execute(
        """SELECT g.id AS game_id, m.id AS match_id, m.osu_match_id, m.round, g.order_index,
                  COALESCE(g.mod_bucket, 'NM') AS bucket, g.pool_slot, g.beatmap_id, g.scoring_type, g.team_type,
                  s.user_id, s.score, s.accuracy, s.max_combo, s.count_miss, s.team, s.passed
           FROM game_scores s
           JOIN match_games g ON g.id = s.game_id
           JOIN tournament_matches m ON m.id = g.match_id
           WHERE m.tournament_id = ? AND m.status = 'imported' AND g.excluded = 0
           ORDER BY m.start_time, g.order_index""",
        (tournament_id,),
    ).fetchall()
    scores = [ScoreRow(**{k: r[k] for k in r.keys()}) for r in rows]
    for s in scores:
        s.passed = bool(s.passed)

    user_ids = {s.user_id for s in scores}
    players = {
        r["user_id"]: dict(r)
        for r in conn.execute("SELECT * FROM players").fetchall() if r["user_id"] in user_ids
    }
    imported = {
        r["id"]: dict(r)
        for r in conn.execute(
            "SELECT * FROM tournament_matches WHERE tournament_id = ? AND status = 'imported'", (tournament_id,)
        ).fetchall()
    }
    with_games = {s.match_id for s in scores}
    matches = {k: v for k, v in imported.items() if k in with_games}   # empty lobbies never reach analytics
    teams = {r["id"]: dict(r) for r in conn.execute(
        "SELECT * FROM tournament_teams WHERE tournament_id = ?", (tournament_id,))}
    member_team = {r["user_id"]: r["team_id"] for r in conn.execute(
        "SELECT user_id, team_id FROM team_memberships WHERE tournament_id = ?", (tournament_id,))}
    bm_ids = {s.beatmap_id for s in scores if s.beatmap_id}
    beatmaps = {r["beatmap_id"]: dict(r) for r in conn.execute("SELECT * FROM beatmaps").fetchall() if r["beatmap_id"] in bm_ids}
    return Dataset(tournament=t, scores=scores, players=players, matches=matches, beatmaps=beatmaps,
                   teams=teams, member_team=member_team, empty_matches=len(imported) - len(matches))
