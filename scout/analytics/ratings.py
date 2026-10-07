"""Performance rating v1: normalized map performance.

For every score:
    z = (metric - mean(reference)) / std(reference)        clipped to ±cfg.z_clip
The reference group is *everyone who played that beatmap in this tournament*
(same mod bucket). That's what makes 950k on a map where everyone averages 600k
worth more than 980k on a map where everyone averages 950k. If too few people
played a map, we fall back to the lobby itself.

A player's rating for any slice (overall, a mod, a round) is:
    z_mean = Σ(w·z) / Σw                  w = round weight (Finals count slightly more)
    z_adj  = Σ(w·z) / (Σw + k)            shrinkage: small samples pulled toward the average (0)
    rating = base + scale · z

Everything tunable lives in RatingConfig, so v2 algorithms (opponent strength,
map difficulty...) can be swapped in by replacing `normalize_scores`.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field

from ..rounds import is_versus_round, round_weight
from .dataset import Dataset, ScoreRow


@dataclass
class RatingConfig:
    base: float = 7.0
    scale: float = 1.5
    z_clip: float = 3.0
    min_reference: int = 6        # scores needed on a beatmap to use tournament-wide reference
    shrink_k: float = 3.0         # pseudo-maps of "average" performance added to every slice
    use_round_weights: bool = True


@dataclass
class Slice:
    n: int = 0
    w_sum: float = 0.0
    wz_sum: float = 0.0
    zs: list[float] = field(default_factory=list)

    def add(self, z: float, w: float) -> None:
        self.n += 1
        self.w_sum += w
        self.wz_sum += w * z
        self.zs.append(z)

    def z_mean(self) -> float:
        return self.wz_sum / self.w_sum if self.w_sum else 0.0

    def z_adj(self, k: float) -> float:
        return self.wz_sum / (self.w_sum + k) if self.w_sum else 0.0

    def z_std(self) -> float | None:
        if self.n < 2:
            return None
        m = sum(self.zs) / self.n
        return math.sqrt(sum((z - m) ** 2 for z in self.zs) / (self.n - 1))


@dataclass
class PlayerStats:
    user_id: int
    overall: Slice = field(default_factory=Slice)
    by_mod: dict[str, Slice] = field(default_factory=lambda: defaultdict(Slice))
    by_round: dict[str, Slice] = field(default_factory=lambda: defaultdict(Slice))
    score_sum: int = 0
    acc_sum: float = 0.0
    acc_n: int = 0
    misses: int = 0
    map_wins: int = 0
    map_losses: int = 0
    match_wins: int = 0
    match_losses: int = 0
    carry_sum: float = 0.0        # Σ (score / team total · team size); 1.0 = an equal share
    carry_n: int = 0
    best: tuple[float, ScoreRow] | None = None
    matches: set[int] = field(default_factory=set)


def _mean_std(values: list[float]) -> tuple[float, float]:
    n = len(values)
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / n
    return m, math.sqrt(var)


def normalize_scores(ds: Dataset, cfg: RatingConfig) -> dict[tuple[int, int], float]:
    """(game_id, user_id) -> z."""
    by_map: dict[tuple, list[ScoreRow]] = defaultdict(list)
    by_game: dict[int, list[ScoreRow]] = defaultdict(list)
    for s in ds.scores:
        by_game[s.game_id].append(s)
        if s.beatmap_id:
            by_map[(s.beatmap_id, s.bucket, s.scoring_type)].append(s)

    ref_stats: dict = {}
    for key, rows in by_map.items():
        if len(rows) >= cfg.min_reference:
            ref_stats[key] = _mean_std([r.metric for r in rows])
    game_stats = {gid: _mean_std([r.metric for r in rows]) for gid, rows in by_game.items() if len(rows) >= 2}

    z: dict[tuple[int, int], float] = {}
    for s in ds.scores:
        stats = ref_stats.get((s.beatmap_id, s.bucket, s.scoring_type)) or game_stats.get(s.game_id)
        if not stats or stats[1] == 0:
            z[(s.game_id, s.user_id)] = 0.0
            continue
        mean, std = stats
        z[(s.game_id, s.user_id)] = max(-cfg.z_clip, min(cfg.z_clip, (s.metric - mean) / std))
    return z


@dataclass
class GameOutcome:
    winner: str | None                  # side
    side_totals: dict[str, float]
    side_sizes: dict[str, int]


@dataclass
class MatchOutcome:
    map_wins: dict[str, int]
    winner: str | None
    sides: dict[int, str]               # user_id -> side (majority over the lobby)


def outcomes(ds: Dataset) -> tuple[dict[int, GameOutcome], dict[int, MatchOutcome]]:
    by_game: dict[int, list[ScoreRow]] = defaultdict(list)
    for s in ds.scores:
        by_game[s.game_id].append(s)

    games: dict[int, GameOutcome] = {}
    match_map_wins: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    user_sides: dict[int, dict[int, dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for gid, rows in by_game.items():
        if not is_versus_round(rows[0].round):   # qualifiers: no winner, no match record
            games[gid] = GameOutcome(None, {}, {})
            continue
        totals: dict[str, float] = defaultdict(float)
        sizes: dict[str, int] = defaultdict(int)
        for r in rows:
            totals[r.side] += r.metric
            sizes[r.side] += 1
            user_sides[r.match_id][r.user_id][r.side] += 1
        ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
        winner = ranked[0][0] if len(ranked) >= 2 and ranked[0][1] > ranked[1][1] else None
        games[gid] = GameOutcome(winner, dict(totals), dict(sizes))
        if winner:
            match_map_wins[rows[0].match_id][winner] += 1
        for side in totals:
            match_map_wins[rows[0].match_id].setdefault(side, 0)

    matches: dict[int, MatchOutcome] = {}
    for mid, wins in match_map_wins.items():
        ranked = sorted(wins.items(), key=lambda kv: kv[1], reverse=True)
        winner = ranked[0][0] if len(ranked) >= 2 and ranked[0][1] > ranked[1][1] else None
        sides = {uid: max(c.items(), key=lambda kv: kv[1])[0] for uid, c in user_sides[mid].items()}
        matches[mid] = MatchOutcome(dict(wins), winner, sides)
    return games, matches


def compute_player_stats(ds: Dataset, cfg: RatingConfig | None = None):
    cfg = cfg or RatingConfig()
    z = normalize_scores(ds, cfg)
    game_out, match_out = outcomes(ds)

    stats: dict[int, PlayerStats] = {}
    for s in ds.scores:
        p = stats.setdefault(s.user_id, PlayerStats(s.user_id))
        zi = z[(s.game_id, s.user_id)]
        w = round_weight(s.round) if cfg.use_round_weights else 1.0
        p.overall.add(zi, w)
        p.by_mod[s.bucket].add(zi, w)
        p.by_round[s.round or "?"].add(zi, w)
        p.score_sum += s.score
        if s.accuracy is not None:
            p.acc_sum += s.accuracy
            p.acc_n += 1
        p.misses += s.count_miss or 0
        p.matches.add(s.match_id)
        if p.best is None or zi > p.best[0]:
            p.best = (zi, s)

        go = game_out[s.game_id]
        if go.winner is not None:
            if go.winner == s.side:
                p.map_wins += 1
            else:
                p.map_losses += 1
        team_size = go.side_sizes.get(s.side, 1)
        team_total = go.side_totals.get(s.side, 0)
        if team_size >= 2 and team_total > 0:
            p.carry_sum += s.metric / team_total * team_size
            p.carry_n += 1

    for mid, mo in match_out.items():
        if mo.winner is None:
            continue
        for uid, side in mo.sides.items():
            if uid in stats:
                if side == mo.winner:
                    stats[uid].match_wins += 1
                else:
                    stats[uid].match_losses += 1
    return stats, z, game_out, match_out


def to_rating(z: float, cfg: RatingConfig) -> float:
    return round(cfg.base + cfg.scale * z, 2)
