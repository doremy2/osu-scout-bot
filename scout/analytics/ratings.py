"""Rating model v2: Performance Rating vs Tournament Rating.

1. Normalize every score against the field that played the same beatmap:
       z = (T(metric) - mean) / std        T = sqrt by default (see cfg.transform), clipped to ±z_clip
   Reference group = same beatmap + mod bucket + stage, where stage is "qualifier" (all qualifier
   lobbies pooled, so a weak or strong lobby can't change anyone's rating) or "bracket".
   Tiny groups (a 1v1 map played in 2 matches) borrow a pooled std from their stage so the
   margin of victory still matters instead of every winner being exactly +1.

2. Performance Rating = how good the scores were:  base + scale · Σ(w·z) / (Σw + perf_k)   (light shrinkage)

3. Tournament Rating = body of work:
       obs  = Σ(w·z') / Σw            z' = z + field_strength_weight · strength(round)
       conf = Σw / (Σw + confidence_k)
       Z_T  = conf · obs + (1 - conf) · prior        prior = average participant
       rating = base + scale · Z_T
   w are round weights (later rounds = more evidence), strength(round) is how much better than
   the qualifier field the players competing in that round were, and conf is the sample confidence.
   There is no qualification bonus: depth only enters through more evidence, a stronger field and
   a higher confidence.

Slice ratings (a mod, a round) stay skill ratings: performance only, with their own confidence shown.
Everything tunable lives in RatingConfig.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field

from ..rounds import is_versus_round, round_order, round_weight
from .dataset import Dataset, ScoreRow


@dataclass
class RatingConfig:
    # --- rating scale -----------------------------------------------------
    base: float = 7.0
    scale: float = 1.5               # rating points per standard deviation above the field average
    scale_below: float = 2.5         # ...and below it: bad performances fall faster than good ones rise
    min_rating: float = 1.0
    z_clip: float = 3.0
    low_score_start: float = 0.5     # a score this many σ below the field starts being punished extra...
    low_score_amp: float = 1.0       # ...every further σ below that counts (1 + amp) times; 0 disables
    # --- normalization ----------------------------------------------------
    transform: str = "sqrt"          # raw | sqrt | log: tames the long right tail of scorev2 scores
    std_prior_n: float = 3.0         # tiny reference groups borrow this many pseudo-samples of pooled std
    pool_qualifiers: bool = True     # compare qualifier scores across ALL qualifier lobbies
    # --- performance rating -----------------------------------------------
    perf_k: float = 3.0              # light shrinkage so one lucky map isn't a 10.0
    # --- tournament rating ------------------------------------------------
    confidence_k: float | None = None   # conf = n / (n + k). None = auto: k_noise + workload term
    below_average_k_factor: float = 0.0  # players below the average participant are shrunk toward it this much less (0 = not at all)
    confidence_workload_weight: float = 1.0  # auto k adds this × (average maps a participant plays)
    field_strength_weight: float = 0.5  # credit for playing against the (stronger) bracket field
    use_round_weights: bool = True
    round_weights: dict | None = None   # override rounds.py weights, e.g. {"GF": 1.3}
    rank_qualified_first: bool = True   # players who reached a bracket round rank above those who did not
    # --- eligibility ------------------------------------------------------
    slice_confidence_k: float = 5.0     # confidence of a single mod/round slice: n / (n + k)
    min_slice_confidence: float = 0.5   # mod/round awards need at least this; below it the UI says 'low confidence'

    def weight(self, round_code: str | None) -> float:
        if not self.use_round_weights:
            return 1.0
        if self.round_weights and round_code in self.round_weights:
            return float(self.round_weights[round_code])
        return round_weight(round_code)

    # kept for older callers
    @property
    def shrink_k(self) -> float:
        return self.perf_k


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

    def confidence(self, k: float) -> float:
        return self.w_sum / (self.w_sum + k) if self.w_sum else 0.0

    def z_std(self) -> float | None:
        if self.n < 2:
            return None
        m = sum(self.zs) / self.n
        return math.sqrt(sum((z - m) ** 2 for z in self.zs) / (self.n - 1))


@dataclass
class PlayerStats:
    user_id: int
    overall: Slice = field(default_factory=Slice)   # raw per-map z  -> Performance Rating
    adj: Slice = field(default_factory=Slice)       # z + field strength -> Tournament Rating
    deepest_round: str | None = None
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


def _transformed(s: ScoreRow, cfg: RatingConfig) -> float:
    m = s.metric
    if s.scoring_type in ("accuracy", "combo") or cfg.transform == "raw":
        return m
    if cfg.transform == "log":
        return math.log(max(m, 1.0))
    return math.sqrt(max(m, 0.0))


def _stage(s: ScoreRow, cfg: RatingConfig):
    """Qualifier scores are pooled across lobbies; bracket scores are compared per beatmap."""
    if not is_versus_round(s.round):
        return "Q" if cfg.pool_qualifiers else ("Q", s.match_id)
    return "B"


def _stage_name(stage) -> str:
    return "Q" if stage == "Q" or isinstance(stage, tuple) else "B"


def _group_key(s: ScoreRow, cfg: RatingConfig):
    if s.beatmap_id:
        return (s.beatmap_id, s.bucket, s.scoring_type, _stage(s, cfg))
    return ("game", s.game_id)


def _punish_low(z: float, cfg: RatingConfig) -> float:
    """Poor maps hurt more than good maps help: below -low_score_start σ, extra σ count (1+amp)x."""
    if z >= -cfg.low_score_start:
        return z
    return z - cfg.low_score_amp * (-z - cfg.low_score_start)


def normalize_scores(ds: Dataset, cfg: RatingConfig) -> dict[tuple[int, int], float]:
    """(game_id, user_id) -> z, using the reference groups described in the module docstring."""
    groups: dict[tuple, list[tuple[ScoreRow, float]]] = defaultdict(list)
    for s in ds.scores:
        groups[_group_key(s, cfg)].append((s, _transformed(s, cfg)))

    # pooled coefficient of variation per (stage, scoring type), from groups big enough to trust
    num: dict[tuple, float] = defaultdict(float)
    den: dict[tuple, float] = defaultdict(float)
    for key, rows in groups.items():
        if len(rows) < 4 or key[0] == "game":
            continue
        vals = [v for _, v in rows]
        m = sum(vals) / len(vals)
        if m <= 0:
            continue
        var = sum((v - m) ** 2 for v in vals) / (len(vals) - 1)
        pk = (_stage_name(key[3]), key[2])
        num[pk] += (len(vals) - 1) * var / (m * m)
        den[pk] += len(vals) - 1
    pooled_cv = {pk: math.sqrt(num[pk] / den[pk]) for pk in num if den[pk]}
    fallback_cv = math.sqrt(sum(num.values()) / sum(den.values())) if den and sum(den.values()) else 0.3

    z: dict[tuple[int, int], float] = {}
    for key, rows in groups.items():
        n = len(rows)
        if n < 2:
            for s, _ in rows:
                z[(s.game_id, s.user_id)] = 0.0
            continue
        vals = [v for _, v in rows]
        mean = sum(vals) / n
        var = sum((v - mean) ** 2 for v in vals) / (n - 1)
        first = rows[0][0]
        cv = pooled_cv.get((_stage_name(_stage(first, cfg)), first.scoring_type), fallback_cv)
        prior_var = (cv * mean) ** 2
        k0 = cfg.std_prior_n
        std = math.sqrt(((n - 1) * var + k0 * prior_var) / ((n - 1) + k0))
        for s, v in rows:
            zi = 0.0 if std == 0 else max(-cfg.z_clip, min(cfg.z_clip, (v - mean) / std))
            z[(s.game_id, s.user_id)] = _punish_low(zi, cfg)
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
        w = cfg.weight(s.round)
        p.overall.add(zi, w)
        if s.round and (p.deepest_round is None or round_order(s.round) > round_order(p.deepest_round)):
            p.deepest_round = s.round
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

    strength = field_strength(ds, stats, cfg)
    for s in ds.scores:     # second pass: tournament-rating z, credited for the strength of the field
        stats[s.user_id].adj.add(z[(s.game_id, s.user_id)] + cfg.field_strength_weight * strength.get(s.round, 0.0),
                                 cfg.weight(s.round))

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
    r = cfg.base + (cfg.scale if z >= 0 else cfg.scale_below) * z
    return round(max(cfg.min_rating, r), 2)


# ---- tournament rating model ---------------------------------------------------
def field_strength(ds: Dataset, stats: dict[int, PlayerStats], cfg: RatingConfig) -> dict[str | None, float]:
    """Per bracket round: how far above the qualifier field its competitors were (average of their
    qualifier performance). Zero for qualifiers themselves and for tournaments without qualifiers."""
    q_perf = {uid: p.by_round["Q"].z_adj(cfg.perf_k) for uid, p in stats.items() if p.by_round.get("Q") and p.by_round["Q"].n}
    if not q_perf:
        return {}
    players_in: dict[str | None, set[int]] = defaultdict(set)
    for s in ds.scores:
        if is_versus_round(s.round):
            players_in[s.round].add(s.user_id)
    out = {}
    for r, users in players_in.items():
        vals = [q_perf[u] for u in users if u in q_perf]
        out[r] = sum(vals) / len(vals) if vals else 0.0
    return out


def estimate_confidence_k(stats: dict[int, PlayerStats]) -> float:
    """Empirical-Bayes prior strength k = σ²_within / τ²_between (one-way random-effects, method of
    moments). It is the number of 'average' maps worth of doubt that best separates real skill
    differences from per-map noise in this tournament."""
    rows = [p.overall for p in stats.values() if p.overall.n >= 2]
    if len(rows) < 3:
        return 12.0
    n_tot = sum(r.n for r in rows)
    within_ss = sum(sum((z - sum(r.zs) / r.n) ** 2 for z in r.zs) for r in rows)
    sigma2 = within_ss / max(1, n_tot - len(rows))
    grand = sum(sum(r.zs) for r in rows) / n_tot
    between_ss = sum(r.n * (sum(r.zs) / r.n - grand) ** 2 for r in rows)
    n0 = (n_tot - sum(r.n ** 2 for r in rows) / n_tot) / (len(rows) - 1)
    tau2 = (between_ss / (len(rows) - 1) - sigma2) / n0
    if tau2 <= 1e-6:
        return 50.0
    return max(1.0, min(60.0, sigma2 / tau2))


@dataclass
class ModelParams:
    k: float            # confidence prior strength actually used
    prior: float        # z of the average participant (shrinkage target)
    k_noise: float = 0.0       # statistical part: within-player noise / between-player skill spread
    k_workload: float = 0.0    # body-of-work part: a typical participant's map count
    below_factor: float = 1.0  # multiplies k when the observed performance is below the prior


def resolve_model(stats: dict[int, PlayerStats], cfg: RatingConfig) -> ModelParams:
    obs = [p.adj.z_mean() for p in stats.values() if p.adj.n]
    prior = sum(obs) / len(obs) if obs else 0.0
    if cfg.confidence_k is not None:
        return ModelParams(k=cfg.confidence_k, prior=prior, below_factor=cfg.below_average_k_factor)
    noise = estimate_confidence_k(stats)
    workload = cfg.confidence_workload_weight * (sum(p.overall.n for p in stats.values()) / max(1, len(stats)))
    return ModelParams(k=noise + workload, prior=prior, k_noise=noise, k_workload=workload,
                       below_factor=cfg.below_average_k_factor)


def tournament_z(p: PlayerStats, m: ModelParams) -> tuple[float, float, float]:
    """(Z_T, confidence, observed) for the Tournament Rating."""
    obs = p.adj.z_mean()
    conf = p.adj.confidence(m.k)
    use = p.adj.confidence(m.k * m.below_factor) if obs < m.prior else conf   # a poor body of work is not rescued by a thin sample
    return use * obs + (1 - use) * m.prior, conf, obs
