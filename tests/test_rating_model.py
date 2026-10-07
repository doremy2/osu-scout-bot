"""Performance Rating vs Tournament Rating: behaviours the model must have, on synthetic fields."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scout.analytics.dataset import Dataset, ScoreRow  # noqa: E402
from scout.analytics.ratings import (RatingConfig, compute_player_stats, normalize_scores,  # noqa: E402
                                     resolve_model, to_rating, tournament_z)


class Field:
    """A tournament where every beatmap/stage has 12 'filler' players with a fixed spread
    (sqrt-score mean 700, sd 100), so a test player's z is whatever we ask for."""

    def __init__(self):
        self.scores: list[ScoreRow] = []
        self.games = 0
        self.filled: set = set()
        self.next_filler = 10_000

    def _row(self, game, match, rnd, beatmap, bucket, uid, sqrt_value):
        return ScoreRow(game_id=game, match_id=match, osu_match_id=match, round=rnd, order_index=0, bucket=bucket,
                        pool_slot=None, beatmap_id=beatmap, scoring_type="scorev2", team_type="head-to-head",
                        user_id=uid, score=int(sqrt_value ** 2), accuracy=0.95, max_combo=500, count_miss=0,
                        team=None, passed=True)

    def fill(self, rnd, beatmap, bucket):
        stage = "Q" if rnd == "Q" else "B"
        if (stage, beatmap) in self.filled:
            return
        self.filled.add((stage, beatmap))
        for i in range(12):
            self.games += 1
            # fillers play in pairs per game so bracket games look like normal 1v1 lobbies
            self.scores.append(self._row(self.games, 900 + self.games, rnd, beatmap, bucket, 10_001 + i,
                                         700 + 100 * (i - 5.5) / 3.45))

    def play(self, uid, rnd, beatmap, z, bucket="NM", lobby=1):
        self.fill(rnd, beatmap, bucket)
        self.games += 1
        self.scores.append(self._row(self.games, lobby if rnd == "Q" else 500 + uid * 100 + self.games, rnd, beatmap,
                                     bucket, uid, 700 + 100 * z))

    def dataset(self) -> Dataset:
        return Dataset(tournament={"id": 1}, scores=self.scores, players={}, matches={}, beatmaps={}, teams={}, member_team={})


def ratings(field: Field, cfg: RatingConfig | None = None):
    cfg = cfg or RatingConfig()
    stats, *_ = compute_player_stats(field.dataset(), cfg)
    model = resolve_model(stats, cfg)
    out = {}
    for uid, p in stats.items():
        zt, conf, _ = tournament_z(p, model)
        out[uid] = {"tournament": to_rating(zt, cfg), "performance": to_rating(p.overall.z_adj(cfg.perf_k), cfg),
                    "confidence": conf, "maps": p.overall.n, "p": p}
    return out


def run(field, uid, rounds, z, per_round):
    """uid plays `per_round` maps in each listed round, performing at z."""
    for rnd in rounds:
        for m in range(per_round):
            field.play(uid, rnd, (1 if rnd == "Q" else 100 + hash(rnd) % 50) * 1000 + m, z)


def baseline(field):
    """A normal participant pool so the tournament has an average to shrink toward."""
    for uid in range(1, 9):
        run(field, uid, ["Q"], 0.0, 10)
    for uid in range(9, 13):
        run(field, uid, ["Q", "RO16"], 0.0, 10)


def test_one_amazing_map_is_not_the_mvp():
    f = Field()
    baseline(f)
    run(f, 20, ["Q", "RO16", "QF", "SF"], 1.0, 10)        # 40 maps of solid play
    f.play(21, "Q", 1000, 3.0)                           # one monster map and nothing else
    r = ratings(f)
    assert r[21]["performance"] < 9.5                    # even the light shrinkage keeps one map in check
    assert r[21]["tournament"] < r[20]["tournament"]
    assert max((u for u in r if u < 10_000), key=lambda u: r[u]["tournament"]) == 20   # fillers are the reference field


def test_ten_strong_qualifier_maps_vs_fifty_strong_tournament_maps():
    f = Field()
    baseline(f)
    run(f, 30, ["Q"], 1.5, 10)                           # 10 great qualifier maps, eliminated
    run(f, 31, ["Q", "RO16", "QF", "SF", "F"], 1.2, 10)  # 50 strong maps across the whole run
    r = ratings(f)
    assert r[30]["performance"] > r[31]["performance"]   # pure performance: the short run looks better
    assert r[31]["tournament"] > r[30]["tournament"]     # body of work: the long run wins
    assert r[31]["confidence"] > r[30]["confidence"] + 0.2


def test_excellent_early_exit_can_outrank_a_poor_deep_run():
    f = Field()
    baseline(f)
    run(f, 40, ["Q"], 2.0, 12)                           # outstanding, eliminated after qualifiers
    run(f, 41, ["Q", "RO16", "QF", "SF", "F"], -0.1, 10) # mediocre but reached the final
    r = ratings(f)
    assert r[40]["tournament"] > r[41]["tournament"]


def test_equal_performance_more_evidence_ranks_higher():
    f = Field()
    baseline(f)
    run(f, 50, ["Q"], 1.0, 10)
    run(f, 51, ["Q", "RO16"], 1.0, 10)
    run(f, 51, ["QF", "SF"], 1.0, 10)
    for m in range(10):
        f.play(51, "F", 7000 + m, 1.0)
    for m in range(10):
        f.play(51, "GF", 8000 + m, 1.0)
    r = ratings(f)
    assert abs(r[50]["performance"] - r[51]["performance"]) < 0.25
    assert r[51]["tournament"] > r[50]["tournament"]


def test_mod_rating_stays_high_when_overall_confidence_is_low():
    f = Field()
    baseline(f)
    run(f, 60, ["Q", "RO16", "QF", "SF"], 0.8, 10)       # established player with plenty of NM
    for m in range(3):
        f.play(61, "Q", 30_000 + m, 2.5, bucket="DT")   # 3 superb DT maps, nothing else
    for m in range(3):
        f.play(60, "Q", 30_000 + m, 0.5, bucket="DT")
    cfg = RatingConfig()
    r = ratings(f, cfg)
    dt = r[61]["p"].by_mod["DT"]
    assert to_rating(dt.z_adj(cfg.perf_k), cfg) > to_rating(r[60]["p"].by_mod["DT"].z_adj(cfg.perf_k), cfg)
    assert to_rating(dt.z_adj(cfg.perf_k), cfg) > 8.0    # the mod skill rating is not dragged down by progression
    assert r[61]["confidence"] < 0.3                     # ...while the overall rating knows it is thin
    assert dt.confidence(cfg.slice_confidence_k) < cfg.min_slice_confidence   # so Best DT must not go to them


def test_qualifier_scores_are_compared_across_lobbies():
    """Same beatmap in a strong lobby and a weak lobby: the same score must get the same z."""
    f = Field()
    f.games = 0
    sc = f._row
    # lobby 1: five strong players; lobby 2: five weak players; same map, same stage
    strong = [800, 820, 840, 860, 880]
    weak = [500, 520, 540, 560, 580]
    for i, v in enumerate(strong):
        f.games += 1
        f.scores.append(sc(f.games, 1, "Q", 7, "NM", 100 + i, v))
    for i, v in enumerate(weak):
        f.games += 1
        f.scores.append(sc(f.games, 2, "Q", 7, "NM", 200 + i, v))
    # same absolute score in both lobbies
    f.games += 1
    f.scores.append(sc(f.games, 1, "Q", 7, "NM", 300, 700))
    f.games += 1
    f.scores.append(sc(f.games, 2, "Q", 7, "NM", 301, 700))

    def z_of(cfg, uid):
        z = normalize_scores(f.dataset(), cfg)
        row = next(s for s in f.scores if s.user_id == uid)
        return z[(row.game_id, uid)]

    pooled = RatingConfig(pool_qualifiers=True)
    assert abs(z_of(pooled, 300) - z_of(pooled, 301)) < 1e-9
    per_lobby = RatingConfig(pool_qualifiers=False)
    assert abs(z_of(per_lobby, 300) - z_of(per_lobby, 301)) > 0.5   # per-lobby would reward the weak lobby


def test_tiny_bracket_groups_keep_the_margin_of_victory():
    """A 1v1 map seen by only two players must not give every winner exactly +1 and loser -1."""
    f = Field()
    baseline(f)
    for uid, (a, b) in enumerate([(850, 800), (850, 500)], start=70):
        f.games += 1
        f.scores.append(f._row(f.games, 900 + uid, "RO16", 5000 + uid, "NM", uid * 10, a))
        f.scores.append(f._row(f.games, 900 + uid, "RO16", 5000 + uid, "NM", uid * 10 + 1, b))
    z = normalize_scores(f.dataset(), RatingConfig())
    win_close = z[(f.scores[-4].game_id, 700)]
    win_big = z[(f.scores[-2].game_id, 710)]
    assert 0 < win_close < win_big
