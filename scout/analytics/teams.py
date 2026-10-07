"""Team-level analytics. Kept apart so the team rating formula can be replaced on its own.

V1 team rating: pool every counted score of the team's players (same z-scores the player
ratings use, same round weights and small-sample shrinkage) and convert like a player rating.
That makes a team's rating "how far above the field its players performed on average",
independent of bracket luck or tournament placement.
"""
from __future__ import annotations

from .ratings import RatingConfig, Slice, to_rating


def merge_slices(slices: list[Slice]) -> Slice:
    out = Slice()
    for s in slices:
        out.n += s.n
        out.w_sum += s.w_sum
        out.wz_sum += s.wz_sum
        out.zs.extend(s.zs)
    return out


def team_rating(player_slices: list[Slice], cfg: RatingConfig) -> float:
    return to_rating(merge_slices(player_slices).z_adj(cfg.shrink_k), cfg)
