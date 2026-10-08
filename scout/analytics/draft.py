"""Pick/ban draft simulator between two sides (players in 1v1, teams in team tournaments).

Everything the UI shows comes from here. Given a round's map pool and the draft so far, it returns,
for every map, each side's expected performance and the chance side A wins it, plus advice for the
side that acts next and the projected chance to win the match.

Expected performance of a side on a map (in field-adjusted z units, the same scale as ratings):
    mu_player(map) = shrink(  map plays  ->  shrink( same-mod plays  ->  shrink( all plays -> average player )))
with each level pulled toward the one above by a few pseudo-observations, so a map nobody has played
falls back to the mod skill, and a mod nobody has played falls back to overall skill.
A team's expected performance is the mean of its best `lineup` players on that map.

    P(A wins map) = Phi( (mu_A - mu_B) / sigma )     sigma from the tournament's own per-map noise
"""
from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass

from ..classify import bucket_from_slot
from ..rounds import round_name
from .ratings import to_rating, within_sigma

MOD_ORDER = ["NM", "HD", "HR", "DT", "EZ", "FL", "HT", "FM", "LM", "TB"]


@dataclass
class DraftConfig:
    best_of: int = 9
    bans_per_side: int = 2
    first_ban: str = "A"
    first_pick: str = "B"
    map_prior_k: float = 2.0       # pseudo-plays pulling a map estimate toward the player's mod skill
    mod_prior_k: float = 5.0       # ... a mod estimate toward overall skill
    overall_prior_k: float = 3.0   # ... overall skill toward the average player


def _phi(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def _other(side: str) -> str:
    return "B" if side == "A" else "A"


# ---- the pool -------------------------------------------------------------------
def round_pools(an) -> list[dict]:
    """Rounds that have a pool, with how many maps it holds (for the setup screen).
    The official pool from the sheet wins; rounds without one fall back to the maps actually played."""
    ds = an.ds
    from ..rounds import round_order
    counts: dict[str, int] = {}
    for r, slots in (ds.pool_slots or {}).items():
        counts[r] = len(slots)
    played: dict[str, set] = defaultdict(set)
    for s in ds.scores:
        if s.round and s.beatmap_id:
            played[s.round].add(s.beatmap_id)
    for r, ids in played.items():
        counts.setdefault(r, len(ids))
    return [{"round": r, "name": round_name(r), "maps": n}
            for r, n in sorted(counts.items(), key=lambda kv: round_order(kv[0])) if n >= 2]


_LABEL_RE = re.compile(r"^(?P<artist>.+?)\s+-\s+(?P<title>.+?)(?:\s*\[(?P<version>[^\[\]]*)\])?$")


def _split_label(label: str | None) -> tuple[str, str, str]:
    if not label:
        return "", "", ""
    m = _LABEL_RE.match(label.strip())
    return (m["artist"], m["title"], m["version"] or "") if m else ("", label.strip(), "")


def pool_for_round(an, round_code: str) -> list[dict]:
    """Every map of the round's pool in official order (NM1, NM2 ... HD1 ... TB1).

    With a sheet-provided pool this includes maps nobody has played yet (known only by their sheet label);
    without one it is the maps played in that round, ordered by mod then difficulty."""
    ds = an.ds
    buckets: dict[int, Counter] = defaultdict(Counter)
    plays: Counter = Counter()
    for s in ds.scores:
        if s.round == round_code and s.beatmap_id:
            buckets[s.beatmap_id][s.bucket] += 1
            plays[s.beatmap_id] += 1

    def played_entry(bid: int) -> dict:
        b = ds.beatmaps.get(bid) or {}
        return {"beatmapset_id": b.get("beatmapset_id"), "star_rating": b.get("star_rating"),
                "title": b.get("title") or f"beatmap {bid}", "artist": b.get("artist") or "", "version": b.get("version") or ""}

    out: list[dict] = []
    slots = (ds.pool_slots or {}).get(round_code)
    if slots:
        for sl in slots:
            bid = sl["beatmap_id"]
            bucket = bucket_from_slot(sl["slot"])
            if bid and bid in buckets:
                info = played_entry(bid)
                observed = buckets[bid].most_common(1)[0][0]
            else:                                                    # in the pool, but never played here
                artist, title, version = _split_label(sl["label"])
                info = {"beatmapset_id": sl["beatmapset_id"], "star_rating": sl["star_rating"],
                        "title": title or sl["slot"], "artist": artist, "version": version}
                if bid and bid in ds.beatmaps:
                    info = played_entry(bid)
                observed = bucket
            out.append({"beatmap_id": bid or -(sl["position"] + 1), "mod": bucket, "obs_mod": observed,
                        "plays": plays.get(bid, 0) if bid else 0, "slot": sl["slot"], "position": sl["position"], **info})
    else:
        for bid, c in buckets.items():
            observed = c.most_common(1)[0][0]
            out.append({"beatmap_id": bid, "mod": observed, "obs_mod": observed, "plays": plays[bid],
                        "slot": None, "position": None, **played_entry(bid)})
    out.sort(key=lambda m: (MOD_ORDER.index(m["mod"]) if m["mod"] in MOD_ORDER else 99,
                            m["position"] if m["position"] is not None else 1_000,
                            m["star_rating"] or 0.0))
    return out


# ---- expected performance ---------------------------------------------------------
def _observations(an) -> dict[int, list[tuple[float, str, int | None]]]:
    """user_id -> [(field-adjusted z, mod bucket, beatmap id)] across every counted score."""
    cache = an.cache
    if "obs" in cache:
        return cache["obs"]
    lam = an.cfg.field_strength_weight
    obs: dict[int, list] = defaultdict(list)
    for s in an.ds.scores:
        zi = an.z[(s.game_id, s.user_id)] + lam * an.strength.get(s.round, 0.0)
        obs[s.user_id].append((zi, s.bucket, s.beatmap_id))
    cache["obs"] = obs
    return obs


def _mu_player(obs: list, bucket: str, beatmap_id: int, prior: float, cfg: DraftConfig) -> tuple[float, int, int, int]:
    """(mu, n_all, n_mod, n_map)"""
    all_z = [z for z, _, _ in obs]
    mod_z = [z for z, b, _ in obs if b == bucket]
    map_z = [z for z, _, bid in obs if bid == beatmap_id]
    mu0 = (sum(all_z) + cfg.overall_prior_k * prior) / (len(all_z) + cfg.overall_prior_k)
    mu1 = (sum(mod_z) + cfg.mod_prior_k * mu0) / (len(mod_z) + cfg.mod_prior_k)
    mu2 = (sum(map_z) + cfg.map_prior_k * mu1) / (len(map_z) + cfg.map_prior_k)
    return mu2, len(all_z), len(mod_z), len(map_z)


def _lineup_size(an) -> int:
    if "lineup" in an.cache:
        return an.cache["lineup"]
    sizes = []
    by_game: dict = defaultdict(Counter)
    for s in an.ds.scores:
        if s.team_type and "team" in s.team_type and s.team in ("red", "blue"):
            by_game[s.game_id][s.team] += 1
    for c in by_game.values():
        sizes.extend(c.values())
    sizes.sort()
    size = sizes[len(sizes) // 2] if sizes else 1
    an.cache["lineup"] = max(1, size)
    return an.cache["lineup"]


# ---- sides --------------------------------------------------------------------------
def list_sides(an) -> list[dict]:
    rep = an.report
    if rep["tournament"]["has_teams"]:
        return [{"slug": t["slug"], "name": t["name"], "country": t["country"], "rating": t["rating"],
                 "detail": f"{t['roster_size']} players"} for t in rep["teams"]]
    return [{"slug": r["slug"], "name": r["username"], "avatar_url": r["avatar_url"], "country": r["country"],
             "rating": r["rating"], "detail": f"{r['maps_played']} maps"} for r in rep["rankings"]]


def _side_members(an, slug: str) -> tuple[str, list[int]]:
    rep = an.report
    if rep["tournament"]["has_teams"]:
        page = rep["team_pages"].get(slug)
        if not page:
            raise KeyError(slug)
        return page["name"], [r["user_id"] for r in page["roster"]]
    page = rep["players"].get(slug)
    if not page:
        raise KeyError(slug)
    return page["username"], [page["user_id"]]


def _side_mu(an, members: list[int], m: dict, cfg: DraftConfig) -> dict:
    obs = _observations(an)
    prior = an.model.prior
    L = _lineup_size(an) if an.report["tournament"]["has_teams"] else 1
    est = []
    for uid in members:
        if uid in obs:
            est.append((_mu_player(obs[uid], m.get("obs_mod", m["mod"]), m["beatmap_id"], prior, cfg), uid))
    if not est:
        return {"mu": prior, "n": 0, "n_map": 0, "n_mod": 0, "players": 0}
    est.sort(key=lambda e: -e[0][0])
    top = est[:L]
    mu = sum(e[0][0] for e in top) / len(top)
    return {"mu": mu, "n": sum(e[0][1] for e in top), "n_map": sum(e[0][3] for e in top),
            "n_mod": sum(e[0][2] for e in top), "players": len(top)}


def _reasons(an, name_a, name_b, m, ea, eb, cfg_rating) -> list[str]:
    out = []
    ra, rb = to_rating(ea["mu"], cfg_rating), to_rating(eb["mu"], cfg_rating)
    diff = ra - rb
    if abs(diff) >= 0.15:
        who, other = (name_a, name_b) if diff > 0 else (name_b, name_a)
        out.append(f"{who} is stronger on {m['mod']}: {max(ra, rb):.2f} vs {min(ra, rb):.2f}")
    for name, e in ((name_a, ea), (name_b, eb)):
        if e["n_map"]:
            out.append(f"{name} has played it ({e['n_map']}×)")
    if not out:
        out.append("even matchup")
    return out


# ---- the draft ---------------------------------------------------------------------------
def build_sequence(cfg: DraftConfig, pool_size: int, has_tb: bool) -> list[dict]:
    seq = []
    for i in range(cfg.bans_per_side * 2):
        seq.append({"type": "ban", "side": cfg.first_ban if i % 2 == 0 else _other(cfg.first_ban)})
    available = pool_size - len(seq) - (1 if has_tb else 0)
    n_picks = max(0, min(cfg.best_of - 1, available))
    for i in range(n_picks):
        seq.append({"type": "pick", "side": cfg.first_pick if i % 2 == 0 else _other(cfg.first_pick)})
    return seq


def match_win_probability(p_a: list[float], best_of: int, tb_p: float = 0.5) -> float:
    """P(A wins the match) when games are played in this order with these map win chances for A."""
    target = best_of // 2 + 1
    games = list(p_a)
    if len(games) < best_of:
        games += [tb_p] * (best_of - len(games))      # unresolved games count as coin flips
    state = {(0, 0): 1.0}
    win = 0.0
    for p in games:
        nxt: dict = defaultdict(float)
        for (a, b), pr in state.items():
            if a >= target or b >= target:
                continue
            nxt[(a + 1, b)] += pr * p
            nxt[(a, b + 1)] += pr * (1 - p)
        state = nxt
        for (a, b), pr in list(state.items()):
            if a >= target:
                win += pr
                del state[(a, b)]
            elif b >= target:
                del state[(a, b)]
    return win


def draft_advice(an, a_slug: str, b_slug: str, round_code: str, taken: list[int], cfg: DraftConfig) -> dict:
    """Full state of a draft: per-map odds, who acts next, ranked advice and the projected match result."""
    pool = pool_for_round(an, round_code)
    if len(pool) < 4:
        raise ValueError("This round has too few maps for a draft.")
    name_a, members_a = _side_members(an, a_slug)
    name_b, members_b = _side_members(an, b_slug)
    if a_slug == b_slug:
        raise ValueError("Pick two different sides.")

    sigma_w = an.cache.get("sigma_w")
    if sigma_w is None:
        sigma_w = an.cache["sigma_w"] = within_sigma(an.stats)
    L = _lineup_size(an) if an.report["tournament"]["has_teams"] else 1
    rating_cfg = an.cfg

    maps = {}
    for m in pool:
        ea, eb = _side_mu(an, members_a, m, cfg), _side_mu(an, members_b, m, cfg)
        var = sigma_w ** 2 * (2.0 / L + 1.0 / (ea["n"] + 3) + 1.0 / (eb["n"] + 3))
        p_a = _phi((ea["mu"] - eb["mu"]) / math.sqrt(var))
        maps[m["beatmap_id"]] = {
            **m, "p_a": round(p_a, 4),
            "rating_a": to_rating(ea["mu"], rating_cfg), "rating_b": to_rating(eb["mu"], rating_cfg),
            "plays_a": ea["n_map"], "plays_b": eb["n_map"],
            "reasons": _reasons(an, name_a, name_b, m, ea, eb, rating_cfg),
        }

    tb_maps = [m for m in pool if m["mod"] == "TB"]
    drafted = [m for m in pool if m["mod"] != "TB"]
    seq = build_sequence(cfg, len(pool), bool(tb_maps))
    valid = {m["beatmap_id"] for m in drafted}
    taken = [t for t in taken if t in valid][: len(seq)]
    for i, bid in enumerate(taken):
        maps[bid]["taken"] = {"type": seq[i]["type"], "side": seq[i]["side"], "order": i}
    state = {t: i for i, t in enumerate(taken)}

    step = len(taken)
    nxt = seq[step] if step < len(seq) else None
    remaining = [m for m in drafted if m["beatmap_id"] not in state]

    def advice_for(side: str, kind: str) -> list[dict]:
        cands = []
        for m in remaining:
            d = maps[m["beatmap_id"]]
            p_side = d["p_a"] if side == "A" else 1 - d["p_a"]
            score = p_side if kind == "pick" else 1 - p_side       # ban what the opponent is most likely to win
            cands.append({"beatmap_id": m["beatmap_id"], "score": round(score, 4), "p_side": round(p_side, 4),
                          "reasons": d["reasons"]})
        cands.sort(key=lambda c: -c["score"])
        return cands[:5]

    suggestions = advice_for(nxt["side"], nxt["type"]) if nxt else []

    # projected match: played picks keep their order; remaining picks are filled greedily by whoever picks next
    p_list = []
    rem = list(remaining)
    for i, act in enumerate(seq):
        if act["type"] != "pick":
            continue
        if i < len(taken):
            p_list.append(maps[taken[i]]["p_a"])
        elif rem:
            best = max(rem, key=lambda m: maps[m["beatmap_id"]]["p_a"] if act["side"] == "A" else 1 - maps[m["beatmap_id"]]["p_a"])
            rem.remove(best)
            p_list.append(maps[best["beatmap_id"]]["p_a"])
    tb_p = maps[tb_maps[0]["beatmap_id"]]["p_a"] if tb_maps else 0.5
    return {
        "a": {"slug": a_slug, "name": name_a}, "b": {"slug": b_slug, "name": name_b},
        "round": round_code, "round_name": round_name(round_code), "config": cfg.__dict__ | {},
        "sequence": seq, "step": step, "next": nxt, "done": nxt is None,
        "maps": list(maps.values()), "tiebreaker": tb_maps[0]["beatmap_id"] if tb_maps else None,
        "suggestions": suggestions,
        "match_win_a": round(match_win_probability(p_list, cfg.best_of, tb_p), 4),
        "target": cfg.best_of // 2 + 1,
        "sigma": round(sigma_w, 3), "lineup": L,
    }
