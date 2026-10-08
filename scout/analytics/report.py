"""Tournament report: rankings, leaderboards, awards, matches, teams, player pages.

`build_analysis` does the work and keeps the intermediate objects (for match detail pages);
`build_report` returns just the plain JSON-serializable dict. The CLI, the web API and the
Discord bot all render this same structure - ratings are never computed anywhere else.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass

from ..db.repo import get_tournament, slugify
from ..clients import CLIENTS
from ..formats import get_format
from ..rounds import is_versus_round, round_name, round_order
from .dataset import Dataset, load_dataset
from .ratings import (ModelParams, PlayerStats, RatingConfig, Slice, compute_player_stats, config_for_client, field_strength, resolve_model,
                      to_rating, tournament_z)
from .teams import merge_slices, team_tournament_rating

MOD_ORDER = ["NM", "HD", "HR", "DT", "EZ", "FL", "HT", "FM", "LM", "TB"]
LEADERBOARD_MODES = ("tournament", "performance", "round", "mod", "consistency", "maps")


@dataclass
class AwardConfig:
    min_maps_overall: int = 8     # MVP, consistency, accuracy, carry
    min_maps_mod: int = 4         # "Best DT player" etc.
    min_maps_finals: int = 3
    min_team_games: int = 6       # carry needs team games


@dataclass
class Analysis:
    report: dict
    ds: Dataset
    z: dict
    game_out: dict
    match_out: dict
    cfg: RatingConfig
    stats: dict | None = None        # user_id -> PlayerStats (draft simulator, etc.)
    model: ModelParams | None = None
    strength: dict | None = None     # round -> field strength
    cache: dict | None = None        # lazily built helpers


def _pct(w: int, l: int) -> float | None:
    return round(w / (w + l), 3) if (w + l) else None


def flag_url(country_code: str | None) -> str | None:
    return f"https://flagcdn.com/w80/{country_code.lower()}.png" if country_code else None


def build_report(conn: sqlite3.Connection, slug: str,
                 cfg: RatingConfig | None = None, acfg: AwardConfig | None = None) -> dict:
    return build_analysis(conn, slug, cfg, acfg).report


def build_analysis(conn: sqlite3.Connection, slug: str,
                   cfg: RatingConfig | None = None, acfg: AwardConfig | None = None) -> Analysis:
    acfg = acfg or AwardConfig()
    t = get_tournament(conn, slug)
    if t is None:
        raise KeyError(f"No tournament '{slug}'")
    cfg = cfg or config_for_client(t["client"])
    fmt = get_format(t["format"])
    ds = load_dataset(conn, t["id"])
    stats, z, game_out, match_out = compute_player_stats(ds, cfg)
    k = cfg.perf_k                      # light shrinkage for performance / slice ratings
    model = resolve_model(stats, cfg)  # confidence prior for the Tournament Rating

    def name(uid: int) -> str:
        return ds.players.get(uid, {}).get("username") or str(uid)

    def avatar(uid: int) -> str:
        return ds.players.get(uid, {}).get("avatar_url") or f"https://a.ppy.sh/{uid}"

    country_names = {p["country_code"]: p["country_name"] for p in ds.players.values()
                     if p.get("country_code") and p.get("country_name")}

    # stable unique page slugs
    slugs: dict[int, str] = {}
    used: set[str] = set()
    for uid in sorted(stats):
        sl = slugify(name(uid)) or str(uid)
        if sl in used:
            sl = f"{sl}-{uid}"
        used.add(sl)
        slugs[uid] = sl

    def rating_of(sl: Slice) -> float:
        return to_rating(sl.z_adj(k), cfg)

    def team_ref(uid: int) -> dict | None:
        tm = ds.teams.get(ds.member_team.get(uid))
        return {"name": tm["name"], "slug": tm["slug"], "country": tm["country_code"]} if tm else None

    # ---- rankings -------------------------------------------------------
    t_z = {uid: tournament_z(p, model) for uid, p in stats.items()}          # (Z_T, confidence, observed)
    def reached_bracket(p: PlayerStats) -> bool:
        return p.deepest_round is not None and is_versus_round(p.deepest_round)

    has_qualifiers = any(not is_versus_round(s.round) for s in ds.scores if s.round)
    n_qualified = sum(reached_bracket(p) for p in stats.values())
    split_qualified = cfg.rank_qualified_first and has_qualifiers and 0 < n_qualified < len(stats)
    # Tournament ranking: qualified players first (if the tournament had qualifiers), then by rating.
    ranked = sorted(stats.values(),
                    key=lambda p: ((not reached_bracket(p)) if split_qualified else 0, -t_z[p.user_id][0]))
    rank_of = {p.user_id: i for i, p in enumerate(ranked, 1)}
    perf_ranked = sorted(stats.values(), key=lambda p: p.overall.z_adj(k), reverse=True)
    perf_rank_of = {p.user_id: i for i, p in enumerate(perf_ranked, 1)}
    mods_present = sorted({s.bucket for s in ds.scores}, key=lambda m: (MOD_ORDER.index(m) if m in MOD_ORDER else 99, m))
    rounds_present = sorted({s.round for s in ds.scores if s.round}, key=round_order)

    def board(code_slices: list[tuple[PlayerStats, Slice]]) -> list[dict]:
        rows = sorted(((p, sl) for p, sl in code_slices if sl.n >= 1),
                      key=lambda ps: ps[1].z_adj(k), reverse=True)
        return [{
            "rank": i, "user_id": p.user_id, "username": name(p.user_id), "slug": slugs[p.user_id],
            "avatar_url": avatar(p.user_id), "country": ds.players.get(p.user_id, {}).get("country_code"),
            "team": team_ref(p.user_id), "rating": rating_of(sl), "rating_raw": to_rating(sl.z_mean(), cfg),
            "maps": sl.n, "confidence": round(sl.confidence(cfg.slice_confidence_k), 3),
            "eligible_for_award": sl.n >= acfg.min_maps_mod and sl.confidence(cfg.slice_confidence_k) >= cfg.min_slice_confidence,
        } for i, (p, sl) in enumerate(rows, 1)]

    mod_leaderboards = {m: board([(p, p.by_mod[m]) for p in stats.values() if p.by_mod.get(m)]) for m in mods_present}
    round_leaderboards = {r: board([(p, p.by_round[r]) for p in stats.values() if p.by_round.get(r)])
                          for r in rounds_present}
    mod_rank = {m: {row["user_id"]: (row["rank"], len(rows)) for row in rows} for m, rows in mod_leaderboards.items()}
    round_rank = {r: {row["user_id"]: (row["rank"], len(rows)) for row in rows} for r, rows in round_leaderboards.items()}

    def summary(p: PlayerStats) -> dict:
        n = p.overall.n
        sigma = p.overall.z_std()
        return {
            "rank": rank_of[p.user_id],
            "qualified": reached_bracket(p) if split_qualified else None,
            "user_id": p.user_id,
            "username": name(p.user_id),
            "slug": slugs[p.user_id],
            "avatar_url": avatar(p.user_id),
            "country": ds.players.get(p.user_id, {}).get("country_code"),
            "country_name": ds.players.get(p.user_id, {}).get("country_name"),
            "team": team_ref(p.user_id),
            "rating": to_rating(t_z[p.user_id][0], cfg),            # Tournament Rating (the default ranking)
            "performance_rating": rating_of(p.overall),             # how good the scores were
            "performance_rank": perf_rank_of[p.user_id],
            "rating_raw": to_rating(p.overall.z_mean(), cfg),
            "confidence": round(t_z[p.user_id][1], 3),
            "deepest_round": p.deepest_round,
            "deepest_round_name": round_name(p.deepest_round) if p.deepest_round else None,
            "qualifier": ({"rating": rating_of(p.by_round["Q"]), "maps": p.by_round["Q"].n,
                           "rank": round_rank["Q"][p.user_id][0], "rank_of": round_rank["Q"][p.user_id][1]}
                          if p.by_round.get("Q") and p.by_round["Q"].n and "Q" in round_rank else None),
            "maps_played": n,
            "consistency_sigma": round(sigma, 3) if sigma is not None else None,
            "mod_ratings": {m: {"rating": rating_of(p.by_mod[m]), "maps": p.by_mod[m].n,
                                "confidence": round(p.by_mod[m].confidence(cfg.slice_confidence_k), 3),
                                "low_confidence": p.by_mod[m].confidence(cfg.slice_confidence_k) < cfg.min_slice_confidence,
                                "rank": mod_rank[m][p.user_id][0], "rank_of": mod_rank[m][p.user_id][1]}
                            for m in mods_present if p.by_mod.get(m) and p.by_mod[m].n},
            "round_ratings": {r: {"rating": rating_of(p.by_round[r]), "maps": p.by_round[r].n,
                                  "confidence": round(p.by_round[r].confidence(cfg.slice_confidence_k), 3),
                                  "rank": round_rank[r][p.user_id][0], "rank_of": round_rank[r][p.user_id][1]}
                              for r in rounds_present if p.by_round.get(r) and p.by_round[r].n},
            "avg_score": round(p.score_sum / n) if n else None,
            "avg_accuracy": round(p.acc_sum / p.acc_n, 4) if p.acc_n else None,
            "map_winrate": _pct(p.map_wins, p.map_losses),
            "map_record": [p.map_wins, p.map_losses],
            "match_record": [p.match_wins, p.match_losses],
        }

    rankings = [summary(p) for p in ranked]

    # ---- awards ---------------------------------------------------------
    awards: list[dict] = []

    def award(key, title, candidates, score_fn, value_fn, rule):
        candidates = list(candidates)
        if not candidates:
            awards.append({"key": key, "title": title, "winner": None, "rule": rule})
            return
        best = max(candidates, key=score_fn)
        awards.append({"key": key, "title": title, "rule": rule, "winner": {
            "user_id": best.user_id, "username": name(best.user_id), "slug": slugs[best.user_id],
            "avatar_url": avatar(best.user_id), "value": value_fn(best)}})

    elig = [p for p in stats.values() if p.overall.n >= acfg.min_maps_overall]
    award("mvp", "Tournament MVP", [p for p in elig if not split_qualified or reached_bracket(p)] or elig, lambda p: t_z[p.user_id][0],
          lambda p: to_rating(t_z[p.user_id][0], cfg), f">= {acfg.min_maps_overall} maps, highest Tournament Rating")
    for m in mods_present:
        if m == "TB":
            continue
        award(f"best_{m.lower()}", f"Best {m} Player",
              [p for p in stats.values() if p.by_mod.get(m) and p.by_mod[m].n >= acfg.min_maps_mod
               and p.by_mod[m].confidence(cfg.slice_confidence_k) >= cfg.min_slice_confidence],
              lambda p, m=m: p.by_mod[m].z_adj(k),
              lambda p, m=m: f"{rating_of(p.by_mod[m])} over {p.by_mod[m].n} maps",
              f">= {acfg.min_maps_mod} {m} maps and {int(cfg.min_slice_confidence * 100)}% sample confidence")
    award("most_consistent", "Most Consistent",
          [p for p in elig if p.overall.z_mean() >= 0 and p.overall.z_std() is not None],
          lambda p: -p.overall.z_std(), lambda p: f"σ = {p.overall.z_std():.2f}",
          "lowest spread of normalized performance; above-average players only")
    award("best_carry", "Best Carry",
          [p for p in stats.values() if p.carry_n >= acfg.min_team_games],
          lambda p: p.carry_sum / p.carry_n, lambda p: f"{p.carry_sum / p.carry_n:.2f}x team share",
          "avg share of team score × team size (1.00 = equal share)")
    award("best_accuracy", "Best Accuracy", [p for p in elig if p.acc_n],
          lambda p: p.acc_sum / p.acc_n, lambda p: f"{100 * p.acc_sum / p.acc_n:.2f}%",
          f">= {acfg.min_maps_overall} maps")

    finals_rounds = [r for r in rounds_present if r in ("F", "GF")] or rounds_present[-1:]
    if finals_rounds:
        def fin(p):
            sl = [p.by_round[r] for r in finals_rounds if r in p.by_round]
            n = sum(x.n for x in sl)
            wz = sum(x.wz_sum for x in sl)
            w = sum(x.w_sum for x in sl)
            return n, wz / (w + k) if w else 0.0
        award("best_finals", f"Best Finals Player ({', '.join(round_name(r) for r in finals_rounds)})",
              [p for p in stats.values() if fin(p)[0] >= acfg.min_maps_finals],
              lambda p: fin(p)[1], lambda p: f"{to_rating(fin(p)[1], cfg)} over {fin(p)[0]} maps",
              f">= {acfg.min_maps_finals} maps in the final rounds")

    best_single = max((p for p in stats.values() if p.best), key=lambda p: p.best[0], default=None)
    if best_single:
        zb, row = best_single.best
        awards.append({"key": "best_performance", "title": "Best Single Performance",
                       "rule": "highest normalized score on one map", "winner": {
                           "user_id": best_single.user_id, "username": name(best_single.user_id),
                           "slug": slugs[best_single.user_id], "avatar_url": avatar(best_single.user_id),
                           "value": f"{row.score:,} on {_map_label(ds, row.beatmap_id)} ({row.bucket}, z = {zb:+.2f})"}})

    # ---- matches --------------------------------------------------------
    def side_ref(m: dict, side: str) -> dict:
        if side in ("red", "blue"):
            tid = m.get("team_red_id" if side == "red" else "team_blue_id")
            tm = ds.teams.get(tid)
            label = (m.get("team_red") if side == "red" else m.get("team_blue")) or side.title()
            if tm:
                return {"name": tm["name"], "slug": tm["slug"], "kind": "team", "country": tm["country_code"]}
            return {"name": label, "slug": None, "kind": "label", "country": None}
        uid = int(side[1:])
        return {"name": name(uid), "slug": slugs.get(uid), "kind": "player", "country": None}

    def side_order(side: str):
        return (side != "red", side != "blue", side)

    match_items: dict[int, dict] = {}
    for mid, m in ds.matches.items():
        mo = match_out.get(mid)
        wins = mo.map_wins if mo else {}
        sides = sorted(wins, key=side_order)
        match_items[mid] = {
            "osu_match_id": m["osu_match_id"],
            "link": f"https://osu.ppy.sh/community/matches/{m['osu_match_id']}",
            "round": m["round"], "round_name": round_name(m["round"]),
            "kind": "match" if is_versus_round(m["round"]) else "qualifier",
            "name": m["name"], "start_time": m["start_time"],
            "sides": [{**side_ref(m, s), "side": s, "map_wins": wins[s]} for s in sides],
            "winner": side_ref(m, mo.winner)["name"] if mo and mo.winner else None,
            "winner_side": mo.winner if mo else None,
        }
    matches = sorted(match_items.values(), key=lambda x: (round_order(x["round"]), x["start_time"] or ""))

    # ---- per-score performance data ---------------------------------------
    perf: dict[int, list[dict]] = defaultdict(list)
    per_match: dict[tuple[int, int], Slice] = defaultdict(Slice)
    for s in ds.scores:
        zi = z[(s.game_id, s.user_id)]
        perf[s.user_id].append({
            "z": round(zi, 3), "rating": to_rating(zi, cfg), "score": s.score, "accuracy": s.accuracy,
            "mod": s.bucket, "round": s.round, "round_name": round_name(s.round),
            "beatmap_id": s.beatmap_id, "map": _map_label(ds, s.beatmap_id),
            "beatmapset_id": (ds.beatmaps.get(s.beatmap_id) or {}).get("beatmapset_id"),
            "osu_match_id": s.osu_match_id, "match_name": ds.matches[s.match_id]["name"],
        })
        per_match[(s.user_id, s.match_id)].add(zi, 1.0)

    # ---- teams ----------------------------------------------------------
    team_pages: dict[str, dict] = {}
    teams_list: list[dict] = []
    if fmt.has_teams:
        t_matches: dict[int, list] = defaultdict(list)
        t_games_won: dict[int, int] = defaultdict(int)
        t_games_lost: dict[int, int] = defaultdict(int)
        for mid, m in ds.matches.items():
            mo = match_out.get(mid)
            if not mo or not is_versus_round(m["round"]):
                continue
            ids = {"red": m.get("team_red_id"), "blue": m.get("team_blue_id")}
            for side, tid in ids.items():
                if tid is None:
                    continue
                other = "blue" if side == "red" else "red"
                t_games_won[tid] += mo.map_wins.get(side, 0)
                t_games_lost[tid] += mo.map_wins.get(other, 0)
                t_matches[tid].append((mid, side))

        members: dict[int, list[int]] = defaultdict(list)
        for uid in stats:
            tid = ds.member_team.get(uid)
            if tid in ds.teams:
                members[tid].append(uid)

        raw_teams = []
        for tid, tm in ds.teams.items():
            mem = [stats[u] for u in members.get(tid, [])]
            if not mem:
                continue
            overall = merge_slices([p.overall for p in mem])
            team_z = team_tournament_rating(merge_slices([p.adj for p in mem]), model)
            wins = sum(1 for mid, side in t_matches[tid] if match_out[mid].winner == side)
            losses = sum(1 for mid, side in t_matches[tid] if match_out[mid].winner not in (None, side))
            raw_teams.append((tid, tm, mem, overall, wins, losses, team_z))
        raw_teams.sort(key=lambda r: r[6], reverse=True)
        n_teams = len(raw_teams)

        for rank, (tid, tm, mem, overall, wins, losses, team_z) in enumerate(raw_teams, 1):
            mem_sorted = sorted(mem, key=lambda p: t_z[p.user_id][0], reverse=True)
            roster = [{
                "team_rank": i, "user_id": p.user_id, "username": name(p.user_id), "slug": slugs[p.user_id],
                "avatar_url": avatar(p.user_id), "country": ds.players.get(p.user_id, {}).get("country_code"),
                "rating": to_rating(t_z[p.user_id][0], cfg), "rank": rank_of[p.user_id], "rank_of": len(ranked),
                "maps": p.overall.n,
            } for i, p in enumerate(mem_sorted, 1)]

            def best_in(slice_of):
                cands = [(p, slice_of(p)) for p in mem_sorted]
                cands = [(p, sl) for p, sl in cands if sl is not None and sl.n]
                if not cands:
                    return None
                p, sl = max(cands, key=lambda c: c[1].z_adj(k))
                return {"user_id": p.user_id, "username": name(p.user_id), "slug": slugs[p.user_id],
                        "avatar_url": avatar(p.user_id), "rating": rating_of(sl), "maps": sl.n}

            best_by_mod = {m: best_in(lambda p, m=m: p.by_mod.get(m)) for m in mods_present}
            round_slices = {r: merge_slices([p.by_round[r] for p in mem if p.by_round.get(r)]) for r in rounds_present}
            round_slices = {r: sl for r, sl in round_slices.items() if sl.n}
            by_round = [{"round": r, "round_name": round_name(r), "rating": rating_of(sl), "maps": sl.n}
                        for r, sl in sorted(round_slices.items(), key=lambda kv: round_order(kv[0]))]
            qualified = [x for x in by_round if x["maps"] >= 3] or by_round

            t_match_items = []
            for mid, side in sorted(t_matches[tid], key=lambda ms: (round_order(ds.matches[ms[0]]["round"]),
                                                                    ds.matches[ms[0]]["start_time"] or "")):
                it = match_items[mid]
                own = [s for s in it["sides"] if s["side"] == side]
                opp = [s for s in it["sides"] if s["side"] != side]
                t_match_items.append({**it, "sides": own + opp,
                                      "result": None if not it["winner_side"] else ("W" if it["winner_side"] == side else "L")})
            fr = max((ds.matches[mid]["round"] for mid, _ in t_matches[tid]), key=round_order, default=None)
            page = {
                "slug": tm["slug"], "name": tm["name"], "country": tm["country_code"],
                "country_name": country_names.get(tm["country_code"]), "flag_url": flag_url(tm["country_code"]),
                "rank": rank, "rank_of": n_teams,
                "rating": to_rating(team_z, cfg),
                "match_record": [wins, losses], "map_record": [t_games_won[tid], t_games_lost[tid]],
                "maps_won": t_games_won[tid],
                "maps_played": overall.n, "players": len(mem),
                "avg_player_rating": round(sum(to_rating(t_z[p.user_id][0], cfg) for p in mem) / len(mem), 2),
                "furthest_round": fr, "furthest_round_name": round_name(fr) if fr else None,
                "best_round": max(qualified, key=lambda x: x["rating"]) if qualified else None,
                "worst_round": min(qualified, key=lambda x: x["rating"]) if qualified else None,
                "best_player": roster[0] if roster else None,
                "best_by_mod": best_by_mod,
                "by_round": by_round,
                "roster": roster,
                "matches": t_match_items,
            }
            team_pages[tm["slug"]] = page
            teams_list.append({
                "rank": rank, "name": tm["name"], "slug": tm["slug"], "country": tm["country_code"],
                "flag_url": flag_url(tm["country_code"]), "rating": page["rating"],
                "match_record": page["match_record"], "map_record": page["map_record"],
                "players": [{"username": r["username"], "slug": r["slug"], "rating": r["rating"]} for r in roster],
                "roster_size": len(roster), "maps": overall.n, "furthest_round_name": page["furthest_round_name"],
            })

    # ---- player pages ---------------------------------------------------
    players = {}
    for p in ranked:
        page = summary(p)
        page["rank_of"] = len(ranked)
        page["carry_index"] = round(p.carry_sum / p.carry_n, 3) if p.carry_n else None
        page["by_round"] = [{"round": r, "round_name": round_name(r), "rating": rating_of(sl), "maps": sl.n,
                             "confidence": round(sl.confidence(cfg.slice_confidence_k), 3),
                             "rank": round_rank[r][p.user_id][0], "rank_of": round_rank[r][p.user_id][1]}
                            for r, sl in sorted(p.by_round.items(), key=lambda kv: round_order(kv[0])) if r in round_rank]
        page["by_mod"] = [{"mod": m, "rating": rating_of(p.by_mod[m]), "maps": p.by_mod[m].n,
                           "confidence": round(p.by_mod[m].confidence(cfg.slice_confidence_k), 3),
                           "low_confidence": p.by_mod[m].confidence(cfg.slice_confidence_k) < cfg.min_slice_confidence,
                           "rank": mod_rank[m][p.user_id][0], "rank_of": mod_rank[m][p.user_id][1]}
                          for m in mods_present if p.by_mod.get(m) and p.by_mod[m].n]
        page["breakdown"] = {
            "tournament": {"rating": page["rating"], "rank": page["rank"]},
            "performance": {"rating": page["performance_rating"], "rank": page["performance_rank"]},
            "confidence": page["confidence"], "maps": p.overall.n,
            "observed_rating": to_rating(t_z[p.user_id][2], cfg),   # field-adjusted, before confidence
            "deepest_round": page["deepest_round"], "deepest_round_name": page["deepest_round_name"],
            "qualifier": page["qualifier"],
        }
        runs = sorted(perf[p.user_id], key=lambda x: x["z"], reverse=True)
        page["best_performances"] = runs[:5]
        page["worst_performances"] = list(reversed(runs[-3:])) if len(runs) > 5 else []
        if p.best:
            zb, row = p.best
            page["best_performance"] = {"score": row.score, "accuracy": row.accuracy, "mod": row.bucket,
                                        "map": _map_label(ds, row.beatmap_id), "round": row.round,
                                        "z": round(zb, 3), "osu_match_id": row.osu_match_id}
        hist = []
        for mid in sorted(p.matches, key=lambda i: ds.matches[i]["start_time"] or ""):
            it, mo = match_items[mid], match_out.get(mid)
            side = mo.sides.get(p.user_id) if mo else None
            own = [s for s in it["sides"] if s["side"] == side]
            opp = [s for s in it["sides"] if s["side"] != side]
            hist.append({
                "osu_match_id": it["osu_match_id"], "round": it["round"], "round_name": it["round_name"],
                "kind": it["kind"], "name": it["name"], "start_time": it["start_time"],
                "result": None if not (mo and mo.winner) else ("W" if mo.winner == side else "L"),
                "score": "-".join(str(mo.map_wins[s]) for s in sorted(mo.map_wins, key=lambda s: s != side)) if mo and mo.map_wins else None,
                "sides": own + opp if own else it["sides"],
                "maps": per_match[(p.user_id, mid)].n,
                "rating": to_rating(per_match[(p.user_id, mid)].z_mean(), cfg),
            })
        page["match_history"] = hist
        tm = ds.teams.get(ds.member_team.get(p.user_id))
        if tm and tm["slug"] in team_pages:
            tp = team_pages[tm["slug"]]
            page["team"] = {"name": tp["name"], "slug": tp["slug"], "country": tp["country"],
                            "flag_url": tp["flag_url"], "rank": tp["rank"], "rating": tp["rating"]}
            page["teammates"] = [r for r in tp["roster"] if r["user_id"] != p.user_id]
        else:
            page["teammates"] = []
        players[slugs[p.user_id]] = page

    mvp = next((a["winner"] for a in awards if a["key"] == "mvp" and a["winner"]), None)
    report = {
        "tournament": {"id": t["id"], "slug": t["slug"], "name": t["name"], "acronym": t["acronym"],
                       "format": fmt.key, "format_label": fmt.label, "has_teams": fmt.has_teams,
                       "client": t["client"], "client_label": CLIENTS[t["client"]].label if t["client"] in CLIENTS else t["client"],
                       "start_date": t["start_date"], "end_date": t["end_date"]},
        "summary": {"matches": len(ds.matches), "games": len({s.game_id for s in ds.scores}),
                    "scores": len(ds.scores), "players": len(stats), "teams": len(team_pages),
                    "beatmaps": len(ds.beatmaps), "empty_matches": ds.empty_matches,
                    "mods": mods_present, "rounds": rounds_present,
                    "qualified_split": {"enabled": split_qualified, "qualified": n_qualified if split_qualified else None,
                                        "eliminated": len(stats) - n_qualified if split_qualified else None},
                    "round_names": {r: round_name(r) for r in rounds_present}},
        "config": {"rating": cfg.__dict__, "awards": acfg.__dict__,
                   "model": {"confidence_k": round(model.k, 2), "k_noise": round(model.k_noise, 2),
                             "k_workload": round(model.k_workload, 2), "prior": round(model.prior, 3),
                             "field_strength": {r: round(v, 3) for r, v in field_strength(ds, stats, cfg).items() if r}}},
        "mvp": ({**next(r for r in rankings if r["user_id"] == mvp["user_id"])} if mvp else None),
        "rankings": rankings,
        "mod_leaderboards": mod_leaderboards,
        "round_leaderboards": round_leaderboards,
        "awards": awards,
        "matches": matches,
        "teams": teams_list,
        "team_pages": team_pages,
        "players": players,
    }
    return Analysis(report=report, ds=ds, z=z, game_out=game_out, match_out=match_out, cfg=cfg, stats=stats,
                    model=model, strength=field_strength(ds, stats, cfg), cache={})


# ---- views over a finished report (still no rating maths) ---------------------
def leaderboard(rep: dict, mode: str = "tournament", key: str | None = None,
                limit: int | None = None, offset: int = 0) -> dict:
    """One ranking table. mode: tournament (default) | performance | round (key = code) | mod (key = mod) |
    consistency | maps. "overall" is accepted as an alias of "tournament"."""
    mode = "tournament" if mode == "overall" else mode
    if mode not in LEADERBOARD_MODES:
        raise ValueError(f"mode must be one of {', '.join(LEADERBOARD_MODES)}")
    awards_min = rep["config"]["awards"]["min_maps_overall"]
    columns = {"value_label": "Tournament Rating" if mode == "tournament" else "Rating"}
    if mode == "tournament":
        rows = [_lb_row(r, r["rating"], r["maps_played"], r) for r in rep["rankings"]]
    elif mode == "performance":
        columns = {"value_label": "Performance Rating"}
        pool = sorted(rep["rankings"], key=lambda r: r["performance_rank"])
        rows = []
        for r in pool:
            row = _lb_row(r, r["performance_rating"], r["maps_played"], r)
            row["rank"] = r["performance_rank"]
            rows.append(row)
    elif mode == "round":
        if key not in rep["round_leaderboards"]:
            raise KeyError(f"No round '{key}' in this tournament")
        rows = [_lb_row(r, r["rating"], r["maps"]) | {"confidence": r["confidence"]} for r in rep["round_leaderboards"][key]]
    elif mode == "mod":
        if key not in rep["mod_leaderboards"]:
            raise KeyError(f"No mod '{key}' in this tournament")
        rows = [_lb_row(r, r["rating"], r["maps"]) | {"confidence": r["confidence"], "low_confidence": not r["eligible_for_award"]}
                for r in rep["mod_leaderboards"][key]]
    elif mode == "consistency":
        columns = {"value_label": "Tournament Rating", "extra_label": "σ (lower = steadier)"}
        pool = [r for r in rep["rankings"] if r["consistency_sigma"] is not None and r["maps_played"] >= awards_min]
        pool.sort(key=lambda r: r["consistency_sigma"])
        rows = []
        for i, r in enumerate(pool, 1):
            row = _lb_row(r, r["rating"], r["maps_played"], r)
            row.update(rank=i, extra=r["consistency_sigma"])
            rows.append(row)
    else:  # maps
        pool = sorted(rep["rankings"], key=lambda r: (-r["maps_played"], r["rank"]))
        rows = []
        for i, r in enumerate(pool, 1):
            row = _lb_row(r, r["rating"], r["maps_played"], r)
            row["rank"] = i
            rows.append(row)
    total = len(rows)
    rows = rows[offset: offset + limit] if limit else rows[offset:]
    note = f"Players need at least {awards_min} maps." if mode == "consistency" else None
    if mode == "performance":
        note = "Performance Rating is how strong the scores were when the player played, regardless of how many maps or rounds that was."
    split = rep["summary"]["qualified_split"]
    cutoff = split["qualified"] if split["enabled"] and mode == "tournament" else None   # rows before this index qualified
    if cutoff is not None:
        note = "Players who reached the bracket rank above players who did not qualify."
    return {"mode": mode, "key": key, "total": total, "columns": columns, "note": note,
            "qualified_cutoff": cutoff, "rows": rows}


def _lb_row(r: dict, rating: float, maps: int, full: dict | None = None) -> dict:
    row = {"rank": r["rank"], "user_id": r["user_id"], "username": r["username"], "slug": r["slug"],
           "avatar_url": r.get("avatar_url"), "country": r.get("country"), "team": r.get("team"),
           "rating": rating, "maps": maps}
    if full:   # whole-tournament rows also carry the explainers
        row.update(tournament_rating=full["rating"], performance_rating=full["performance_rating"],
                   confidence=full["confidence"], deepest_round_name=full["deepest_round_name"],
                   qualified=full.get("qualified"))
    return row


def search_players(rep: dict, query: str, limit: int = 8) -> list[dict]:
    """Players in THIS tournament by username (partial, case-insensitive) or exact osu! user id."""
    q = (query or "").strip().lower()
    if not q:
        return []
    hits = []
    for r in rep["rankings"]:
        uname = r["username"].lower()
        if (q.isdigit() and str(r["user_id"]) == q) or uname == q:
            score = 0
        elif uname.startswith(q):
            score = 1
        elif q in uname:
            score = 2
        else:
            continue
        hits.append((score, r["rank"], r))
    hits.sort(key=lambda h: h[:2])
    return [{
        "user_id": r["user_id"], "username": r["username"], "slug": r["slug"], "avatar_url": r["avatar_url"],
        "country": r["country"], "team": r["team"], "rating": r["rating"], "rank": r["rank"],
        "rank_of": len(rep["rankings"]), "maps_played": r["maps_played"],
        "mod_ratings": {m: v["rating"] for m, v in r["mod_ratings"].items()},
    } for _, _, r in hits[:limit]]


def match_detail(conn: sqlite3.Connection, an: Analysis, osu_match_id: int) -> dict | None:
    """Every game of one lobby (counted and excluded) with each player's score and normalized rating."""
    ds, rep, cfg = an.ds, an.report, an.cfg
    mid = next((i for i, m in ds.matches.items() if m["osu_match_id"] == osu_match_id), None)
    if mid is None:
        return None
    item = next(m for m in rep["matches"] if m["osu_match_id"] == osu_match_id)
    slug_of = {r["user_id"]: r["slug"] for r in rep["rankings"]}
    games = []
    for g in conn.execute("SELECT * FROM match_games WHERE match_id = ? ORDER BY order_index", (mid,)):
        bm = dict(conn.execute("SELECT * FROM beatmaps WHERE beatmap_id = ?", (g["beatmap_id"],)).fetchone() or {})
        scores = []
        for s in conn.execute(
                """SELECT s.*, p.username, p.avatar_url FROM game_scores s JOIN players p ON p.user_id = s.user_id
                   WHERE s.game_id = ? ORDER BY s.score DESC""", (g["id"],)):
            zi = an.z.get((g["id"], s["user_id"]))
            scores.append({
                "user_id": s["user_id"], "username": s["username"], "slug": slug_of.get(s["user_id"]),
                "avatar_url": s["avatar_url"] or f"https://a.ppy.sh/{s['user_id']}",
                "score": s["score"], "accuracy": s["accuracy"], "max_combo": s["max_combo"],
                "misses": s["count_miss"], "mods": s["mods"], "side": s["team"],
                "rating": to_rating(zi, cfg) if zi is not None else None,
            })
        go = an.game_out.get(g["id"])
        games.append({
            "order": g["order_index"] + 1, "osu_game_id": g["osu_game_id"], "beatmap_id": g["beatmap_id"],
            "map": (f"{bm.get('artist') or '?'} - {bm['title']} [{bm.get('version') or '?'}]"
                    if bm.get("title") else f"beatmap {g['beatmap_id']}"),
            "beatmapset_id": bm.get("beatmapset_id"), "star_rating": bm.get("star_rating"),
            "mod": g["mod_bucket"], "excluded": bool(g["excluded"]), "exclude_reason": g["exclude_reason"],
            "winner_side": go.winner if go else None,
            "side_totals": go.side_totals if go else {}, "scores": scores,
        })
    return {"match": item, "games": games}


def _map_label(ds, beatmap_id) -> str:
    b = ds.beatmaps.get(beatmap_id) or {}
    if b.get("title"):
        return f"{b.get('artist') or '?'} - {b['title']} [{b.get('version') or '?'}]"
    return f"beatmap {beatmap_id}"
