# analysis.py

import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

DATA_PATH = Path("data/matches.json")

SLOT_GROUPS = {
    "NM": ["NM1", "NM2", "NM3", "NM4", "NM5", "NM6"],
    "HD": ["HD1", "HD2", "HD3"],
    "HR": ["HR1", "HR2", "HR3"],
    "DT": ["DT1", "DT2", "DT3"],
    "FM": ["FM1", "FM2"],
}

ALL_SLOTS = [slot for slots in SLOT_GROUPS.values() for slot in slots]
ALL_MODS = list(SLOT_GROUPS.keys())


def load_matches() -> list[dict[str, Any]]:
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def get_player_matches(username: str) -> list[dict[str, Any]]:
    matches = load_matches()
    player_matches = [m for m in matches if m["player"].lower() == username.lower()]
    player_matches.sort(key=lambda x: x["date"], reverse=True)
    return player_matches


def get_recent_matches(username: str, limit: int = 5) -> list[dict[str, Any]]:
    return get_player_matches(username)[:limit]


def get_matches_last_n_days(username: str, days: int = 90) -> list[dict[str, Any]]:
    matches = get_player_matches(username)
    if not matches:
        return []

    newest_date = max(parse_date(m["date"]) for m in matches)
    cutoff = newest_date - timedelta(days=days)
    return [m for m in matches if parse_date(m["date"]) >= cutoff]


def _empty_stat() -> dict[str, Any]:
    return {
        "avg_score": "N/A",
        "avg_accuracy": "N/A",
        "winrate": "N/A",
        "matches": 0,
    }


def build_slot_stats(matches: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    slot_stats = defaultdict(
        lambda: {"total": 0, "wins": 0, "score_sum": 0, "acc_sum": 0.0}
    )

    for match in matches:
        slot = match["slot"]
        slot_stats[slot]["total"] += 1
        slot_stats[slot]["wins"] += 1 if match["result"].lower() == "win" else 0
        slot_stats[slot]["score_sum"] += match["score"]
        slot_stats[slot]["acc_sum"] += match["accuracy"]

    result: dict[str, dict[str, Any]] = {}
    for slot in ALL_SLOTS:
        stats = slot_stats.get(slot)
        if not stats or stats["total"] == 0:
            result[slot] = _empty_stat()
            continue

        total = stats["total"]
        result[slot] = {
            "avg_score": round(stats["score_sum"] / total),
            "avg_accuracy": round(stats["acc_sum"] / total, 2),
            "winrate": round((stats["wins"] / total) * 100, 1),
            "matches": total,
        }

    return result


def build_mod_stats(matches: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mod_stats = defaultdict(
        lambda: {"total": 0, "wins": 0, "score_sum": 0, "acc_sum": 0.0}
    )

    for match in matches:
        mod = match["mod"]
        mod_stats[mod]["total"] += 1
        mod_stats[mod]["wins"] += 1 if match["result"].lower() == "win" else 0
        mod_stats[mod]["score_sum"] += match["score"]
        mod_stats[mod]["acc_sum"] += match["accuracy"]

    result: dict[str, dict[str, Any]] = {}
    for mod in ALL_MODS:
        stats = mod_stats.get(mod)
        if not stats or stats["total"] == 0:
            result[mod] = _empty_stat()
            continue

        total = stats["total"]
        result[mod] = {
            "avg_score": round(stats["score_sum"] / total),
            "avg_accuracy": round(stats["acc_sum"] / total, 2),
            "winrate": round((stats["wins"] / total) * 100, 1),
            "matches": total,
        }

    return result


def get_strengths_and_weaknesses(
    mod_stats: dict[str, dict[str, Any]],
) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
    valid = [
        (mod, stats["winrate"])
        for mod, stats in mod_stats.items()
        if stats["winrate"] != "N/A"
    ]

    if not valid:
        return [], []

    valid.sort(key=lambda x: x[1], reverse=True)
    strengths = valid[:2]

    remaining = [x for x in valid if x not in strengths]
    remaining.sort(key=lambda x: x[1])
    weaknesses = remaining[:2]

    return strengths, weaknesses


def get_overall_summary(username: str) -> dict[str, Any] | None:
    matches = get_player_matches(username)
    if not matches:
        return None

    total = len(matches)
    wins = sum(1 for m in matches if m["result"].lower() == "win")
    overall_winrate = round((wins / total) * 100, 1) if total else 0

    if overall_winrate >= 70:
        consistency = "High"
    elif overall_winrate >= 40:
        consistency = "Medium"
    else:
        consistency = "Low"

    recent_90 = get_matches_last_n_days(username, 90)
    slot_stats_90 = build_slot_stats(recent_90)
    mod_stats_90 = build_mod_stats(recent_90)
    strengths, weaknesses = get_strengths_and_weaknesses(mod_stats_90)

    return {
        "player": username,
        "total_matches": total,
        "wins": wins,
        "overall_winrate": overall_winrate,
        "consistency": consistency,
        "recent_matches": get_recent_matches(username, 5),
        "slot_stats_90": slot_stats_90,
        "mod_stats_90": mod_stats_90,
        "strengths": strengths,
        "weaknesses": weaknesses,
        "ratings": {
            "romai": "N/A",
            "elitebotix_duel": "N/A",
            "skillissue": "N/A",
        },
    }


def get_full_slot_summary(username: str) -> dict[str, dict[str, Any]] | None:
    matches_90 = get_matches_last_n_days(username, 90)
    if not matches_90:
        return None
    return build_slot_stats(matches_90)


def _has_data(stats: dict[str, Any]) -> bool:
    return stats["matches"] > 0


def _sample_weight(matches: int) -> float:
    return min(1.0, 0.65 + (matches * 0.1))


def _confidence_label(matches1: int, matches2: int) -> str:
    minimum = min(matches1, matches2)
    maximum = max(matches1, matches2)

    if minimum >= 4:
        return "High"
    if minimum >= 2:
        return "Medium"
    if maximum >= 2:
        return "Low"
    return "Very Low"


def _advantage_label(edge: float | None, confidence: str) -> str:
    if edge is None:
        return "Unknown"

    if confidence == "High":
        strong_cutoff = 100000
        lean_cutoff = 35000
    elif confidence == "Medium":
        strong_cutoff = 130000
        lean_cutoff = 50000
    elif confidence == "Low":
        strong_cutoff = 160000
        lean_cutoff = 70000
    else:
        strong_cutoff = 200000
        lean_cutoff = 90000

    if edge >= strong_cutoff:
        return "Strong"
    if edge >= lean_cutoff:
        return "Lean"
    return "Close"


def _mod_metric(stats: dict[str, Any]) -> float | None:
    if not _has_data(stats):
        return None

    base = (
        float(stats["avg_score"])
        + float(stats["winrate"]) * 2500
        + float(stats["avg_accuracy"]) * 500
    )
    return round(base * _sample_weight(stats["matches"]), 1)


def _slot_metric(stats: dict[str, Any]) -> float | None:
    if not _has_data(stats):
        return None

    base = (
        float(stats["avg_score"])
        + float(stats["winrate"]) * 1800
        + float(stats["avg_accuracy"]) * 350
    )
    return round(base * _sample_weight(stats["matches"]), 1)


def _compare_block(
    label: str,
    player1: str,
    player2: str,
    p1_stats: dict[str, Any],
    p2_stats: dict[str, Any],
    metric_func,
) -> dict[str, Any]:
    p1_metric = metric_func(p1_stats)
    p2_metric = metric_func(p2_stats)

    p1_matches = p1_stats["matches"]
    p2_matches = p2_stats["matches"]
    confidence = _confidence_label(p1_matches, p2_matches)

    if p1_metric is None and p2_metric is None:
        winner = "Tie"
        edge = None
    elif p1_metric is None:
        winner = player2
        edge = None
    elif p2_metric is None:
        winner = player1
        edge = None
    elif p1_metric > p2_metric:
        winner = player1
        edge = round(p1_metric - p2_metric, 1)
    elif p2_metric > p1_metric:
        winner = player2
        edge = round(p2_metric - p1_metric, 1)
    else:
        winner = "Tie"
        edge = 0.0

    advantage = _advantage_label(edge, confidence)

    return {
        "label": label,
        "winner": winner,
        "edge": edge,
        "advantage": advantage,
        "confidence": confidence,
        "player1": p1_stats,
        "player2": p2_stats,
        "player1_metric": p1_metric,
        "player2_metric": p2_metric,
    }


def _top_comfort_picks(
    slot_stats: dict[str, dict[str, Any]],
    limit: int = 3,
) -> list[dict[str, Any]]:
    picks: list[dict[str, Any]] = []

    for slot, stats in slot_stats.items():
        metric = _slot_metric(stats)
        if metric is None:
            continue

        picks.append(
            {
                "slot": slot,
                "matches": stats["matches"],
                "avg_score": stats["avg_score"],
                "winrate": stats["winrate"],
                "avg_accuracy": stats["avg_accuracy"],
                "metric": metric,
                "reliability": _confidence_label(stats["matches"], stats["matches"]),
            }
        )

    picks.sort(key=lambda x: (x["metric"], x["matches"]), reverse=True)
    return picks[:limit]


def _recommended_bans(
    favored_player: str,
    slot_comparisons: list[dict[str, Any]],
    comfort_picks: list[dict[str, Any]],
    limit: int = 2,
) -> list[str]:
    scored_rows = []

    for row in slot_comparisons:
        if row["label"] not in ALL_SLOTS:
            continue
        if row["winner"] != favored_player:
            continue
        if row["advantage"] not in {"Strong", "Lean"}:
            continue

        favored_stats = row["player1"] if row["winner"] == favored_player else row["player2"]
        if favored_stats["matches"] < 2:
            continue

        advantage_score = {"Strong": 2, "Lean": 1}[row["advantage"]]
        confidence_score = {"High": 3, "Medium": 2, "Low": 1, "Very Low": 0}[row["confidence"]]
        edge_score = row["edge"] if row["edge"] is not None else 0

        scored_rows.append((advantage_score, confidence_score, edge_score, row["label"]))

    scored_rows.sort(reverse=True)

    seen: set[str] = set()
    bans: list[str] = []

    for _, _, _, slot in scored_rows:
        if slot in seen:
            continue
        bans.append(slot)
        seen.add(slot)
        if len(bans) >= limit:
            return bans

    for pick in comfort_picks:
        if pick["matches"] < 2:
            continue
        slot = pick["slot"]
        if slot in seen:
            continue
        bans.append(slot)
        seen.add(slot)
        if len(bans) >= limit:
            return bans

    return bans


def compare_players(player1: str, player2: str) -> dict[str, Any] | None:
    summary1 = get_overall_summary(player1)
    summary2 = get_overall_summary(player2)

    if summary1 is None or summary2 is None:
        return None

    mod_comparisons = [
        _compare_block(
            mod,
            player1,
            player2,
            summary1["mod_stats_90"][mod],
            summary2["mod_stats_90"][mod],
            _mod_metric,
        )
        for mod in ALL_MODS
    ]

    slot_comparisons = [
        _compare_block(
            slot,
            player1,
            player2,
            summary1["slot_stats_90"][slot],
            summary2["slot_stats_90"][slot],
            _slot_metric,
        )
        for slot in ALL_SLOTS
        if _has_data(summary1["slot_stats_90"][slot])
        or _has_data(summary2["slot_stats_90"][slot])
    ]

    advantage_order = {"Strong": 3, "Lean": 2, "Close": 1, "Unknown": 0}
    confidence_order = {"High": 3, "Medium": 2, "Low": 1, "Very Low": 0}

    slot_comparisons.sort(
        key=lambda row: (
            row["winner"] != "Tie",
            advantage_order[row["advantage"]],
            confidence_order[row["confidence"]],
            row["edge"] or 0,
        ),
        reverse=True,
    )

    comfort_picks_1 = _top_comfort_picks(summary1["slot_stats_90"], limit=3)
    comfort_picks_2 = _top_comfort_picks(summary2["slot_stats_90"], limit=3)

    bans_vs_1 = _recommended_bans(player1, slot_comparisons, comfort_picks_1, limit=2)
    bans_vs_2 = _recommended_bans(player2, slot_comparisons, comfort_picks_2, limit=2)

    return {
        "player1": summary1,
        "player2": summary2,
        "comparisons": mod_comparisons,
        "mod_comparisons": mod_comparisons,
        "slot_comparisons": slot_comparisons,
        "comfort_picks": {
            player1: comfort_picks_1,
            player2: comfort_picks_2,
        },
        "recommended_bans": {
            player1: bans_vs_1,
            player2: bans_vs_2,
        },
    }

