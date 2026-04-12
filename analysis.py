#analysis.py
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
    player_matches = [
        m for m in matches
        if m["player"].strip().lower() == username.strip().lower()
    ]
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
        "avg_star_rating": "N/A",
        "winrate": "N/A",
        "matches": 0,
    }


def build_slot_stats(matches: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    slot_stats = defaultdict(
        lambda: {
            "total": 0,
            "wins": 0,
            "score_sum": 0,
            "acc_sum": 0.0,
            "star_sum": 0.0,
            "star_count": 0,
        }
    )

    for match in matches:
        slot = match["slot"]
        slot_stats[slot]["total"] += 1
        slot_stats[slot]["wins"] += 1 if match["result"].lower() == "win" else 0
        slot_stats[slot]["score_sum"] += match["score"]
        slot_stats[slot]["acc_sum"] += match["accuracy"]

        star_rating = match.get("star_rating")
        if isinstance(star_rating, (int, float)):
            slot_stats[slot]["star_sum"] += star_rating
            slot_stats[slot]["star_count"] += 1

    result: dict[str, dict[str, Any]] = {}
    for slot in ALL_SLOTS:
        stats = slot_stats.get(slot)
        if not stats or stats["total"] == 0:
            result[slot] = _empty_stat()
            continue

        total = stats["total"]
        avg_star_rating = (
            round(stats["star_sum"] / stats["star_count"], 2)
            if stats["star_count"] > 0
            else "N/A"
        )

        result[slot] = {
            "avg_score": round(stats["score_sum"] / total),
            "avg_accuracy": round(stats["acc_sum"] / total, 2),
            "avg_star_rating": avg_star_rating,
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
            result[mod] = {
                "avg_score": "N/A",
                "avg_accuracy": "N/A",
                "winrate": "N/A",
                "matches": 0,
            }
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


def _top_comfort_picks(
    slot_stats: dict[str, dict[str, Any]],
    limit: int = 3,
) -> list[dict[str, Any]]:
    picks: list[dict[str, Any]] = []

    for slot, stats in slot_stats.items():
        if not _has_data(stats):
            continue

        picks.append(
            {
                "slot": slot,
                "avg_score": stats["avg_score"],
                "avg_accuracy": stats["avg_accuracy"],
                "avg_star_rating": stats["avg_star_rating"],
                "winrate": stats["winrate"],
                "matches": stats["matches"],
            }
        )

    picks.sort(
        key=lambda x: (x["avg_score"], x["avg_accuracy"], x["matches"]),
        reverse=True,
    )
    return picks[:limit]


def _build_key_picks(
    player1: str,
    player2: str,
    slot_stats_1: dict[str, dict[str, Any]],
    slot_stats_2: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    picks: list[dict[str, Any]] = []

    for slot in ALL_SLOTS:
        p1 = slot_stats_1[slot]
        p2 = slot_stats_2[slot]

        if not _has_data(p1) or not _has_data(p2):
            continue

        score_gap = p1["avg_score"] - p2["avg_score"]
        if score_gap <= 0:
            continue

        picks.append(
            {
                "slot": slot,
                "winner": player1,
                "player1_score": p1["avg_score"],
                "player2_score": p2["avg_score"],
                "player1_winrate": p1["winrate"],
                "player2_winrate": p2["winrate"],
                "score_gap": score_gap,
            }
        )

    picks.sort(key=lambda x: x["score_gap"], reverse=True)
    return picks


def _build_recommended_bans(
    player1: str,
    player2: str,
    slot_stats_1: dict[str, dict[str, Any]],
    slot_stats_2: dict[str, dict[str, Any]],
    limit: int = 2,
) -> list[dict[str, Any]]:
    bans: list[dict[str, Any]] = []

    for slot in ALL_SLOTS:
        p1 = slot_stats_1[slot]
        p2 = slot_stats_2[slot]

        if not _has_data(p1) or not _has_data(p2):
            continue

        score_gap = p2["avg_score"] - p1["avg_score"]
        if score_gap <= 0:
            continue

        bans.append(
            {
                "slot": slot,
                "target": player2,
                "player1_score": p1["avg_score"],
                "player2_score": p2["avg_score"],
                "score_gap": score_gap,
            }
        )

    bans.sort(key=lambda x: x["score_gap"], reverse=True)
    return bans[:limit]


def _build_slot_winrates(
    slot_stats_1: dict[str, dict[str, Any]],
    slot_stats_2: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for slot in ALL_SLOTS:
        p1 = slot_stats_1[slot]
        p2 = slot_stats_2[slot]

        rows.append(
            {
                "slot": slot,
                "player1_winrate": p1["winrate"],
                "player2_winrate": p2["winrate"],
            }
        )

    return rows


def compare_players(player1: str, player2: str) -> dict[str, Any] | None:
    summary1 = get_overall_summary(player1)
    summary2 = get_overall_summary(player2)

    if summary1 is None or summary2 is None:
        return None

    slot_stats_1 = summary1["slot_stats_90"]
    slot_stats_2 = summary2["slot_stats_90"]

    key_picks = _build_key_picks(player1, player2, slot_stats_1, slot_stats_2)
    slot_winrates = _build_slot_winrates(slot_stats_1, slot_stats_2)
    comfort_picks_1 = _top_comfort_picks(slot_stats_1, limit=3)
    comfort_picks_2 = _top_comfort_picks(slot_stats_2, limit=3)
    recommended_bans = _build_recommended_bans(
        player1,
        player2,
        slot_stats_1,
        slot_stats_2,
        limit=2,
    )

    return {
        "player1": summary1,
        "player2": summary2,
        "key_picks": key_picks,
        "slot_winrates": slot_winrates,
        "comfort_picks": {
            player1: comfort_picks_1,
            player2: comfort_picks_2,
        },
        "recommended_bans": recommended_bans,
    }

