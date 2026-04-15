import statistics


from storage import (
    fetch_all_matches,
    fetch_player_matches,
    fetch_player_scores,
    fetch_player_tournament_matches,
)

from collections import defaultdict
from datetime import datetime, timedelta

from typing import Any



SLOT_GROUPS = {
    "NM": ["NM1", "NM2", "NM3", "NM4", "NM5", "NM6"],
    "HD": ["HD1", "HD2", "HD3"],
    "HR": ["HR1", "HR2", "HR3"],
    "DT": ["DT1", "DT2", "DT3"],
    "FM": ["FM1", "FM2"],
}

ALL_SLOTS = [slot for slots in SLOT_GROUPS.values() for slot in slots]
ALL_MODS = list(SLOT_GROUPS.keys())

MOD_ORDER = {
    "NM": 0,
    "HD": 1,
    "HR": 2,
    "DT": 3,
    "FM": 4,
    "TB": 5,
}

STAGE_ORDER = {
    "Group Stage": 1,
    "Round of 16": 2,
    "Quarterfinals": 3,
    "Semifinals": 4,
    "Finals": 5,
    "Grand Finals": 6,
}


def _stage_rank(stage: str | None) -> int:
    if not stage:
        return 0
    return STAGE_ORDER.get(stage, 0)

def _slot_prefix(slot: str) -> str:
    return "".join(ch for ch in slot if ch.isalpha()).upper()


def _slot_number(slot: str) -> int:
    digits = "".join(ch for ch in slot if ch.isdigit())
    return int(digits) if digits else 999


def _slot_sort_key(slot: str) -> tuple[int, int, str]:
    prefix = _slot_prefix(slot)
    return (MOD_ORDER.get(prefix, 99), _slot_number(slot), slot)


def get_all_slots(matches: list[dict[str, Any]] | None = None) -> list[str]:
    slots = set(ALL_SLOTS)
    if matches:
        for match in matches:
            slot = (match.get("slot") or "").strip()
            if slot:
                slots.add(slot)
    return sorted(slots, key=_slot_sort_key)


def _observed_slots_from_stats(*slot_stats_dicts: dict[str, dict[str, Any]]) -> list[str]:
    slots: set[str] = set()
    for slot_stats in slot_stats_dicts:
        for slot, stats in slot_stats.items():
            if stats.get("matches", 0) > 0:
                slots.add(slot)
    return sorted(slots, key=_slot_sort_key)

def load_matches() -> list[dict[str, Any]]:
    return fetch_all_matches()


def parse_date(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d")



def get_player_matches(username: str) -> list[dict[str, Any]]:
    return fetch_player_matches(username)


def get_recent_maps(username: str, limit: int = 5) -> list[dict[str, Any]]:
    """Return the player's most recent MAP-level rows.

    A "map" here means a single beatmap played inside some match. This is
    NOT the same as a match (a full BO9/BO11/BO13 series). For match-level
    history use get_recent_match_history().
    """
    return get_player_matches(username)[:limit]


# Kept as an alias so older callers / tests don't break, but new code should
# use get_recent_maps() because these are individual maps, not matches.
def get_recent_matches(username: str, limit: int = 5) -> list[dict[str, Any]]:
    return get_recent_maps(username, limit)


def get_recent_match_history(username: str, limit: int = 5) -> list[dict[str, Any]]:
    """Return the player's most recent MATCH-level history.

    A "match" here is a full series (e.g. BO9, BO11, BO13) between two
    players or two teams, with a final score like "5-0" and ideally a link
    back to the source match (osu! match page, ROMAI page, etc.).

    Expected row shape (when real data is available):
        {
            "opponent": "playerX",
            "player_score": 5,
            "opponent_score": 0,
            "result": "win" | "loss",
            "match_link": "https://...",   # optional
            "event": "OWC 2025",            # optional
            "stage": "Grand Finals",        # optional
            "date": "2026-04-01",           # optional
        }

    Reads from the `tournament_matches` table, which the OWC Team Stats
    importer populates. OWC is a team tournament, so for now "opponent"
    here means the opposing team (e.g. "USA"). Once player-level result
    sources land (ROMAI / Elitebotix / Skillissue), they can write into
    the same table with real player opponents and this function will
    transparently start returning richer rows.
    """
    rows = fetch_player_tournament_matches(username, limit=100)
    if not rows:
        return []

    def sort_key(row: dict[str, Any]):
        parsed_date = parse_date(row.get("date"))
        return (
            parsed_date or datetime.min,
            _stage_rank(row.get("stage")),
            int(row.get("match_index") or 0),
        )

    rows.sort(key=sort_key, reverse=True)

    history: list[dict[str, Any]] = []
    for row in rows[:limit]:
        history.append(
            {
                "opponent": row.get("opponent_team_name")
                    or row.get("opponent_team")
                    or "Unknown",
                "opponent_team": row.get("opponent_team"),
                "opponent_team_name": row.get("opponent_team_name"),
                "player_team": row.get("team_code") or row.get("team"),
                "player_team_name": row.get("team_name"),
                "player_score": row.get("team_score"),
                "opponent_score": row.get("opponent_score"),
                "result": row.get("result"),
                "match_link": row.get("match_link"),
                "event": row.get("event"),
                "stage": row.get("stage"),
                "date": row.get("date"),
            }
        )
    return history


def get_matches_last_n_days(username: str, days: int = 90) -> list[dict[str, Any]]:
    matches = get_player_matches(username)
    if not matches:
        return []

    dated_matches = [m for m in matches if parse_date(m.get("date")) is not None]


    if not dated_matches:
        return matches

    newest_date = max(parse_date(m["date"]) for m in dated_matches)
    cutoff = newest_date - timedelta(days=days)

    return [
        m for m in dated_matches
        if parse_date(m["date"]) >= cutoff
    ]



def _empty_stat() -> dict[str, Any]:
    return {
        "avg_score": "N/A",
        "avg_accuracy": "N/A",
        "avg_star_rating": "N/A",
        "winrate": "N/A",
        "matches": 0,
    }


def build_slot_stats(
    matches: list[dict[str, Any]],
    slots: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    slot_stats = defaultdict(
        lambda: {
            "total": 0,
            "wins": 0,
            "scored_results": 0,
            "score_sum": 0,
            "acc_sum": 0.0,
            "star_sum": 0.0,
            "star_count": 0,
        }
    )

    for match in matches:
        slot = match["slot"]
        result_value = (match.get("result") or "").lower()

        slot_stats[slot]["total"] += 1
        slot_stats[slot]["score_sum"] += match["score"]
        slot_stats[slot]["acc_sum"] += match["accuracy"]

        if result_value in {"win", "loss"}:
            slot_stats[slot]["scored_results"] += 1
            if result_value == "win":
                slot_stats[slot]["wins"] += 1

        star_rating = match.get("star_rating")
        if isinstance(star_rating, (int, float)):
            slot_stats[slot]["star_sum"] += star_rating
            slot_stats[slot]["star_count"] += 1

    ordered_slots = slots or get_all_slots(matches)

    result: dict[str, dict[str, Any]] = {}
    for slot in ordered_slots:
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

        winrate = (
            round((stats["wins"] / stats["scored_results"]) * 100, 1)
            if stats["scored_results"] > 0
            else "N/A"
        )

        result[slot] = {
            "avg_score": round(stats["score_sum"] / total),
            "avg_accuracy": round(stats["acc_sum"] / total, 2),
            "avg_star_rating": avg_star_rating,
            "winrate": winrate,
            "matches": total,
        }

    return result




def build_mod_stats(matches: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mod_stats = defaultdict(
        lambda: {
            "total": 0,
            "wins": 0,
            "scored_results": 0,
            "score_sum": 0,
            "acc_sum": 0.0,
        }
    )

    for match in matches:
        mod = match["mod"]
        result_value = (match.get("result") or "").lower()

        mod_stats[mod]["total"] += 1
        mod_stats[mod]["score_sum"] += match["score"]
        mod_stats[mod]["acc_sum"] += match["accuracy"]

        if result_value in {"win", "loss"}:
            mod_stats[mod]["scored_results"] += 1
            if result_value == "win":
                mod_stats[mod]["wins"] += 1

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
        winrate = (
            round((stats["wins"] / stats["scored_results"]) * 100, 1)
            if stats["scored_results"] > 0
            else "N/A"
        )

        result[mod] = {
            "avg_score": round(stats["score_sum"] / total),
            "avg_accuracy": round(stats["acc_sum"] / total, 2),
            "winrate": winrate,
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

def _format_numeric_or_na(value: float | str) -> float | str:
    return round(value, 2) if isinstance(value, (int, float)) else "N/A"


def _compute_map_winrate(matches: list[dict[str, Any]]) -> tuple[int, int, float | str]:
    result_rows = [
        m for m in matches
        if (m.get("result") or "").lower() in {"win", "loss"}
    ]
    if not result_rows:
        return 0, 0, "N/A"

    wins = sum(1 for m in result_rows if (m.get("result") or "").lower() == "win")
    total = len(result_rows)
    return wins, total, round((wins / total) * 100, 1)


def _compute_match_winrate(matches: list[dict[str, Any]]) -> tuple[int, int, float | str]:
    # Future-ready:
    # expects rows to optionally have match_result = "win" / "loss"
    match_rows = [
        m for m in matches
        if (m.get("match_result") or "").lower() in {"win", "loss"}
    ]
    if not match_rows:
        return 0, 0, "N/A"

    wins = sum(1 for m in match_rows if (m.get("match_result") or "").lower() == "win")
    total = len(match_rows)
    return wins, total, round((wins / total) * 100, 1)


def _build_slot_median_score_index(matches: list[dict[str, Any]]) -> dict[tuple[str | None, str | None, str], float]:
    buckets: dict[tuple[str | None, str | None, str], list[int]] = defaultdict(list)

    for match in matches:
        score = match.get("score")
        slot = match.get("slot")
        if not isinstance(score, (int, float)) or not slot:
            continue

        key = (
            match.get("event"),
            match.get("stage"),
            slot,
        )
        buckets[key].append(int(score))

    return {
        key: float(statistics.median(values))
        for key, values in buckets.items()
        if values
    }

def _lookup_real_pscore(username: str) -> float | str:
    """If a Performance Scores importer has loaded official pscore values
    for this player, return their average across rounds. Returns 'N/A' if
    no real pscore rows exist."""
    rows = fetch_player_scores(username)
    if not rows:
        return "N/A"
    values = [row["pscore"] for row in rows if isinstance(row.get("pscore"), (int, float))]
    if not values:
        return "N/A"
    return round(sum(values) / len(values), 3)


def _compute_avg_performance_score(player_matches: list[dict[str, Any]]) -> float | str:
    if not player_matches:
        return "N/A"

    all_matches = load_matches()
    median_index = _build_slot_median_score_index(all_matches)

    ratios: list[float] = []

    for match in player_matches:
        score = match.get("score")
        slot = match.get("slot")
        if not isinstance(score, (int, float)) or not slot:
            continue

        key = (
            match.get("event"),
            match.get("stage"),
            slot,
        )
        median_score = median_index.get(key)
        if not median_score or median_score <= 0:
            continue

        ratios.append(float(score) / float(median_score))

    if not ratios:
        return "N/A"

    return round(sum(ratios) / len(ratios), 2)


def get_overall_summary(username: str) -> dict[str, Any] | None:
    matches = get_player_matches(username)
    if not matches:
        return None

    total_maps_played = len(matches)

    map_wins, maps_with_results, map_winrate = _compute_map_winrate(matches)
    match_wins, matches_with_results, match_winrate = _compute_match_winrate(matches)

    # Prefer the real pscore values from the imported Performance Scores
    # sheets when they exist; fall back to the median-ratio proxy otherwise.
    real_pscore = _lookup_real_pscore(username)
    if isinstance(real_pscore, (int, float)):
        avg_performance_score: float | str = real_pscore
    else:
        avg_performance_score = _compute_avg_performance_score(matches)

    # overall_winrate remains for compatibility with old formatting logic.
    # Prefer true match WR if available, otherwise fall back to map WR.
    if isinstance(match_winrate, (int, float)):
        overall_winrate: float | str = match_winrate
    elif isinstance(map_winrate, (int, float)):
        overall_winrate = map_winrate
    else:
        overall_winrate = "N/A"

    if isinstance(overall_winrate, (int, float)):
        if overall_winrate >= 70:
            consistency = "High"
        elif overall_winrate >= 40:
            consistency = "Medium"
        else:
            consistency = "Low"
    else:
        consistency = "Unknown"

    recent_90 = get_matches_last_n_days(username, 90)
    slot_stats_90 = build_slot_stats(recent_90, slots=get_all_slots(recent_90))
    mod_stats_90 = build_mod_stats(recent_90)
    strengths, weaknesses = get_strengths_and_weaknesses(mod_stats_90)

    return {
        "player": username,
        "total_maps_played": total_maps_played,
        "map_wins": map_wins,
        "maps_with_results": maps_with_results,
        "map_winrate": map_winrate,
        "match_wins": match_wins,
        "matches_with_results": matches_with_results,
        "match_winrate": match_winrate,
        "avg_performance_score": avg_performance_score,
        "overall_winrate": overall_winrate,
        "consistency": consistency,
        "recent_matches": get_recent_maps(username, 5),  # legacy: map rows
        "recent_maps": get_recent_maps(username, 5),
        "recent_match_history": get_recent_match_history(username, 5),
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
    return build_slot_stats(matches_90, slots=get_all_slots(matches_90))


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
    slots: list[str],
) -> list[dict[str, Any]]:
    picks: list[dict[str, Any]] = []

    for slot in slots:
        p1 = slot_stats_1.get(slot, _empty_stat())
        p2 = slot_stats_2.get(slot, _empty_stat())

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
    slots: list[str],
    limit: int = 2,
) -> list[dict[str, Any]]:
    bans: list[dict[str, Any]] = []

    for slot in slots:
        p1 = slot_stats_1.get(slot, _empty_stat())
        p2 = slot_stats_2.get(slot, _empty_stat())

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
    slots: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for slot in slots:
        p1 = slot_stats_1.get(slot, _empty_stat())
        p2 = slot_stats_2.get(slot, _empty_stat())

        if not _has_data(p1) and not _has_data(p2):
            continue

        rows.append(
            {
                "slot": slot,
                "player1_winrate": p1["winrate"],
                "player2_winrate": p2["winrate"],
            }
        )

    return rows


def _build_accuracy_edges(
    player1: str,
    player2: str,
    slot_stats_1: dict[str, dict[str, Any]],
    slot_stats_2: dict[str, dict[str, Any]],
    slots: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for slot in slots:
        p1 = slot_stats_1.get(slot, _empty_stat())
        p2 = slot_stats_2.get(slot, _empty_stat())

        if not _has_data(p1) or not _has_data(p2):
            continue
        if p1["avg_accuracy"] == "N/A" or p2["avg_accuracy"] == "N/A":
            continue

        acc_gap = p1["avg_accuracy"] - p2["avg_accuracy"]
        if acc_gap <= 0:
            continue

        rows.append(
            {
                "slot": slot,
                "winner": player1,
                "player1_accuracy": p1["avg_accuracy"],
                "player2_accuracy": p2["avg_accuracy"],
                "accuracy_gap": round(acc_gap, 2),
            }
        )

    rows.sort(key=lambda x: x["accuracy_gap"], reverse=True)
    return rows


def compare_players(player1: str, player2: str) -> dict[str, Any] | None:
    summary1 = get_overall_summary(player1)
    summary2 = get_overall_summary(player2)

    if summary1 is None or summary2 is None:
        return None

    slot_stats_1 = summary1["slot_stats_90"]
    slot_stats_2 = summary2["slot_stats_90"]
    observed_slots = _observed_slots_from_stats(slot_stats_1, slot_stats_2)

    key_picks = _build_key_picks(player1, player2, slot_stats_1, slot_stats_2, observed_slots)
    slot_winrates = _build_slot_winrates(slot_stats_1, slot_stats_2, observed_slots)
    accuracy_edges = _build_accuracy_edges(player1, player2, slot_stats_1, slot_stats_2, observed_slots)

    comfort_picks_1 = _top_comfort_picks(slot_stats_1, limit=3)
    comfort_picks_2 = _top_comfort_picks(slot_stats_2, limit=3)
    recommended_bans = _build_recommended_bans(
        player1,
        player2,
        slot_stats_1,
        slot_stats_2,
        observed_slots,
        limit=2,
    )

    return {
        "player1": summary1,
        "player2": summary2,
        "slots": observed_slots,
        "key_picks": key_picks,
        "slot_winrates": slot_winrates,
        "accuracy_edges": accuracy_edges,
        "comfort_picks": {
            player1: comfort_picks_1,
            player2: comfort_picks_2,
        },
        "recommended_bans": recommended_bans,
    }


