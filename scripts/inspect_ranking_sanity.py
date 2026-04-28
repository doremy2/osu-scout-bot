from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


WATCHLIST = (
    "MALISZEWSKI",
    "mrekk",
    "lifeline",
    "NINERIK",
    "ASecretBox",
    "rektygon",
    "FlyingTuna",
    "WindowLife",
    "Raikouhou",
    "liliel",
    "milosz",
)


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _truthy(value: Any) -> bool:
    return str(value).casefold() in {"true", "yes", "1"}


def _load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _event_name(event: dict[str, Any]) -> str:
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    return metadata.get("event") or (event.get("event_name") or "").split(" - ")[0]


def _stage_name(event: dict[str, Any]) -> str:
    metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
    return metadata.get("stage") or (event.get("event_name") or "").split(" - ")[-1]


def _fmt(value: Any, decimals: int = 2) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return "N/A"
    return f"{numeric:.{decimals}f}"


def _top_recent_events(events: list[dict[str, Any]], *, limit: int = 3) -> str:
    def sort_key(event: dict[str, Any]) -> tuple[float, str]:
        days = _to_float(event.get("days_since_event"))
        return (days if days is not None else float("inf"), event.get("event_name") or "")

    labels = []
    for event in sorted(events, key=sort_key)[:limit]:
        labels.append(f"{event.get('event_name')} ({_fmt(event.get('match_cost'), 1)} cost)")
    return "; ".join(labels) or "no recent events"


def _build_report(
    *,
    ranking_rows: list[dict[str, Any]],
    player_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    leaderboard_rows: list[dict[str, Any]],
    top_n: int,
) -> str:
    players_by_name = {row["username"].casefold(): row for row in player_rows}
    events_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    tournaments_by_name: dict[str, set[str]] = defaultdict(set)
    stages_by_name: dict[str, set[str]] = defaultdict(set)
    for event in event_rows:
        key = event["username"].casefold()
        events_by_name[key].append(event)
        tournaments_by_name[key].add(_event_name(event))
        stages_by_name[key].add(event.get("event_name") or "")

    leaderboard_by_name = {row["username"].casefold(): row for row in leaderboard_rows}
    alias_to_player: dict[str, dict[str, Any]] = {}
    for row in leaderboard_rows:
        for alias in row.get("aliases") or []:
            alias_to_player[str(alias).casefold()] = row

    lines = [
        "# Ranking Sanity Report",
        "",
        "## Summary",
        "",
        f"- Ranking rows inspected: {len(ranking_rows)}",
        f"- Top rows reviewed: {min(top_n, len(ranking_rows))}",
        f"- Provisional players: {sum(_truthy(row.get('provisional')) for row in ranking_rows)}",
        f"- Non-provisional players: {sum(not _truthy(row.get('provisional')) for row in ranking_rows)}",
        f"- Low-confidence leaderboard rows: {sum((leaderboard_by_name.get(row['username'].casefold(), {}).get('confidence_label') == 'low') for row in ranking_rows)}",
        f"- Rows needing formula review: {sum(('needs_formula_review' in (leaderboard_by_name.get(row['username'].casefold(), {}).get('warning_flags') or [])) for row in ranking_rows)}",
        "- Provisional rule: fewer than 3 unique tournaments in the last 12 months",
        "",
        "## Top 20 Explanations",
        "",
    ]

    for rank, row in enumerate(ranking_rows[:20], start=1):
        key = row["username"].casefold()
        player = players_by_name.get(key, {})
        country = player.get("country_code") or "??"
        tournaments = sorted(tournaments_by_name.get(key, set()))
        recent = _top_recent_events(events_by_name.get(key, []))
        provisional = "provisional" if _truthy(row.get("provisional")) else "non-provisional"
        lines.append(
            f"{rank}. **{row['username']}** ({country}) is here because final={_fmt(row.get('final_power_score'))}, "
            f"form={_fmt(row.get('recent_tournament_form'))}, consistency={_fmt(row.get('consistency_score'))}, "
            f"reliability={_fmt(row.get('reliability_multiplier'), 3)}, activity={_fmt(row.get('activity_multiplier'), 3)}, "
            f"{provisional}, confidence={leaderboard_by_name.get(key, {}).get('confidence_label', 'unknown')}, "
            f"dominant={leaderboard_by_name.get(key, {}).get('dominant_event') or 'N/A'} "
            f"({_fmt((leaderboard_by_name.get(key, {}).get('dominant_event_score_share') or 0) * 100, 0)}%), "
            f"tournaments={len(tournaments)} [{', '.join(tournaments) or 'none'}]. "
            f"Recent: {recent}."
        )
    lines.append("")

    lines.extend(["## Potentially Over-Ranked", ""])
    over_ranked = []
    for rank, row in enumerate(ranking_rows[:top_n], start=1):
        tournaments = len(tournaments_by_name.get(row["username"].casefold(), set()))
        form = _to_float(row.get("recent_tournament_form")) or 0.0
        consistency = _to_float(row.get("consistency_score")) or 0.0
        if _truthy(row.get("provisional")) and rank <= 50 and tournaments <= 1 and form >= 70:
            over_ranked.append((rank, row, "high top-50 score from one tournament"))
        elif _truthy(row.get("provisional")) and rank <= 25 and consistency == 50.0:
            over_ranked.append((rank, row, "top-25 provisional with default one-event consistency"))

    if not over_ranked:
        lines.append("- No obvious top-100 over-rank flags from the current heuristics.")
    else:
        for rank, row, reason in over_ranked[:20]:
            lines.append(
                f"- #{rank} {row['username']}: {reason}; final={_fmt(row.get('final_power_score'))}, "
                f"form={_fmt(row.get('recent_tournament_form'))}, tournaments={row.get('tournaments_played_last_12m')}."
            )
    lines.append("")

    lines.extend(["## Potentially Under-Ranked / Missing Watchlist", ""])
    for name in WATCHLIST:
        match = next(
            ((rank, row) for rank, row in enumerate(ranking_rows, start=1) if row["username"].casefold() == name.casefold()),
            None,
        )
        alias_match = alias_to_player.get(name.casefold())
        if match is None and alias_match is not None:
            alias_rank = _to_int(alias_match.get("rank"))
            lines.append(
                f"- {name}: represented as #{alias_rank} {alias_match.get('username')} via alias mapping."
            )
            continue
        if match is None:
            lines.append(f"- {name}: not present in this dataset/export.")
            continue
        rank, row = match
        if rank > top_n:
            lines.append(
                f"- #{rank} {row['username']}: outside top {top_n}; final={_fmt(row.get('final_power_score'))}, "
                f"form={_fmt(row.get('recent_tournament_form'))}, tournaments={row.get('tournaments_played_last_12m')}."
            )
        elif rank > 40:
            lines.append(
                f"- #{rank} {row['username']}: lower than an obvious-name expectation; current data gives "
                f"form={_fmt(row.get('recent_tournament_form'))}, reliability={_fmt(row.get('reliability_multiplier'), 3)}."
            )
    lines.append("")

    lines.extend(["## Display/Data Checks", ""])
    duplicate_names = [
        name
        for name in {row["username"].casefold() for row in ranking_rows}
        if sum(1 for row in ranking_rows if row["username"].casefold() == name) > 1
    ]
    missing_country_top100 = [
        row["username"]
        for row in ranking_rows[:top_n]
        if not (players_by_name.get(row["username"].casefold(), {}).get("country_code"))
    ]
    missing_rank_top100 = [
        row["username"]
        for row in ranking_rows[:top_n]
        if not (players_by_name.get(row["username"].casefold(), {}).get("bancho_rank"))
    ]
    bad_labels = sorted(
        {
            event.get("event_name")
            for event in event_rows
            if not event.get("event_name")
            or "unknown" in str(event.get("event_name")).casefold()
            or "none" in str(event.get("event_name")).casefold()
        }
    )
    lines.extend(
        [
            f"- Duplicate player rows: {len(duplicate_names)}",
            f"- Missing country in top {top_n}: {len(missing_country_top100)}",
            f"- Missing Bancho rank in top {top_n}: {len(missing_rank_top100)}",
            f"- Bad event labels: {len(bad_labels)}",
            f"- Leaderboard rows with tier: {sum(1 for row in leaderboard_rows if row.get('tier'))}",
            f"- Leaderboard rows with warning flags: {sum(1 for row in leaderboard_rows if row.get('warning_flags'))}",
        ]
    )
    if missing_country_top100:
        lines.append(f"- Missing country names: {', '.join(missing_country_top100[:20])}")
    if missing_rank_top100:
        lines.append(f"- Missing rank names: {', '.join(missing_rank_top100[:20])}")
    if bad_labels:
        lines.append(f"- Bad labels: {', '.join(str(label) for label in bad_labels[:20])}")
    lines.append("")

    lines.extend(["## Top 100 Table", ""])
    lines.append("| Rank | Player | Tier | Final | Form | Consistency | Reliability | Activity | Tournaments | Confidence | Flags | Provisional |")
    lines.append("| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |")
    for rank, row in enumerate(ranking_rows[:top_n], start=1):
        leader = leaderboard_by_name.get(row["username"].casefold(), {})
        lines.append(
            f"| {rank} | {row['username']} | {leader.get('tier', 'N/A')} | {_fmt(row.get('final_power_score'))} | "
            f"{_fmt(row.get('recent_tournament_form'))} | {_fmt(row.get('consistency_score'))} | "
            f"{_fmt(row.get('reliability_multiplier'), 3)} | {_fmt(row.get('activity_multiplier'), 3)} | "
            f"{leader.get('unique_tournaments_count', row.get('tournaments_played_last_12m'))} | "
            f"{leader.get('confidence_label', 'N/A')} | {', '.join(leader.get('warning_flags') or []) or 'none'} | "
            f"{row.get('provisional')} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect ranking output for leaderboard sanity.")
    parser.add_argument("--ranking-csv", default="data/power_ranking_multi_event.csv")
    parser.add_argument("--players-json", default="data/power_players_multi_event.json")
    parser.add_argument("--events-json", default="data/power_events_multi_event.json")
    parser.add_argument("--leaderboard-json", default="data/leaderboard_multi_event.json")
    parser.add_argument("--out", default="data/reports/ranking_sanity_top100.md")
    parser.add_argument("--top", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ranking_rows = _load_csv(Path(args.ranking_csv))
    player_rows = _load_json(Path(args.players_json))
    event_rows = _load_json(Path(args.events_json))
    leaderboard_rows = _load_json(Path(args.leaderboard_json))
    report = _build_report(
        ranking_rows=ranking_rows,
        player_rows=player_rows,
        event_rows=event_rows,
        leaderboard_rows=leaderboard_rows,
        top_n=args.top,
    )
    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")
    print(output_path)


if __name__ == "__main__":
    main()
