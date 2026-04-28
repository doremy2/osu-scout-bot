"""Build a manual approval queue for tournament imports.

This script does not import tournaments. It reads `tournament_sources`,
filters for usable standard osu! sources, ranks candidates, and writes a
human-review queue.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage import fetch_tournament_sources  # noqa: E402

QUEUE_JSON = PROJECT_ROOT / "data" / "import_queue_2025_2026.json"
QUEUE_MD = PROJECT_ROOT / "data" / "reports" / "import_queue_2025_2026.md"

MODE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("taiko", re.compile(r"\b(?:twc|taiko(?:\s+world\s+cup)?|osu!taiko|o!t)\b", re.IGNORECASE)),
    ("catch", re.compile(r"\b(?:cwc|catch(?:\s+world\s+cup)?|ctb|catch\s+the\s+beat|osu!catch|o!c)\b", re.IGNORECASE)),
    ("mania", re.compile(r"\b(?:mwc|mania(?:\s+world\s+cup)?|osu!mania|o!m|4k|7k)\b", re.IGNORECASE)),
    ("mixed", re.compile(r"\b(?:mixed|multi[-\s]?mode|multimode|all\s*modes?)\b", re.IGNORECASE)),
]
STANDARD_RE = re.compile(
    r"\b(?:osu!?standard|osu!\s*std|o!std|std|standard\s+osu!|standard)\b|"
    r"/Tournaments/(?:OWC|OIT|LGA|RESC|FDC|3WC|4WC)/",
    re.IGNORECASE,
)
QUALITY_RANK = {"verified": 0, "high": 1}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value] if value else []
        if isinstance(parsed, list):
            return [str(item) for item in parsed if item]
    return []


def parse_dt(value: str | None) -> str:
    return value or ""


def timestamp_value(value: str | None) -> float:
    if not value:
        return 0
    cleaned = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned).timestamp()
    except ValueError:
        return 0


def detect_game_mode(row: dict[str, Any]) -> tuple[str, str | None]:
    explicit_mode = str(row.get("game_mode") or "").casefold().strip()
    if explicit_mode in {"osu", "taiko", "catch", "mania", "mixed"}:
        return explicit_mode, "explicit game_mode"

    haystack = " ".join(
        str(value or "")
        for value in (
            row.get("tournament_name"),
            row.get("source_url"),
            row.get("forum_url"),
            row.get("wiki_url"),
            row.get("notes"),
            row.get("format"),
        )
    )
    metadata = row.get("metadata_json")
    if metadata:
        haystack += f" {metadata}"

    for game_mode, pattern in MODE_PATTERNS:
        match = pattern.search(haystack)
        if match:
            return game_mode, match.group(0)
    if STANDARD_RE.search(haystack):
        return "osu", None
    return "unknown", None


def usable_candidate(row: dict[str, Any]) -> bool:
    notes = row.get("notes") or ""
    if row.get("status") in {"failed", "imported"}:
        return False
    if row.get("data_quality") not in {"verified", "high"}:
        return False
    game_mode, _reason = detect_game_mode(row)
    if game_mode != "osu" or "non-standard" in notes.casefold():
        return False
    match_links = as_list(row.get("linked_match_urls"))
    room_links = as_list(row.get("lazer_room_urls"))
    if not match_links and not room_links:
        return False
    return True


def exclusion_reason(row: dict[str, Any]) -> str | None:
    notes = row.get("notes") or ""
    game_mode, mode_reason = detect_game_mode(row)
    if row.get("status") in {"failed", "imported"}:
        return f"{row.get('status')} status"
    if game_mode != "osu":
        return f"game_mode={game_mode} ({mode_reason})"
    if "non-standard" in notes.casefold():
        return "non-standard note"
    if row.get("data_quality") not in {"verified", "high"}:
        return f"data_quality={row.get('data_quality') or 'missing'}"
    match_links = as_list(row.get("linked_match_urls"))
    room_links = as_list(row.get("lazer_room_urls"))
    if not match_links and not room_links:
        return "no usable match or room links"
    return None


def queue_score(row: dict[str, Any]) -> int:
    match_count = len(as_list(row.get("linked_match_urls")))
    room_count = len(as_list(row.get("lazer_room_urls")))
    score = 0
    score += 1000 if row.get("data_quality") == "verified" else 700
    score += min(match_count + room_count, 200) * 3
    score += 120 if row.get("spreadsheet_url") else 0
    score += 120 if row.get("bracket_url") else 0
    score += int(row.get("priority_score") or 0)
    return score


def build_queue(years: list[int], limit: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    for year in years:
        rows.extend(fetch_tournament_sources(year=year, limit=10_000))

    candidates = [row for row in rows if usable_candidate(row)]
    excluded_rows = [
        row for row in rows
        if detect_game_mode(row)[0] != "osu" and row.get("data_quality") in {"verified", "high"}
    ]
    candidates.sort(
        key=lambda row: (
            QUALITY_RANK.get(row.get("data_quality"), 99),
            -timestamp_value(row.get("created_at") or row.get("last_post_at") or row.get("last_checked_at")),
            -(len(as_list(row.get("linked_match_urls"))) + len(as_list(row.get("lazer_room_urls")))),
            -int(bool(row.get("spreadsheet_url"))),
            -int(bool(row.get("bracket_url"))),
            -int(row.get("priority_score") or 0),
        )
    )

    queue: list[dict[str, Any]] = []
    for index, row in enumerate(candidates[:limit], start=1):
        match_links = as_list(row.get("linked_match_urls"))
        room_links = as_list(row.get("lazer_room_urls"))
        queue.append(
            {
                "queue_rank": index,
                "import_status": "manual_approval_required",
                "tournament_key": row.get("tournament_key"),
                "tournament_name": row.get("tournament_name"),
                "year": row.get("year"),
                "data_quality": row.get("data_quality"),
                "game_mode": detect_game_mode(row)[0],
                "queue_score": queue_score(row),
                "source_type": row.get("source_type"),
                "source_url": row.get("source_url"),
                "forum_url": row.get("forum_url"),
                "wiki_url": row.get("wiki_url"),
                "spreadsheet_url": row.get("spreadsheet_url"),
                "bracket_url": row.get("bracket_url"),
                "discord_url": row.get("discord_url"),
                "match_link_count": len(match_links),
                "lazer_room_count": len(room_links),
                "sample_match_links": match_links[:5],
                "sample_lazer_room_links": room_links[:5],
                "rank_range": row.get("rank_range"),
                "format": row.get("format"),
                "team_size": row.get("team_size"),
                "created_at": row.get("created_at"),
                "last_post_at": row.get("last_post_at"),
                "notes": row.get("notes"),
            }
        )
    excluded = []
    for row in sorted(excluded_rows, key=lambda item: (item.get("year") or 0, item.get("tournament_name") or ""), reverse=True):
        match_links = as_list(row.get("linked_match_urls"))
        room_links = as_list(row.get("lazer_room_urls"))
        excluded.append(
            {
                "tournament_key": row.get("tournament_key"),
                "tournament_name": row.get("tournament_name"),
                "year": row.get("year"),
                "data_quality": row.get("data_quality"),
                "game_mode": detect_game_mode(row)[0],
                "exclusion_reason": exclusion_reason(row),
                "match_link_count": len(match_links),
                "lazer_room_count": len(room_links),
                "source_url": row.get("source_url"),
                "wiki_url": row.get("wiki_url"),
                "forum_url": row.get("forum_url"),
            }
        )
    return queue, excluded


def write_outputs(queue: list[dict[str, Any]], excluded: list[dict[str, Any]], years: list[int], limit: int) -> None:
    QUEUE_JSON.parent.mkdir(parents=True, exist_ok=True)
    QUEUE_MD.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": utc_now_iso(),
        "years": years,
        "limit": limit,
        "status": "manual_approval_required",
        "items": queue,
        "exclusion_rules": {
            "game_mode": "Only game_mode=osu is importable.",
            "excluded_modes": ["taiko", "catch", "mania", "mixed", "unknown"],
            "required_links": "At least one osu! match link or lazer room link is required.",
            "required_quality": ["verified", "high"],
        },
        "excluded_items": excluded,
    }
    QUEUE_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Import Queue: 2025-2026",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "This queue is for manual approval only. No tournament rows were imported.",
        "",
        "| # | Year | Mode | Quality | Tournament | Links | Sources |",
        "|---:|---:|---|---|---|---:|---|",
    ]
    for item in queue:
        links = item["match_link_count"] + item["lazer_room_count"]
        sources = []
        if item.get("spreadsheet_url"):
            sources.append("sheet")
        if item.get("bracket_url"):
            sources.append("bracket")
        if item.get("forum_url"):
            sources.append("forum")
        if item.get("wiki_url"):
            sources.append("wiki")
        lines.append(
            "| {queue_rank} | {year} | {game_mode} | {data_quality} | [{name}]({url}) | {links} | {sources} |".format(
                queue_rank=item["queue_rank"],
                year=item["year"],
                game_mode=item["game_mode"],
                data_quality=item["data_quality"],
                name=str(item["tournament_name"]).replace("|", "\\|"),
                url=item.get("source_url") or item.get("forum_url") or item.get("wiki_url"),
                links=links,
                sources=", ".join(sources),
            )
        )
    if excluded:
        lines.extend(
            [
                "",
                "## Excluded Mode-Specific Candidates",
                "",
                "| Year | Mode | Quality | Tournament | Reason |",
                "|---:|---|---|---|---|",
            ]
        )
        for item in excluded:
            lines.append(
                "| {year} | {game_mode} | {data_quality} | [{name}]({url}) | {reason} |".format(
                    year=item["year"],
                    game_mode=item["game_mode"],
                    data_quality=item["data_quality"],
                    name=str(item["tournament_name"]).replace("|", "\\|"),
                    url=item.get("source_url") or item.get("forum_url") or item.get("wiki_url"),
                    reason=str(item.get("exclusion_reason") or "").replace("|", "\\|"),
                )
            )
    QUEUE_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build manual tournament import queue")
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--limit", type=int, default=30)
    args = parser.parse_args()

    queue, excluded = build_queue(args.years, args.limit)
    write_outputs(queue, excluded, args.years, args.limit)
    print(f"Wrote {len(queue)} queue items to {QUEUE_JSON}")
    print(f"Marked {len(excluded)} mode-specific candidates as excluded")
    print(f"Wrote markdown report to {QUEUE_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
