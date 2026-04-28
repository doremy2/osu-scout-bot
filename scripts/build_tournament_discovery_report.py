from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.build_import_queue import as_list, detect_game_mode  # noqa: E402
from storage import fetch_tournament_sources, init_db  # noqa: E402


DEFAULT_JSON = PROJECT_ROOT / "data" / "reports" / "tournament_sources_2025_discovery.json"
DEFAULT_MD = PROJECT_ROOT / "data" / "reports" / "tournament_sources_2025_discovery.md"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def source_group(row: dict[str, Any]) -> str:
    has_forum = bool(row.get("forum_url") or "forum" in str(row.get("source") or "").casefold())
    has_wiki = bool(row.get("wiki_url") or "wiki" in str(row.get("source") or "").casefold())
    if has_forum and has_wiki:
        return "forum+wiki"
    if has_forum:
        return "forum"
    if has_wiki:
        return "wiki"
    return "other"


def link_count(row: dict[str, Any]) -> int:
    return len(as_list(row.get("linked_match_urls"))) + len(as_list(row.get("lazer_room_urls")))


def review_reason(row: dict[str, Any]) -> list[str]:
    reasons = []
    mode, mode_reason = detect_game_mode(row)
    if mode != "osu":
        reasons.append(f"mode={mode}" + (f" ({mode_reason})" if mode_reason else ""))
    if row.get("status") == "failed":
        reasons.append("failed")
    if row.get("status") == "imported":
        reasons.append("already imported")
    if row.get("data_quality") not in {"verified", "high"}:
        reasons.append(f"quality={row.get('data_quality') or 'unknown'}")
    if link_count(row) == 0:
        reasons.append("no match or room links")
    if not row.get("spreadsheet_url"):
        reasons.append("missing spreadsheet")
    if not row.get("bracket_url"):
        reasons.append("missing bracket")
    return reasons


def report_row(row: dict[str, Any]) -> dict[str, Any]:
    mode, mode_reason = detect_game_mode(row)
    reasons = review_reason(row)
    production_candidate = (
        mode == "osu"
        and row.get("status") not in {"failed", "imported"}
        and row.get("data_quality") in {"verified", "high"}
        and link_count(row) > 0
    )
    return {
        "tournament_key": row.get("tournament_key"),
        "tournament_name": row.get("tournament_name"),
        "year": row.get("year"),
        "source_group": source_group(row),
        "source": row.get("source"),
        "source_type": row.get("source_type"),
        "source_url": row.get("source_url"),
        "forum_url": row.get("forum_url"),
        "wiki_url": row.get("wiki_url"),
        "spreadsheet_url": row.get("spreadsheet_url"),
        "bracket_url": row.get("bracket_url"),
        "status": row.get("status"),
        "data_quality": row.get("data_quality"),
        "game_mode": mode,
        "game_mode_reason": mode_reason,
        "rank_range": row.get("rank_range"),
        "team_size": row.get("team_size"),
        "format": row.get("format"),
        "match_or_room_links": link_count(row),
        "priority_score": row.get("priority_score") or 0,
        "production_candidate": production_candidate,
        "review_reasons": reasons,
        "notes": row.get("notes"),
    }


def sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    quality_rank = {"verified": 0, "high": 1, "partial": 2, "low": 3}
    return (
        not row["production_candidate"],
        quality_rank.get(row.get("data_quality"), 9),
        -int(row.get("match_or_room_links") or 0),
        -int(row.get("priority_score") or 0),
        row.get("tournament_name") or "",
    )


def build_report(year: int) -> dict[str, Any]:
    init_db()
    rows = [report_row(row) for row in fetch_tournament_sources(year=year, limit=20_000)]
    rows.sort(key=sort_key)
    return {
        "generated_at": utc_now_iso(),
        "year": year,
        "total": len(rows),
        "source_counts": dict(Counter(row["source_group"] for row in rows)),
        "quality_counts": dict(Counter(row["data_quality"] or "unknown" for row in rows)),
        "status_counts": dict(Counter(row["status"] or "unknown" for row in rows)),
        "mode_counts": dict(Counter(row["game_mode"] for row in rows)),
        "production_candidate_count": sum(1 for row in rows if row["production_candidate"]),
        "rows": rows,
    }


def write_markdown(path: Path, payload: dict[str, Any]) -> None:
    rows = payload["rows"]
    production = [row for row in rows if row["production_candidate"]]
    needs_review = [row for row in rows if not row["production_candidate"]]
    lines = [
        "# 2025 Tournament Discovery Index",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "This is an index/review report only. It does not import tournament match rows.",
        "",
        "## Summary",
        "",
        f"- Total indexed 2025 sources: {payload['total']}",
        f"- Production candidates: {payload['production_candidate_count']}",
        f"- Source counts: `{payload['source_counts']}`",
        f"- Quality counts: `{payload['quality_counts']}`",
        f"- Status counts: `{payload['status_counts']}`",
        f"- Mode counts: `{payload['mode_counts']}`",
        "",
        "## Production Candidate Queue",
        "",
        "| # | Quality | Mode | Tournament | Links | Source | Notes |",
        "|---:|---|---|---|---:|---|---|",
    ]
    if production:
        for index, row in enumerate(production[:50], start=1):
            lines.append(
                "| {index} | {quality} | {mode} | [{name}]({url}) | {links} | {source} | {notes} |".format(
                    index=index,
                    quality=row["data_quality"],
                    mode=row["game_mode"],
                    name=str(row["tournament_name"]).replace("|", "\\|"),
                    url=row.get("source_url") or row.get("forum_url") or row.get("wiki_url"),
                    links=row["match_or_room_links"],
                    source=row["source_group"],
                    notes=str(row.get("notes") or "").replace("|", "\\|"),
                )
            )
    else:
        lines.append("| - | - | - | No production-safe candidates remain | 0 | - | - |")

    lines.extend(
        [
            "",
            "## Manual Review Queue",
            "",
            "| Tournament | Quality | Status | Mode | Links | Review reasons |",
            "|---|---|---|---|---:|---|",
        ]
    )
    for row in needs_review[:100]:
        lines.append(
            "| [{name}]({url}) | {quality} | {status} | {mode} | {links} | {reasons} |".format(
                name=str(row["tournament_name"]).replace("|", "\\|"),
                url=row.get("source_url") or row.get("forum_url") or row.get("wiki_url"),
                quality=row["data_quality"],
                status=row["status"],
                mode=row["game_mode"],
                links=row["match_or_room_links"],
                reasons=", ".join(row["review_reasons"]).replace("|", "\\|") or "none",
            )
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a 2025 tournament source discovery report.")
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD)
    args = parser.parse_args()

    payload = build_report(args.year)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    write_markdown(args.md_out, payload)
    print(args.json_out)
    print(args.md_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
