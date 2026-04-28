"""Index o!TR Stage tournaments as discovery/enrichment sources.

This script does not import tournament match rows. It only discovers Stage
tournament metadata, cross-references any already-indexed forum/wiki sources,
and writes review/import-queue artifacts for manual approval.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.stage_index import (  # noqa: E402
    STAGE_TOURNAMENTS_URL,
    build_stage_import_queue,
    load_stage_source,
    stage_tournaments_to_source_rows,
    utc_now_iso,
)
from storage import fetch_tournament_sources, upsert_tournament_sources  # noqa: E402


DEFAULT_DISCOVERY_JSON = PROJECT_ROOT / "data" / "reports" / "stage_tournament_discovery.json"
DEFAULT_DISCOVERY_MD = PROJECT_ROOT / "data" / "reports" / "stage_tournament_discovery.md"
DEFAULT_QUEUE_JSON = PROJECT_ROOT / "data" / "import_queue_stage.json"
DEFAULT_QUEUE_MD = PROJECT_ROOT / "data" / "reports" / "import_queue_stage.md"
DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "stage"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_discovery_markdown(path: Path, payload: dict) -> None:
    rows = payload["rows"]
    lines = [
        "# Stage Tournament Discovery",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "This is a discovery/enrichment report only. Stage is not treated as the sole source of truth.",
        "",
        "## Summary",
        "",
        f"- Total Stage rows: {payload['total']}",
        f"- Years: `{payload['years']}`",
        f"- Classification counts: `{payload['classification_counts']}`",
        f"- Quality counts: `{payload['quality_counts']}`",
        f"- Mode counts: `{payload['mode_counts']}`",
        f"- Fetch warning: `{payload.get('fetch_warning') or 'none'}`",
        "",
        "## Candidate Ranking",
        "",
        "| # | Class | Mode | Tournament | Dates | Format | Players | Matches | Verified | Cross-ref |",
        "|---:|---|---|---|---|---|---:|---:|---:|---|",
    ]
    for index, row in enumerate(rows[:100], start=1):
        cross_ref = []
        if row.get("forum_url"):
            cross_ref.append("forum")
        if row.get("wiki_url"):
            cross_ref.append("wiki")
        if row.get("spreadsheet_url"):
            cross_ref.append("sheet")
        if row.get("bracket_url"):
            cross_ref.append("bracket")
        verified = row.get("verified_ratio")
        lines.append(
            "| {index} | {classification} | {mode} | [{name}]({url}) | {dates} | {fmt} | {players} | {matches} | {verified} | {cross_ref} |".format(
                index=index,
                classification=row.get("classification") or "",
                mode=row.get("game_mode") or "",
                name=str(row.get("tournament_name") or "").replace("|", "\\|"),
                url=row.get("stage_url") or row.get("source_url"),
                dates=f"{row.get('start_date') or '?'} to {row.get('end_date') or '?'}",
                fmt=row.get("format") or "?",
                players=row.get("player_count") or 0,
                matches=row.get("match_count") or 0,
                verified=f"{float(verified) * 100:.1f}%" if verified is not None else "?",
                cross_ref=", ".join(cross_ref) or "stage-only",
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_queue_markdown(path: Path, payload: dict) -> None:
    lines = [
        "# Stage Import Queue",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "Manual approval required. Import 2-5 tournaments per batch only after validation.",
        "",
        "| # | Class | Tournament | Dates | Matches | Verified | External sources |",
        "|---:|---|---|---|---:|---:|---|",
    ]
    for item in payload["items"]:
        sources = []
        if item.get("forum_url"):
            sources.append("forum")
        if item.get("wiki_url"):
            sources.append("wiki")
        if item.get("spreadsheet_url"):
            sources.append("sheet")
        if item.get("bracket_url"):
            sources.append("bracket")
        verified = item.get("verified_ratio")
        lines.append(
            "| {rank} | {classification} | [{name}]({url}) | {dates} | {matches} | {verified} | {sources} |".format(
                rank=item["queue_rank"],
                classification=item.get("classification") or "",
                name=str(item.get("tournament_name") or "").replace("|", "\\|"),
                url=item.get("stage_url"),
                dates=f"{item.get('start_date') or '?'} to {item.get('end_date') or '?'}",
                matches=item.get("match_count") or 0,
                verified=f"{float(verified) * 100:.1f}%" if verified is not None else "?",
                sources=", ".join(sources) or "stage-only",
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_years(values: list[str] | None) -> set[int] | None:
    if not values:
        return None
    return {int(value) for value in values}


def newest_cache_file(cache_dir: Path) -> Path | None:
    if not cache_dir.exists():
        return None
    candidates = [
        path for path in cache_dir.iterdir()
        if path.is_file() and path.suffix.casefold() in {".json", ".html", ".htm", ".har"}
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def main() -> int:
    parser = argparse.ArgumentParser(description="Index o!TR Stage tournament sources.")
    parser.add_argument("--url", default=STAGE_TOURNAMENTS_URL)
    parser.add_argument("--cache-path", type=Path, default=None, help="Cached Stage HTML/JSON export to parse instead of live fetch.")
    parser.add_argument("--cache-dir", type=Path, default=None, help="Directory containing Stage exports; newest .json/.html/.har is used.")
    parser.add_argument("--years", nargs="*", help="Optional years to keep, e.g. --years 2026 2025 2024")
    parser.add_argument("--limit", type=int, default=50, help="Import queue limit.")
    parser.add_argument("--write", action="store_true", help="Upsert Stage discovery rows into tournament_sources.")
    parser.add_argument("--discovery-json", type=Path, default=DEFAULT_DISCOVERY_JSON)
    parser.add_argument("--discovery-md", type=Path, default=DEFAULT_DISCOVERY_MD)
    parser.add_argument("--queue-json", type=Path, default=DEFAULT_QUEUE_JSON)
    parser.add_argument("--queue-md", type=Path, default=DEFAULT_QUEUE_MD)
    args = parser.parse_args()

    cache_path = args.cache_path
    if cache_path is None and args.cache_dir is not None:
        cache_path = newest_cache_file(args.cache_dir)
        if cache_path is None:
            raise SystemExit(f"No Stage export found in {args.cache_dir}")

    fetch_warning = None
    try:
        tournaments, source_meta = load_stage_source(cache_path=cache_path, url=args.url)
    except (HTTPError, URLError, OSError, json.JSONDecodeError, ValueError) as exc:
        tournaments = []
        source_meta = {"source_url": args.url, "cache_path": str(cache_path) if cache_path else None}
        fetch_warning = f"{type(exc).__name__}: {exc}"

    keep_years = parse_years(args.years)
    if keep_years is not None:
        tournaments = [tournament for tournament in tournaments if tournament.year in keep_years]

    existing_sources = []
    for year in sorted({tournament.year for tournament in tournaments}, reverse=True):
        try:
            existing_sources.extend(fetch_tournament_sources(year=year, limit=20_000))
        except Exception:
            # Existing DB can be unavailable during corruption recovery; Stage
            # discovery should still produce a standalone report.
            pass

    rows = stage_tournaments_to_source_rows(tournaments, existing_sources=existing_sources)
    rows.sort(
        key=lambda row: (
            {"production_safe": 0, "likely_importable": 1, "stage_only": 2, "partial": 3, "ignore": 4}.get(
                str(row.get("classification")), 9
            ),
            -float(row.get("verified_ratio") or 0.0),
            -int(row.get("match_count") or 0),
            -(int(row.get("year") or 0)),
            str(row.get("tournament_name") or "").casefold(),
        )
    )

    if args.write and rows:
        upsert_tournament_sources(rows)

    generated_at = utc_now_iso()
    discovery_payload = {
        "generated_at": generated_at,
        "source": "stage",
        "source_meta": source_meta,
        "fetch_warning": fetch_warning,
        "years": sorted({row["year"] for row in rows}, reverse=True),
        "total": len(rows),
        "classification_counts": dict(Counter(row.get("classification") or "unknown" for row in rows)),
        "quality_counts": dict(Counter(row.get("data_quality") or "unknown" for row in rows)),
        "mode_counts": dict(Counter(row.get("game_mode") or "unknown" for row in rows)),
        "rows": rows,
    }
    queue = build_stage_import_queue(rows, limit=args.limit)
    queue_payload = {
        "generated_at": generated_at,
        "source": "stage",
        "status": "manual_approval_required",
        "items": queue,
        "rules": {
            "production_safe": "Stage has high verification and external forum/wiki/sheet/bracket corroboration.",
            "likely_importable": "Stage has useful verified match data plus at least one external corroborating source.",
            "stage_only": "Stage has match data but no external source yet; review before import.",
        },
    }

    write_json(args.discovery_json, discovery_payload)
    write_discovery_markdown(args.discovery_md, discovery_payload)
    write_json(args.queue_json, queue_payload)
    write_queue_markdown(args.queue_md, queue_payload)

    print(f"Stage tournaments discovered: {len(rows)}")
    if fetch_warning:
        print(f"Fetch warning: {fetch_warning}")
    print(args.discovery_json)
    print(args.discovery_md)
    print(args.queue_json)
    print(args.queue_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
