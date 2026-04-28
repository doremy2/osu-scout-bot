"""Build the historical tournament source index.

This script discovers tournament source pages without importing match rows.
It is intentionally conservative: index first, rank data quality, generate
manual-review reports, then import verified packages later.

Examples:
    python -m scripts.index_historical_tournaments --sources existing --years 2025 2024
    python -m scripts.index_historical_tournaments --sources wiki forum --forum-pages 2 --dry-run
    python -m scripts.index_historical_tournaments --sources existing wiki --write
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.historical_tournament_index import (  # noqa: E402
    DiscoveryConfig,
    dedupe_sources,
    discover_from_forum55,
    discover_from_wiki,
    rows_from_discovered_tournaments,
    utc_now_iso,
)
from storage import (  # noqa: E402
    build_tournament_sources_review_report,
    fetch_discovered_tournaments,
    fetch_tournament_sources,
    init_db,
    upsert_tournament_sources,
)

REPORTS_DIR = PROJECT_ROOT / "data" / "reports" / "tournament_sources"


def parse_years(values: list[str] | None) -> tuple[int, ...]:
    if not values:
        return (2025, 2024, 2023, 2022, 2021, 2020)
    years = tuple(int(value) for value in values)
    invalid = [year for year in years if year < 2020 or year > 2026]
    if invalid:
        raise SystemExit(f"Years must be between 2020 and 2026: {invalid}")
    return years


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "tournament_name",
        "year",
        "data_quality",
        "status",
        "source",
        "source_url",
        "forum_url",
        "wiki_url",
        "spreadsheet_url",
        "bracket_url",
        "discord_url",
        "rank_range",
        "team_size",
        "format",
        "notes",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in fields})


def canonical_event_name(value: str) -> str:
    cleaned = value.casefold()
    cleaned = cleaned.replace("osu!", "osu")
    cleaned = cleaned.replace("o!std", "")
    cleaned = cleaned.replace("[std]", "")
    cleaned = cleaned.replace("[std", "")
    cleaned = cleaned.replace("]", " ")
    cleaned = cleaned.replace("|", " ")
    cleaned = cleaned.replace(":", " ")
    cleaned = " ".join(part for part in cleaned.split() if part not in {"regs", "open", "registration", "registrations"})
    return "".join(ch for ch in cleaned if ch.isalnum())


def merge_with_existing_sources(rows: list[dict]) -> list[dict]:
    existing = fetch_tournament_sources(limit=10000)
    existing_by_year: dict[int, list[dict]] = {}
    for row in existing:
        existing_by_year.setdefault(int(row["year"]), []).append(row)

    merged: list[dict] = []
    for row in rows:
        candidates = existing_by_year.get(int(row["year"]), [])
        row_name = canonical_event_name(row["tournament_name"])
        linked = None
        for candidate in candidates:
            candidate_name = canonical_event_name(candidate["tournament_name"])
            if not candidate_name:
                continue
            if row_name == candidate_name or row_name.startswith(candidate_name) or candidate_name.startswith(row_name):
                linked = candidate
                break
        if linked and linked.get("source") == "osu_wiki":
            row = dict(row)
            row["tournament_key"] = linked["tournament_key"]
            row["linked_source_key"] = linked["tournament_key"]
            row["wiki_url"] = row.get("wiki_url") or linked.get("wiki_url")
            row["source"] = "forum_55+osu_wiki"
            metadata = dict(row.get("metadata_json") or {})
            metadata["merged_with_existing_source"] = linked["tournament_key"]
            metadata["merged_existing_name"] = linked["tournament_name"]
            row["metadata_json"] = metadata
        merged.append(row)
    return merged


def discover(args: argparse.Namespace, years: tuple[int, ...]) -> list[dict]:
    rows: list[dict] = []
    source_names = set(args.sources)

    if "existing" in source_names:
        existing = fetch_discovered_tournaments(limit=args.existing_limit)
        rows.extend(rows_from_discovered_tournaments(existing, years=years))

    if "wiki" in source_names:
        try:
            rows.extend(
                discover_from_wiki(
                    years=years,
                    enrich_pages=args.enrich_wiki_pages,
                    request_delay=args.request_delay,
                )
            )
        except (HTTPError, URLError, OSError) as exc:
            print(f"! Wiki discovery failed: {exc}")

    if "forum" in source_names:
        try:
            rows.extend(
                discover_from_forum55(
                    config=DiscoveryConfig(
                        years=years,
                        request_delay=args.request_delay,
                        forum_pages=args.forum_pages,
                        forum_start_page=args.forum_start_page,
                        enrich_forum_threads=not args.no_enrich,
                    )
                )
            )
        except (HTTPError, URLError, OSError) as exc:
            print(f"! Forum discovery failed: {exc}")

    return merge_with_existing_sources(dedupe_sources(rows))


def main() -> int:
    parser = argparse.ArgumentParser(description="Index historical osu! tournament sources")
    parser.add_argument(
        "--years",
        nargs="*",
        help="Years to index. Default: 2025 2024 2023 2022 2021 2020",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=("existing", "wiki", "forum"),
        default=("existing",),
        help="Discovery sources. Use existing for local discovered_tournaments only.",
    )
    parser.add_argument("--forum-pages", type=int, default=3, help="Forum listing pages to crawl")
    parser.add_argument("--forum-start-page", type=int, default=1, help="First Forum 55 listing page to crawl")
    parser.add_argument("--existing-limit", type=int, default=5000, help="Existing discovered_tournaments limit")
    parser.add_argument("--request-delay", type=float, default=1.0, help="Delay between network requests")
    parser.add_argument("--no-enrich", action="store_true", help="Do not visit forum thread detail pages")
    parser.add_argument("--enrich-wiki-pages", action="store_true", help="Visit each discovered wiki page to extract linked sources")
    parser.add_argument("--dry-run", action="store_true", help="Do not write discovered rows to SQLite")
    parser.add_argument("--write", action="store_true", help="Write discovered rows to SQLite")
    parser.add_argument("--report-only", action="store_true", help="Only generate reports from existing DB rows")
    parser.add_argument("--output-dir", type=Path, default=REPORTS_DIR)
    args = parser.parse_args()

    years = parse_years(args.years)
    init_db()
    batch = utc_now_iso().replace(":", "").replace("+", "Z")
    output_dir = args.output_dir / batch
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.report_only:
        rows = []
    else:
        rows = discover(args, years)
        write_json(output_dir / "discovered_tournament_sources.json", rows)
        write_csv(output_dir / "discovered_tournament_sources.csv", rows)

        if args.write and not args.dry_run:
            saved = upsert_tournament_sources(rows)
            print(f"Saved {saved}/{len(rows)} tournament source rows.")
        else:
            print(f"[dry-run] Discovered {len(rows)} tournament source rows. Use --write to upsert.")

    reports: dict[str, object] = {
        "generated_at": utc_now_iso(),
        "years": years,
        "discovered_this_run": len(rows),
        "by_year": {},
    }
    for year in years:
        report = build_tournament_sources_review_report(year=year)
        reports["by_year"][str(year)] = report
        write_json(output_dir / f"manual_review_{year}.json", report)

    all_rows = fetch_tournament_sources(limit=10000)
    reports["indexed_total"] = len(all_rows)
    write_json(output_dir / "summary.json", reports)
    write_csv(output_dir / "indexed_tournament_sources.csv", all_rows)

    print(f"Reports written to {output_dir}")
    for year in years:
        report = reports["by_year"][str(year)]
        print(
            f"{year}: total={report['total']} review={report['manual_review_count']} "
            f"quality={report['quality_counts']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
