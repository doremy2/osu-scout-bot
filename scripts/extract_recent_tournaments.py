from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.osu_api import OsuApiClient
from importers.tournament_package import apply_tournament_package, parse_tournament_package
from importers.wiki_tournament_package import (
    RECENT_TOURNAMENT_CONFIGS,
    build_wiki_tournament_package,
    render_validation_report_markdown,
    write_package_and_report,
)


DEFAULT_PACKAGE_DIR = PROJECT_ROOT / "data" / "packages"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "reports" / "tournament_packages"


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract recent non-OWC tournaments into package JSON.")
    parser.add_argument(
        "--only",
        nargs="*",
        default=sorted(RECENT_TOURNAMENT_CONFIGS.keys()),
        help=f"Configs to extract. Choices: {', '.join(sorted(RECENT_TOURNAMENT_CONFIGS.keys()))}",
    )
    parser.add_argument("--package-dir", type=Path, default=DEFAULT_PACKAGE_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--import-db", action="store_true", help="Import production-safe packages into SQLite after extraction.")
    parser.add_argument(
        "--allow-non-production",
        action="store_true",
        help="Allow importing packages marked partial/sample.",
    )
    args = parser.parse_args()

    unknown = [name for name in args.only if name not in RECENT_TOURNAMENT_CONFIGS]
    if unknown:
        print(f"Unknown configs: {', '.join(unknown)}")
        return 1

    client = OsuApiClient.from_env()
    combined_reports = []
    extracted = []
    imported = []
    try:
        for name in args.only:
            config = RECENT_TOURNAMENT_CONFIGS[name]
            package, report = build_wiki_tournament_package(config, client=client)
            package_path, report_json_path, report_md_path = write_package_and_report(
                package=package,
                report=report,
                package_dir=args.package_dir,
                report_dir=args.report_dir,
            )
            combined_reports.append(report)
            extracted.append(
                {
                    "config": name,
                    "package_path": str(package_path),
                    "report_json": str(report_json_path),
                    "report_md": str(report_md_path),
                    "package_status": package.get("package_status"),
                    "production_safe": package.get("production_safe"),
                }
            )

            if args.import_db:
                parsed = parse_tournament_package(package_path)
                stats = apply_tournament_package(
                    parsed,
                    allow_non_production=args.allow_non_production,
                )
                imported.append(
                    {
                        "config": name,
                        "package_path": str(package_path),
                        "stats": stats,
                    }
                )
    finally:
        client.close()

    combined_path = args.report_dir / "recent_tournaments_validation.md"
    combined_path.parent.mkdir(parents=True, exist_ok=True)
    combined_path.write_text(render_validation_report_markdown(combined_reports), encoding="utf-8")

    print(
        json.dumps(
            {
                "extracted": extracted,
                "imported": imported,
                "combined_report": str(combined_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
