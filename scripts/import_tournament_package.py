from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.tournament_package import (
    apply_tournament_package,
    parse_tournament_package,
    summarize_tournament_package,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Import a reusable tournament package JSON file.")
    parser.add_argument("package", type=Path, help="Path to the package JSON file.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and summarize without writing to SQLite.")
    parser.add_argument(
        "--allow-non-production",
        action="store_true",
        help="Allow importing packages marked sample/partial/non-production-safe.",
    )
    args = parser.parse_args()

    package = parse_tournament_package(args.package)
    summary = summarize_tournament_package(package)

    print(json.dumps(summary, indent=2))
    if args.dry_run:
        return 0

    stats = apply_tournament_package(
        package,
        allow_non_production=args.allow_non_production,
    )
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
