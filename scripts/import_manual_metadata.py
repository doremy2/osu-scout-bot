"""Apply a hand-authored mapping CSV to enrich match rows with beatmap_id
(and optionally star_rating).

See importers/manual_map_metadata.py for the expected CSV format.

Usage:
    python scripts/import_manual_metadata.py data/raw/owc_2025_beatmap_map.csv
    python scripts/import_manual_metadata.py path/to/mapping.csv --dry-run

After running this you can also call:
    python scripts/enrich_metadata.py
to fill in any star ratings still missing, using the osu! API exact-ID
lookup path (much more accurate than name search).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.manual_map_metadata import (
    apply_manual_map_metadata,
    parse_manual_map_metadata_csv,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply manual beatmap mapping CSV.")
    parser.add_argument("csv", type=Path, help="Path to the mapping CSV.")
    parser.add_argument("--dry-run", action="store_true", help="Parse only; do not write.")
    args = parser.parse_args()

    if not args.csv.is_file():
        print(f"ERROR: file not found: {args.csv}", file=sys.stderr)
        return 2

    rows = parse_manual_map_metadata_csv(args.csv)
    print(f"Parsed {len(rows)} mapping rows from {args.csv.name}")

    if args.dry_run:
        for row in rows[:10]:
            print(f"  - {row['stage']!s:>14} {row['slot']:>4}  id={row['beatmap_id']:>9}  {row['map_name']}")
        if len(rows) > 10:
            print(f"  ... ({len(rows) - 10} more)")
        print("(dry-run: nothing was written to SQLite)")
        return 0

    stats = apply_manual_map_metadata(rows)
    print()
    print("Done.")
    print(f"  mappings seen     : {stats['mappings_seen']}")
    print(f"  rows updated      : {stats['rows_updated']}")
    print(f"  mappings with no matching DB rows : {stats['mappings_missed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
