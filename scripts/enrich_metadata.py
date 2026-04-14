"""Enrich SQLite match rows with beatmap metadata from the osu! API v2.

Usage:
    python scripts/enrich_metadata.py            # do it for real
    python scripts/enrich_metadata.py --dry-run  # show what would change
    python scripts/enrich_metadata.py --limit 5  # only first 5 unique maps

Requires OSU_CLIENT_ID and OSU_CLIENT_SECRET in your .env.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running as `python scripts/enrich_metadata.py` from project root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.osu_api import OsuApiClient, OsuApiError, enrich_database


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich match rows via osu! API v2.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve but do not write back.")
    parser.add_argument("--limit", type=int, default=None, help="Cap number of unique map groups.")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-map output.")
    args = parser.parse_args()

    try:
        client = OsuApiClient.from_env()
    except OsuApiError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print("Starting osu! API enrichment...")
    if args.dry_run:
        print("  (dry-run mode: no rows will be updated)")

    try:
        stats = enrich_database(
            client,
            dry_run=args.dry_run,
            limit=args.limit,
            verbose=not args.quiet,
        )
    finally:
        client.close()

    print()
    print("Done.")
    print(f"  unique map groups seen   : {stats['groups_seen']}")
    print(f"  resolved via osu! API    : {stats['groups_resolved']}")
    print(f"  no confident match       : {stats['groups_missed']}")
    print(f"  rows updated             : {stats['rows_updated']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
