"""Apply all three manual OWC metadata layers in one pass.

Runs, in order:
    1. teams.csv    -> teams table (team_code -> team_name)
    2. maps.csv     -> matches.beatmap_id / star_rating / difficulty_name
    3. matches.csv  -> tournament_matches opponent_team / match_link

Usage:
    python scripts/import_owc_metadata.py
    python scripts/import_owc_metadata.py data/metadata/owc_2025
    python scripts/import_owc_metadata.py data/metadata/owc_2025 --dry-run
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
from importers.manual_match_metadata import (
    apply_manual_matches,
    parse_manual_matches_csv,
)
from importers.manual_team_metadata import (
    apply_manual_teams,
    parse_manual_teams_csv,
)

DEFAULT_DIR = PROJECT_ROOT / "data" / "metadata" / "owc_2025"


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply manual OWC metadata layers.")
    parser.add_argument(
        "directory",
        nargs="?",
        type=Path,
        default=DEFAULT_DIR,
        help="Folder containing teams.csv / maps.csv / matches.csv.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Parse only; do not write.")
    args = parser.parse_args()

    directory: Path = args.directory
    if not directory.is_dir():
        print(f"ERROR: not a directory: {directory}", file=sys.stderr)
        return 2

    teams_csv = directory / "teams.csv"
    maps_csv = directory / "maps.csv"
    matches_csv = directory / "matches.csv"

    # ---- teams ----
    if teams_csv.is_file():
        team_rows = parse_manual_teams_csv(teams_csv)
        print(f"Parsed {len(team_rows)} team rows from {teams_csv.name}")
        if not args.dry_run:
            stats = apply_manual_teams(team_rows)
            print(f"  teams written: {stats['teams_written']}")
    else:
        print(f"(skip) no {teams_csv.name}")

    # ---- maps ----
    if maps_csv.is_file():
        map_rows = parse_manual_map_metadata_csv(maps_csv)
        print(f"Parsed {len(map_rows)} map rows from {maps_csv.name}")
        if not args.dry_run:
            stats = apply_manual_map_metadata(map_rows)
            print(
                f"  mappings seen : {stats['mappings_seen']}  "
                f"rows updated : {stats['rows_updated']}  "
                f"missed : {stats['mappings_missed']}"
            )
    else:
        print(f"(skip) no {maps_csv.name}")

    # ---- matches ----
    if matches_csv.is_file():
        match_rows = parse_manual_matches_csv(matches_csv)
        print(f"Parsed {len(match_rows)} match rows from {matches_csv.name}")
        if not args.dry_run:
            stats = apply_manual_matches(match_rows)
            print(
                f"  matches seen  : {stats['matches_seen']}  "
                f"rows updated  : {stats['rows_updated']}  "
                f"missed : {stats['rows_missed']}"
            )
    else:
        print(f"(skip) no {matches_csv.name}")

    if args.dry_run:
        print()
        print("(dry-run: nothing was written to SQLite)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
