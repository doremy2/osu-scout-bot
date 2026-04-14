"""Bulk-ingest a full OWC tournament directory into SQLite.

Walks a folder of OWC stats CSV exports and ingests every recognized
sheet type for every round it finds:

    Player Leaderboards   -> matches table         (map-level rows)
    Performance Scores    -> player_scores table   (real pscore values)
    Team Statistics       -> tournament_matches    (BO9/BO11/BO13 rows)

Mappool Statistics CSVs are skipped here because they don't contain
beatmap_id / star_rating in the export. Use the manual_map_metadata
importer for that, run via scripts/import_manual_metadata.py.

Usage:
    python scripts/import_owc_full.py data/raw/owc_2025
    python scripts/import_owc_full.py data/raw/owc_2025 --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.owc_csv import (
    parse_owc_player_leaderboard_csv,
    parse_owc_performance_scores_csv,
    parse_owc_team_statistics_csv,
)
from storage import (
    insert_matches,
    insert_player_scores,
    insert_tournament_matches,
)


def _classify(path: Path) -> str | None:
    name = path.name.lower()
    if "player leaderboards" in name:
        return "leaderboard"
    if "performance scores" in name:
        return "pscore"
    if "team statistics" in name:
        return "team_stats"
    return None  # mappool statistics, team hiscores, etc. are skipped here


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest a full OWC tournament directory.")
    parser.add_argument("directory", type=Path, help="Folder containing OWC CSV exports.")
    parser.add_argument("--dry-run", action="store_true", help="Parse but do not write to SQLite.")
    args = parser.parse_args()

    directory: Path = args.directory
    if not directory.is_dir():
        print(f"ERROR: not a directory: {directory}", file=sys.stderr)
        return 2

    csv_files = sorted(directory.glob("*.csv"))
    if not csv_files:
        print(f"No CSVs found in {directory}", file=sys.stderr)
        return 1

    totals = {
        "lb_files": 0, "lb_rows": 0,
        "ps_files": 0, "ps_rows": 0,
        "ts_files": 0, "ts_rows": 0,
        "skipped": 0,
    }

    for path in csv_files:
        kind = _classify(path)
        if kind is None:
            totals["skipped"] += 1
            print(f"  - skip      {path.name}")
            continue

        try:
            if kind == "leaderboard":
                rows = parse_owc_player_leaderboard_csv(path)
                totals["lb_files"] += 1
                totals["lb_rows"] += len(rows)
                if not args.dry_run:
                    insert_matches(rows, source_file=str(path), source_type="csv")
                print(f"  + lb        {path.name}  ({len(rows)} rows)")

            elif kind == "pscore":
                rows = parse_owc_performance_scores_csv(path)
                totals["ps_files"] += 1
                totals["ps_rows"] += len(rows)
                if not args.dry_run:
                    insert_player_scores(rows)
                print(f"  + pscore    {path.name}  ({len(rows)} players)")

            elif kind == "team_stats":
                rows = parse_owc_team_statistics_csv(path)
                totals["ts_files"] += 1
                totals["ts_rows"] += len(rows)
                if not args.dry_run:
                    insert_tournament_matches(rows)
                print(f"  + team      {path.name}  ({len(rows)} matches)")

        except Exception as exc:
            print(f"  ! ERROR     {path.name}: {exc}", file=sys.stderr)

    print()
    print("Summary:")
    print(f"  player leaderboards : {totals['lb_files']} files, {totals['lb_rows']} map rows")
    print(f"  performance scores  : {totals['ps_files']} files, {totals['ps_rows']} player rows")
    print(f"  team statistics     : {totals['ts_files']} files, {totals['ts_rows']} match rows")
    print(f"  skipped             : {totals['skipped']} files")
    if args.dry_run:
        print("  (dry-run: nothing was written to SQLite)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
