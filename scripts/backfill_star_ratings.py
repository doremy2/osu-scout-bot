"""Backfill star_rating and beatmap_id into the legacy matches table.

Reads cached match JSON files to build a title->star_rating lookup,
then updates all matching rows in the matches table.

Usage:
    python -m scripts.backfill_star_ratings
    python -m scripts.backfill_star_ratings --dry-run
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "owc_2025" / "matches"
DB_PATH = PROJECT_ROOT / "data" / "osu_scout.db"


def build_lookup(cache_dir: Path) -> dict[str, dict]:
    """Build title (lowercase) -> {beatmap_id, star_rating} from cached match JSONs."""
    lookup: dict[str, dict] = {}
    for f in sorted(cache_dir.glob("*.json")):
        data = json.loads(f.read_text(encoding="utf-8"))
        for g in data.get("games", []):
            bid = g.get("beatmap_id")
            sr = g.get("star_rating")
            title = (g.get("beatmap_title") or "").strip()
            if not bid or not sr or not title:
                continue
            lookup[title.lower()] = {
                "beatmap_id": bid,
                "star_rating": round(sr, 2),
            }
    return lookup


def backfill(db_path: Path, lookup: dict[str, dict], *, dry_run: bool = False) -> dict[str, int]:
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row

    # Get all distinct map_names
    rows = db.execute(
        "SELECT DISTINCT map_name FROM matches WHERE map_name IS NOT NULL"
    ).fetchall()

    stats = {"matched": 0, "unmatched": 0, "rows_updated": 0}
    updates: list[tuple] = []

    for row in rows:
        mn = row["map_name"] or ""
        # Legacy format: "Artist - Title"
        parts = mn.split(" - ", 1)
        title_part = parts[-1].strip().lower() if len(parts) > 1 else mn.strip().lower()

        match = lookup.get(title_part)
        if not match:
            match = lookup.get(mn.strip().lower())
        if match:
            stats["matched"] += 1
            updates.append((match["star_rating"], match["beatmap_id"], mn))
        else:
            stats["unmatched"] += 1

    if dry_run:
        print(f"[dry-run] Would update {len(updates)} distinct map_names")
        for sr, bid, mn in updates[:10]:
            print(f"  {mn} -> SR={sr}, bid={bid}")
        return stats

    # Batch update
    for sr, bid, mn in updates:
        cur = db.execute(
            "UPDATE matches SET star_rating = ?, beatmap_id = ? WHERE map_name = ?",
            (sr, bid, mn),
        )
        stats["rows_updated"] += cur.rowcount

    db.commit()
    db.close()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill star ratings into matches table")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    args = parser.parse_args()

    if not args.cache_dir.exists():
        print(f"Cache dir not found: {args.cache_dir}")
        return 1

    print(f"Building lookup from {args.cache_dir}...")
    lookup = build_lookup(args.cache_dir)
    print(f"  {len(lookup)} unique titles found")

    print(f"Backfilling {args.db}...")
    stats = backfill(args.db, lookup, dry_run=args.dry_run)
    print(f"Done: {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
