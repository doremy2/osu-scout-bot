"""Ingest cached match JSON files into the v2 normalized tables.

Walks data/cache/owc_2025/matches/*.json (and any future cache dirs)
and feeds each one through database.ingest_match_json().

This is the v2 equivalent of sync_owc_2025.py's layer 3.5 — but writes
to the new normalized tables (v2_matches, v2_games, v2_scores, players)
instead of the legacy match_games/match_scores tables.

Usage:
    python -m scripts.ingest_match_history
    python -m scripts.ingest_match_history --dry-run
    python -m scripts.ingest_match_history --cache-dir data/cache/owc_2025/matches
    python -m scripts.ingest_match_history --event "OWC 2025"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database import init_v2_db, ingest_match_json, upsert_tournament  # noqa: E402


DEFAULT_CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "owc_2025" / "matches"


def _load_bracket_stages(bracket_path: Path | None = None) -> dict[int, str]:
    """Build match_id -> stage mapping from bracket.json if available."""
    if bracket_path is None:
        bracket_path = PROJECT_ROOT / "data" / "cache" / "owc_2025" / "bracket.json"
    if not bracket_path.exists():
        return {}
    try:
        data = json.loads(bracket_path.read_text(encoding="utf-8"))
        return {
            m["match_id"]: m["stage"]
            for m in (data.get("matches") or [])
            if m.get("match_id") and m.get("stage")
        }
    except Exception:
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest cached match JSON into v2 tables.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--event", type=str, default="OWC 2025")
    parser.add_argument("--tournament-id", type=str, default="owc-2025")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if not args.cache_dir.exists():
        print(f"Cache dir not found: {args.cache_dir}")
        print("Run 'python -m scripts.sync_owc_2025' first to populate the cache.")
        return 1

    init_v2_db()

    # Register the tournament
    if not args.dry_run:
        upsert_tournament(
            args.tournament_id,
            name=args.event,
            abbreviation="OWC",
            year=2025,
            format="4v4",
            team_size=8,
            tier="premier",
            source="osu_wiki",
            source_url="https://osu.ppy.sh/wiki/en/Tournaments/OWC/2025",
        )

    stage_map = _load_bracket_stages()
    json_files = sorted(args.cache_dir.glob("*.json"))
    print(f"Found {len(json_files)} cached match files in {args.cache_dir}")

    stats = {"files": 0, "games": 0, "scores": 0, "failed": 0}
    for i, json_file in enumerate(json_files):
        if args.limit and i >= args.limit:
            break
        stats["files"] += 1

        try:
            mid = int(json_file.stem)
        except ValueError:
            mid = None

        stage = stage_map.get(mid) if mid is not None else None

        if args.dry_run:
            print(f"  [dry-run] {json_file.name} stage={stage}")
            continue

        try:
            payload = json.loads(json_file.read_text(encoding="utf-8"))
            result = ingest_match_json(payload, event=args.event, stage=stage)
            stats["games"] += result.get("games", 0)
            stats["scores"] += result.get("scores", 0)
        except Exception as exc:
            print(f"  ! {json_file.name}: {exc}")
            stats["failed"] += 1

    print(f"\nDone: {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
