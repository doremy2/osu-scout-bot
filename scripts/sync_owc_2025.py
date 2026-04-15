"""OWC 2025 layered sync — the automated path.

Treats OWC 2025 as a multi-source package and ingests it into the database
in distinct layers, each owned by a single importer. The manual
`data/metadata/owc_2025/*.csv` files are treated as OVERRIDES / FALLBACKS,
not the source of truth.

Layers, in dependency order:

  1. Teams layer              — data/metadata/owc_2025/teams.csv
                                 (later: importers/owc_wiki.py team list)
  2. Bracket / match-list     — importers/owc_wiki.fetch_bracket()
                                 yields (stage, match_link, score_a, score_b)
  3. Match-detail layer       — importers/osu_api.OsuApiClient.get_match()
                                 per match_link -> full per-map + per-player
  4. Mappool layer            — importers/owc_wiki.fetch_bracket().mappool
                                 (stage, slot, beatmap_id, map_name)
                                 enriched by osu_api.get_beatmap() for SR
  5. Derived player perf      — aggregation over layer 3 (later pass,
                                 replaces the current CSV leaderboard import)

  FALLBACK: after layers 1-4, we replay the manual matches.csv /
  maps.csv / teams.csv override files via storage backfill helpers so
  manual corrections still win where the automated layers were wrong or
  missing.

Usage:
    python -m scripts.sync_owc_2025 --dry-run        # fetch + print, no DB writes
    python -m scripts.sync_owc_2025                   # full run
    python -m scripts.sync_owc_2025 --skip-match-detail  # layers 1, 2, 4, fallback only
    python -m scripts.sync_owc_2025 --only-layer 2    # run a single layer

Environment:
    OSU_CLIENT_ID, OSU_CLIENT_SECRET required for layers 3 and 4.

This script is idempotent by design. Re-running should fill any new gaps
without clobbering existing metadata (storage.backfill_* helpers use
COALESCE-style updates).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure the project root is importable when run as a script file.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from importers.osu_api import (  # noqa: E402
    Match,
    OsuApiClient,
    OsuApiError,
    parse_match_id,
)
from importers.owc_wiki import (  # noqa: E402
    OWC_2025,
    TournamentBracket,
    fetch_bracket,
)


METADATA_DIR = PROJECT_ROOT / "data" / "metadata" / "owc_2025"
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "owc_2025"


# ---------- layer 1: teams ----------

def run_layer_1_teams(*, dry_run: bool) -> dict[str, int]:
    """Apply manual teams.csv. The wiki-driven team list is a future
    addition; for now the hand-maintained override file is the source."""
    from importers.manual_team_metadata import (
        apply_manual_teams,
        parse_manual_teams_csv,
    )

    teams_csv = METADATA_DIR / "teams.csv"
    if not teams_csv.exists():
        return {"teams_seen": 0, "teams_applied": 0, "error": 1}

    rows = parse_manual_teams_csv(teams_csv)
    if dry_run:
        return {"teams_seen": len(rows), "teams_applied": 0, "dry_run": 1}
    stats = apply_manual_teams(rows)
    stats["teams_seen"] = len(rows)
    return stats


# ---------- layer 2: bracket / match-list ----------

def run_layer_2_bracket(*, dry_run: bool) -> tuple[TournamentBracket, dict[str, int]]:
    """Pull (stage, match_link, score_a, score_b) tuples from the osu! wiki."""
    bracket = fetch_bracket(OWC_2025)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = CACHE_DIR / "bracket.json"
    payload = {
        "event": bracket.event,
        "matches": [
            {
                "stage": m.stage,
                "match_id": m.match_id,
                "match_link": m.match_link,
                "score_a": m.score_a,
                "score_b": m.score_b,
            }
            for m in bracket.matches
        ],
        "mappool": [
            {
                "stage": m.stage,
                "slot": m.slot,
                "beatmap_id": m.beatmap_id,
                "map_name": m.map_name,
            }
            for m in bracket.mappool
        ],
    }
    if not dry_run:
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Per-stage counts for the summary.
    by_stage: dict[str, int] = {}
    for m in bracket.matches:
        by_stage[m.stage] = by_stage.get(m.stage, 0) + 1

    stats = {
        "bracket_matches": len(bracket.matches),
        "mappool_entries": len(bracket.mappool),
        **{f"stage::{k}": v for k, v in by_stage.items()},
    }
    return bracket, stats


# ---------- layer 3: match-detail ----------

def run_layer_3_match_detail(
    bracket: TournamentBracket,
    *,
    dry_run: bool,
    limit: int | None = None,
) -> dict[str, int]:
    """For each match_link in the bracket, call /matches/{id} and cache
    the per-map per-player result to disk.

    Storage integration is intentionally deferred to a second pass: this
    layer's job is to make every match's canonical data sit on disk in a
    predictable shape so downstream writers can consume it without
    re-hitting the API. Writing this data into the SQLite schema is the
    next change (needs a `matches_games` table + per-player-per-map
    rows), and is small once the JSON exists.
    """
    client = OsuApiClient.from_env()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    match_dir = CACHE_DIR / "matches"
    match_dir.mkdir(parents=True, exist_ok=True)

    stats = {
        "matches_seen": 0,
        "matches_fetched": 0,
        "matches_cached": 0,
        "matches_failed": 0,
        "games_total": 0,
    }

    seen_ids: set[int] = set()
    for bm in bracket.matches:
        mid = bm.match_id
        if mid is None or mid in seen_ids:
            continue
        seen_ids.add(mid)
        stats["matches_seen"] += 1

        if limit is not None and stats["matches_fetched"] >= limit:
            break

        out_file = match_dir / f"{mid}.json"
        if out_file.exists() and not dry_run:
            # Already cached — count as cached but don't re-fetch.
            stats["matches_cached"] += 1
            continue

        try:
            match = client.get_match(mid)
        except OsuApiError as exc:
            print(f"  ! match {mid}: {exc}")
            stats["matches_failed"] += 1
            continue

        if match is None:
            print(f"  ! match {mid}: 404 not found")
            stats["matches_failed"] += 1
            continue

        stats["matches_fetched"] += 1
        stats["games_total"] += len(match.games)

        if not dry_run:
            out_file.write_text(
                json.dumps(_match_to_dict(match), indent=2), encoding="utf-8"
            )

    client.close()
    return stats


def _match_to_dict(match: Match) -> dict:
    return {
        "match_id": match.match_id,
        "name": match.name,
        "start_time": match.start_time,
        "end_time": match.end_time,
        "red_score": match.red_score,
        "blue_score": match.blue_score,
        "users": match.users,
        "games": [
            {
                "game_id": g.game_id,
                "beatmap_id": g.beatmap_id,
                "beatmap_title": g.beatmap_title,
                "beatmap_version": g.beatmap_version,
                "star_rating": g.star_rating,
                "mode": g.mode,
                "scoring_type": g.scoring_type,
                "team_type": g.team_type,
                "mods": g.mods,
                "start_time": g.start_time,
                "end_time": g.end_time,
                "winning_team": g.winning_team,
                "red_total": g.red_total,
                "blue_total": g.blue_total,
                "scores": [
                    {
                        "user_id": s.user_id,
                        "username": match.users.get(s.user_id),
                        "score": s.score,
                        "accuracy": s.accuracy,
                        "max_combo": s.max_combo,
                        "mods": s.mods,
                        "team": s.team,
                        "passed": s.passed,
                        "slot": s.slot,
                    }
                    for s in g.scores
                ],
            }
            for g in match.games
        ],
    }


# ---------- layer 4: mappool ----------

def run_layer_4_mappool(
    bracket: TournamentBracket,
    *,
    dry_run: bool,
) -> dict[str, int]:
    """Apply mappool entries from the wiki + enrich SR via osu! API.

    Falls back to the manual maps.csv afterwards so hand-corrected rows
    win. This layer is safe to run even if match-detail failed.
    """
    stats = {
        "wiki_entries": len(bracket.mappool),
        "enriched": 0,
        "manual_applied": 0,
    }

    # Wiki-layer apply:
    # The wiki mappool parser yields (stage, slot, beatmap_id, map_name)
    # but the current storage schema stores SR per (event, stage, slot,
    # map_name) via update_enrichment_for_map. Once beatmap_id is known,
    # we can call osu_api.get_beatmap(id) to pull a canonical SR and
    # push it to storage.
    if not dry_run and bracket.mappool:
        try:
            from storage import update_enrichment_for_map
            client = OsuApiClient.from_env()
            for entry in bracket.mappool:
                if entry.beatmap_id is None:
                    continue
                try:
                    bm = client.get_beatmap(entry.beatmap_id)
                except OsuApiError as exc:
                    print(f"  ! beatmap {entry.beatmap_id}: {exc}")
                    continue
                if bm is None:
                    continue
                updated = update_enrichment_for_map(
                    event=OWC_2025.event,
                    stage=entry.stage,
                    slot=entry.slot,
                    map_name=entry.map_name or bm.title,
                    beatmap_id=bm.beatmap_id,
                    star_rating=bm.star_rating,
                )
                if updated:
                    stats["enriched"] += updated
            client.close()
        except ImportError:
            pass

    # Manual override fallback:
    try:
        from importers.manual_map_metadata import (
            apply_manual_maps,
            parse_manual_maps_csv,
        )
        maps_csv = METADATA_DIR / "maps.csv"
        if maps_csv.exists():
            rows = parse_manual_maps_csv(maps_csv)
            if not dry_run:
                applied = apply_manual_maps(rows)
                stats["manual_applied"] = applied.get("rows_updated", 0)
            else:
                stats["manual_applied"] = len(rows)
    except ImportError:
        pass

    return stats


# ---------- fallback: manual matches.csv ----------

def run_fallback_manual_matches(*, dry_run: bool) -> dict[str, int]:
    """Replay the manual matches.csv as a fallback override.

    Runs AFTER the automated layers so manual corrections win on fields
    where both sources disagree (via storage COALESCE semantics, where
    both sides null out one will fill — and our storage backfill
    prefers non-null existing values when update targets are null).
    """
    from importers.manual_match_metadata import (
        apply_manual_matches,
        parse_manual_matches_csv,
    )

    matches_csv = METADATA_DIR / "matches.csv"
    if not matches_csv.exists():
        return {"rows_seen": 0, "rows_updated": 0, "rows_missed": 0}

    rows = parse_manual_matches_csv(matches_csv)
    if dry_run:
        return {"rows_seen": len(rows), "rows_updated": 0, "dry_run": 1}

    stats = apply_manual_matches(rows)
    stats["rows_seen"] = len(rows)
    return stats


# ---------- driver ----------

LAYER_CHOICES = ("1", "2", "3", "4", "fallback", "all")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="OWC 2025 layered tournament sync."
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--only-layer",
        choices=LAYER_CHOICES,
        default="all",
        help="Run a single layer (default: all)",
    )
    parser.add_argument(
        "--skip-match-detail",
        action="store_true",
        help="Skip layer 3 (saves osu! API quota and time)",
    )
    parser.add_argument(
        "--match-limit",
        type=int,
        default=None,
        help="Max number of matches to fetch in layer 3 (for testing)",
    )
    args = parser.parse_args()

    def want(layer: str) -> bool:
        return args.only_layer in (layer, "all")

    bracket: TournamentBracket | None = None

    if want("1"):
        print("== layer 1: teams ==")
        try:
            stats = run_layer_1_teams(dry_run=args.dry_run)
            print(f"  {stats}")
        except Exception as exc:
            print(f"  ! layer 1 failed: {exc}")

    if want("2"):
        print("== layer 2: bracket (wiki) ==")
        try:
            bracket, stats = run_layer_2_bracket(dry_run=args.dry_run)
            print(f"  {stats}")
        except Exception as exc:
            print(f"  ! layer 2 failed: {exc}")

    if want("3") and not args.skip_match_detail:
        print("== layer 3: match detail (/matches/{id}) ==")
        if bracket is None:
            try:
                bracket, _ = run_layer_2_bracket(dry_run=True)
            except Exception as exc:
                print(f"  ! cannot run layer 3 without bracket: {exc}")
                bracket = None
        if bracket is not None:
            try:
                stats = run_layer_3_match_detail(
                    bracket, dry_run=args.dry_run, limit=args.match_limit
                )
                print(f"  {stats}")
            except Exception as exc:
                print(f"  ! layer 3 failed: {exc}")

    if want("4"):
        print("== layer 4: mappool ==")
        if bracket is None:
            try:
                bracket, _ = run_layer_2_bracket(dry_run=True)
            except Exception as exc:
                print(f"  ! cannot run layer 4 without bracket: {exc}")
                bracket = None
        if bracket is not None:
            try:
                stats = run_layer_4_mappool(bracket, dry_run=args.dry_run)
                print(f"  {stats}")
            except Exception as exc:
                print(f"  ! layer 4 failed: {exc}")

    if want("fallback"):
        print("== fallback: manual matches.csv override ==")
        try:
            stats = run_fallback_manual_matches(dry_run=args.dry_run)
            print(f"  {stats}")
        except Exception as exc:
            print(f"  ! fallback failed: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
