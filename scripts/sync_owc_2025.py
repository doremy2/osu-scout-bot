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
    python -m scripts.sync_owc_2025 --refresh-match-detail  # refetch cached /matches/{id}.json
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
    refresh: bool = False,
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
        if out_file.exists() and not dry_run and not refresh:
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


# ---------- layer 3.5: ingest cached match JSON into DB ----------

def run_layer_3_5_ingest(
    bracket: TournamentBracket | None,
    *,
    dry_run: bool,
) -> dict[str, int]:
    """Walk cached /matches/{id}.json files written by layer 3 and write
    them into match_games + match_scores via storage.ingest_cached_match.

    Stage is looked up from the bracket (match_id -> stage). If no bracket
    is available, rows are ingested without a stage tag.
    """
    from storage import ingest_cached_match

    match_dir = CACHE_DIR / "matches"
    stats = {
        "files_seen": 0,
        "matches_ingested": 0,
        "games_written": 0,
        "scores_written": 0,
        "failed": 0,
    }
    if not match_dir.exists():
        return stats

    stage_by_id: dict[int, str] = {}
    if bracket is not None:
        for bm in bracket.matches:
            if bm.match_id is not None and bm.stage:
                stage_by_id.setdefault(bm.match_id, bm.stage)

    for json_file in sorted(match_dir.glob("*.json")):
        stats["files_seen"] += 1
        try:
            mid = int(json_file.stem)
        except ValueError:
            mid = None
        stage = stage_by_id.get(mid) if mid is not None else None

        if dry_run:
            continue

        try:
            result = ingest_cached_match(
                json_file, event=OWC_2025.event, stage=stage
            )
        except Exception as exc:
            print(f"  ! {json_file.name}: {exc}")
            stats["failed"] += 1
            continue

        stats["matches_ingested"] += 1
        stats["games_written"] += result.get("games_written", 0)
        stats["scores_written"] += result.get("scores_written", 0)

    return stats


# ---------- layer 3.6: bridge cached match JSON -> tournament_matches ----------

def run_layer_3_6_tm_bridge(
    bracket: TournamentBracket | None,
    *,
    dry_run: bool,
) -> dict[str, int]:
    """Bridge the cached /matches/{id}.json files back into
    tournament_matches.opponent_team / match_link / date.

    The original OWC team-stats CSV seeded tournament_matches with real
    scorelines but NULL opponents (flags didn't export). layer 3 + 3.5 have
    the real truth in match.name: "(Country A) vs (Country B)". This step
    matches those back onto tournament_matches rows by (event, stage,
    team_code, team_score, opponent_score) and fills the missing fields.

    Only touches rows where opponent_team IS NULL, so manually-verified
    matches.csv data is never overwritten.
    """
    from storage import backfill_tm_opponent_by_scores, get_connection

    match_dir = CACHE_DIR / "matches"
    stats = {
        "files_seen": 0,
        "rows_updated": 0,
        "name_parse_fail": 0,
        "no_stage": 0,
        "no_match": 0,
    }
    if not match_dir.exists():
        return stats

    # Build stage lookup from bracket (match_id -> stage)
    stage_by_id: dict[int, str] = {}
    if bracket is not None:
        for bm in bracket.matches:
            if bm.match_id is not None and bm.stage:
                stage_by_id.setdefault(bm.match_id, bm.stage)

    # Build team_name -> team_code lookup from DB
    with get_connection() as connection:
        team_rows = connection.execute(
            "SELECT team_code, team_name FROM teams WHERE team_name IS NOT NULL"
        ).fetchall()
    name_to_code = {
        (r["team_name"] or "").strip().lower(): r["team_code"] for r in team_rows
    }

    import re as _re
    pair_re = _re.compile(
        r"\(([^()]+)\)\s*(?:vs\.?|v\.?)\s*\(([^()]+)\)", _re.IGNORECASE
    )

    def _resolve(name: str | None) -> str | None:
        if not name:
            return None
        key = name.strip().lower()
        if key in name_to_code:
            return name_to_code[key]
        u = name.strip().upper()
        if 2 <= len(u) <= 4 and u.isalpha():
            return u
        return None

    for json_file in sorted(match_dir.glob("*.json")):
        stats["files_seen"] += 1
        try:
            mid = int(json_file.stem)
        except ValueError:
            mid = None
        try:
            payload = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  ! {json_file.name}: {exc}")
            continue

        name = payload.get("name") or ""
        m = pair_re.search(name)
        if not m:
            stats["name_parse_fail"] += 1
            continue
        red_code = _resolve(m.group(1))
        blue_code = _resolve(m.group(2))
        if not red_code or not blue_code:
            stats["name_parse_fail"] += 1
            continue

        stage = stage_by_id.get(mid) if mid is not None else None
        if not stage:
            stats["no_stage"] += 1
            continue

        red_score = payload.get("red_score")
        blue_score = payload.get("blue_score")
        # Fall back: count game wins per side if top-level scores missing.
        if red_score is None or blue_score is None:
            r_wins = b_wins = 0
            for g in (payload.get("games") or []):
                w = (g.get("winning_team") or "").lower()
                if w == "red":
                    r_wins += 1
                elif w == "blue":
                    b_wins += 1
            red_score, blue_score = r_wins, b_wins

        if red_score is None or blue_score is None:
            continue

        start_time = payload.get("start_time")
        date = start_time[:10] if isinstance(start_time, str) and len(start_time) >= 10 else None
        match_link = f"https://osu.ppy.sh/community/matches/{mid}" if mid else None

        if dry_run:
            continue

        # Backfill both perspectives.
        # From RED team's perspective: team_score=red_score, opp_score=blue_score, opponent=blue_code
        updated = backfill_tm_opponent_by_scores(
            event=OWC_2025.event, stage=stage,
            team_code=red_code,
            team_score=red_score, opponent_score=blue_score,
            opponent_team=blue_code, match_link=match_link, date=date,
        )
        # From BLUE team's perspective
        updated += backfill_tm_opponent_by_scores(
            event=OWC_2025.event, stage=stage,
            team_code=blue_code,
            team_score=blue_score, opponent_score=red_score,
            opponent_team=red_code, match_link=match_link, date=date,
        )
        stats["rows_updated"] += updated
        if updated == 0:
            stats["no_match"] += 1

    return stats


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
            apply_manual_map_metadata,
            parse_manual_map_metadata_csv,
        )
        maps_csv = METADATA_DIR / "maps.csv"
        if maps_csv.exists():
            rows = parse_manual_map_metadata_csv(maps_csv)
            if not dry_run:
                applied = apply_manual_map_metadata(rows)
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

LAYER_CHOICES = ("1", "2", "3", "3.5", "3.6", "4", "fallback", "all")


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
    parser.add_argument(
        "--refresh-match-detail",
        action="store_true",
        help="Refetch cached /matches/{id}.json files instead of reusing them.",
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
                    bracket,
                    dry_run=args.dry_run,
                    limit=args.match_limit,
                    refresh=args.refresh_match_detail,
                )
                print(f"  {stats}")
            except Exception as exc:
                print(f"  ! layer 3 failed: {exc}")

    if want("3.5"):
        print("== layer 3.5: ingest cached match JSON ==")
        if bracket is None:
            try:
                bracket, _ = run_layer_2_bracket(dry_run=True)
            except Exception as exc:
                print(f"  ! proceeding without bracket (no stage tags): {exc}")
                bracket = None
        try:
            stats = run_layer_3_5_ingest(bracket, dry_run=args.dry_run)
            print(f"  {stats}")
        except Exception as exc:
            print(f"  ! layer 3.5 failed: {exc}")

    if want("3.6"):
        print("== layer 3.6: bridge match detail -> tournament_matches ==")
        if bracket is None:
            try:
                bracket, _ = run_layer_2_bracket(dry_run=True)
            except Exception as exc:
                print(f"  ! proceeding without bracket (no stage tags): {exc}")
                bracket = None
        try:
            stats = run_layer_3_6_tm_bridge(bracket, dry_run=args.dry_run)
            print(f"  {stats}")
        except Exception as exc:
            print(f"  ! layer 3.6 failed: {exc}")

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
