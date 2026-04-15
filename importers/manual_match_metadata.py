"""Manual match metadata importer.

Hand-authored CSV of tournament match-level metadata. This is the
authoritative source for match-level truth (opponent team, scoreline,
match link) since OWC CSV exports strip the hyperlinks and flag icons
that encode the opposing team in the spreadsheet UI.

Expected mapping CSV columns (header row required):

    event,stage,match_index,team_code,opponent_team_code,team_score,opponent_score,result,match_link,date

- event:              e.g. 'OWC 2025'
- stage:              e.g. 'Group Stage', 'Finals', 'Grand Finals'
- match_index:        integer index the team_code played in this stage,
                      starting at 1. Used as part of the join key so
                      repeated matches can be disambiguated.
- team_code:          the team whose perspective this row is from (e.g. 'PL')
- opponent_team_code: the opposing team (e.g. 'US')
- team_score:         final score for team_code
- opponent_score:     final score for opponent_team_code
- result:             optional; 'win' / 'loss' / 'draw'. If blank, derived
                      from the scoreline.
- match_link:         optional osu! match URL
- date:               optional ISO date

Empty cells / blank lines are skipped. Re-running is safe; the storage
layer's backfill only fills NULL fields on existing rows (COALESCE) and
inserts brand new rows if no matching (event, stage, team_code,
match_index) row exists yet.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from storage import backfill_tournament_match_metadata


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> int | None:
    text = _clean(value if not isinstance(value, (int, float)) else str(value))
    if text is None:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def parse_manual_matches_csv(csv_path: str | Path) -> list[dict[str, Any]]:
    path = Path(csv_path)
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            event = _clean(raw.get("event"))
            stage = _clean(raw.get("stage"))
            team_code = _clean(raw.get("team_code"))
            opponent_team_code = _clean(raw.get("opponent_team_code"))
            match_index = _to_int(raw.get("match_index"))
            team_score = _to_int(raw.get("team_score"))
            opponent_score = _to_int(raw.get("opponent_score"))
            result = _clean(raw.get("result"))
            match_link = _clean(raw.get("match_link"))
            date = _clean(raw.get("date"))

            if not event or not team_code:
                continue

            rows.append(
                {
                    "event": event,
                    "stage": stage,
                    "match_index": match_index,
                    "team_code": team_code,
                    "opponent_team": opponent_team_code,
                    "team_score": team_score,
                    "opponent_score": opponent_score,
                    "result": result,
                    "match_link": match_link,
                    "date": date,
                }
            )
    return rows


def apply_manual_matches(rows: list[dict[str, Any]]) -> dict[str, int]:
    stats = {"matches_seen": len(rows), "rows_updated": 0, "rows_missed": 0}
    for row in rows:
        updated = backfill_tournament_match_metadata(
            event=row.get("event"),
            stage=row.get("stage"),
            team_code=row.get("team_code"),
            match_index=row.get("match_index"),
            opponent_team=row.get("opponent_team"),
            match_link=row.get("match_link"),
            date=row.get("date"),
            team_score=row.get("team_score"),
            opponent_score=row.get("opponent_score"),
        )
        if updated:
            stats["rows_updated"] += updated
        else:
            stats["rows_missed"] += 1
    return stats
