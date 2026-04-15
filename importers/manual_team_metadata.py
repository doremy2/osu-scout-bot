"""Manual team metadata importer.

Hand-authored CSV that maps a team code to its full display name so UI
can render 'vs United States' instead of 'vs US'.

Expected mapping CSV columns (header row required):

    team_code,team_name,event

- team_code: short code used in leaderboards and team statistics (e.g. 'US',
             'PL', 'KR'). Case-sensitive; keep it exactly as it appears in
             the rest of the data so joins work.
- team_name: full display name (e.g. 'United States', 'Poland').
- event:     optional; pin this team to one tournament when the same code
             might mean different things across events. Leave blank for a
             global mapping.

Empty cells / blank lines are skipped. Re-running is safe; the storage
layer upserts on team_code.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from storage import insert_or_update_teams


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_manual_teams_csv(csv_path: str | Path) -> list[dict[str, Any]]:
    path = Path(csv_path)
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            team_code = _clean(raw.get("team_code"))
            team_name = _clean(raw.get("team_name"))
            event = _clean(raw.get("event"))
            if not team_code or not team_name:
                continue
            rows.append(
                {
                    "team_code": team_code,
                    "team_name": team_name,
                    "event": event,
                }
            )
    return rows


def apply_manual_teams(rows: list[dict[str, Any]]) -> dict[str, int]:
    written = insert_or_update_teams(rows)
    return {"teams_seen": len(rows), "teams_written": written}
