"""Manual map metadata importer.

When the source tournament spreadsheet stores beatmap IDs / star ratings
behind hyperlinks (which Google Sheets CSV export drops), the cleanest
workaround is to maintain a small hand-authored mapping CSV that pins
each tournament slot to the right osu! beatmap.

Expected mapping CSV columns (header row required):

    event,stage,slot,map_name,difficulty_name,beatmap_id,star_rating

- event:           e.g. "OWC 2025"
- stage:           e.g. "Finals", "Group Stage"
- slot:            e.g. "NM1", "DT4", "TB"
- map_name:        the exact "Artist - Title" string used in the leaderboard
                   CSV. Used to match rows when writing back.
- difficulty_name: optional, e.g. "Insane", "Extra", "Tournament"
- beatmap_id:      integer osu! beatmap ID (required)
- star_rating:     optional float; can be left blank and filled in later via
                   osu! API exact-ID enrichment.

Empty cells / blank lines are skipped. Re-running is safe (the storage
layer's update only fills NULLs).
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from storage import update_enrichment_for_map


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_manual_map_metadata_csv(csv_path: str | Path) -> list[dict[str, Any]]:
    path = Path(csv_path)
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            event = _clean(raw.get("event"))
            stage = _clean(raw.get("stage"))
            slot = _clean(raw.get("slot"))
            map_name = _clean(raw.get("map_name"))
            difficulty_name = _clean(raw.get("difficulty_name"))
            beatmap_id_raw = _clean(raw.get("beatmap_id"))
            star_rating_raw = _clean(raw.get("star_rating"))

            if not event or not slot or not map_name or not beatmap_id_raw:
                continue

            try:
                beatmap_id = int(beatmap_id_raw)
            except ValueError:
                continue

            star_rating: float | None = None
            if star_rating_raw:
                try:
                    star_rating = float(star_rating_raw)
                except ValueError:
                    star_rating = None

            rows.append(
                {
                    "event": event,
                    "stage": stage,
                    "slot": slot,
                    "map_name": map_name,
                    "difficulty_name": difficulty_name,
                    "beatmap_id": beatmap_id,
                    "star_rating": star_rating,
                }
            )
    return rows


def apply_manual_map_metadata(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Write each mapping row back to all matching `matches` rows.

    Returns a small stats dict for the runner to print.
    """
    stats = {"mappings_seen": len(rows), "rows_updated": 0, "mappings_missed": 0}
    for row in rows:
        updated = update_enrichment_for_map(
            event=row["event"],
            stage=row.get("stage"),
            slot=row["slot"],
            map_name=row["map_name"],
            beatmap_id=row["beatmap_id"],
            star_rating=row.get("star_rating"),
            difficulty_name=row.get("difficulty_name"),
        )
        if updated:
            stats["rows_updated"] += updated
        else:
            stats["mappings_missed"] += 1
    return stats
