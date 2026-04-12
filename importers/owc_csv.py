from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

BLOCK_WIDTH = 11
HEADER_ROW_INDEX = 5
COLUMN_HEADER_ROW_INDEX = 7
DATA_START_ROW_INDEX = 9

SLOT_RE = re.compile(r"^(NM|HD|HR|DT|FM|TB)\d*$")


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: str | None) -> int | None:
    if not value:
        return None
    return int(value.replace(",", "").strip())


def _to_float_percent(value: str | None) -> float | None:
    if not value:
        return None
    return float(value.replace("%", "").strip())


def _read_csv_rows(csv_path: str | Path) -> list[list[str]]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f))


def _extract_slot_blocks(rows: list[list[str]]) -> list[dict[str, Any]]:
    header_row = rows[HEADER_ROW_INDEX]
    blocks: list[dict[str, Any]] = []

    for start_col in range(2, len(header_row), BLOCK_WIDTH):
        slot = _clean_text(header_row[start_col])
        map_name = _clean_text(header_row[start_col + 1]) if start_col + 1 < len(header_row) else None

        if not slot or not SLOT_RE.match(slot):
            continue

        mod = "TB" if slot == "TB" else re.match(r"^[A-Z]+", slot).group(0)

        blocks.append(
            {
                "slot": slot,
                "mod": mod,
                "map_name": map_name,
                "start_col": start_col,
            }
        )

    return blocks


def _parse_player_cell_block(
    row: list[str],
    block: dict[str, Any],
    *,
    event: str,
    stage: str,
    source: str,
    source_file: str,
) -> dict[str, Any] | None:
    c = block["start_col"]

    def get(offset: int) -> str | None:
        idx = c + offset
        if idx >= len(row):
            return None
        return _clean_text(row[idx])

    rank = get(0)
    player_team = get(1)
    player_name = get(3)
    mods_or_grade = get(5)
    score = get(6)
    accuracy = get(7)
    opponent_team = get(8)

    if not rank or not player_name or not score:
        return None

    return {
        "player": player_name,
        "opponent": None,
        "event": event,
        "stage": stage,
        "source": source,
        "source_type": "csv",
        "source_file": source_file,
        "date": None,
        "mod": block["mod"],
        "slot": block["slot"],
        "score": _to_int(score),
        "accuracy": _to_float_percent(accuracy),
        "result": "unknown",
        "star_rating": None,
        "beatmap_id": None,
        "map_name": block["map_name"],
        "player_team": player_team.rstrip(":") if player_team else None,
        "opponent_team": opponent_team,
        "match_id": "owc-2025-grand-finals",
        "extra_mods": mods_or_grade,
        "leaderboard_rank": rank.lstrip("#") if rank else None,
    }


def parse_owc_player_leaderboard_csv(
    csv_path: str | Path,
    *,
    event: str = "OWC 2025 Grand Finals",
    stage: str = "Grand Finals",
    source: str = "OWC_CSV",
) -> list[dict[str, Any]]:
    rows = _read_csv_rows(csv_path)
    if len(rows) <= DATA_START_ROW_INDEX:
        raise ValueError("CSV is too short or not in the expected OWC leaderboard format.")

    blocks = _extract_slot_blocks(rows)
    if not blocks:
        raise ValueError("Could not detect slot blocks in the OWC CSV.")

    source_file = str(Path(csv_path))
    parsed_rows: list[dict[str, Any]] = []

    for row in rows[DATA_START_ROW_INDEX:]:
        if not any(cell.strip() for cell in row):
            continue

        for block in blocks:
            parsed = _parse_player_cell_block(
                row,
                block,
                event=event,
                stage=stage,
                source=source,
                source_file=source_file,
            )
            if parsed is not None:
                parsed_rows.append(parsed)

    return parsed_rows


