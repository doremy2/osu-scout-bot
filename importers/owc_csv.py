"""OWC 2025 (and similarly-shaped tournaments) CSV ingestion.

This module knows how to parse three different sheet types exported from the
OWC stats Google Sheets:

  1. Player Leaderboards   -> map-level rows for the `matches` table
  2. Performance Scores    -> per-player real pscore for the `player_scores` table
  3. Team Statistics       -> per-team match scorelines for `tournament_matches`

Each parser is filename-aware: it derives `event` and `stage` from the source
filename so the same code handles group stage / RO16 / quarterfinals /
semifinals / finals without per-round duplication.

The Mappool Statistics CSVs have the same wide block format as Player
Leaderboards but their data is per-map aggregates rather than per-row scores,
and the CSV export loses the beatmap hyperlinks that hold beatmap_id and
star_rating. For metadata we use a separate manual mapping CSV instead
(see importers/manual_map_metadata.py).
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

# ---------- shared layout constants ----------

# Player Leaderboards layout
LB_BLOCK_WIDTH = 11
LB_HEADER_ROW = 5            # slot + map name row
LB_DATA_START_ROW = 9

# Performance Scores layout
PS_HEADER_ROW = 5
PS_DATA_START_ROW = 7

# Team Statistics layout
TS_HEADER_ROW = 5
TS_DATA_START_ROW = 7
TS_MATCH_BLOCK_START_COL = 17
TS_MATCH_BLOCK_WIDTH = 3     # [score, "vs", blank]

SLOT_RE = re.compile(r"^(NM|HD|HR|DT|FM|EZ|HT|TB)\d*$")
RANK_RE = re.compile(r"^#\d+$")
SCORE_LINE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")
PLAYED_RE = re.compile(r"^\s*(\d+)\s*/\s*(\d+)\s*$")


# ---------- shared helpers ----------

def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: str | None) -> int | None:
    text = _clean_text(value)
    if text is None:
        return None
    text = text.replace(",", "")
    try:
        return int(float(text))
    except ValueError:
        return None


def _to_float(value: str | None) -> float | None:
    text = _clean_text(value)
    if text is None:
        return None
    text = text.replace("%", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _read_csv_rows(csv_path: str | Path) -> list[list[str]]:
    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f))


# Filename pattern:
#   "<stage> statistics _ osu! world cup 2025 - <Sheet Type>.csv"
# stage examples: "finals", "semifinals", "quarterfinals",
#                 "round of 16", "group stage"
_STAGE_TITLES = {
    "group stage": "Group Stage",
    "round of 16": "Round of 16",
    "round of 32": "Round of 32",
    "quarterfinals": "Quarterfinals",
    "semifinals": "Semifinals",
    "finals": "Finals",
    "grand finals": "Grand Finals",
}


def derive_event_and_stage(csv_path: str | Path, default_event: str = "OWC 2025") -> tuple[str, str]:
    """Pull the round name out of an OWC CSV filename."""
    name = Path(csv_path).stem.lower()
    # strip trailing " - sheet type"
    name = re.sub(r"\s*-\s*[a-z ]+$", "", name)
    # strip trailing "statistics _ osu! world cup 2025"
    name = re.sub(r"\s+statistics\s*_\s*osu!?\s*world cup\s*\d{4}\s*$", "", name)
    name = name.strip()

    stage = _STAGE_TITLES.get(name, name.title() if name else "Unknown Stage")
    return default_event, stage


# ============================================================
# 1. Player Leaderboards
# ============================================================

def _extract_slot_blocks(rows: list[list[str]]) -> list[dict[str, Any]]:
    header_row = rows[LB_HEADER_ROW]
    blocks: list[dict[str, Any]] = []

    for start_col in range(2, len(header_row), LB_BLOCK_WIDTH):
        slot = _clean_text(header_row[start_col]) if start_col < len(header_row) else None
        map_name = (
            _clean_text(header_row[start_col + 1])
            if start_col + 1 < len(header_row)
            else None
        )

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
    match_id: str,
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
        "accuracy": _to_float(accuracy),
        "result": "unknown",
        "star_rating": None,
        "beatmap_id": None,
        "map_name": block["map_name"],
        "player_team": player_team.rstrip(":") if player_team else None,
        "opponent_team": opponent_team,
        "match_id": match_id,
        "extra_mods": mods_or_grade,
        "leaderboard_rank": rank.lstrip("#") if rank else None,
    }


def parse_owc_player_leaderboard_csv(
    csv_path: str | Path,
    *,
    event: str | None = None,
    stage: str | None = None,
    source: str = "OWC_CSV",
) -> list[dict[str, Any]]:
    """Parse one Player Leaderboards CSV into normalized map-level rows."""
    rows = _read_csv_rows(csv_path)
    if len(rows) <= LB_DATA_START_ROW:
        raise ValueError(f"CSV too short for OWC leaderboard format: {csv_path}")

    derived_event, derived_stage = derive_event_and_stage(csv_path)
    event = event or derived_event
    stage = stage or derived_stage
    match_id = f"owc-2025-{stage.lower().replace(' ', '-')}"

    blocks = _extract_slot_blocks(rows)
    if not blocks:
        raise ValueError(f"Could not detect slot blocks in {csv_path}")

    source_file = str(Path(csv_path))
    parsed_rows: list[dict[str, Any]] = []

    for row in rows[LB_DATA_START_ROW:]:
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
                match_id=match_id,
            )
            if parsed is not None:
                parsed_rows.append(parsed)

    return parsed_rows


# ============================================================
# 2. Performance Scores
# ============================================================

def parse_owc_performance_scores_csv(
    csv_path: str | Path,
    *,
    event: str | None = None,
    stage: str | None = None,
    source: str = "OWC_CSV",
) -> list[dict[str, Any]]:
    """Parse one Performance Scores CSV into per-player score rows.

    Output shape (one dict per player):
        {
            "player": "mrekk",
            "event": "OWC 2025",
            "stage": "Finals",
            "source": "OWC_CSV",
            "player_team": "AU",
            "rank": 1,
            "pscore": 1.834,
            "played_count": 19,  "played_total": 19,
            "counted_count": 14, "counted_total": 11,
            "avg_score": 672486,
            "avg_accuracy": 98.37,
            "highest_slot": "DT1",
            "highest_score": 969676,
        }
    """
    rows = _read_csv_rows(csv_path)
    if len(rows) <= PS_DATA_START_ROW:
        raise ValueError(f"CSV too short for OWC performance scores: {csv_path}")

    derived_event, derived_stage = derive_event_and_stage(csv_path)
    event = event or derived_event
    stage = stage or derived_stage

    parsed: list[dict[str, Any]] = []

    for row in rows[PS_DATA_START_ROW:]:
        if len(row) < 16:
            continue
        rank_cell = _clean_text(row[3]) if len(row) > 3 else None
        if not rank_cell or not RANK_RE.match(rank_cell):
            # Stop on the trailing notes/multi-line area.
            continue

        team_cell = _clean_text(row[4]) if len(row) > 4 else None
        player_name = _clean_text(row[6]) if len(row) > 6 else None
        if not player_name:
            continue

        played_match = PLAYED_RE.match(_clean_text(row[8]) or "")
        counted_match = PLAYED_RE.match(_clean_text(row[9]) or "")

        parsed.append(
            {
                "player": player_name,
                "player_team": team_cell.rstrip(":") if team_cell else None,
                "event": event,
                "stage": stage,
                "source": source,
                "rank": _to_int(rank_cell.lstrip("#")),
                "pscore": _to_float(row[7] if len(row) > 7 else None),
                "played_count": int(played_match.group(1)) if played_match else None,
                "played_total": int(played_match.group(2)) if played_match else None,
                "counted_count": int(counted_match.group(1)) if counted_match else None,
                "counted_total": int(counted_match.group(2)) if counted_match else None,
                "avg_score": _to_int(row[10] if len(row) > 10 else None),
                "avg_accuracy": _to_float(row[11] if len(row) > 11 else None),
                "highest_slot": _clean_text(row[13]) if len(row) > 13 else None,
                "highest_score": _to_int(row[15] if len(row) > 15 else None),
            }
        )

    return parsed


# ============================================================
# 3. Team Statistics  ->  match-level rows
# ============================================================

def parse_owc_team_statistics_csv(
    csv_path: str | Path,
    *,
    event: str | None = None,
    stage: str | None = None,
    source: str = "OWC_CSV",
) -> list[dict[str, Any]]:
    """Parse one Team Statistics CSV into per-team match-level rows.

    Each output row represents one match (BO9/BO11/BO13) from the perspective
    of a single team. Opponent team is left None because the source CSV
    strips the hyperlinks that hold that information; opponents can be
    reconstructed downstream by pairing complementary scorelines, or filled
    in later when hyperlinks are recoverable.

    Output shape:
        {
            "event": "OWC 2025",
            "stage": "Finals",
            "source": "OWC_CSV",
            "team": "Australia",
            "team_code": "AU",
            "opponent_team": None,
            "team_score": 7,
            "opponent_score": 2,
            "result": "win",
            "match_link": None,
            "match_index": 0,         # 0-based ordering within the round
        }
    """
    rows = _read_csv_rows(csv_path)
    if len(rows) <= TS_DATA_START_ROW:
        raise ValueError(f"CSV too short for OWC team statistics: {csv_path}")

    derived_event, derived_stage = derive_event_and_stage(csv_path)
    event = event or derived_event
    stage = stage or derived_stage

    parsed: list[dict[str, Any]] = []

    for row in rows[TS_DATA_START_ROW:]:
        if len(row) < 8:
            continue
        rank_cell = _clean_text(row[2]) if len(row) > 2 else None
        if not rank_cell or not RANK_RE.match(rank_cell):
            continue

        team_code = _clean_text(row[4]) if len(row) > 4 else None
        team_name = _clean_text(row[5]) if len(row) > 5 else None
        if not team_name:
            continue

        # Walk the matches block in width-3 strides. A scoreline like
        # "7 - 2" in the score column means a real match; empty means no
        # more matches for this team in this round.
        match_index = 0
        col = TS_MATCH_BLOCK_START_COL
        while col < len(row):
            score_cell = _clean_text(row[col])
            if score_cell:
                m = SCORE_LINE_RE.match(score_cell)
                if m:
                    team_score = int(m.group(1))
                    opponent_score = int(m.group(2))
                    if team_score > opponent_score:
                        result = "win"
                    elif team_score < opponent_score:
                        result = "loss"
                    else:
                        result = "draw"
                    parsed.append(
                        {
                            "event": event,
                            "stage": stage,
                            "source": source,
                            "team": team_name,
                            "team_code": team_code,
                            "opponent_team": None,
                            "team_score": team_score,
                            "opponent_score": opponent_score,
                            "result": result,
                            "match_link": None,
                            "match_index": match_index,
                        }
                    )
                    match_index += 1
            col += TS_MATCH_BLOCK_WIDTH

    return parsed
