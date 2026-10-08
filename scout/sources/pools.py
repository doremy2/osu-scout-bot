"""Read mappool slots (NM1, HD2, TB...) out of a sheet's mappool tab.

Used only for *display order* (the draft simulator), so a wrong guess can never change ratings.
A slot row has a slot label in one of its first cells and a beatmap id somewhere in the row: either
a hyperlink to the beatmap or a plain numeric id column. Round headers above the rows ("GRAND FINALS")
say which round the following slots belong to.
"""
from __future__ import annotations

import re

from ..rounds import normalize_round
from .extract import Table, _row_label

SLOT_RE = re.compile(r"^(NM|HD|HR|DT|EZ|FM|FL|HT|TB|CM|LM)\s*-?\s*(\d{0,2})$", re.IGNORECASE)
_LINK_ID_RES = [
    re.compile(r"#(?:osu|taiko|fruits|catch|mania)/(\d+)"),
    re.compile(r"/(?:beatmaps|b)/(\d+)"),
]
_BARE_ID = re.compile(r"^\d{5,9}$")
_SET_RE = re.compile(r"/beatmapsets/(\d+)")


def _beatmap_id(row) -> int | None:
    for cell in row:
        for text in (cell.link or "", cell.text):
            for rx in _LINK_ID_RES:
                m = rx.search(text)
                if m:
                    return int(m.group(1))
    for cell in row:                       # numeric ID column (no link)
        if not cell.link and _BARE_ID.match(cell.text.strip()):
            return int(cell.text.strip())
    return None


def _set_id(row) -> int | None:
    for cell in row:
        m = _SET_RE.search(cell.link or "")
        if m:
            return int(m.group(1))
    return None


_SR_RE = re.compile(r"^★?\s*(\d{1,2}\.\d+)$")


def _star_rating(row) -> float | None:
    for cell in row:
        m = _SR_RE.match(cell.text.strip())
        if m and 1.0 <= float(m.group(1)) <= 15.0:
            return float(m.group(1))
    return None


def _label(row, slot_cell_text: str) -> str | None:
    """"Artist - Title [Difficulty]" text of the row (the first cell that looks like one)."""
    best = None
    for cell in row:
        t = cell.text.strip()
        if not t or t == slot_cell_text or _BARE_ID.match(t) or _SR_RE.match(t):
            continue
        if " - " in t:
            return t
        if best is None and cell.link and len(t) > 3:
            best = t
    return best


def _slot(row) -> str | None:
    for cell in row[:3]:
        m = SLOT_RE.match(cell.text.strip())
        if m:
            return f"{m.group(1).upper()}{m.group(2)}"
    return None


def extract_pool_slots(tables: list[Table]) -> list[dict]:
    """[{round, slot, beatmap_id, position}] in sheet order. Only tables that look like mappool tabs are read."""
    out: list[dict] = []
    for table in tables:
        if "pool" not in table.name.lower():
            continue
        round_code = normalize_round(table.name)
        position = 0
        seen: set[tuple] = set()
        for row in table.rows:
            slot = _slot(row)
            if slot is None:
                label = _row_label(row)
                code = normalize_round(label) if label and len(label) < 200 else None
                if code and _beatmap_id(row) is None:
                    round_code, position = code, 0
                continue
            bid, set_id = _beatmap_id(row), _set_id(row)
            if (bid is None and set_id is None) or not round_code or (round_code, slot) in seen:
                continue
            seen.add((round_code, slot))
            out.append({"round": round_code, "slot": slot, "beatmap_id": bid, "beatmapset_id": set_id, "position": position,
                        "label": _label(row, row[0].text.strip()), "star_rating": _star_rating(row)})
            position += 1
    return out
