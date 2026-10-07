"""Find osu! multiplayer links in tabular data and attach a round hint to each."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..models import MatchLink
from ..rounds import normalize_round

# osu.ppy.sh/community/matches/123, osu.ppy.sh/mp/123, old.ppy.sh/mp/123, with or without scheme
MP_LINK_RE = re.compile(
    r"(?:https?://)?(?:osu|old)\.ppy\.sh/(?:community/matches|mp)/(\d+)",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://\S+")
_DECOR_RE = re.compile(r"[─-▟■-◿]+")     # box-drawing rules like ━  ━  ━ in section headers
_BARE_ID_RE = re.compile(r"^\d{7,10}$")                        # a lone match id typed without its URL


@dataclass
class Cell:
    text: str = ""
    link: str | None = None  # hyperlink target, if the cell had one

    def blob(self) -> str:
        return f"{self.text} {self.link or ''}"


@dataclass
class Table:
    name: str                                  # sheet/tab name or file name
    rows: list[list[Cell]] = field(default_factory=list)


def find_match_ids(text: str) -> list[int]:
    return [int(m) for m in MP_LINK_RE.findall(text or "")]


def _row_label(row: list[Cell]) -> str:
    """Row text without URLs, used for round detection."""
    parts = (_DECOR_RE.sub("", _URL_RE.sub("", c.text)).strip() for c in row)
    return " | ".join(p for p in parts if p)


def extract_links(tables: list[Table]) -> list[MatchLink]:
    """Scan every table top-to-bottom.

    Round hint priority for a link: text in its own row > nearest section header row
    above it (a row with a round name but no links) > the tab name.
    """
    seen: set[int] = set()
    out: list[MatchLink] = []
    for table in tables:
        section_raw = table.name if normalize_round(table.name) else None
        # Some sheets lose the hyperlink and keep only the numeric id under an "MP LINK" column.
        bare_ids_ok = any("mp link" in c.text.lower() for r in table.rows for c in r)
        for row in table.rows:
            ids: list[int] = []
            for cell in row:
                ids.extend(find_match_ids(cell.blob()))
                if bare_ids_ok and not cell.link and _BARE_ID_RE.match(cell.text.strip()):
                    ids.append(int(cell.text.strip()))
            label = _row_label(row)
            if not ids:
                if label and normalize_round(label) and len(label) < 80:
                    section_raw = label  # looks like a header such as "Quarterfinals"
                continue
            row_raw = label if normalize_round(label) else None
            for mid in ids:
                if mid in seen:
                    continue
                seen.add(mid)
                out.append(MatchLink(
                    osu_match_id=mid,
                    round_raw=row_raw or section_raw,
                    context=f"[{table.name}] {label}"[:300],
                ))
    return out


def extract_links_from_text(text: str) -> list[MatchLink]:
    """Plain text / pasted list / forum post: each line may carry a round hint;
    lines with only a round name act as section headers."""
    rows = [[Cell(text=line)] for line in text.splitlines()]
    return extract_links([Table(name="text", rows=rows)])
