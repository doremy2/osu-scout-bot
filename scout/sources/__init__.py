"""Tournament sources -> list[MatchLink].

Every reader turns its input into `Table`s (a grid of cells that may carry hyperlinks).
One shared extractor then finds MP links and round hints. Adding a new source type
(forum post, website) only means writing a new reader that produces Tables or text.
"""
from __future__ import annotations

from pathlib import Path

from ..models import MatchLink
from .extract import Cell, Table, extract_links, extract_links_from_text
from .pools import extract_pool_slots
from .readers import read_csv, read_google_sheet, read_xlsx


def detect_kind(location: str) -> str:
    loc = location.lower()
    if "docs.google.com/spreadsheets" in loc:
        return "google_sheet"
    if loc.endswith((".xlsx", ".xlsm")):
        return "xlsx"
    if loc.endswith((".csv", ".tsv")):
        return "csv"
    if loc.endswith((".txt", ".md")):
        return "text"
    raise ValueError(f"Can't tell what kind of source this is: {location}")


def discover_links(location: str, kind: str | None = None) -> tuple[str, list[MatchLink]]:
    kind, links, _slots = discover_all(location, kind)
    return kind, links


def discover_all(location: str, kind: str | None = None) -> tuple[str, list[MatchLink], list[dict]]:
    """Match links plus mappool slots (the latter only from spreadsheet-like sources)."""
    kind = kind or detect_kind(location)
    if kind == "google_sheet":
        tables = read_google_sheet(location)
    elif kind == "xlsx":
        tables = read_xlsx(Path(location))
    elif kind == "csv":
        tables = read_csv(Path(location))
    elif kind == "text":
        return kind, extract_links_from_text(Path(location).read_text(encoding="utf-8")), []
    else:
        raise ValueError(f"Unsupported source kind: {kind}")
    return kind, extract_links(tables), extract_pool_slots(tables)


__all__ = ["Cell", "Table", "discover_links", "discover_all", "extract_pool_slots", "detect_kind", "extract_links", "extract_links_from_text"]
