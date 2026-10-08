"""Decide, deterministically, whether a spreadsheet is a tournament and what we can say about it.

Everything here is a plain rule over the sheet's cells (no network, no model): name from the link text or a title cell,
acronym and year from the name, format from "2v2"-style text and Teams tabs, osu! client from the kind of MP links, and a
confidence score that adds up visible signals. The reasons are kept so a reviewer can see why a candidate scored as it did.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from ..config import settings
from ..db.repo import slugify
from ..models import MatchLink
from ..rounds import normalize_round
from ..sources.extract import Table, extract_links

GENERIC_TEXT = {
    "here", "link", "this", "sheet", "sheets", "spreadsheet", "google sheet", "google sheets", "google spreadsheet",
    "click here", "docs", "document", "schedule", "mappool", "mappools", "pool", "registration", "stats", "bracket",
    "results", "rules", "website", "forum", "forum post", "info", "more info", "details", "tournament", "link here",
}
TITLE_RE = re.compile(
    r"\b(tournament|cup|championship|championships|open|league|invitational|series|showdown|classic|games|masters|"
    r"royale|rumble|battle|clash|circuit|festival|derby|trials|world cup|owc|cwc|4wc|5wc|6wc)\b", re.I)
VS_RE = re.compile(r"\b([1-9])\s*(?:v|vs|x)\s*([1-9])\b", re.I)
YEAR_RE = re.compile(r"\b(20[12]\d)\b")
PAREN_ACRONYM_RE = re.compile(r"\(([A-Za-z0-9!]{2,8})\)")
STOP_WORDS = {"of", "the", "and", "osu", "osu!", "tournament", "for", "a", "an", "in", "on"}
USEFUL_TABS = re.compile(r"schedule|match|bracket|mappool|map pool|pool|team|player|qualifier|seeding|registration|staff|"
                         r"round|results|groups?|swiss", re.I)
INFO_TABS = re.compile(r"info|overview|home|main|welcome|cover|rules|about|index", re.I)


@dataclass
class SheetAnalysis:
    name: str
    name_from: str                         # hint | title cell | fallback
    acronym: str
    year: int | None
    format: str                            # 1v1 | team
    client: str                            # stable | lazer
    links: list[MatchLink] = field(default_factory=list)
    rooms: int = 0
    tabs: list[str] = field(default_factory=list)
    confidence: float = 0.0
    reasons: list[str] = field(default_factory=list)

    @property
    def match_count(self) -> int:
        return len(self.links)


def clean_name(text: str | None) -> str:
    t = re.sub(r"https?://\S+", "", text or "")
    t = re.sub(r"\s+", " ", t).strip(" \t-–—:|•·​")
    return t


def is_informative(text: str) -> bool:
    t = text.strip().lower().rstrip(":")
    return 4 <= len(t) <= 90 and t not in GENERIC_TEXT and not re.fullmatch(r"[\W\d_]+", t)


def _title_from_cells(tables: list[Table]) -> str | None:
    """A cell near the top of the first tabs that reads like a tournament title ("Osu! Taiko World Cup 2026")."""
    ordered = sorted(tables[:6], key=lambda tb: 0 if INFO_TABS.search(tb.name) else 1)
    for tb in ordered:
        for row in tb.rows[:12]:
            for cell in row:
                text = clean_name(cell.text)
                if (5 <= len(text) <= 80 and TITLE_RE.search(text) and "mp link" not in text.lower()
                        and not normalize_round(text) and len(text.split()) <= 10):
                    return text
    return None


def detect_acronym(name: str) -> str:
    m = PAREN_ACRONYM_RE.search(name)
    if m and not YEAR_RE.fullmatch(m.group(1)):
        return m.group(1).upper()
    for tok in re.findall(r"\b[A-Z][A-Z0-9]{1,6}\b", name):
        if not YEAR_RE.fullmatch(tok) and tok not in ("OSU", "THE"):
            return tok
    words = [w for w in re.findall(r"[A-Za-z]+", YEAR_RE.sub("", name)) if w.lower() not in STOP_WORDS and len(w) > 1]
    if 3 <= len(words) <= 6:
        return "".join(w[0] for w in words).upper()
    return ""


def detect_format(name: str, tables: list[Table]) -> tuple[str, str]:
    """(format, why). `NvN` text wins; otherwise a Teams tab / team columns; otherwise assume individual players."""
    m = VS_RE.search(name)
    if m:
        return ("1v1" if m.group(1) == "1" and m.group(2) == "1" else "team"), f"“{m.group(0)}” in the name"
    for tb in tables:
        for row in tb.rows[:25]:
            for cell in row:
                m = VS_RE.fullmatch(cell.text.strip()) or (VS_RE.search(cell.text) if len(cell.text) < 60 else None)
                if m and "format" in cell.text.lower() + tb.name.lower():
                    return ("1v1" if m.group(1) == "1" and m.group(2) == "1" else "team"), f"“{m.group(0)}” in the sheet"
    names = " ".join(tb.name.lower() for tb in tables)
    if re.search(r"\bteams?\b|squad", names):
        return "team", "a Teams tab"
    for tb in tables:
        for row in tb.rows[:15]:
            for cell in row:
                if re.fullmatch(r"(team name|captain|team captain|team members?)", cell.text.strip().lower()):
                    return "team", f"“{cell.text.strip()}” column"
    return "1v1", "no team signals, assumed 1v1"


def analyze_tables(tables: list[Table], hint: str | None = None) -> SheetAnalysis:
    links = extract_links(tables)
    rooms = sum(1 for l in links if l.kind == "room")
    tabs = [t.name for t in tables]

    name, name_from = "", "fallback"
    h = clean_name(hint)
    if h and is_informative(h):
        name, name_from = h, "hint"
    else:
        title = _title_from_cells(tables)
        if title:
            name, name_from = title, "title cell"
    year_m = YEAR_RE.search(name) if name else None
    year = int(year_m.group(1)) if year_m else None
    fmt, fmt_why = detect_format(name, tables)
    acronym = detect_acronym(name) if name else ""
    client = "lazer" if rooms > len(links) - rooms else "stable"

    out = SheetAnalysis(name=name or "Untitled tournament", name_from=name_from, acronym=acronym, year=year, format=fmt,
                        client=client, links=links, rooms=rooms, tabs=tabs)
    out.confidence, out.reasons = score(out, fmt_why)
    return out


def score(a: SheetAnalysis, fmt_why: str = "") -> tuple[float, list[str]]:
    """Add up the signals. Each one is listed with its weight so the review queue can explain itself."""
    n = a.match_count
    why: list[str] = []
    total = 0.0

    def add(points: float, text: str) -> None:
        nonlocal total
        total += points
        why.append(f"{text} ({points:+.2f})")

    if n == 0:
        return 0.0, ["no osu! multiplayer links (+0.00)"]
    if n >= 20:
        add(0.55, f"{n} MP links")
    elif n >= 8:
        add(0.45, f"{n} MP links")
    elif n >= 3:
        add(0.30, f"{n} MP links")
    else:
        add(0.15, f"only {n} MP link{'s' if n != 1 else ''}")

    with_round = sum(1 for l in a.links if normalize_round(l.round_raw)) / n
    if with_round >= 0.6:
        add(0.15, f"{with_round:.0%} of links sit under a recognised round")
    elif with_round >= 0.25:
        add(0.08, f"{with_round:.0%} of links sit under a recognised round")

    useful = [t for t in a.tabs if USEFUL_TABS.search(t)]
    if len(useful) >= 2:
        add(0.15, f"tournament-style tabs: {', '.join(useful[:4])}")
    elif useful:
        add(0.07, f"tournament-style tab: {useful[0]}")

    if a.name_from != "fallback":
        add(0.10, f"name read from the {a.name_from}")
        if a.year:
            add(0.05, f"year {a.year}")
        if a.acronym:
            add(0.05, f"acronym {a.acronym}")
    else:
        why.append("no tournament name found (+0.00)")

    minority = min(a.rooms, n - a.rooms)
    if minority / n > 0.2:
        add(-0.10, "mixes stable matches and lazer rooms")
    if n > settings.max_matches:
        add(-0.20, f"{n} links is over the {settings.max_matches}-match import limit")
    if fmt_why:
        why.append(f"format {a.format}: {fmt_why}")
    return round(max(0.0, min(0.99, total)), 2), why


def suggest_slug(a: SheetAnalysis, taken: set[str]) -> str:
    base = slugify(f"{a.acronym} {a.year or ''}" if a.acronym else a.name) or "tournament"
    base = base[:44].strip("-") or "tournament"
    slug, i = base, 2
    while slug in taken:
        slug = f"{base}-{i}"
        i += 1
    return slug
