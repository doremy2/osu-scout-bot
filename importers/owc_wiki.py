"""osu! wiki scraper for OWC tournament bracket + mappool data.

This is the "bracket / match-list layer" of the layered OWC ingestion model.
Unlike the CSV exports from the public stats sheets (which lose hyperlinks,
flags, and beatmap IDs), the osu! wiki tournament pages are authoritative
and structured: they contain every match link, every team pairing, and the
full mappool with beatmap IDs for every stage.

Layers this module populates:
  - bracket/match-list layer:
        (stage, team_a, team_b, score_a, score_b, match_link) tuples
  - mappool layer:
        (stage, slot, beatmap_id, map_name, difficulty_name) tuples

Downstream consumers (scripts/sync_owc_2025.py) then feed those match links
into importers.osu_api.OsuApiClient.get_match(...) to pull match-detail
layer data (per-map scores per player), and the mappool layer is merged
directly into storage without needing osu! API round-trips.

Design notes:
  - This module is intentionally HTTP-only via requests; no JS rendering.
    The wiki pages are static server-rendered HTML, so regex+stdlib is
    sufficient and keeps the dep surface small.
  - Parsing is tolerant. If a page section changes shape (new stage naming,
    new table layout), the parser degrades to "best effort" and surfaces
    what it found. The sync script is expected to merge this with other
    layers rather than treat it as ground truth.
  - The overall fetcher is structured so that OWC 2025 is just one
    concrete `TournamentWikiConfig`; adding OWC 2024 / OWC 2026 / other
    tournaments is a matter of adding a new config, not rewriting parsers.

Usage:
    from importers.owc_wiki import fetch_owc_2025_bracket, OWC_2025

    bracket = fetch_owc_2025_bracket()
    for row in bracket.matches:
        print(row.stage, row.team_a, row.team_b, row.match_link)
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import Iterable

import requests

from importers.osu_api import parse_match_id


WIKI_USER_AGENT = "osu!scout/0.1 (+tournament ingestion)"
DEFAULT_TIMEOUT = 20


# ---------- data classes ----------

@dataclass
class BracketMatch:
    """One match-level row parsed from a wiki bracket page."""
    stage: str
    team_a: str | None
    team_b: str | None
    score_a: int | None
    score_b: int | None
    match_link: str | None
    match_id: int | None = None

    def __post_init__(self) -> None:
        if self.match_link and self.match_id is None:
            self.match_id = parse_match_id(self.match_link)


@dataclass
class MappoolEntry:
    """One map in a tournament mappool, from the wiki mappool table."""
    stage: str
    slot: str                # e.g. 'NM1', 'HD2', 'TB'
    beatmap_id: int | None
    map_name: str | None
    difficulty_name: str | None
    star_rating: float | None = None


@dataclass
class TournamentBracket:
    """Everything we pulled for one tournament in one fetch pass."""
    event: str
    matches: list[BracketMatch] = field(default_factory=list)
    mappool: list[MappoolEntry] = field(default_factory=list)

    def match_links(self) -> list[str]:
        return [m.match_link for m in self.matches if m.match_link]

    def match_ids(self) -> list[int]:
        return [m.match_id for m in self.matches if m.match_id is not None]


@dataclass
class TournamentWikiConfig:
    """Describes how to fetch one tournament from the osu! wiki."""
    event: str
    bracket_url: str
    mappool_url: str | None = None
    # Human-readable stage names we expect to find as section headers.
    known_stages: tuple[str, ...] = (
        "Group Stage",
        "Round of 32",
        "Round of 16",
        "Quarterfinals",
        "Semifinals",
        "Finals",
        "Grand Finals",
    )


OWC_2025 = TournamentWikiConfig(
    event="OWC 2025",
    bracket_url="https://osu.ppy.sh/wiki/en/Tournaments/OWC/2025",
    mappool_url="https://osu.ppy.sh/wiki/en/Tournaments/OWC/2025",
)


# ---------- HTTP ----------

def _get(url: str, *, session: requests.Session | None = None) -> str:
    sess = session or requests.Session()
    try:
        resp = sess.get(
            url,
            headers={"User-Agent": WIKI_USER_AGENT, "Accept": "text/html"},
            timeout=DEFAULT_TIMEOUT,
        )
    finally:
        if session is None:
            sess.close()
    if resp.status_code != 200:
        raise RuntimeError(f"Wiki fetch {url} -> {resp.status_code}")
    return resp.text


# ---------- HTML helpers (regex-based, no BS4 dep) ----------

_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def _strip_tags(html_fragment: str) -> str:
    text = _TAG_RE.sub(" ", html_fragment or "")
    text = html.unescape(text)
    return _WHITESPACE_RE.sub(" ", text).strip()


# Find <h2>/<h3> section headings and their body until the next heading.
_SECTION_RE = re.compile(
    r"<h([23])[^>]*>(?P<title>.*?)</h\1>(?P<body>.*?)(?=<h[23]\b|\Z)",
    re.DOTALL | re.IGNORECASE,
)

# Find every anchor tag with its href and inner text.
_ANCHOR_RE = re.compile(
    r'<a\b[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<text>.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

# Scoreline inside a link text, e.g. "7 - 3" / "7-3" / "6 : 3"
_SCORELINE_RE = re.compile(
    r"(?P<a>\d+)\s*[-:]\s*(?P<b>\d+)",
)

# A match-link URL.
_MATCH_URL_RE = re.compile(
    r"https?://osu\.ppy\.sh/community/matches/(?P<id>\d+)",
    re.IGNORECASE,
)


def _iter_sections(page_html: str) -> Iterable[tuple[str, str]]:
    """Yield (heading_text, section_html) pairs."""
    for m in _SECTION_RE.finditer(page_html):
        title = _strip_tags(m.group("title"))
        body = m.group("body") or ""
        yield title, body


def _nearest_stage(section_title: str, known_stages: Iterable[str]) -> str | None:
    """Match a heading like 'Quarterfinals', 'Semifinals — Bracket',
    'Round of 16 results' to one of our canonical stage names."""
    norm = section_title.lower()
    # Longest-match wins so 'Grand Finals' beats 'Finals'.
    for stage in sorted(known_stages, key=len, reverse=True):
        if stage.lower() in norm:
            return stage
    return None


# ---------- bracket parsing ----------

def _extract_matches_from_section(
    stage: str, section_html: str
) -> list[BracketMatch]:
    """Pull BracketMatch rows from a section of the page body.

    Strategy: find all osu! community match links, and for each try to
    recover the local context (team names nearby, scoreline from the
    link text).
    """
    rows: list[BracketMatch] = []
    seen_ids: set[int] = set()

    # Snapshot the plain-text version of the section so we can look
    # around each match link for team names.
    plain = _strip_tags(section_html)

    for anchor in _ANCHOR_RE.finditer(section_html):
        href = anchor.group("href") or ""
        if not _MATCH_URL_RE.search(href):
            continue

        mid = parse_match_id(href)
        if mid is None or mid in seen_ids:
            continue
        seen_ids.add(mid)

        link_text = _strip_tags(anchor.group("text"))
        score_a = score_b = None
        sm = _SCORELINE_RE.search(link_text)
        if sm:
            score_a = int(sm.group("a"))
            score_b = int(sm.group("b"))

        # Team names are not reliably recoverable from raw HTML without
        # a proper DOM walk, so we leave them None here. The sync script
        # pairs these match_ids against our CSV-driven scorelines to
        # recover team codes, then later against /matches/{id}.name which
        # usually contains the team names verbatim.
        rows.append(
            BracketMatch(
                stage=stage,
                team_a=None,
                team_b=None,
                score_a=score_a,
                score_b=score_b,
                match_link=f"https://osu.ppy.sh/community/matches/{mid}",
                match_id=mid,
            )
        )

    # Fallback: some wiki pages list bare match URLs outside of anchor
    # tags (pre-formatted or in wiki markup). Harvest those too.
    for m in _MATCH_URL_RE.finditer(plain):
        mid = int(m.group("id"))
        if mid in seen_ids:
            continue
        seen_ids.add(mid)
        rows.append(
            BracketMatch(
                stage=stage,
                team_a=None,
                team_b=None,
                score_a=None,
                score_b=None,
                match_link=f"https://osu.ppy.sh/community/matches/{mid}",
                match_id=mid,
            )
        )

    return rows


def parse_bracket_page(
    page_html: str, config: TournamentWikiConfig
) -> list[BracketMatch]:
    """Walk a wiki page, partition by stage headings, extract matches."""
    results: list[BracketMatch] = []
    for title, body in _iter_sections(page_html):
        stage = _nearest_stage(title, config.known_stages)
        if stage is None:
            continue
        results.extend(_extract_matches_from_section(stage, body))
    return results


# ---------- mappool parsing ----------

# Mappool sections on the osu! wiki are typically rendered as tables with
# columns like: Slot | Song | Length | BPM | SR | Link. The link column
# contains an anchor to https://osu.ppy.sh/beatmaps/<id>. We only need
# (slot, map title, beatmap_id) — SR we can re-pull cheaply via osu_api.

_BEATMAP_URL_RE = re.compile(
    r"https?://osu\.ppy\.sh/beatmaps/(?P<id>\d+)",
    re.IGNORECASE,
)
_SLOT_RE = re.compile(r"\b(NM|HD|HR|DT|FM|EZ|HT|TB)\d*\b", re.IGNORECASE)
_TABLE_ROW_RE = re.compile(r"<tr\b[^>]*>(?P<body>.*?)</tr>", re.DOTALL | re.IGNORECASE)
_TABLE_CELL_RE = re.compile(r"<t[dh]\b[^>]*>(?P<body>.*?)</t[dh]>", re.DOTALL | re.IGNORECASE)


def _extract_mappool_from_section(
    stage: str, section_html: str
) -> list[MappoolEntry]:
    entries: list[MappoolEntry] = []
    for row in _TABLE_ROW_RE.finditer(section_html):
        cells_raw = [
            c.group("body") for c in _TABLE_CELL_RE.finditer(row.group("body"))
        ]
        if len(cells_raw) < 2:
            continue

        cells_text = [_strip_tags(c) for c in cells_raw]
        joined = " | ".join(cells_text)

        slot_m = _SLOT_RE.search(joined)
        if not slot_m:
            continue
        slot = slot_m.group(0).upper()

        beatmap_id: int | None = None
        map_name: str | None = None
        for raw in cells_raw:
            bm = _BEATMAP_URL_RE.search(raw)
            if bm:
                beatmap_id = int(bm.group("id"))
                # The anchor text usually is the song title.
                anc = _ANCHOR_RE.search(raw)
                if anc:
                    map_name = _strip_tags(anc.group("text")) or None
                break

        if beatmap_id is None and map_name is None:
            continue

        entries.append(
            MappoolEntry(
                stage=stage,
                slot=slot,
                beatmap_id=beatmap_id,
                map_name=map_name,
                difficulty_name=None,
            )
        )
    return entries


def parse_mappool_page(
    page_html: str, config: TournamentWikiConfig
) -> list[MappoolEntry]:
    results: list[MappoolEntry] = []
    for title, body in _iter_sections(page_html):
        stage = _nearest_stage(title, config.known_stages)
        if stage is None:
            continue
        results.extend(_extract_mappool_from_section(stage, body))
    return results


# ---------- high-level entry points ----------

def fetch_bracket(
    config: TournamentWikiConfig,
    *,
    session: requests.Session | None = None,
) -> TournamentBracket:
    """Fetch bracket + (if configured) mappool for one tournament."""
    page_html = _get(config.bracket_url, session=session)
    matches = parse_bracket_page(page_html, config)

    mappool: list[MappoolEntry] = []
    if config.mappool_url:
        if config.mappool_url == config.bracket_url:
            mappool = parse_mappool_page(page_html, config)
        else:
            mappool_html = _get(config.mappool_url, session=session)
            mappool = parse_mappool_page(mappool_html, config)

    return TournamentBracket(
        event=config.event,
        matches=matches,
        mappool=mappool,
    )


def fetch_owc_2025_bracket(
    session: requests.Session | None = None,
) -> TournamentBracket:
    """Convenience: fetch OWC 2025 with the default config."""
    return fetch_bracket(OWC_2025, session=session)
