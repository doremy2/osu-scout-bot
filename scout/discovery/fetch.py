"""Reading discovery sources: which Google Sheets does this page / index sheet point at?

A source is just somewhere public that tends to list tournaments: a forum thread, a wiki page, a community index
spreadsheet. We only follow links to Google Sheets (the one source type the importer understands), and keep the text the
link was written with as a hint for the tournament's name.
"""
from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import parse_qs, unquote, urlparse

import requests

from ..sources.extract import Table
from ..sources.readers import read_google_sheet
from .analysis import clean_name, is_informative

SHEET_URL_RE = re.compile(r"https?://docs\.google\.com/spreadsheets/d/(?:e/)?[A-Za-z0-9_-]+[^\s\"'<>)\]]*", re.I)
_KEY_RE = re.compile(r"/spreadsheets/d/(e/)?([A-Za-z0-9_-]+)")
MAX_PAGE_BYTES = 2_000_000
USER_AGENT = "osu-scout-discovery/1.0 (+https://github.com; tournament analytics)"


@dataclass
class SheetRef:
    url: str
    hint: str
    key: str
    fallback: str = ""        # the linking page's title, used as the name when neither the link text nor the sheet has one


def sheet_key(url: str) -> str | None:
    """Stable identity of a spreadsheet, whatever tab (gid) or view the link points at. Published links (/d/e/...) are
    a different document id space, so they get their own prefix."""
    m = _KEY_RE.search(url or "")
    if not m:
        return None
    return f"gsheet{'-pub' if m.group(1) else ''}:{m.group(2)}"


def normalize_sheet_url(url: str) -> str:
    """Drop tracking parameters and fragments; keep the document (and the tab, which may hold the schedule)."""
    u = urlparse(url)
    q = parse_qs(u.query)
    gid = q.get("gid", [None])[0] or (re.search(r"gid=(\d+)", u.fragment).group(1) if re.search(r"gid=(\d+)", u.fragment) else None)
    base = f"https://docs.google.com{u.path}"
    base = re.sub(r"/(edit|view|pubhtml|htmlview)$", "/edit", base)
    return f"{base}#gid={gid}" if gid and "/edit" in base else base


def _unwrap(href: str) -> str:
    """Forums and Google wrap outgoing links (google.com/url?q=...); the real target is in the query."""
    u = urlparse(href)
    if u.netloc.endswith("google.com") and u.path == "/url":
        q = parse_qs(u.query)
        return unquote((q.get("q") or q.get("url") or [href])[0])
    return href


class _Anchors(HTMLParser):
    """Collects (href, link text, text just before the link) for every anchor."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.found: list[tuple[str, str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []
        self._before = ""
        self._skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "title"):
            self._skip += 1
        elif tag == "a":
            self._href = dict(attrs).get("href")
            self._text = []
        elif tag in ("br", "p", "li", "div", "h1", "h2", "h3", "tr"):
            self._before = ""

    def handle_endtag(self, tag):
        if tag in ("script", "style", "title") and self._skip:
            self._skip -= 1
        elif tag == "a" and self._href is not None:
            self.found.append((self._href, "".join(self._text).strip(), self._before.strip()))
            self._href = None

    def handle_data(self, data):
        if self._skip:
            return
        if self._href is not None:
            self._text.append(data)
        else:
            self._before = (self._before + " " + data)[-160:]


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
_SITE_SUFFIX = re.compile(r"\s+[|·—–]\s+.*$|\s+-\s+osu!.*$", re.I)
_GENERIC_TITLE = re.compile(r"^(osu!?|forum|home|tournaments?|google (sheets|docs)|untitled)$", re.I)


def page_title(html: str) -> str:
    """The page's own title without the site suffix ("North America Tournament 2026 · osu!" -> the first part)."""
    m = _TITLE_RE.search(html)
    if not m:
        return ""
    import html as htmllib
    t = clean_name(_SITE_SUFFIX.sub("", htmllib.unescape(re.sub(r"<[^>]+>", "", m.group(1)))))
    return t if is_informative(t) and not _GENERIC_TITLE.match(t) else ""


def refs_from_html(html: str) -> list[SheetRef]:
    p = _Anchors()
    p.feed(html)
    refs: dict[str, SheetRef] = {}
    for href, text, before in p.found:
        url = _unwrap(href)
        key = sheet_key(url) if "docs.google.com/spreadsheets" in url else None
        if not key:
            continue
        hint = clean_name(text) if is_informative(clean_name(text)) else clean_name(before.rsplit(":", 1)[0] if ":" in before else before)
        if key not in refs or (not refs[key].hint and hint):
            refs[key] = SheetRef(normalize_sheet_url(url), hint if is_informative(hint) else "", key)
    for url in SHEET_URL_RE.findall(html):          # plain-text URLs (markdown, forum code blocks)
        key = sheet_key(url)
        if key and key not in refs:
            refs[key] = SheetRef(normalize_sheet_url(url), "", key)
    out = list(refs.values())
    if len(out) == 1:                      # one sheet on the page: the page title is that tournament's name.
        out[0].fallback = page_title(html)  # (with several sheets the same title would label them all alike)
    return out


def refs_from_tables(tables: list[Table]) -> list[SheetRef]:
    """An index spreadsheet: every cell that links to another sheet, with the rest of its row as the name hint."""
    refs: dict[str, SheetRef] = {}
    for tb in tables:
        for row in tb.rows:
            for cell in row:
                for url in ([cell.link] if cell.link else []) + SHEET_URL_RE.findall(cell.text):
                    key = sheet_key(_unwrap(url))
                    if not key:
                        continue
                    others = [clean_name(c.text) for c in row if c is not cell and not SHEET_URL_RE.search(c.text)]
                    hint = next((h for h in [clean_name(cell.text), *others] if is_informative(h)), "")
                    if key not in refs or (not refs[key].hint and hint):
                        refs[key] = SheetRef(normalize_sheet_url(_unwrap(url)), hint, key)
    return list(refs.values())


def _check_public_http(url: str) -> None:
    u = urlparse(url)
    if u.scheme not in ("http", "https") or not u.hostname:
        raise ValueError("Discovery sources must be http(s) URLs")
    host = u.hostname.lower()
    if host == "localhost" or host.endswith((".local", ".internal")):
        raise ValueError("Discovery sources must be public web addresses")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return
    if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
        raise ValueError("Discovery sources must be public web addresses")


class Fetcher:
    """The network side of discovery, behind one small interface so tests can supply canned pages and sheets."""

    def page_refs(self, url: str) -> list[SheetRef]:
        _check_public_http(url)
        resp = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT}, stream=True)
        resp.raise_for_status()
        body = resp.raw.read(MAX_PAGE_BYTES, decode_content=True)
        return refs_from_html(body.decode(resp.encoding or "utf-8", errors="replace"))

    def sheet_refs(self, url: str) -> list[SheetRef]:
        return refs_from_tables(read_google_sheet(url))

    def sheet_tables(self, url: str) -> list[Table]:
        return read_google_sheet(url)
