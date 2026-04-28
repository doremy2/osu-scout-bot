from __future__ import annotations

import hashlib
import html
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


USER_AGENT = "osu-scout/1.0 (historical-tournament-index)"
OSU_WIKI_TOURNAMENTS_URL = "https://osu.ppy.sh/wiki/en/Tournaments"
OSU_WIKI_GITHUB_TREE_URL = "https://api.github.com/repos/ppy/osu-wiki/git/trees/master?recursive=1"
FORUM_55_URL = "https://osu.ppy.sh/community/forums/55"

YEAR_RE = re.compile(r"\b(20(?:2[0-6]|1[9]))\b")
FORMAT_RE = re.compile(r"\b(\d)\s*(?:v|vs\.?|versus)\s*(\d)\b", re.IGNORECASE)
TEAM_SIZE_RE = re.compile(r"\b(\d)\s*(?:players?|members?)\s*(?:per\s*team|teams?)\b", re.IGNORECASE)
RANK_RANGE_RE = re.compile(
    r"(?:rank\s*(?:range|limit)?|bws|badge\s*weighted)[:\s#]*"
    r"([0-9][0-9,k]*)\s*[-–]\s*#?([0-9][0-9,k]*)",
    re.IGNORECASE,
)
NO_RANK_LIMIT_RE = re.compile(r"no\s+rank\s+(?:limit|restriction)", re.IGNORECASE)

LINK_RE = re.compile(r'https?://[^\s"<>\\)]+', re.IGNORECASE)
WIKI_LINK_RE = re.compile(r'href="([^"]*/wiki/[^"]+)"[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
FORUM_TOPIC_RE = re.compile(
    r'<a[^>]*href="(https://osu\.ppy\.sh/community/forums/topics/(\d+)(?:\?[^"]*)?)"[^>]*>'
    r"(.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
FORUM_TOPIC_BLOCK_RE = re.compile(
    r'<div[^>]+class="[^"]*forum-topic-entry[^"]*"[^>]*>(.*?)'
    r'(?=<div[^>]+class="[^"]*forum-topic-entry[^"]*"|<div[^>]+class="[^"]*pagination|</div>\s*</div>\s*</div>\s*</div>\s*</div>\s*<div class="osu-page-footer")',
    re.IGNORECASE | re.DOTALL,
)
NEXT_PAGE_RE = re.compile(r'<a[^>]*class="[^"]*pagination-next[^"]*"[^>]*href="([^"]+)"', re.IGNORECASE)
PAGINATION_LINK_RE = re.compile(r'href="([^"]*/community/forums/55\?page=(\d+)[^"]*)"', re.IGNORECASE)
TITLE_LINK_RE = re.compile(
    r'<a[^>]*href="(https://osu\.ppy\.sh/community/forums/topics/(\d+)(?:\?[^"]*)?)"[^>]*class="[^"]*forum-topic-entry__title[^"]*"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
AUTHOR_RE = re.compile(r"\bby\s*<a[^>]*>(?:<span[^>]*></span>)?([^<]+)</a>", re.IGNORECASE | re.DOTALL)
LAST_REPLY_AUTHOR_RE = re.compile(r"last reply by\s*<a[^>]*>(?:<span[^>]*></span>)?([^<]+)</a>", re.IGNORECASE | re.DOTALL)
TIME_RE = re.compile(r"<time[^>]+datetime=['\"]([^'\"]+)['\"]", re.IGNORECASE)
TOPIC_POSTED_RE = re.compile(r"posted\s*<time[^>]+datetime=['\"]([^'\"]+)['\"]", re.IGNORECASE)
FIRST_POST_AUTHOR_RE = re.compile(r'data-post-position="1"[^>]*data-post-username="([^"]+)"|data-post-username="([^"]+)"[^>]*data-post-position="1"', re.IGNORECASE | re.DOTALL)

SPREADSHEET_RE = re.compile(r"https?://docs\.google\.com/spreadsheets/d/[A-Za-z0-9_-]+(?:/[^\s\"<>]*)?")
BRACKET_RE = re.compile(r"https?://(?:www\.)?(?:challonge\.com|brackethq\.com|battlefy\.com|start\.gg)/[^\s\"<>]+", re.IGNORECASE)
DISCORD_RE = re.compile(r"https?://(?:discord\.gg|discord\.com/invite)/[A-Za-z0-9_-]+", re.IGNORECASE)
MATCH_LINK_RE = re.compile(r"https?://osu\.ppy\.sh/(?:community/matches|mp)/(\d+)", re.IGNORECASE)
LAZER_ROOM_RE = re.compile(r"https?://osu\.ppy\.sh/(?:multiplayer/rooms|rooms)/(\d+)", re.IGNORECASE)

NON_STANDARD_RE = re.compile(
    r"\b(?:taiko|catch|ctb|mania|4k|7k|o!m|osu!mania|fruits?|all\s*modes?|multimode)\b",
    re.IGNORECASE,
)
STANDARD_RE = re.compile(r"\b(?:std|standard|o!std|osu!standard|osu! ?std)\b", re.IGNORECASE)
TOURNAMENT_RE = re.compile(
    r"\b(?:tournament|cup|open|world\s*cup|championship|league|draft|battle|showdown|masters|invitational)\b",
    re.IGNORECASE,
)
MISC_THREAD_RE = re.compile(
    r"\b(?:looking\s+for|support changes|resources|how to|staff lists?|contest|mapping|skin|recruit(?:ing|ment))\b",
    re.IGNORECASE,
)
OPEN_RANK_RE = re.compile(r"\b(?:open rank|no rank limit|#?\s*1\s*[-–]\s*(?:inf|∞)|#?\s*1\s*\+)\b", re.IGNORECASE)
LOW_RANK_CAP_RE = re.compile(r"\b(?:5\s*digit|6\s*digit|5-6\s*digit|100k|999k|500k|6d)\b", re.IGNORECASE)


@dataclass(frozen=True)
class DiscoveryConfig:
    years: tuple[int, ...] = (2025, 2024, 2023, 2022, 2021, 2020)
    request_delay: float = 1.0
    forum_pages: int = 3
    forum_start_page: int = 1
    enrich_forum_threads: bool = True


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_html(url: str, *, timeout: float = 20.0) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def fetch_json(url: str, *, timeout: float = 30.0) -> Any:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/vnd.github+json,application/json",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def normalize_url(url: str, *, base_url: str = "https://osu.ppy.sh") -> str:
    cleaned = html.unescape(url).strip()
    if cleaned.startswith("/"):
        cleaned = urljoin(base_url, cleaned)
    parsed = urlparse(cleaned)
    return parsed._replace(fragment="").geturl().rstrip("/")


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"<[^>]+>", "", html.unescape(value))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def unique(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = normalize_url(item)
        key = normalized.casefold()
        if key not in seen:
            seen.add(key)
            result.append(normalized)
    return result


def infer_year(*values: str | None, allowed_years: Iterable[int]) -> int | None:
    allowed = set(allowed_years)
    for value in values:
        if not value:
            continue
        for match in YEAR_RE.findall(value):
            year = int(match)
            if year in allowed:
                return year
    return None


def infer_format(text: str) -> str | None:
    match = FORMAT_RE.search(text)
    if not match:
        return None
    return f"{match.group(1)}v{match.group(2)}"


def infer_team_size(text: str) -> str | None:
    fmt = infer_format(text)
    if fmt and "v" in fmt:
        left, right = fmt.split("v", 1)
        return left if left == right else fmt
    match = TEAM_SIZE_RE.search(text)
    return match.group(1) if match else None


def infer_rank_range(text: str) -> str | None:
    match = RANK_RANGE_RE.search(text)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    if NO_RANK_LIMIT_RE.search(text):
        return "no rank limit"
    return None


def is_standard_tournament(text: str) -> bool:
    lowered = text.casefold()
    if MISC_THREAD_RE.search(text):
        return False
    if not TOURNAMENT_RE.search(text):
        return False
    if NON_STANDARD_RE.search(text):
        return False
    has_standard = bool(STANDARD_RE.search(text) or "[std" in lowered or "std]" in lowered)
    return has_standard


def priority_score_for_thread(text: str, row: dict[str, Any]) -> int:
    score = 0
    if OPEN_RANK_RE.search(text):
        score += 45
    if row.get("rank_range") == "no rank limit":
        score += 40
    if not LOW_RANK_CAP_RE.search(text):
        score += 15
    if row.get("linked_match_urls") or row.get("lazer_room_urls"):
        score += 20
    if row.get("spreadsheet_url"):
        score += 15
    if row.get("bracket_url"):
        score += 15
    if row.get("format") in {"1v1", "2v2", "3v3", "4v4"}:
        score += 5
    return score


def should_prioritize_thread(text: str, row: dict[str, Any], *, min_priority_score: int = 20) -> bool:
    if not is_standard_tournament(text):
        return False
    return priority_score_for_thread(text, row) >= min_priority_score


def first_or_none(items: list[str]) -> str | None:
    return items[0] if items else None


def source_key(name: str, year: int, source_url: str) -> str:
    raw = f"{year}|{name.casefold()}|{normalize_url(source_url).casefold()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def quality_for_source(row: dict[str, Any]) -> tuple[str, str]:
    signals: list[str] = []
    if row.get("linked_match_urls") or row.get("lazer_room_urls"):
        signals.append("match_links")
    if row.get("spreadsheet_url"):
        signals.append("spreadsheet")
    if row.get("bracket_url"):
        signals.append("bracket")
    if row.get("wiki_url"):
        signals.append("wiki")
    if row.get("rank_range"):
        signals.append("rank_range")
    if row.get("team_size") or row.get("format"):
        signals.append("format")

    core_signals = {"match_links", "spreadsheet", "bracket"}
    present_core = core_signals.intersection(signals)
    if len(present_core) == 3:
        return "verified", "has match, spreadsheet, and bracket links"
    if len(present_core) >= 2:
        return "high", "has two core sources among match links, spreadsheet, and bracket"
    if "spreadsheet" in signals or "bracket" in signals or "wiki" in signals:
        return "partial", "has at least one structured source"
    return "low", "discovered but missing structured data links"


def make_source_row(
    *,
    tournament_name: str,
    year: int,
    source_url: str,
    source: str,
    source_type: str,
    forum_url: str | None = None,
    wiki_url: str | None = None,
    page_text: str = "",
    spreadsheet_links: list[str] | None = None,
    bracket_links: list[str] | None = None,
    discord_links: list[str] | None = None,
    match_links: list[str] | None = None,
    lazer_room_links: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    text = f"{tournament_name} {page_text[:8000]}"
    spreadsheets = unique(spreadsheet_links or SPREADSHEET_RE.findall(page_text))
    brackets = unique(bracket_links or BRACKET_RE.findall(page_text))
    discords = unique(discord_links or DISCORD_RE.findall(page_text))
    matches = unique(match_links or [f"https://osu.ppy.sh/community/matches/{m}" for m in MATCH_LINK_RE.findall(page_text)])
    rooms = unique(lazer_room_links or [f"https://osu.ppy.sh/multiplayer/rooms/{m}" for m in LAZER_ROOM_RE.findall(page_text)])
    row: dict[str, Any] = {
        "tournament_key": source_key(tournament_name, year, source_url),
        "tournament_name": tournament_name,
        "year": year,
        "source_url": normalize_url(source_url),
        "forum_url": normalize_url(forum_url) if forum_url else None,
        "wiki_url": normalize_url(wiki_url) if wiki_url else None,
        "spreadsheet_url": first_or_none(spreadsheets),
        "bracket_url": first_or_none(brackets),
        "discord_url": first_or_none(discords),
        "forum_author": metadata.get("forum_author") if metadata else None,
        "created_at": metadata.get("created_at") if metadata else None,
        "last_post_at": metadata.get("last_post_at") if metadata else None,
        "rank_range": infer_rank_range(text),
        "team_size": infer_team_size(text),
        "format": infer_format(text),
        "status": "discovered",
        "last_checked_at": utc_now_iso(),
        "source": source,
        "source_type": source_type,
        "linked_match_urls": matches,
        "lazer_room_urls": rooms,
        "linked_source_key": metadata.get("linked_source_key") if metadata else None,
        "metadata_json": {
            "all_spreadsheet_urls": spreadsheets,
            "all_bracket_urls": brackets,
            "all_discord_urls": discords,
            **(metadata or {}),
        },
    }
    quality, notes = quality_for_source(row)
    row["data_quality"] = quality
    row["notes"] = notes
    row["priority_score"] = priority_score_for_thread(text, row)
    return row


def discover_from_wiki(
    *,
    years: Iterable[int],
    enrich_pages: bool = False,
    request_delay: float = 0.5,
) -> list[dict[str, Any]]:
    year_tuple = tuple(years)
    html_text = fetch_html(OSU_WIKI_TOURNAMENTS_URL)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for href, label_html in WIKI_LINK_RE.findall(html_text):
        label = clean_text(label_html)
        if not label:
            continue
        url = normalize_url(href)
        year = infer_year(label, url, allowed_years=year_tuple)
        if year is None:
            continue
        key = f"{year}|{url.casefold()}"
        if key in seen:
            continue
        seen.add(key)
        page_text = label
        if enrich_pages:
            try:
                page_text = fetch_html(url)
                time.sleep(request_delay)
            except (HTTPError, URLError, OSError):
                page_text = label
        rows.append(
            make_source_row(
                tournament_name=label,
                year=year,
                source_url=url,
                wiki_url=url,
                source="osu_wiki",
                source_type="wiki_index",
                page_text=page_text,
                metadata={"discovery_method": "wiki_tournaments_index"},
            )
        )
    if rows:
        return rows
    return discover_from_wiki_github_tree(
        years=year_tuple,
        enrich_pages=enrich_pages,
        request_delay=request_delay,
    )


def discover_from_wiki_github_tree(
    *,
    years: Iterable[int],
    enrich_pages: bool = False,
    request_delay: float = 0.5,
) -> list[dict[str, Any]]:
    year_tuple = tuple(years)
    payload = fetch_json(OSU_WIKI_GITHUB_TREE_URL)
    tree = payload.get("tree") if isinstance(payload, dict) else []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    pattern = re.compile(r"^wiki/Tournaments/([^/]+)/(\d{4})/en\.md$")
    for entry in tree:
        if not isinstance(entry, dict):
            continue
        path = entry.get("path") or ""
        match = pattern.match(path)
        if not match:
            continue
        slug, year_text = match.groups()
        year = int(year_text)
        if year not in year_tuple:
            continue
        wiki_url = f"https://osu.ppy.sh/wiki/en/Tournaments/{slug}/{year}"
        if wiki_url.casefold() in seen:
            continue
        seen.add(wiki_url.casefold())
        tournament_name = f"{slug.replace('_', ' ')} {year}"
        page_text = tournament_name
        if enrich_pages:
            try:
                page_text = fetch_html(wiki_url)
                time.sleep(request_delay)
            except (HTTPError, URLError, OSError):
                page_text = tournament_name
        rows.append(
            make_source_row(
                tournament_name=tournament_name,
                year=year,
                source_url=wiki_url,
                wiki_url=wiki_url,
                source="osu_wiki",
                source_type="github_wiki_tree",
                page_text=page_text,
                metadata={
                    "discovery_method": "osu_wiki_github_tree",
                    "wiki_repo_path": path,
                    "wiki_slug": slug,
                },
            )
        )
    return rows


def parse_int_text(value: str | None) -> int | None:
    if not value:
        return None
    cleaned = re.sub(r"[^0-9]", "", value)
    return int(cleaned) if cleaned else None


def parse_forum_listing_topics(html_text: str) -> list[dict[str, Any]]:
    matches = list(TITLE_LINK_RE.finditer(html_text))
    blocks: list[str] = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(html_text)
        block_start = html_text.rfind('<div', 0, start)
        blocks.append(html_text[max(block_start, 0):end])
    if not blocks:
        blocks = FORUM_TOPIC_BLOCK_RE.findall(html_text)
    if not blocks:
        blocks = html_text.split('class="forum-topic-entry')
    topics: list[dict[str, Any]] = []
    seen: set[str] = set()
    for block in blocks:
        title_match = TITLE_LINK_RE.search(block)
        if not title_match:
            continue
        href, thread_id, label_html = title_match.groups()
        if thread_id in seen:
            continue
        seen.add(thread_id)
        name = clean_text(label_html)
        if not name:
            continue
        times = TIME_RE.findall(block)
        author_match = AUTHOR_RE.search(block)
        last_author_match = LAST_REPLY_AUTHOR_RE.search(block)
        counts = [parse_int_text(match) for match in re.findall(r'<strong class="forum-topic-entry__count">([^<]+)</strong>', block)]
        topics.append(
            {
                "forum_thread_id": int(thread_id),
                "tournament_name": name,
                "forum_url": normalize_url(href),
                "forum_author": clean_text(author_match.group(1)) if author_match else None,
                "last_reply_author": clean_text(last_author_match.group(1)) if last_author_match else None,
                "last_post_at": times[-1] if times else None,
                "created_at": times[0] if len(times) == 1 else None,
                "posts": counts[0] if counts else None,
                "views": counts[1] if len(counts) > 1 else None,
            }
        )
    return topics


def parse_forum_thread_detail(html_text: str) -> dict[str, Any]:
    created_match = TOPIC_POSTED_RE.search(html_text)
    author_match = FIRST_POST_AUTHOR_RE.search(html_text)
    author = None
    if author_match:
        author = author_match.group(1) or author_match.group(2)
    return {
        "created_at": created_match.group(1) if created_match else None,
        "forum_author": clean_text(author),
    }


def next_forum_page_url(html_text: str, current_page: int) -> str | None:
    next_match = NEXT_PAGE_RE.search(html_text)
    if next_match:
        return normalize_url(next_match.group(1))
    candidates: list[tuple[int, str]] = []
    for href, page_text in PAGINATION_LINK_RE.findall(html_text):
        page = int(page_text)
        if page > current_page:
            candidates.append((page, href))
    if not candidates:
        return None
    page, href = min(candidates, key=lambda item: item[0])
    if page != current_page + 1:
        return None
    return normalize_url(href)


def discover_from_forum55(*, config: DiscoveryConfig) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    url: str | None = FORUM_55_URL if config.forum_start_page <= 1 else f"{FORUM_55_URL}?page={config.forum_start_page}#topics"
    page = config.forum_start_page - 1
    pages_seen = 0
    seen_thread_ids: set[str] = set()

    while url:
        page += 1
        pages_seen += 1
        if config.forum_pages > 0 and pages_seen > config.forum_pages:
            break
        try:
            listing_html = fetch_html(url)
        except (HTTPError, URLError, OSError):
            break
        topics = parse_forum_listing_topics(listing_html)
        for topic in topics:
            thread_id = str(topic["forum_thread_id"])
            if thread_id in seen_thread_ids:
                continue
            seen_thread_ids.add(thread_id)
            name = topic["tournament_name"]
            forum_url = topic["forum_url"]
            if not is_standard_tournament(name):
                continue
            detail_text = ""
            detail_meta: dict[str, Any] = {}
            if config.enrich_forum_threads:
                try:
                    detail_text = fetch_html(forum_url)
                    detail_meta = parse_forum_thread_detail(detail_text)
                    time.sleep(config.request_delay)
                except (HTTPError, URLError, OSError):
                    detail_text = ""
            text = f"{name} {detail_text[:8000]}"
            year = infer_year(name, forum_url, detail_text[:4000], topic.get("created_at"), topic.get("last_post_at"), allowed_years=config.years)
            if year is None:
                continue
            row = make_source_row(
                tournament_name=name,
                year=year,
                source_url=forum_url,
                forum_url=forum_url,
                source="forum_55",
                source_type="forum",
                page_text=detail_text or name,
                metadata={
                    "forum_thread_id": thread_id,
                    "forum_author": detail_meta.get("forum_author") or topic.get("forum_author"),
                    "created_at": detail_meta.get("created_at") or topic.get("created_at"),
                    "last_post_at": topic.get("last_post_at"),
                    "last_reply_author": topic.get("last_reply_author"),
                    "posts": topic.get("posts"),
                    "views": topic.get("views"),
                    "discovery_method": "forum_55_listing",
                },
            )
            if not should_prioritize_thread(text, row):
                continue
            rows.append(row)
        url = next_forum_page_url(listing_html, page)
        time.sleep(config.request_delay)
    return rows


def rows_from_discovered_tournaments(rows: Iterable[dict[str, Any]], *, years: Iterable[int]) -> list[dict[str, Any]]:
    allowed_years = tuple(years)
    indexed: list[dict[str, Any]] = []
    for row in rows:
        name = row.get("name") or row.get("tournament_name")
        forum_url = row.get("forum_url")
        if not name or not forum_url:
            continue
        text = " ".join(
            str(row.get(key) or "")
            for key in ("name", "rank_range", "format", "notes", "posted_date", "updated_date")
        )
        year = infer_year(text, forum_url, allowed_years=allowed_years)
        if year is None:
            continue
        indexed.append(
            make_source_row(
                tournament_name=name,
                year=year,
                source_url=forum_url,
                forum_url=forum_url,
                source=row.get("source") or "forum_55",
                source_type="existing_discovered_tournament",
                page_text=text,
                spreadsheet_links=row.get("spreadsheet_links") or [],
                bracket_links=row.get("bracket_links") or [],
                discord_links=row.get("discord_links") or [],
                match_links=row.get("match_links") or [],
                metadata={
                    "discovery_method": "existing_discovered_tournaments",
                    "original_status": row.get("status"),
                    "registration_url": row.get("registration_url"),
                    "mappool_links": row.get("mappool_links") or [],
                },
            )
        )
    return indexed


def dedupe_sources(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    quality_rank = {"verified": 0, "high": 1, "partial": 2, "low": 3}
    for row in rows:
        key = row["tournament_key"]
        existing = best.get(key)
        if existing is None:
            best[key] = row
            continue
        if quality_rank.get(row.get("data_quality"), 99) < quality_rank.get(existing.get("data_quality"), 99):
            best[key] = row
    return list(best.values())
