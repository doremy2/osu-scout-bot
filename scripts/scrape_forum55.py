"""Scrape osu! Forum 55 (Tournaments) for tournament thread discovery.

Walks paginated forum listing pages, extracts thread metadata,
then visits each thread to extract links (spreadsheets, brackets,
mappools, Discord, match links).

Usage:
    python -m scripts.scrape_forum55
    python -m scripts.scrape_forum55 --pages 5 --dry-run
    python -m scripts.scrape_forum55 --pages 0   # 0 = all pages
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlparse, parse_qs
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


FORUM_URL = "https://osu.ppy.sh/community/forums/55"
USER_AGENT = "osu-scout/1.0 (tournament-discovery)"
REQUEST_DELAY = 1.5  # seconds between requests to be polite


# ─── HTTP helpers ────────────────────────────────────────────────

def _fetch_html(url: str, *, timeout: float = 10.0) -> str:
    req = Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
    })
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


# ─── Forum listing parser ───────────────────────────────────────

# Regex to match forum topic links in the listing page
TOPIC_LINK_RE = re.compile(
    r'<a[^>]*href="(https://osu\.ppy\.sh/community/forums/topics/(\d+)(?:\?[^"]*)?)"[^>]*>'
    r'(.*?)</a>',
    re.DOTALL,
)

# Pagination: find "next" page link
NEXT_PAGE_RE = re.compile(
    r'<a[^>]*class="[^"]*pagination-next[^"]*"[^>]*href="([^"]+)"',
    re.DOTALL,
)

# Date patterns in forum listing
DATE_RE = re.compile(r'(\d{4}-\d{2}-\d{2})')
RELATIVE_DATE_RE = re.compile(r'data-tooltip="([^"]+)"')


def parse_forum_listing(html: str) -> list[dict]:
    """Extract thread entries from a forum listing page."""
    threads = []
    seen_ids: set[int] = set()

    for match in TOPIC_LINK_RE.finditer(html):
        url = match.group(1).split("?")[0]  # strip query params
        thread_id = int(match.group(2))
        raw_title = match.group(3).strip()

        if thread_id in seen_ids:
            continue
        seen_ids.add(thread_id)

        # Clean HTML tags from title
        title = re.sub(r'<[^>]+>', '', raw_title).strip()
        if not title:
            continue

        # Skip stickied / meta threads
        lower_title = title.lower()
        if any(skip in lower_title for skip in [
            "tournament listing", "forum rules", "how to host",
            "pinned", "sticky", "megathread",
        ]):
            continue

        threads.append({
            "forum_thread_id": thread_id,
            "name": title,
            "forum_url": url,
        })

    return threads


def find_next_page_url(html: str) -> str | None:
    """Find the URL of the next page in pagination."""
    m = NEXT_PAGE_RE.search(html)
    if m:
        url = m.group(1).replace("&amp;", "&")
        if url.startswith("/"):
            url = f"https://osu.ppy.sh{url}"
        return url
    return None


# ─── Thread detail parser ───────────────────────────────────────

# Link patterns to extract from thread body
SPREADSHEET_RE = re.compile(
    r'https?://docs\.google\.com/spreadsheets/d/[a-zA-Z0-9_-]+(?:/[^\s"<]*)?',
)
BRACKET_RE = re.compile(
    r'https?://(?:challonge\.com|www\.challonge\.com)/[^\s"<]+',
)
MAPPOOL_RE = re.compile(
    r'https?://(?:osu\.ppy\.sh/beatmaps/packs/|osucollector\.com/)[^\s"<]+',
)
DISCORD_RE = re.compile(
    r'https?://(?:discord\.gg|discord\.com/invite)/[a-zA-Z0-9]+',
)
MATCH_LINK_RE = re.compile(
    r'https?://osu\.ppy\.sh/(?:community/matches|mp)/(\d+)',
)
REGISTRATION_RE = re.compile(
    r'https?://(?:docs\.google\.com/forms/|forms\.gle/)[^\s"<]+',
)

# Format detection
FORMAT_RE = re.compile(
    r'\b(\d)v(\d)\b|\b(\d)\s*vs?\s*(\d)\b',
    re.IGNORECASE,
)
RANK_RANGE_RE = re.compile(
    r'(?:rank\s*(?:range)?|bws|badge\s*weighted)[:\s]*'
    r'(?:#?\s*)?(\d[\d,k]*)\s*[-–]\s*(?:#?\s*)?(\d[\d,k]*)',
    re.IGNORECASE,
)
NO_RANK_LIMIT_RE = re.compile(
    r'no\s+rank\s+(?:limit|restriction)',
    re.IGNORECASE,
)
GAME_MODE_RE = re.compile(
    r'\b(osu!?(?:std|standard)?|osu!?taiko|osu!?catch|osu!?mania)\b',
    re.IGNORECASE,
)


def _unique_list(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = item.lower().rstrip("/")
        if key not in seen:
            seen.add(key)
            out.append(item)
    return out


def parse_thread_detail(html: str, thread_info: dict) -> dict:
    """Extract links and metadata from a thread page."""
    # Only look at the first post (OP) — find the first post body
    # The OP is usually in the first .bbcode or .forum-post-content block
    # For safety, use the full page but prefer first ~50% of content
    first_half = html[:len(html) // 2]
    search_text = first_half if len(html) > 20000 else html

    result = dict(thread_info)

    result["spreadsheet_links"] = _unique_list(SPREADSHEET_RE.findall(search_text))
    result["bracket_links"] = _unique_list(BRACKET_RE.findall(search_text))
    result["mappool_links"] = _unique_list(MAPPOOL_RE.findall(search_text))
    result["discord_links"] = _unique_list(DISCORD_RE.findall(search_text))
    result["match_links"] = _unique_list(
        [f"https://osu.ppy.sh/community/matches/{m}" for m in MATCH_LINK_RE.findall(search_text)]
    )

    reg = REGISTRATION_RE.findall(search_text)
    result["registration_url"] = reg[0] if reg else None

    # Format detection
    fmt_match = FORMAT_RE.search(thread_info.get("name", "") + " " + search_text[:5000])
    if fmt_match:
        a = fmt_match.group(1) or fmt_match.group(3)
        b = fmt_match.group(2) or fmt_match.group(4)
        if a and b:
            result["format"] = f"{a}v{b}"

    # Rank range
    rank_match = RANK_RANGE_RE.search(search_text[:5000])
    if rank_match:
        result["rank_range"] = f"{rank_match.group(1)}-{rank_match.group(2)}"
    elif NO_RANK_LIMIT_RE.search(search_text[:5000]):
        result["rank_range"] = "no rank limit"

    # Game mode
    mode_match = GAME_MODE_RE.search(thread_info.get("name", ""))
    if mode_match:
        mode_text = mode_match.group(1).lower().replace("!", "")
        if "taiko" in mode_text:
            result["game_mode"] = "taiko"
        elif "catch" in mode_text:
            result["game_mode"] = "catch"
        elif "mania" in mode_text:
            result["game_mode"] = "mania"
        else:
            result["game_mode"] = "osu"

    return result


# ─── Main scraper ───────────────────────────────────────────────

def scrape_forum_listings(*, max_pages: int = 0) -> list[dict]:
    """Scrape forum listing pages to discover tournament threads.

    Args:
        max_pages: Maximum pages to scrape (0 = unlimited).
    """
    all_threads: list[dict] = []
    url: str | None = FORUM_URL
    page = 0

    while url:
        page += 1
        if max_pages > 0 and page > max_pages:
            break

        print(f"  Listing page {page}: {url}")
        try:
            html = _fetch_html(url)
        except (HTTPError, URLError, OSError) as exc:
            print(f"    ! Failed: {exc}")
            break

        threads = parse_forum_listing(html)
        if not threads:
            print(f"    No threads found, stopping.")
            break

        all_threads.extend(threads)
        print(f"    Found {len(threads)} threads (total: {len(all_threads)})")

        url = find_next_page_url(html)
        if url:
            time.sleep(REQUEST_DELAY)

    return all_threads


def enrich_threads(threads: list[dict], *, delay: float = REQUEST_DELAY) -> list[dict]:
    """Visit each thread page to extract links and metadata."""
    enriched = []
    for i, thread in enumerate(threads):
        print(f"  [{i + 1}/{len(threads)}] {thread['name'][:60]}...")
        try:
            html = _fetch_html(thread["forum_url"])
            detail = parse_thread_detail(html, thread)
            detail["scraped_at"] = _utc_now_iso()
            enriched.append(detail)
        except (HTTPError, URLError, OSError) as exc:
            print(f"    ! Failed: {exc}")
            thread["scraped_at"] = _utc_now_iso()
            enriched.append(thread)

        if i < len(threads) - 1:
            time.sleep(delay)

    return enriched


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape osu! Forum 55 for tournaments")
    parser.add_argument("--pages", type=int, default=0, help="Max listing pages (0=all)")
    parser.add_argument("--dry-run", action="store_true", help="Don't save to DB")
    parser.add_argument("--no-enrich", action="store_true", help="Skip thread detail scraping")
    parser.add_argument("--output-json", type=Path, help="Also save to JSON file")
    args = parser.parse_args()

    batch = _utc_now_iso()
    print(f"=== Forum 55 Scraper (batch: {batch}) ===\n")

    print("Phase 1: Scraping listing pages...")
    threads = scrape_forum_listings(max_pages=args.pages)
    print(f"\nDiscovered {len(threads)} tournament threads.\n")

    if not threads:
        return 0

    if not args.no_enrich:
        print("Phase 2: Enriching thread details...")
        threads = enrich_threads(threads)
        print(f"\nEnriched {len(threads)} threads.\n")

    # Add batch info
    for t in threads:
        t["scrape_batch"] = batch
        t["source"] = "forum_55"

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(threads, indent=2, ensure_ascii=False))
        print(f"Saved JSON to {args.output_json}")

    if args.dry_run:
        print(f"[dry-run] Would save {len(threads)} tournaments to DB.")
        for t in threads[:10]:
            links = sum(len(t.get(k) or []) for k in [
                "spreadsheet_links", "bracket_links", "discord_links", "match_links",
            ])
            print(f"  {t['name'][:60]} | links={links} | fmt={t.get('format', '?')}")
        return 0

    from storage import upsert_discovered_tournament
    saved = 0
    for t in threads:
        try:
            upsert_discovered_tournament(t)
            saved += 1
        except Exception as exc:
            print(f"  ! DB error for {t.get('name', '?')}: {exc}")

    print(f"\nSaved {saved}/{len(threads)} tournaments to DB.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
