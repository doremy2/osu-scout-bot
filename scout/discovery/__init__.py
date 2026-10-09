"""Tournament discovery: find tournaments we do not have yet, and queue them for review.

    discovery source (forum thread, wiki page, index sheet)
        -> Google Sheets it links to
        -> analyse each sheet: name, format, client, MP links, confidence          (analysis.py, no network model, no API quota)
        -> drop what we already have (same sheet, or mostly the same lobbies)
        -> a CANDIDATE in the review queue

Discovery never publishes. A candidate becomes a tournament only when it is approved, and approval is nothing more than
starting the normal import job (origin "discovery"), so a discovered tournament is imported, resumed, rated and tracked
exactly like one a visitor pasted in. `SCOUT_DISCOVERY_AUTO_APPROVE` can later let very confident candidates skip review.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from ..config import settings
from ..db import connect, repo
from ..importer import ImportRequest, JobRegistry
from .analysis import SheetAnalysis, analyze_tables, clean_name, suggest_slug
from .fetch import Fetcher, SheetRef, _check_public_http, sheet_key

KINDS = ("page", "sheet")
MAX_ANALYSES_PER_SCAN = 6                  # sheets downloaded per source per run; the rest are picked up by the next run
RETRY_AFTER = {"no_links": timedelta(hours=24), "error": timedelta(hours=6), "candidate": timedelta(hours=24)}
DUPLICATE_SHARE = 0.5                      # this much of a candidate's lobbies already in one tournament = the same tournament


# ---- sources ------------------------------------------------------------------------------------------------
def add_source(conn, name: str, kind: str, url: str, scan_interval_min: int = 360) -> int:
    name, url = clean_name(name), url.strip()
    if kind not in KINDS:
        raise ValueError(f"Source kind must be one of: {', '.join(KINDS)}")
    if not name:
        raise ValueError("Give the source a name")
    if kind == "sheet":
        if "docs.google.com/spreadsheets" not in url:
            raise ValueError("A sheet source must be a Google Sheets link")
    else:
        _check_public_http(url)
    if conn.execute("SELECT 1 FROM discovery_sources WHERE url = ?", (url,)).fetchone():
        raise ValueError("That source is already registered")
    cur = conn.execute("INSERT INTO discovery_sources (name, kind, url, scan_interval_min) VALUES (?, ?, ?, ?)",
                       (name[:80], kind, url, max(15, int(scan_interval_min))))
    conn.commit()
    return cur.lastrowid


def list_sources(conn) -> list[dict]:
    return [dict(r) | {"last_scanned_at": repo.iso(r["last_scanned_at"]), "created_at": repo.iso(r["created_at"])}
            for r in conn.execute("SELECT * FROM discovery_sources ORDER BY id")]


def set_source_enabled(conn, source_id: int, enabled: bool) -> bool:
    cur = conn.execute("UPDATE discovery_sources SET enabled = ? WHERE id = ?", (1 if enabled else 0, source_id))
    conn.commit()
    return bool(cur.rowcount)


def due_sources(conn, now: datetime | None = None) -> list:
    now = now or repo.utcnow()
    out = []
    for s in conn.execute("SELECT * FROM discovery_sources WHERE enabled = 1 ORDER BY COALESCE(last_scanned_at, ''), id"):
        last = repo.parse_stamp(s["last_scanned_at"])
        if last is None or now - last >= timedelta(minutes=s["scan_interval_min"]):
            out.append(s)
    return out


# ---- dedupe -------------------------------------------------------------------------------------------------
def known_keys(conn) -> dict[str, str]:
    """sheet key -> slug, for every spreadsheet an existing tournament was imported from."""
    out: dict[str, str] = {}
    for r in conn.execute("SELECT t.slug, t.source_url AS url FROM tournaments t WHERE t.source_url IS NOT NULL "
                          "UNION SELECT t.slug, s.location FROM tournament_sources s JOIN tournaments t ON t.id = s.tournament_id"):
        k = sheet_key(r["url"])
        if k:
            out.setdefault(k, r["slug"])
    return out


def overlap_with_existing(conn, ids: list[int]) -> dict[str, int]:
    """slug -> how many of these lobby ids that tournament already holds."""
    found: dict[str, int] = {}
    for i in range(0, len(ids), 400):
        chunk = ids[i:i + 400]
        for r in conn.execute(
                f"SELECT t.slug AS slug, COUNT(*) AS n FROM tournament_matches m JOIN tournaments t ON t.id = m.tournament_id "
                f"WHERE m.osu_match_id IN ({','.join('?' * len(chunk))}) GROUP BY t.slug", chunk):
            found[r["slug"]] = found.get(r["slug"], 0) + r["n"]
    return found


def _taken_slugs(conn) -> set[str]:
    return ({r[0] for r in conn.execute("SELECT slug FROM tournaments")}
            | {r[0] for r in conn.execute("SELECT suggested_slug FROM discovery_candidates WHERE status = 'pending'")})


# ---- scanning -----------------------------------------------------------------------------------------------
def _remember(conn, ref: SheetRef, source_id: int | None, outcome: str, detail: str | None, now: datetime) -> None:
    conn.execute(
        """INSERT INTO discovery_seen (key, source_id, url, hint, outcome, detail, first_seen_at, last_checked_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(key) DO UPDATE SET outcome = excluded.outcome, detail = excluded.detail,
             hint = COALESCE(NULLIF(excluded.hint, ''), discovery_seen.hint), last_checked_at = excluded.last_checked_at""",
        (ref.key, source_id, ref.url, ref.hint, outcome, detail, repo.stamp(now), repo.stamp(now)))


def _worth_looking(conn, ref: SheetRef, now: datetime) -> bool:
    """False for sheets decided on already, or looked at too recently to look again."""
    cand = conn.execute("SELECT status FROM discovery_candidates WHERE key = ?", (ref.key,)).fetchone()
    if cand is not None and cand["status"] != "pending":
        return False                                          # approved / ignored / duplicate: settled
    seen = conn.execute("SELECT outcome, last_checked_at FROM discovery_seen WHERE key = ?", (ref.key,)).fetchone()
    if seen is None:
        return True
    wait = RETRY_AFTER.get(seen["outcome"])
    last = repo.parse_stamp(seen["last_checked_at"])
    return wait is not None and last is not None and now - last >= wait


def _save_candidate(conn, ref: SheetRef, source_id: int | None, a: SheetAnalysis, now: datetime) -> tuple[str, int]:
    ids = [l.osu_match_id for l in a.links]
    overlap = overlap_with_existing(conn, ids)
    best_slug, best_n = max(overlap.items(), key=lambda kv: kv[1], default=(None, 0))
    reasons = list(a.reasons)
    duplicate_of = None
    if best_slug and best_n / len(ids) >= DUPLICATE_SHARE:
        duplicate_of = best_slug
        reasons.append(f"{best_n} of {len(ids)} lobbies are already in “{best_slug}”")
    elif best_slug:
        reasons.append(f"shares {best_n} lobbies with “{best_slug}”")
    row = conn.execute("SELECT id, status, name, suggested_slug FROM discovery_candidates WHERE key = ?", (ref.key,)).fetchone()
    fields = (a.name, a.acronym or None, a.format, a.client, a.match_count, a.rooms, a.confidence, json.dumps(reasons))
    if row is not None:
        # only a pending candidate is touched; once approved its slug (and the tournament URL) never changes
        slug = row["suggested_slug"]
        if row["status"] == "pending" and a.name != row["name"]:
            slug = suggest_slug(a, _taken_slugs(conn) - {row["suggested_slug"]})
        conn.execute("UPDATE discovery_candidates SET name = ?, acronym = ?, detected_format = ?, detected_client = ?, "
                     "match_count = ?, room_count = ?, confidence = ?, reasons = ?, suggested_slug = ? "
                     "WHERE id = ? AND status = 'pending'", (*fields, slug, row["id"]))
        return "updated", row["id"]
    status = "duplicate" if duplicate_of else "pending"
    cur = conn.execute(
        """INSERT INTO discovery_candidates (source_id, key, source_url, source_type, name, acronym, suggested_slug,
             detected_format, detected_client, match_count, room_count, confidence, reasons, status, duplicate_of, discovered_at)
           VALUES (?, ?, ?, 'google_sheet', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (source_id, ref.key, ref.url, a.name, a.acronym or None, suggest_slug(a, _taken_slugs(conn)), a.format, a.client,
         a.match_count, a.rooms, a.confidence, json.dumps(reasons), status, duplicate_of, repo.stamp(now)))
    return ("duplicate" if duplicate_of else "created"), cur.lastrowid


def scan_source(db_path: str | Path, source_id: int, fetcher: Fetcher | None = None, deadline: float | None = None,
                now: datetime | None = None, max_analyses: int = MAX_ANALYSES_PER_SCAN) -> dict:
    """Read one discovery source and turn the new tournament-looking sheets into candidates. Idempotent: a sheet that was
    already looked at is not downloaded again until its cool-down passes, and a candidate is keyed by its spreadsheet."""
    fetcher = fetcher or Fetcher()
    now = now or repo.utcnow()
    conn = connect(db_path)
    stats = {"source_id": source_id, "refs": 0, "known": 0, "created": 0, "updated": 0, "duplicate": 0, "no_links": 0,
             "errors": 0, "skipped": 0, "deferred": 0, "candidates": []}
    try:
        src = conn.execute("SELECT * FROM discovery_sources WHERE id = ?", (source_id,)).fetchone()
        if src is None:
            raise KeyError(source_id)
        try:
            refs = fetcher.sheet_refs(src["url"]) if src["kind"] == "sheet" else fetcher.page_refs(src["url"])
        except Exception as e:  # noqa: BLE001 - recorded on the source, retried on its next turn
            conn.execute("UPDATE discovery_sources SET last_scanned_at = ?, last_status = 'error', last_error = ? WHERE id = ?",
                         (repo.stamp(now), f"{type(e).__name__}: {e}"[:300], source_id))
            conn.commit()
            return {**stats, "error": str(e)}
        stats["refs"] = len(refs)
        known = known_keys(conn)
        analysed = 0
        for ref in refs:
            if ref.key in known:
                _remember(conn, ref, source_id, "known", known[ref.key], now)
                stats["known"] += 1
                continue
            if not _worth_looking(conn, ref, now):
                stats["skipped"] += 1
                continue
            if analysed >= max_analyses or (deadline is not None and time.monotonic() >= deadline):
                stats["deferred"] += 1          # not marked as seen, so the next run starts with these
                continue
            analysed += 1
            try:
                a = analyze_tables(fetcher.sheet_tables(ref.url), ref.hint, ref.fallback)
            except Exception as e:  # noqa: BLE001 - private sheet, Google error, broken workbook
                _remember(conn, ref, source_id, "error", f"{type(e).__name__}: {e}"[:300], now)
                stats["errors"] += 1
                continue
            if a.match_count == 0:
                _remember(conn, ref, source_id, "no_links", None, now)
                stats["no_links"] += 1
                continue
            outcome, cid = _save_candidate(conn, ref, source_id, a, now)
            _remember(conn, ref, source_id, "candidate", f"#{cid}", now)
            stats[outcome] += 1
            if outcome in ("created", "updated"):
                stats["candidates"].append({"id": cid, "name": a.name, "confidence": a.confidence})
        conn.execute("UPDATE discovery_sources SET last_scanned_at = ?, last_status = 'ok', last_error = NULL, last_found = ? "
                     "WHERE id = ?", (repo.stamp(now), len(refs), source_id))
        conn.commit()
        return stats
    finally:
        conn.close()


def run_due_sources(db_path: str | Path, deadline: float | None = None, now: datetime | None = None,
                    fetcher: Fetcher | None = None, registry: JobRegistry | None = None,
                    client_factory: Callable | None = None) -> dict:
    """Scan every source that is due, then apply the auto-approval policy (off unless configured)."""
    conn = connect(db_path)
    try:
        due = [s["id"] for s in due_sources(conn, now)]
    finally:
        conn.close()
    out: dict = {"scanned": [], "auto_approved": []}
    for sid in due:
        if deadline is not None and time.monotonic() >= deadline:
            break
        out["scanned"].append(scan_source(db_path, sid, fetcher, deadline, now))
    if settings.discovery_auto_approve > 0:
        out["auto_approved"] = auto_approve(db_path, registry or JobRegistry(db_path), client_factory)
    return out


def auto_approve(db_path: str | Path, registry: JobRegistry, client_factory: Callable | None = None,
                 threshold: float | None = None) -> list[str]:
    """Approve pending candidates at or above the confidence threshold. Disabled (threshold 0) by default."""
    threshold = settings.discovery_auto_approve if threshold is None else threshold
    if threshold <= 0:
        return []
    conn = connect(db_path)
    try:
        ids = [r["id"] for r in conn.execute(
            "SELECT id FROM discovery_candidates WHERE status = 'pending' AND confidence >= ? AND match_count >= 8 "
            "ORDER BY confidence DESC", (threshold,))]
    finally:
        conn.close()
    done = []
    for cid in ids:
        try:
            done.append(approve_candidate(db_path, cid, registry, client_factory, mode="queue")["candidate"]["tournament_slug"])
        except (ValueError, KeyError):
            continue
    return done


# ---- the review queue -----------------------------------------------------------------------------------------
def _candidate_dict(r, job=None) -> dict:
    d = dict(r)
    d["reasons"] = json.loads(d.get("reasons") or "[]")
    d["discovered_at"] = repo.iso(d["discovered_at"])
    d["decided_at"] = repo.iso(d.get("decided_at"))
    d["job"] = None
    if job is not None:
        d["job"] = {"id": job["id"], "phase": job["phase"], "done": job["done"], "total": job["total"], "error": job["error"]}
    return d


def list_candidates(conn, status: str = "pending") -> list[dict]:
    where, args = ("", ()) if status == "all" else ("WHERE c.status = ?", (status,))
    rows = conn.execute(
        f"""SELECT c.*, j.id AS j_id, j.phase AS j_phase, j.done AS j_done, j.total AS j_total, j.error AS j_error
            FROM discovery_candidates c LEFT JOIN import_jobs j ON j.id = c.job_id {where}
            ORDER BY CASE c.status WHEN 'pending' THEN 0 ELSE 1 END, c.confidence DESC, c.discovered_at DESC""", args).fetchall()
    out = []
    for r in rows:
        job = {"id": r["j_id"], "phase": r["j_phase"], "done": r["j_done"], "total": r["j_total"], "error": r["j_error"]} if r["j_id"] else None
        d = _candidate_dict({k: r[k] for k in r.keys() if not k.startswith("j_")}, job)
        out.append(d)
    return out


def approve_candidate(db_path: str | Path, candidate_id: int, registry: JobRegistry, client_factory: Callable | None = None,
                      mode: str = "step", overrides: dict | None = None) -> dict:
    """Publish a candidate by starting the normal import job for it. Approving twice returns the same job."""
    ov = overrides or {}
    conn = connect(db_path)
    try:
        row = conn.execute("SELECT * FROM discovery_candidates WHERE id = ?", (candidate_id,)).fetchone()
        if row is None:
            raise KeyError(candidate_id)
        if row["status"] == "approved" and row["job_id"]:
            return {"candidate": _candidate_dict(row), "job": registry.get(row["job_id"])}
        if row["status"] != "pending":
            raise ValueError(f"This candidate is {row['status']}" + (f" (it is “{row['duplicate_of']}”)" if row["duplicate_of"] else ""))
        slug = (ov.get("slug") or row["suggested_slug"]).strip().lower()
        taken = {r[0] for r in conn.execute("SELECT slug FROM tournaments")}
        if slug in taken:
            if ov.get("slug"):
                raise ValueError(f"The slug “{slug}” is already used by another tournament")
            i = 2
            while f"{slug}-{i}" in taken:
                i += 1
            slug = f"{slug}-{i}"
        req = ImportRequest(name=clean_name(ov.get("name") or row["name"]), acronym=(ov.get("acronym") or row["acronym"] or "").strip(),
                            slug=slug, sheet_url=row["source_url"], format=ov.get("format") or row["detected_format"],
                            client=ov.get("client") or row["detected_client"], origin="discovery")
        req.validate()
        job = registry.start(req, client_factory=client_factory, mode=mode)
        conn.execute("UPDATE discovery_candidates SET status = 'approved', job_id = ?, tournament_slug = ?, decided_at = ? "
                     "WHERE id = ? AND status = 'pending'", (job.id, req.slug, repo.stamp(), candidate_id))
        conn.commit()
        row = conn.execute("SELECT * FROM discovery_candidates WHERE id = ?", (candidate_id,)).fetchone()
        return {"candidate": _candidate_dict(row), "job": job}
    finally:
        conn.close()


def check_slug(conn, slug: str) -> str | None:
    """None if the slug can be used for a new tournament, otherwise a plain-language reason."""
    from ..importer import SLUG_RE
    slug = (slug or "").strip()
    if not slug:
        return "Enter a slug."
    if len(slug) > 48 or not SLUG_RE.match(slug):
        return "Use lowercase letters, numbers and single dashes only (max 48 characters), e.g. na-2026."
    if conn.execute("SELECT 1 FROM tournaments WHERE slug = ?", (slug,)).fetchone():
        return f"The slug “{slug}” is already used by another tournament."
    return None


def set_candidate_status(conn, candidate_id: int, status: str) -> bool:
    """ignore: pending -> ignored. restore: ignored (or duplicate) -> pending."""
    if status == "ignored":
        cur = conn.execute("UPDATE discovery_candidates SET status = 'ignored', decided_at = ? WHERE id = ? AND status = 'pending'",
                           (repo.stamp(), candidate_id))
    elif status == "pending":
        cur = conn.execute("UPDATE discovery_candidates SET status = 'pending', decided_at = NULL, duplicate_of = NULL "
                           "WHERE id = ? AND status IN ('ignored', 'duplicate')", (candidate_id,))
    else:
        raise ValueError(status)
    conn.commit()
    return bool(cur.rowcount and cur.rowcount > 0)
