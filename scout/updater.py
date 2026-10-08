"""Keep imported tournaments up to date.

A tournament remembers the source it was imported from (`source_url`, `source_type`). Every so often the scheduler
rescans that source and compares the lobbies it lists with the ones the tournament already holds:

    scheduled scan -> lobbies the sheet lists now - lobbies we hold = the new ones
                   -> register only those as 'pending'
                   -> an import job (origin "update") downloads just those, then recalculates the analytics

Nothing is ever downloaded twice: existing lobbies are skipped by the diff, and every write is an upsert on
(tournament, lobby id), so running a check any number of times leaves exactly the same rows behind.

A scan is cheap (one spreadsheet download) and only *registers* work; the slow part, calling the osu! API, is a normal
resumable import job. `tick()` does a time-boxed slice of everything due, so it fits a serverless function's time limit,
and whatever does not fit simply continues on the next tick.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from . import ingest
from .analytics import service
from .config import settings
from .db import connect, repo
from .importer import FINISHED, ImportJob, ImportRequest, JobStore, step

log = logging.getLogger("scout.updater")

# How often a tournament's source is looked at, by how recently something happened in it.
LIVE_WINDOW = timedelta(days=3)         # something changed in the last 3 days: the tournament is "running"
SCHEDULE = (                            # (age of last activity, seconds between checks)
    (LIVE_WINDOW, 15 * 60),
    (timedelta(days=30), 3 * 3600),
    (timedelta(days=90), 24 * 3600),
)
DORMANT_EVERY = 7 * 24 * 3600
BACKOFF_BASE = 30 * 60                  # after a failed look: 30 min, 1 h, 2 h ... at most a week


def _activity(t, now: datetime) -> datetime | None:
    """When something last happened in the tournament: the newest of its last detected change and its last match."""
    seen = [d for d in (repo.parse_stamp(t["last_changed_at"]), repo.parse_stamp(t["end_date"])) if d]
    return max(seen) if seen else None


def check_interval(t, now: datetime) -> float:
    """Seconds to wait between looks at this tournament's source."""
    if t["check_failures"]:
        return min(DORMANT_EVERY, BACKOFF_BASE * 2 ** (t["check_failures"] - 1))
    last = _activity(t, now)
    if last is None:
        return SCHEDULE[0][1]            # nothing known yet: keep an eye on it
    age = now - last
    for limit, every in SCHEDULE:
        if age < limit:
            return every
    return DORMANT_EVERY


def is_due(t, now: datetime) -> bool:
    last = repo.parse_stamp(t["last_checked_at"])
    return last is None or (now - last).total_seconds() >= check_interval(t, now)


def due_tournaments(conn, now: datetime | None = None) -> list:
    now = now or repo.utcnow()
    rows = conn.execute(
        "SELECT * FROM tournaments WHERE auto_update = 1 AND source_url IS NOT NULL AND source_type = 'google_sheet' "
        "ORDER BY COALESCE(last_checked_at, '') ASC, id").fetchall()
    return [t for t in rows if is_due(t, now)]


def _job_for(store: JobStore, t, pending: int, found: int) -> ImportJob:
    """The import job that will download the pending lobbies: the one already running for this tournament, or a new one."""
    running = store.running(t["slug"])
    if running:
        if running.request.origin == "update" and pending > running.total - running.done:
            running.total = running.done + pending            # more lobbies arrived while it was working
            store.save(running)
        return running
    req = ImportRequest(name=t["name"], acronym=t["acronym"] or "", slug=t["slug"], sheet_url=t["source_url"],
                        format=t["format"], client=t["client"], keep_metadata=True, origin="update")
    return store.create(req, phase="importing", message=f"{pending} new matches detected", found=found, total=pending)


def check_tournament(db_path: str | Path, slug: str, now: datetime | None = None,
                     scanner: Callable = ingest.scan_source) -> dict:
    """Rescan one tournament's source and register what is new. Returns a summary; starts (but does not run) an import job
    when there is something to download. Safe to call at any time and any number of times."""
    conn = connect(db_path)
    try:
        t = repo.get_tournament(conn, slug)
        if t is None or not t["source_url"]:
            return {"slug": slug, "state": "no_source"}
        tid = t["id"]
        out = {"slug": slug, "state": "unchanged", "new": 0, "pending": 0, "job": None}
        try:
            scan = scanner(conn, tid, t["source_url"], t["source_type"])
        except Exception as e:  # noqa: BLE001 - a sheet that went private, a Google hiccup... recorded, retried with back-off
            repo.record_check(conn, tid, error=f"{type(e).__name__}: {e}", now=now)
            return {**out, "state": "error", "error": str(e)}
        if scan.hash != t["last_source_hash"]:
            repo.save_pool_slots(conn, tid, scan.slots)
            if scan.new:
                repo.add_match_links(conn, tid, scan.links)          # upsert: existing lobbies are untouched
        out["new"] = len(scan.new)
        repo.record_check(conn, tid, source_hash=scan.hash, new=len(scan.new), now=now)
        repo.requeue_retryable(conn, tid, now)                       # earlier failures that have cooled down
        repo.reopen_live_matches(conn, tid, now)                     # lobbies that may still be running
        pending = len(repo.pending_matches(conn, tid))
        out["pending"] = pending
        if pending:
            job = _job_for(JobStore(db_path), t, pending, len(scan.links))
            out.update(state="updating", job=job.id)
        elif scan.new:
            out["state"] = "changed"
        return out
    finally:
        conn.close()


def drive(store: JobStore, job_id: str, client_factory: Callable | None, deadline: float) -> ImportJob | None:
    """Advance a job until it finishes or the time box is used up. The calculating step is always allowed to run,
    because a job parked right before it would leave a tournament without fresh analytics until the next tick."""
    while True:
        left = deadline - time.monotonic()
        job = store.get(job_id)
        if job is None or job.phase in FINISHED:
            return job
        if left <= 0 and job.phase != "calculating":
            return job
        job = step(store, job_id, client_factory, budget=max(left, 1.0))
        if job.phase in FINISHED:
            return job


def tick(db_path: str | Path, client_factory: Callable | None = None, budget: float | None = None,
         max_checks: int | None = None, now: datetime | None = None, discovery: bool = True,
         scanner: Callable = ingest.scan_source) -> dict:
    """One scheduled run, time-boxed to `budget` seconds:
       1. continue unfinished scheduled jobs (new matches already registered by an earlier run)
       2. rescan the sources that are due and start jobs for the ones with new lobbies
       3. let those jobs download what they can; the rest waits for the next run
       4. look for new tournaments in the discovery sources that are due (queue only, nothing is published)"""
    budget = settings.tick_budget if budget is None else budget
    max_checks = settings.max_checks_per_tick if max_checks is None else max_checks
    deadline = time.monotonic() + budget
    store = JobStore(db_path)
    summary: dict = {"resumed": [], "checked": [], "discovery": None}
    conn = connect(db_path)
    try:
        due = due_tournaments(conn, now)
    finally:
        conn.close()

    for job in store.resumable():
        if time.monotonic() >= deadline:
            break
        done = drive(store, job.id, client_factory, deadline)
        service.invalidate(job.request.slug)
        summary["resumed"].append({"slug": job.request.slug, "phase": done.phase if done else "gone"})

    for t in due[:max_checks]:
        if time.monotonic() >= deadline:
            break
        res = check_tournament(db_path, t["slug"], now, scanner)
        summary["checked"].append(res)
        if res.get("job"):
            drive(store, res["job"], client_factory, deadline)
            service.invalidate(t["slug"])

    if discovery and time.monotonic() < deadline:
        from .discovery import run_due_sources
        summary["discovery"] = run_due_sources(db_path, deadline=deadline, now=now)
    return summary


def tracking_status(db_path: str | Path, slug: str, writable: bool = True, now: datetime | None = None) -> dict | None:
    """What the tournament page shows: Live tracking / last checked / matches tracked / N new matches, updating."""
    now = now or repo.utcnow()
    conn = connect(db_path)
    try:
        t = repo.get_tournament(conn, slug)
        if t is None:
            return None
        counts = {r["status"]: r["n"] for r in conn.execute(
            "SELECT status, COUNT(*) AS n FROM tournament_matches WHERE tournament_id = ? GROUP BY status", (t["id"],))}
    finally:
        conn.close()
    enabled = bool(t["auto_update"] and t["source_url"] and t["source_type"] == "google_sheet" and writable)
    job = JobStore(db_path).running(slug)
    updating = bool(job) and enabled
    pending = counts.get("pending", 0)
    activity = _activity(t, now)
    live = enabled and (activity is not None and now - activity < LIVE_WINDOW)
    last = repo.parse_stamp(t["last_checked_at"])
    nxt = (last + timedelta(seconds=check_interval(t, now))) if (enabled and last) else None
    if not enabled:
        state = "off"
    elif updating:
        state = "updating"
    elif t["last_check_error"]:
        state = "error"
    else:
        state = "live" if live else "tracking"
    return {
        "enabled": enabled, "state": state, "live": bool(live), "updating": updating,
        "source_type": t["source_type"], "source_url": t["source_url"],
        "last_checked_at": repo.iso(t["last_checked_at"]), "last_changed_at": repo.iso(t["last_changed_at"]),
        "next_check_at": nxt.strftime("%Y-%m-%dT%H:%M:%SZ") if nxt else None,
        "matches_tracked": counts.get("imported", 0), "pending": pending,
        "new_detected": max(t["last_new_matches"], pending) if updating else 0,
        "error": t["last_check_error"],
        "job": ({"id": job.id, "phase": job.phase, "done": job.done, "total": job.total} if updating and job else None),
    }
