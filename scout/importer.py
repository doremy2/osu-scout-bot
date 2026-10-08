"""Run a full tournament import (sheet -> osu! API -> DB -> ratings) with progress reporting.

Server-side only: it needs the osu! API credentials, which never leave this process.

An import is a *job stored in the database* and advanced by `step()`. Three ways to drive it:
  mode "thread"  a background thread calls step() until the job is finished (local machine, VPS)
  mode "step"    the browser calls step() repeatedly with short requests (serverless hosts, where nothing may run
                 in the background and one request can only live for a minute or so)
  mode "inline"  step() until done, synchronously (tests)
Because the job lives in the database, any server instance can pick it up and continue it.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Callable

from . import ingest
from .analytics import service
from .config import settings
from .db import connect, repo
from .clients import validate_client
from .formats import validate_format

log = logging.getLogger("scout.importer")
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
FINISHED = ("done", "error")
STALE_AFTER = 15 * 60       # a job nobody has touched for this long no longer blocks new imports (seconds)
ORIGINS = ("import", "update", "discovery")     # who started a job: a visitor's import, a scheduled update, an approved candidate


@dataclass
class ImportRequest:
    name: str
    acronym: str
    slug: str
    sheet_url: str
    format: str
    warmups: int | None = None
    keep_metadata: bool = False       # re-import of an existing tournament: leave its name/format/acronym alone
    client: str = "stable"            # osu! client the tournament was played on: stable | lazer
    origin: str = "import"            # import (a visitor) | update (scheduled rescan) | discovery (approved candidate)

    def validate(self) -> None:
        if not self.name.strip():
            raise ValueError("Tournament name is required")
        if len(self.name) > 80 or len(self.acronym) > 12 or len(self.slug) > 48:
            raise ValueError("Name (80), acronym (12) and slug (48) are limited in length")
        if not SLUG_RE.match(self.slug):
            raise ValueError("Slug may only contain lowercase letters, numbers and dashes (e.g. 4wc-2026)")
        if "docs.google.com/spreadsheets" not in self.sheet_url:
            raise ValueError("Paste a Google Sheets link (docs.google.com/spreadsheets/...)")
        validate_format(self.format)
        validate_client(self.client)
        if self.origin not in ORIGINS:
            raise ValueError(f"Unknown import origin {self.origin!r}")


@dataclass
class ImportJob:
    id: str
    request: ImportRequest
    phase: str = "queued"            # queued | scanning | importing | calculating | done | error
    message: str = "Waiting to start"
    found: int = 0
    done: int = 0
    total: int = 0
    failed: int = 0
    not_found: int = 0
    error: str | None = None
    created: bool = False            # this job created the tournament row

    def public(self) -> dict:
        return {"id": self.id, "slug": self.request.slug, "origin": self.request.origin, "phase": self.phase,
                "message": self.message,
                "found": self.found, "done": self.done, "total": self.total, "failed": self.failed,
                "not_found": self.not_found, "error": self.error}


class JobStore:
    """import_jobs table access."""

    def __init__(self, db_path):
        self.db_path = db_path

    def _row_to_job(self, r) -> ImportJob:
        return ImportJob(id=r["id"], request=ImportRequest(**json.loads(r["request"])), phase=r["phase"],
                         message=r["message"], found=r["found"], done=r["done"], total=r["total"], failed=r["failed"],
                         not_found=r["not_found"], error=r["error"], created=bool(r["created"]))

    def create(self, req: ImportRequest, phase: str = "queued", message: str = "Waiting to start",
               found: int = 0, total: int = 0) -> ImportJob:
        job = ImportJob(id=uuid.uuid4().hex[:12], request=req, phase=phase, message=message, found=found, total=total)
        conn = connect(self.db_path)
        try:
            conn.execute("INSERT INTO import_jobs (id, slug, request, phase, message, found, total, origin) "
                         "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                         (job.id, req.slug, json.dumps(asdict(req)), phase, message, found, total, req.origin))
            conn.commit()
        finally:
            conn.close()
        return job

    def get(self, job_id: str) -> ImportJob | None:
        conn = connect(self.db_path)
        try:
            r = conn.execute("SELECT * FROM import_jobs WHERE id = ?", (job_id,)).fetchone()
            return self._row_to_job(r) if r else None
        finally:
            conn.close()

    def save(self, job: ImportJob) -> None:
        conn = connect(self.db_path)
        try:
            conn.execute(
                """UPDATE import_jobs SET phase = ?, message = ?, found = ?, done = ?, total = ?, failed = ?,
                          not_found = ?, error = ?, created = ?, updated_at = datetime('now') WHERE id = ?""",
                (job.phase, job.message, job.found, job.done, job.total, job.failed, job.not_found, job.error,
                 int(job.created), job.id))
            conn.commit()
        finally:
            conn.close()

    def running(self, slug: str | None = None) -> ImportJob | None:
        """An unfinished, recently active job (optionally for one slug). Scheduled jobs (update / discovery) are driven by
        a timer that may be hours apart, so they never go stale the way a visitor's abandoned import does."""
        conn = connect(self.db_path)
        try:
            q = ("SELECT * FROM import_jobs WHERE phase NOT IN ('done', 'error') "
                 "AND (origin != 'import' OR (julianday('now') - julianday(updated_at)) * 86400 < ?)")
            args: list = [STALE_AFTER]
            if slug:
                q += " AND slug = ?"
                args.append(slug)
            r = conn.execute(q + " ORDER BY created_at DESC LIMIT 1", tuple(args)).fetchone()
            return self._row_to_job(r) if r else None
        finally:
            conn.close()

    def resumable(self, idle_for: float = 30.0) -> list[ImportJob]:
        """Unfinished scheduled jobs nobody is working on right now (oldest first). A job touched within `idle_for`
        seconds is being driven by someone else, so it is skipped rather than fetched twice."""
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM import_jobs WHERE phase NOT IN ('done', 'error') AND origin != 'import' "
                "AND (julianday('now') - julianday(updated_at)) * 86400 >= ? ORDER BY created_at", (idle_for,)).fetchall()
            return [self._row_to_job(r) for r in rows]
        finally:
            conn.close()


def purge_abandoned(db_path) -> list[str]:
    """Delete tournaments left half-imported by a web import that died: matches still waiting to be fetched, and the
    latest import job for the slug is unfinished and either errored or has gone quiet. Only tournaments a visitor's web
    import created are ever candidates: command-line tournaments, scheduled updates and approved candidates are
    driven by a timer and are left alone."""
    conn = connect(db_path)
    removed: list[str] = []
    try:
        rows = conn.execute(
            """SELECT t.slug FROM tournaments t
               WHERE EXISTS (SELECT 1 FROM tournament_matches m WHERE m.tournament_id = t.id AND m.status = 'pending')
                 AND EXISTS (SELECT 1 FROM import_jobs j WHERE j.slug = t.slug AND j.created = 1 AND j.origin = 'import')
                 AND NOT EXISTS (SELECT 1 FROM import_jobs j WHERE j.slug = t.slug AND (
                       j.phase = 'done'
                       OR (j.phase != 'error' AND (julianday('now') - julianday(j.updated_at)) * 86400 < ?)))""",
            (STALE_AFTER,)).fetchall()
        for r in rows:
            if repo.delete_tournament(conn, r["slug"]):
                removed.append(r["slug"])
                service.invalidate(r["slug"])
    finally:
        conn.close()
    return removed


def _default_client():
    from .osu import OsuClient
    return OsuClient(settings.osu_client_id, settings.osu_client_secret, settings.osu_min_interval)


def _cleanup_ghost(conn, job: ImportJob) -> None:
    """A failed import must not leave a tournament behind that this job created and never managed to import anything into."""
    t = repo.get_tournament(conn, job.request.slug)
    if job.created and t and not conn.execute(
            "SELECT 1 FROM tournament_matches WHERE tournament_id = ? AND status = 'imported'", (t["id"],)).fetchone():
        conn.execute("DELETE FROM tournaments WHERE id = ?", (t["id"],))
        conn.commit()


def step(store: JobStore, job_id: str, client_factory: Callable | None = None, budget: float | None = None) -> ImportJob:
    """Do up to `budget` seconds of work on a job and return its new state. Safe to call on a finished job."""
    job = store.get(job_id)
    if job is None:
        raise KeyError(job_id)
    if job.phase in FINISHED:
        return job
    budget = settings.step_budget if budget is None else budget
    deadline = time.monotonic() + budget
    req = job.request
    conn = connect(store.db_path)
    try:
        # ---- 1. scan the sheet, register the lobbies ------------------------------------------
        if job.phase in ("queued", "scanning"):
            job.phase, job.message = "scanning", "Scanning sheet..."
            store.save(job)
            (client_factory or _default_client)()               # fails fast if credentials are missing
            job.created = repo.get_tournament(conn, req.slug) is None
            if req.keep_metadata and not job.created:
                tid = repo.upsert_tournament(conn, req.slug)
            else:
                tid = repo.upsert_tournament(conn, req.slug, req.name, req.acronym or None, req.warmups, req.format,
                                             req.client)
            found, _new = ingest.discover(conn, tid, req.sheet_url, kind="google_sheet")
            repo.set_source(conn, tid, req.sheet_url, "google_sheet", overwrite=not req.keep_metadata)
            if found == 0:
                raise ValueError("No osu! multiplayer links were found in that sheet. "
                                 "Is it shared as 'anyone with the link can view'?")
            if found > settings.max_matches:
                raise ValueError(f"That sheet has {found} matches; the limit for web imports is {settings.max_matches}.")
            kinds = {r["kind"]: r["n"] for r in conn.execute(
                "SELECT kind, COUNT(*) AS n FROM tournament_matches WHERE tournament_id = ? GROUP BY kind", (tid,))}
            if kinds.get("room") and req.client == "stable":
                raise ValueError("This sheet links lazer multiplayer rooms (osu.ppy.sh/multiplayer/rooms/...). "
                                 "Choose the Lazer client and import again.")
            if req.client == "lazer" and not kinds.get("room"):
                raise ValueError("No lazer multiplayer room links (osu.ppy.sh/multiplayer/rooms/...) were found in that "
                                 "sheet. If the tournament was played on stable, choose the Stable client.")
            job.found = found
            job.total = len(repo.pending_matches(conn, tid))
            job.phase, job.message = "importing", f"Found {found} multiplayer matches"
            store.save(job)

        tid = repo.get_tournament(conn, req.slug)["id"]

        # ---- 2. fetch + store lobbies until the time budget is used --------------------------
        if job.phase == "importing":
            client = (client_factory or _default_client)()
            for m in repo.pending_matches(conn, tid):
                status, _detail = ingest.fetch_one(conn, m, client, use_cache=not getattr(conn, "is_remote", False))
                if status == "failed":
                    job.failed += 1
                elif status == "not_found":
                    job.not_found += 1
                job.done += 1
                job.message = f"{'Importing new matches' if req.origin == 'update' else 'Importing matches'}... {job.done} / {job.total}"
                store.save(job)
                if time.monotonic() > deadline:      # always finishes at least one lobby per step, so it can't stall
                    break
            if not repo.pending_matches(conn, tid):
                job.phase, job.message = "calculating", "Calculating ratings..."
                store.save(job)
            return job

        # ---- 3. classify games, derive teams, warm the report ---------------------------------
        if job.phase == "calculating":
            ingest.finalize(conn, tid)
            service.invalidate(req.slug)
            service.get_analysis(conn, req.slug)
            job.phase, job.message = "done", "Tournament ready ✓"
            store.save(job)
        return job
    except Exception as e:  # noqa: BLE001 - surfaced to the UI verbatim (never contains secrets)
        log.exception("import failed")
        job.phase, job.error, job.message = "error", str(e), "Import failed"
        try:
            _cleanup_ghost(conn, job)
            t = repo.get_tournament(conn, req.slug)
            if t is not None and req.origin == "update":
                repo.record_check(conn, t["id"], error=str(e))
        finally:
            store.save(job)
        return job
    finally:
        conn.close()


class JobRegistry:
    """What the web server talks to."""

    def __init__(self, db_path):
        self.store = JobStore(db_path)

    def start(self, req: ImportRequest, client_factory: Callable | None = None, mode: str = "thread") -> ImportJob:
        req.validate()
        running = self.store.running(req.slug)
        if running:
            return running
        job = self.store.create(req)
        if mode == "thread":
            threading.Thread(target=self._drive, args=(job.id, client_factory), daemon=True).start()
        elif mode == "inline":
            self._drive(job.id, client_factory)
            job = self.store.get(job.id) or job
        return job

    def _drive(self, job_id: str, client_factory: Callable | None) -> None:
        while True:
            job = step(self.store, job_id, client_factory, budget=float("inf"))
            if job.phase in FINISHED:
                return

    def drive_in_background(self, job_id: str, client_factory: Callable | None = None) -> None:
        threading.Thread(target=self._drive, args=(job_id, client_factory), daemon=True).start()

    def step(self, job_id: str, client_factory: Callable | None = None) -> ImportJob:
        return step(self.store, job_id, client_factory)

    def get(self, job_id: str) -> ImportJob | None:
        return self.store.get(job_id)

    def any_running(self) -> bool:
        return self.store.running() is not None
