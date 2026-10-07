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
from .formats import validate_format

log = logging.getLogger("scout.importer")
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
FINISHED = ("done", "error")
STALE_AFTER = 15 * 60       # a job nobody has touched for this long no longer blocks new imports (seconds)


@dataclass
class ImportRequest:
    name: str
    acronym: str
    slug: str
    sheet_url: str
    format: str
    warmups: int | None = None

    def validate(self) -> None:
        if not self.name.strip():
            raise ValueError("Tournament name is required")
        if not SLUG_RE.match(self.slug):
            raise ValueError("Slug may only contain lowercase letters, numbers and dashes (e.g. 4wc-2026)")
        if "docs.google.com/spreadsheets" not in self.sheet_url:
            raise ValueError("Paste a Google Sheets link (docs.google.com/spreadsheets/...)")
        validate_format(self.format)


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
        return {"id": self.id, "slug": self.request.slug, "phase": self.phase, "message": self.message,
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

    def create(self, req: ImportRequest) -> ImportJob:
        job = ImportJob(id=uuid.uuid4().hex[:12], request=req)
        conn = connect(self.db_path)
        try:
            conn.execute("INSERT INTO import_jobs (id, slug, request, phase, message) VALUES (?, ?, ?, ?, ?)",
                         (job.id, req.slug, json.dumps(asdict(req)), job.phase, job.message))
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
        """An unfinished, recently active job (optionally for one slug)."""
        conn = connect(self.db_path)
        try:
            q = ("SELECT * FROM import_jobs WHERE phase NOT IN ('done', 'error') "
                 "AND (julianday('now') - julianday(updated_at)) * 86400 < ?")
            args: list = [STALE_AFTER]
            if slug:
                q += " AND slug = ?"
                args.append(slug)
            r = conn.execute(q + " ORDER BY created_at DESC LIMIT 1", tuple(args)).fetchone()
            return self._row_to_job(r) if r else None
        finally:
            conn.close()


def _default_client():
    from .osu import OsuClient
    return OsuClient(settings.osu_client_id, settings.osu_client_secret, settings.osu_min_interval)


def _cleanup_ghost(conn, job: ImportJob) -> None:
    """A failed import must not leave an empty tournament behind."""
    t = repo.get_tournament(conn, job.request.slug)
    if job.created and t and not conn.execute(
            "SELECT 1 FROM tournament_matches WHERE tournament_id = ?", (t["id"],)).fetchone():
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
            tid = repo.upsert_tournament(conn, req.slug, req.name, req.acronym or None, req.warmups, req.format)
            found, _new = ingest.discover(conn, tid, req.sheet_url, kind="google_sheet")
            if found == 0:
                raise ValueError("No osu! multiplayer links were found in that sheet. "
                                 "Is it shared as 'anyone with the link can view'?")
            if found > settings.max_matches:
                raise ValueError(f"That sheet has {found} matches; the limit for web imports is {settings.max_matches}.")
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
                job.message = f"Importing matches... {job.done} / {job.total}"
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

    def step(self, job_id: str, client_factory: Callable | None = None) -> ImportJob:
        return step(self.store, job_id, client_factory)

    def get(self, job_id: str) -> ImportJob | None:
        return self.store.get(job_id)

    def any_running(self) -> bool:
        return self.store.running() is not None
