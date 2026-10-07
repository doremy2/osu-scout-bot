"""Run a full tournament import (sheet -> osu! API -> DB -> ratings) with progress reporting.

Used by the web server (in a background thread) and usable from scripts. Server-side only:
it needs the osu! API credentials, which never leave this process.
"""
from __future__ import annotations

import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

from . import ingest
from .analytics import service
from .config import settings
from .db import connect, repo
from .formats import validate_format

log = logging.getLogger("scout.importer")
SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


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
    created: float = field(default_factory=time.time)

    def public(self) -> dict:
        return {"id": self.id, "slug": self.request.slug, "phase": self.phase, "message": self.message,
                "found": self.found, "done": self.done, "total": self.total, "failed": self.failed,
                "not_found": self.not_found, "error": self.error}


class JobRegistry:
    def __init__(self) -> None:
        self.jobs: dict[str, ImportJob] = {}
        self.lock = threading.Lock()

    def running_for(self, slug: str) -> ImportJob | None:
        with self.lock:
            return next((j for j in self.jobs.values()
                         if j.request.slug == slug and j.phase not in ("done", "error")), None)

    def start(self, db_path, req: ImportRequest, client_factory: Callable | None = None,
              threaded: bool = True) -> ImportJob:
        req.validate()
        running = self.running_for(req.slug)
        if running:
            return running
        job = ImportJob(id=uuid.uuid4().hex[:12], request=req)
        with self.lock:
            self.jobs[job.id] = job
        if threaded:
            threading.Thread(target=run_import, args=(db_path, job, client_factory), daemon=True).start()
        else:
            run_import(db_path, job, client_factory)
        return job

    def get(self, job_id: str) -> ImportJob | None:
        return self.jobs.get(job_id)


def _default_client():
    from .osu import OsuClient
    return OsuClient(settings.osu_client_id, settings.osu_client_secret, settings.osu_min_interval)


def run_import(db_path, job: ImportJob, client_factory: Callable | None = None) -> None:
    req = job.request
    conn = None
    created, tid = False, None
    try:
        conn = connect(db_path)
        job.phase, job.message = "scanning", "Scanning sheet..."
        client = (client_factory or _default_client)()      # fails fast if credentials are missing
        created = repo.get_tournament(conn, req.slug) is None
        tid = repo.upsert_tournament(conn, req.slug, req.name, req.acronym or None, req.warmups, req.format)
        found, _new = ingest.discover(conn, tid, req.sheet_url, kind="google_sheet")
        job.found = found
        if found == 0:
            raise ValueError("No osu! multiplayer links were found in that sheet. "
                             "Is it shared as 'anyone with the link can view'?")
        job.message = f"Found {found} multiplayer matches"

        def on_match(done: int, total: int, counts: dict) -> None:
            job.phase = "importing"
            job.done, job.total = done, total
            job.failed, job.not_found = counts["failed"], counts["not_found"]
            job.message = f"Importing matches... {done} / {total}"

        ingest.fetch(conn, tid, client, progress=lambda *_: None, on_match=on_match)
        job.phase, job.message = "calculating", "Calculating ratings..."
        service.invalidate(req.slug)
        service.get_analysis(conn, req.slug)       # warm the cache so the report opens instantly
        job.phase, job.message = "done", "Tournament ready ✓"
    except Exception as e:  # noqa: BLE001 - surfaced to the UI verbatim (never contains secrets)
        log.exception("import failed")
        job.phase, job.error = "error", str(e)
        job.message = "Import failed"
        if conn is not None and created and tid is not None and not conn.execute(
                "SELECT 1 FROM tournament_matches WHERE tournament_id = ?", (tid,)).fetchone():
            conn.execute("DELETE FROM tournaments WHERE id = ?", (tid,))   # don't leave a ghost tournament behind
            conn.commit()
    finally:
        if conn:
            conn.close()
