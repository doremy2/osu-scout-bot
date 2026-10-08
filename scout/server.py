"""Local web API for the scouting site (FastAPI). Run with:  python -m scout serve

The browser never touches SQLite or the osu! credentials: the Next.js app proxies /api/scout/*
to this process, and every number it shows comes from `scout.analytics`.
"""
from __future__ import annotations

import dataclasses
import hmac
import sqlite3
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .analytics import service
from .analytics.report import LEADERBOARD_MODES, AwardConfig, leaderboard, match_detail, search_players
from .analytics.draft import DraftConfig, draft_advice, list_sides, round_pools
from .analytics.ratings import RatingConfig
from .rounds import ROUNDS
from .config import settings
from . import limits
from .db import connect, repo, writable_copy
from .formats import FORMATS
from .importer import ImportRequest, JobRegistry, purge_abandoned


class DraftBody(BaseModel):
    a: str
    b: str
    round: str
    taken: list[int] = []
    best_of: int = 9
    bans_per_side: int = 2
    first_ban: str = "A"
    first_pick: str = "B"


class ImportBody(BaseModel):
    name: str
    acronym: str = ""
    slug: str
    sheet_url: str
    format: str


def import_policy(admin_token: str, mode: str) -> str:
    """"off" | "token" | "public" | "open" - who may start an import.
    token = invite code required; public = anyone, within limits (the admin token is then the owner key);
    open = anyone, no limits (local use)."""
    if mode == "off":
        return "off"
    if mode == "public":
        return "public"
    return "token" if admin_token else "open"


def create_app(db_path: str | Path | None = None, client_factory=None, threaded_imports: bool = True,
               import_mode: str | None = None,
               admin_token: str | None = None, imports_mode: str | None = None) -> FastAPI:
    db_path = Path(db_path or settings.db_path)
    if settings.readonly and not settings.turso_url and str(db_path) != ":memory:":
        db_path = writable_copy(db_path)
    token = settings.admin_token if admin_token is None else admin_token
    policy = import_policy(token, settings.imports_mode if imports_mode is None else imports_mode)
    app = FastAPI(title="osu! scout API", docs_url="/api/docs" if settings.enable_docs else None,
                  redoc_url=None, openapi_url="/api/openapi.json" if settings.enable_docs else None)
    app.add_middleware(CORSMiddleware, allow_origins=[o.strip() for o in settings.allowed_origins.split(",") if o.strip()],
                       allow_methods=["GET", "POST"], allow_headers=["*"])
    jobs = JobRegistry(db_path)
    app.state.jobs = jobs
    mode = "inline" if threaded_imports is False else (import_mode or settings.import_mode or ("step" if settings.turso_url else "thread"))

    def is_admin(x_admin_token: str | None) -> bool:
        return bool(token and x_admin_token and hmac.compare_digest(x_admin_token.encode(), token.encode()))

    def require_import_access(x_admin_token: str | None) -> bool:
        """Raises unless this request may import. Returns True when it carries the owner's token."""
        if policy == "off":
            raise HTTPException(403, "Importing is disabled on this server.")
        if policy == "token" and not is_admin(x_admin_token):
            raise HTTPException(401, "Imports need the admin token.")
        return is_admin(x_admin_token)

    def db() -> sqlite3.Connection:
        return connect(db_path)

    def analysis(conn: sqlite3.Connection, slug: str):
        try:
            return service.get_analysis(conn, slug)
        except KeyError:
            raise HTTPException(404, f"Unknown tournament '{slug}'") from None

    # ---- meta ---------------------------------------------------------------
    @app.get("/api/health")
    def health():
        return {"ok": True, "imports": policy,   # lets the UI ask for the admin token / hide the form
                "osu_credentials_configured": bool(settings.osu_client_id and settings.osu_client_secret)}

    @app.get("/api/formats")
    def formats():
        return [{"key": f.key, "label": f.label, "description": f.description, "has_teams": f.has_teams}
                for f in FORMATS.values()]

    @app.get("/api/tournaments")
    def tournaments():
        try:
            purge_abandoned(db_path)       # half-imported leftovers from dead web imports
        except Exception:  # noqa: BLE001 - never let housekeeping break the list
            pass
        conn = db()
        try:
            return service.list_tournaments(conn)
        finally:
            conn.close()

    @app.get("/api/model")
    def model(slug: str | None = None):
        """The live constants behind every rating, so the methodology page can never drift from the code."""
        out = {"rating": dataclasses.asdict(RatingConfig()), "awards": dataclasses.asdict(AwardConfig()),
               "rounds": [{"code": c, "name": n, "order": o, "weight": w}
                          for c, (n, o, w) in sorted(ROUNDS.items(), key=lambda kv: kv[1][1])],
               "tournament": None}
        if slug:
            conn = db()
            try:
                rep = analysis(conn, slug).report
            finally:
                conn.close()
            out["tournament"] = {"slug": slug, "name": rep["tournament"]["name"], **rep["config"]["model"],
                                 "qualified_split": rep["summary"]["qualified_split"]}
        return out

    # ---- imports ------------------------------------------------------------
    @app.post("/api/imports", status_code=202)
    def start_import(body: ImportBody, request: Request, x_admin_token: str | None = Header(default=None)):
        admin = require_import_access(x_admin_token)
        req = ImportRequest(name=body.name.strip(), acronym=body.acronym.strip(), slug=body.slug.strip().lower(),
                            sheet_url=body.sheet_url.strip(), format=body.format)
        try:
            req.validate()
        except ValueError as e:
            raise HTTPException(422, str(e)) from None
        if not client_factory and not (settings.osu_client_id and settings.osu_client_secret):
            raise HTTPException(500, "Server is missing OSU_CLIENT_ID / OSU_CLIENT_SECRET (set them in .env).")
        conn = db()
        try:
            existing = conn.execute("SELECT 1 FROM tournaments WHERE slug = ?", (req.slug,)).fetchone() is not None
            visitor = None
            if policy == "public" and not admin:
                if existing:
                    # strangers may refresh or finish a tournament from the SAME sheet (it only fetches what is missing),
                    # but may not point an existing slug at a different tournament
                    if not repo.tournament_uses_sheet(conn, req.slug, req.sheet_url):
                        raise HTTPException(409, "That slug belongs to a different tournament. Choose another slug.")
                    req.keep_metadata = True
                visitor = limits.client_key((request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
                                            or (request.client.host if request.client else None))
                reason = limits.check(conn, visitor)
                if reason:
                    raise HTTPException(429, reason)
        finally:
            conn.close()
        job = jobs.start(req, client_factory=client_factory, mode=mode)
        if visitor and job.phase == "queued":
            conn = db()
            try:
                limits.record(conn, visitor, req.slug)
            finally:
                conn.close()
        return {**job.public(), "existing": existing, "mode": mode}

    @app.post("/api/imports/{job_id}/step")
    def import_step(job_id: str, x_admin_token: str | None = Header(default=None)):
        """Advance a job by one short slice of work. In "step" mode the browser calls this until the job is done."""
        require_import_access(x_admin_token)
        if jobs.get(job_id) is None:
            raise HTTPException(404, "Unknown import job")
        job = jobs.step(job_id, client_factory) if mode == "step" else jobs.get(job_id)
        return {**job.public(), "mode": mode}

    @app.delete("/api/tournaments/{slug}")
    def delete_tournament(slug: str, x_admin_token: str | None = Header(default=None)):
        """Owner-only moderation: remove a tournament (e.g. one imported by mistake or abusively)."""
        if not is_admin(x_admin_token):
            raise HTTPException(401, "Only the site owner can delete tournaments.")
        conn = db()
        try:
            if not repo.delete_tournament(conn, slug):
                raise HTTPException(404, f"Unknown tournament '{slug}'")
        finally:
            conn.close()
        service.invalidate(slug)
        return {"deleted": slug}

    @app.get("/api/imports/{job_id}")
    def import_status(job_id: str):
        job = jobs.get(job_id)
        if not job:
            raise HTTPException(404, "Unknown import job")
        return job.public()

    # ---- tournament views ---------------------------------------------------
    @app.get("/api/tournaments/{slug}")
    def overview(slug: str):
        conn = db()
        try:
            rep = analysis(conn, slug).report
        finally:
            conn.close()
        mods = [m for m in rep["summary"]["mods"]]
        mod_leaders = []
        for m in mods:
            board = [r for r in rep["mod_leaderboards"][m] if r["eligible_for_award"]]
            if board:
                top = board[0]
                mod_leaders.append({"mod": m, **{k: top[k] for k in ("user_id", "username", "slug", "avatar_url", "rating", "maps")}})
        recent = sorted(rep["matches"], key=lambda x: x["start_time"] or "", reverse=True)[:8]
        return {
            "tournament": rep["tournament"], "summary": rep["summary"], "mvp": rep["mvp"],
            "mod_leaders": mod_leaders,
            "top_players": rep["rankings"][:10],
            "top_teams": rep["teams"][:5],
            "awards": rep["awards"],
            "recent_matches": recent,
            "modes": list(LEADERBOARD_MODES),
        }

    @app.get("/api/tournaments/{slug}/leaderboard")
    def board(slug: str, mode: str = "overall", key: str | None = None, q: str | None = None,
              limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
        conn = db()
        try:
            rep = analysis(conn, slug).report
        finally:
            conn.close()
        try:
            data = leaderboard(rep, mode, key, limit=None)
        except (ValueError, KeyError) as e:
            raise HTTPException(422, str(e).strip("'\"")) from None
        if q:
            needle = q.strip().lower()
            data["rows"] = [r for r in data["rows"] if needle in r["username"].lower() or needle == str(r["user_id"])]
            data["total"] = len(data["rows"])
        data["rows"] = data["rows"][offset: offset + limit]
        data["options"] = {"rounds": [{"key": r, "name": rep["summary"]["round_names"][r]} for r in rep["summary"]["rounds"]],
                           "mods": rep["summary"]["mods"]}
        return data

    @app.get("/api/tournaments/{slug}/search")
    def search(slug: str, q: str = "", limit: int = Query(8, ge=1, le=25)):
        conn = db()
        try:
            rep = analysis(conn, slug).report
        finally:
            conn.close()
        return search_players(rep, q, limit)

    @app.get("/api/tournaments/{slug}/players/{player}")
    def player(slug: str, player: str):
        conn = db()
        try:
            rep = analysis(conn, slug).report
        finally:
            conn.close()
        page = rep["players"].get(player)
        if not page:
            raise HTTPException(404, f"No player '{player}' in this tournament")
        return {"tournament": rep["tournament"], "player": page, "mods": rep["summary"]["mods"]}

    @app.get("/api/tournaments/{slug}/teams")
    def teams(slug: str):
        conn = db()
        try:
            rep = analysis(conn, slug).report
        finally:
            conn.close()
        if not rep["tournament"]["has_teams"]:
            raise HTTPException(404, "This tournament is 1v1 and has no teams")
        return {"tournament": rep["tournament"], "teams": rep["teams"]}

    @app.get("/api/tournaments/{slug}/teams/{team}")
    def team(slug: str, team: str):
        conn = db()
        try:
            rep = analysis(conn, slug).report
        finally:
            conn.close()
        page = rep["team_pages"].get(team)
        if not page:
            raise HTTPException(404, f"No team '{team}' in this tournament")
        return {"tournament": rep["tournament"], "team": page, "mods": rep["summary"]["mods"]}

    @app.get("/api/tournaments/{slug}/matches")
    def matches(slug: str, round: str | None = None):
        conn = db()
        try:
            rep = analysis(conn, slug).report
        finally:
            conn.close()
        rows = [m for m in rep["matches"] if not round or m["round"] == round]
        return {"tournament": rep["tournament"], "rounds": rep["summary"]["rounds"],
                "round_names": rep["summary"]["round_names"], "matches": rows}

    @app.get("/api/tournaments/{slug}/matches/{osu_match_id}")
    def match(slug: str, osu_match_id: int):
        conn = db()
        try:
            an = analysis(conn, slug)
            detail = match_detail(conn, an, osu_match_id)
        finally:
            conn.close()
        if not detail:
            raise HTTPException(404, "No such match in this tournament")
        return {"tournament": an.report["tournament"], **detail}

    @app.get("/api/tournaments/{slug}/draft/setup")
    def draft_setup(slug: str):
        conn = db()
        try:
            an = analysis(conn, slug)
        finally:
            conn.close()
        return {"tournament": an.report["tournament"], "sides": list_sides(an), "rounds": round_pools(an),
                "side_kind": "team" if an.report["tournament"]["has_teams"] else "player"}

    @app.post("/api/tournaments/{slug}/draft/advice")
    def draft_state(slug: str, body: DraftBody):
        if body.first_ban not in ("A", "B") or body.first_pick not in ("A", "B"):
            raise HTTPException(422, "first_ban / first_pick must be A or B")
        if not (1 <= body.best_of <= 21 and 0 <= body.bans_per_side <= 4):
            raise HTTPException(422, "best_of must be 1-21 and bans_per_side 0-4")
        conn = db()
        try:
            an = analysis(conn, slug)
        finally:
            conn.close()
        cfg = DraftConfig(best_of=body.best_of, bans_per_side=body.bans_per_side,
                          first_ban=body.first_ban, first_pick=body.first_pick)
        try:
            return draft_advice(an, body.a, body.b, body.round, body.taken, cfg)
        except KeyError as e:
            raise HTTPException(404, f"Unknown side {e}") from None
        except ValueError as e:
            raise HTTPException(422, str(e)) from None

    @app.get("/api/tournaments/{slug}/awards")
    def awards(slug: str):
        conn = db()
        try:
            rep = analysis(conn, slug).report
        finally:
            conn.close()
        return {"tournament": rep["tournament"], "awards": rep["awards"]}

    return app


def run(host: str = "127.0.0.1", port: int = 8001) -> None:
    import uvicorn
    uvicorn.run(create_app(), host=settings.host if host == "127.0.0.1" else host,
                port=settings.port if port == 8001 else port, log_level="info", proxy_headers=True)
