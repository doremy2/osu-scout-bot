"""Automatic updating: rescans of a tournament's source import only what is new, idempotently."""
import copy
import dataclasses
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scout import ingest, server, updater  # noqa: E402
from scout.db import connect, repo  # noqa: E402
from scout.importer import ImportRequest, JobRegistry, JobStore, purge_abandoned  # noqa: E402
from scout.server import create_app  # noqa: E402

import fake_tournament as ft  # noqa: E402

SHEET = "https://docs.google.com/spreadsheets/d/FAKESHEET/edit"
PAYLOADS, ROUNDS = ft.build()
IDS = sorted(PAYLOADS)                       # 19 lobbies


class FakeSheet:
    """Stands in for the Google Sheet: a text file whose lobby list can grow between scans."""

    def __init__(self, path: Path):
        self.path = path
        self.fail: Exception | None = None

    def publish(self, ids):
        lines = []
        for rnd in dict.fromkeys(ROUNDS[i] for i in ids):
            lines.append(rnd)
            lines += [f"https://osu.ppy.sh/community/matches/{i}" for i in ids if ROUNDS[i] == rnd]
        self.path.write_text("\n".join(lines))


@pytest.fixture()
def world(tmp_path, monkeypatch):
    sheet = FakeSheet(tmp_path / "sheet.txt")
    real = ingest.discover_all

    def fake_discover_all(location, kind=None):
        if sheet.fail:
            raise sheet.fail
        _, links, slots = real(str(sheet.path), "text")
        return "google_sheet", links, slots

    monkeypatch.setattr(ingest, "discover_all", fake_discover_all)
    payloads = copy.deepcopy(PAYLOADS)
    for p in payloads.values():                                   # real lobbies are closed once they are over
        p["match"]["end_time"] = "2027-01-09T23:00:00Z"
    client = ft.FakeClient(payloads)
    db = tmp_path / "scout.db"

    def import_first(n=10):
        sheet.publish(IDS[:n])
        job = JobRegistry(db).start(
            ImportRequest(name="Fake Open", acronym="FAKE", slug="fake", sheet_url=SHEET, format="team"),
            client_factory=lambda: client, mode="inline")
        assert job.phase == "done", job.error
        return job

    return dict(sheet=sheet, client=client, db=db, import_first=import_first, factory=lambda: client)


def _counts(db):
    conn = connect(db)
    try:
        return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                for t in ("tournament_matches", "match_games", "game_scores")}
    finally:
        conn.close()


def _tournament(db):
    conn = connect(db)
    try:
        return dict(repo.get_tournament(conn, "fake"))
    finally:
        conn.close()


# ---------- the source is remembered ------------------------------------------------------------------
def test_import_records_source_and_enables_tracking(world):
    world["import_first"](10)
    t = _tournament(world["db"])
    assert t["source_url"] == SHEET and t["source_type"] == "google_sheet" and t["auto_update"] == 1
    assert t["last_checked_at"] and t["last_source_hash"]


def test_tournaments_imported_before_tracking_get_their_source_backfilled(tmp_path):
    conn = connect(tmp_path / "old.db")
    tid = repo.upsert_tournament(conn, "old", "Old Cup")
    conn.execute("INSERT INTO tournament_sources (tournament_id, kind, location) VALUES (?, 'google_sheet', ?)", (tid, SHEET))
    conn.execute("UPDATE tournaments SET source_url = NULL, auto_update = 0")
    conn.commit()
    conn.execute("ALTER TABLE tournaments DROP COLUMN source_url")      # pretend the v8 columns never existed
    conn.execute("UPDATE schema_version SET version = 7")
    conn.commit()
    conn.close()
    conn = connect(tmp_path / "old.db")
    t = repo.get_tournament(conn, "old")
    assert t["source_url"] == SHEET and t["source_type"] == "google_sheet" and t["auto_update"] == 1


# ---------- incremental update ------------------------------------------------------------------------
def test_update_downloads_only_the_new_lobbies_and_is_idempotent(world):
    world["import_first"](10)
    client, sheet, db = world["client"], world["sheet"], world["db"]
    assert client.calls == 10
    before = _counts(db)

    sheet.publish(IDS[:15])                                           # five new lobbies appear
    res = updater.check_tournament(db, "fake")
    assert res["state"] == "updating" and res["new"] == 5 and res["pending"] == 5
    assert client.calls == 10                                         # registering is free: nothing downloaded yet
    out = updater.tick(db, world["factory"], budget=30, max_checks=5, discovery=False,
                       now=repo.utcnow() + timedelta(hours=1))
    assert client.calls == 15                                         # exactly the five new ones
    assert _tournament(db)["last_new_matches"] == 5
    after = _counts(db)
    assert after["tournament_matches"] == 15 and after["match_games"] > before["match_games"]

    # again, and again: nothing new, nothing downloaded, no duplicate rows
    for hours in (2, 3):
        updater.tick(db, world["factory"], budget=30, max_checks=5, discovery=False,
                     now=repo.utcnow() + timedelta(hours=hours))
        res = updater.check_tournament(db, "fake")
        assert res["new"] == 0 and res["pending"] == 0 and res["job"] is None
    assert client.calls == 15
    assert _counts(db) == after
    conn = connect(db)
    assert conn.execute("SELECT COUNT(*) FROM (SELECT osu_match_id FROM tournament_matches GROUP BY osu_match_id HAVING COUNT(*) > 1)").fetchone()[0] == 0
    assert out["checked"] and out["checked"][0]["slug"] == "fake"


def test_update_recalculates_the_analytics(world):
    from scout.analytics import service
    world["import_first"](10)
    db = world["db"]
    conn = connect(db)
    assert service.get_analysis(conn, "fake").report["summary"]["matches"] == 10
    world["sheet"].publish(IDS)
    updater.check_tournament(db, "fake")
    updater.tick(db, world["factory"], budget=60, max_checks=5, discovery=False, now=repo.utcnow() + timedelta(hours=1))
    assert service.get_analysis(conn, "fake").report["summary"]["matches"] == 19


def test_unchanged_sheet_only_touches_the_check_time(world):
    world["import_first"](10)
    db = world["db"]
    h = _tournament(db)["last_source_hash"]
    first = _counts(db)
    res = updater.check_tournament(db, "fake", now=repo.utcnow() + timedelta(minutes=20))
    assert res["state"] == "unchanged" and res["job"] is None
    t = _tournament(db)
    assert t["last_source_hash"] == h and _counts(db) == first and world["client"].calls == 10
    assert repo.parse_stamp(t["last_checked_at"]) > repo.utcnow() + timedelta(minutes=19)


def test_an_unfinished_job_is_continued_by_the_next_tick_but_only_once_it_is_idle(world):
    world["import_first"](10)
    db, client = world["db"], world["client"]
    world["sheet"].publish(IDS)
    updater.check_tournament(db, "fake")                              # 9 pending, job registered (not run)
    store = JobStore(db)
    assert store.running("fake").request.origin == "update"
    updater.tick(db, world["factory"], budget=30, max_checks=0, discovery=False)
    assert client.calls == 10                                         # touched seconds ago: someone else may be driving it
    conn = connect(db)
    conn.execute("UPDATE import_jobs SET updated_at = datetime('now', '-2 minutes')")
    conn.commit()
    conn.close()
    out = updater.tick(db, world["factory"], budget=30, max_checks=0, discovery=False)
    assert out["resumed"] == [{"slug": "fake", "phase": "done"}]
    assert store.running("fake") is None and client.calls == 19 and _counts(db)["tournament_matches"] == 19


# ---------- when to look ----------------------------------------------------------------------------
def _t(**kw):
    base = dict(check_failures=0, last_changed_at=None, end_date=None, last_checked_at=None)
    base.update(kw)
    return base


def test_check_interval_slows_down_as_a_tournament_goes_quiet():
    now = repo.utcnow()
    day = lambda n: repo.stamp(now - timedelta(days=n))      # noqa: E731
    assert updater.check_interval(_t(), now) == 15 * 60
    assert updater.check_interval(_t(last_changed_at=day(1)), now) == 15 * 60
    assert updater.check_interval(_t(end_date=day(10)[:10]), now) == 3 * 3600
    assert updater.check_interval(_t(end_date=day(60)[:10]), now) == 24 * 3600
    assert updater.check_interval(_t(end_date=day(400)[:10]), now) == 7 * 24 * 3600
    assert updater.check_interval(_t(check_failures=1), now) == 30 * 60
    assert updater.check_interval(_t(check_failures=3), now) == 2 * 3600
    assert updater.check_interval(_t(check_failures=30), now) == 7 * 24 * 3600
    due = _t(last_checked_at=repo.stamp(now - timedelta(minutes=16)))
    assert updater.is_due(due, now) and not updater.is_due(_t(last_checked_at=repo.stamp(now - timedelta(minutes=5))), now)


def test_only_tracked_google_sheet_tournaments_are_due(world):
    world["import_first"](10)
    db = world["db"]
    conn = connect(db)
    later = repo.utcnow() + timedelta(days=30)
    assert [t["slug"] for t in updater.due_tournaments(conn, later)] == ["fake"]
    repo.set_auto_update(conn, "fake", False)
    assert updater.due_tournaments(conn, later) == []


def test_failed_check_is_recorded_backed_off_and_recovers(world):
    world["import_first"](10)
    db, sheet = world["db"], world["sheet"]
    sheet.fail = PermissionError("Google refused the export")
    res = updater.check_tournament(db, "fake")
    t = _tournament(db)
    assert res["state"] == "error" and t["check_failures"] == 1 and "Google refused" in t["last_check_error"]
    assert updater.tracking_status(db, "fake")["state"] == "error"
    sheet.fail = None
    updater.check_tournament(db, "fake")
    t = _tournament(db)
    assert t["check_failures"] == 0 and t["last_check_error"] is None


def test_failed_and_missing_lobbies_are_retried_after_a_cooldown_then_given_up(world):
    world["import_first"](10)
    db = world["db"]
    conn = connect(db)
    tid = _tournament(db)["id"]
    conn.execute("UPDATE tournament_matches SET status = 'failed', attempts = 1, fetched_at = ? WHERE id = "
                 "(SELECT MIN(id) FROM tournament_matches)", (repo.stamp(repo.utcnow() - timedelta(hours=1)),))
    conn.execute("UPDATE tournament_matches SET status = 'failed', attempts = 5, fetched_at = ? WHERE id = "
                 "(SELECT MAX(id) FROM tournament_matches)", (repo.stamp(repo.utcnow() - timedelta(hours=1)),))
    conn.commit()
    assert repo.requeue_retryable(conn, tid) == 1                    # the one with attempts left
    assert conn.execute("SELECT COUNT(*) FROM tournament_matches WHERE status = 'failed'").fetchone()[0] == 1
    conn.execute("UPDATE tournament_matches SET status = 'failed', attempts = 1, fetched_at = ? WHERE status = 'pending'",
                 (repo.stamp(repo.utcnow() - timedelta(minutes=5)),))
    conn.commit()
    assert repo.requeue_retryable(conn, tid) == 0                    # too soon


def test_running_lobbies_are_refreshed_but_finished_ones_are_not(world):
    world["import_first"](10)
    db = world["db"]
    conn = connect(db)
    tid = _tournament(db)["id"]
    now = repo.utcnow()
    old_fetch = repo.stamp(now - timedelta(minutes=30))
    conn.execute("UPDATE tournament_matches SET end_time = NULL, start_time = ?, fetched_at = ? WHERE tournament_id = ?",
                 (now.strftime("%Y-%m-%dT%H:%M:%SZ"), old_fetch, tid))
    conn.execute("UPDATE tournament_matches SET end_time = ? WHERE id = (SELECT MIN(id) FROM tournament_matches)", (now.strftime("%Y-%m-%dT%H:%M:%SZ"),))
    conn.commit()
    assert repo.reopen_live_matches(conn, tid, now) == 9             # one is closed, nine may still be running
    assert repo.reopen_live_matches(conn, tid, now) == 0             # already queued: nothing more to do


def test_scheduled_jobs_never_get_a_tournament_purged(world):
    world["import_first"](10)
    db = world["db"]
    world["sheet"].publish(IDS)
    updater.check_tournament(db, "fake")                             # pending lobbies + an update job
    conn = connect(db)
    conn.execute("UPDATE import_jobs SET updated_at = datetime('now', '-3 days')")
    conn.commit()
    assert purge_abandoned(db) == []
    assert repo.get_tournament(conn, "fake") is not None


# ---------- status + endpoints ----------------------------------------------------------------------------
def test_tracking_status_reports_what_the_page_shows(world):
    world["import_first"](10)
    db, sheet = world["db"], world["sheet"]
    s = updater.tracking_status(db, "fake")
    assert s["enabled"] and s["matches_tracked"] == 10 and not s["updating"] and s["last_checked_at"].endswith("Z")
    assert updater.tracking_status(db, "fake", writable=False)["state"] == "off"
    sheet.publish(IDS[:15])
    updater.check_tournament(db, "fake")
    s = updater.tracking_status(db, "fake")
    assert s["state"] == "updating" and s["updating"] and s["new_detected"] == 5 and s["job"]["total"] == 5
    assert updater.tracking_status(db, "nope") is None


@pytest.fixture()
def api(world, monkeypatch):
    world["import_first"](10)
    monkeypatch.setattr(server, "settings", dataclasses.replace(server.settings, cron_secret="cs", osu_client_id="x", osu_client_secret="y"))
    app = create_app(world["db"], client_factory=world["factory"], admin_token="owner", imports_mode="public", import_mode="step")
    return TestClient(app), world


def test_cron_endpoint_is_protected_and_does_the_update(api):
    c, w = api
    w["sheet"].publish(IDS)
    assert c.get("/api/cron/tick").status_code == 401
    assert c.get("/api/cron/tick", headers={"Authorization": "Bearer wrong"}).status_code == 401
    # Vercel Cron: GET with the bearer secret. Not due yet (just checked), so nothing happens.
    assert c.get("/api/cron/tick", headers={"Authorization": "Bearer cs"}).json()["checked"] == []
    # the owner can force a check, which registers the 9 new lobbies; the next tick (later) downloads them
    forced = c.post("/api/admin/tournaments/fake/check", headers={"x-admin-token": "owner"}).json()
    assert forced["new"] == 9 and forced["job"]
    assert c.get("/api/tournaments/fake/tracking").json()["new_detected"] == 9
    assert c.get("/api/tournaments/fake").json()["tracking"]["state"] == "updating"
    conn = connect(w["db"])
    conn.execute("UPDATE import_jobs SET updated_at = datetime('now', '-5 minutes')")
    conn.commit()
    out = c.post("/api/cron/tick", headers={"x-admin-token": "owner"}).json()
    assert [r["phase"] for r in out["resumed"]] == ["done"] and w["client"].calls == 19
    assert c.get("/api/tournaments/fake/tracking").json()["matches_tracked"] == 19


def test_tracking_can_be_switched_off_by_the_owner_only(api):
    c, _ = api
    body = {"auto_update": False}
    assert c.post("/api/admin/tournaments/fake/tracking", json=body).status_code == 401
    r = c.post("/api/admin/tournaments/fake/tracking", json=body, headers={"x-admin-token": "owner"})
    assert r.status_code == 200 and r.json()["enabled"] is False
    assert c.post("/api/admin/tournaments/fake/tracking", json={"auto_update": True, "source_url": "https://example.com"},
                  headers={"x-admin-token": "owner"}).status_code == 422


def test_admin_features_are_closed_on_a_public_server_without_a_token(world):
    world["import_first"](10)
    c = TestClient(create_app(world["db"], client_factory=world["factory"], imports_mode="public", admin_token=""))
    assert c.get("/api/admin/discovery/candidates").status_code == 403
    assert c.post("/api/cron/tick").status_code == 403
