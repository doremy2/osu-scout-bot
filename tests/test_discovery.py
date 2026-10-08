"""Tournament discovery: sources -> candidates -> approval -> the normal import job."""
import dataclasses
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scout import discovery, ingest, server, updater  # noqa: E402
from scout.db import connect, repo  # noqa: E402
from scout.discovery import analysis  # noqa: E402
from scout.discovery.fetch import SheetRef, refs_from_html, refs_from_tables, sheet_key  # noqa: E402
from scout.importer import JobRegistry  # noqa: E402
from scout.server import create_app  # noqa: E402
from scout.sources.extract import Cell, Table  # noqa: E402

import fake_tournament as ft  # noqa: E402

PAYLOADS, ROUNDS = ft.build()
IDS = sorted(PAYLOADS)


def mp(i):
    return f"https://osu.ppy.sh/community/matches/{i}"


def room(i):
    return f"https://osu.ppy.sh/multiplayer/rooms/{i}"


def schedule(ids, link=mp, rounds=None):
    rows, last = [], None
    for i in ids:
        rnd = (rounds or ROUNDS).get(i, "Group Stage")
        if rnd != last:
            rows.append([Cell(rnd)])
            last = rnd
        rows.append([Cell(f"Match {i}"), Cell("MP", link=link(i))])
    return rows


def sheet(ids=IDS, title="Test Open 2026 (2v2 Tournament)", tabs=("Schedule", "Mappools", "Teams"), link=mp):
    return [Table("Info", [[Cell(title)], [Cell("Welcome")]]),
            Table(tabs[0], schedule(ids, link)), *[Table(t, [[Cell("x")]]) for t in tabs[1:]]]


def ref(n, hint=""):
    url = f"https://docs.google.com/spreadsheets/d/SHEET{n}/edit"
    return SheetRef(url, hint, sheet_key(url))


class FakeFetcher:
    def __init__(self, pages=None, sheets=None):
        self.pages = pages or {}
        self.sheets = sheets or {}
        self.downloads: list[str] = []

    def page_refs(self, url):
        return self.pages[url]

    def sheet_refs(self, url):
        return self.pages[url]

    def sheet_tables(self, url):
        self.downloads.append(url)
        if isinstance(self.sheets[url], Exception):
            raise self.sheets[url]
        return self.sheets[url]


# ---------- analysis ---------------------------------------------------------------------------------
def test_a_tournament_sheet_is_recognised_and_explained():
    a = analysis.analyze_tables(sheet(), hint="")
    assert a.name == "Test Open 2026 (2v2 Tournament)" and a.name_from == "title cell"
    assert a.year == 2026 and a.format == "team" and a.client == "stable" and a.match_count == len(IDS)
    assert a.confidence >= 0.9 and any("MP links" in r for r in a.reasons)
    assert a.acronym == "" and analysis.suggest_slug(a, set()) == "test-open-2026-2v2-tournament"
    assert analysis.suggest_slug(a, {"test-open-2026-2v2-tournament"}) == "test-open-2026-2v2-tournament-2"


def test_link_text_beats_the_title_cell_and_generic_text_is_ignored():
    assert analysis.analyze_tables(sheet(), hint="Corsace Closed 2026").name == "Corsace Closed 2026"
    assert analysis.analyze_tables(sheet(), hint="Spreadsheet").name_from == "title cell"
    assert analysis.analyze_tables([Table("Schedule", schedule(IDS))]).name == "Untitled tournament"


def test_format_and_client_are_detected_from_the_sheet():
    assert analysis.analyze_tables(sheet(title="Spring Invitational 2026", tabs=("Schedule",)), "").format == "1v1"
    assert analysis.analyze_tables(sheet(title="Spring Invitational 2026"), "").format == "team"          # a Teams tab
    assert analysis.analyze_tables(sheet(title="Spring Cup 2026 1v1"), "").format == "1v1"
    lz = analysis.analyze_tables(sheet(link=room), "")
    assert lz.client == "lazer" and lz.rooms == len(IDS)


def test_confidence_grows_with_evidence_and_zero_links_is_not_a_tournament():
    few = analysis.analyze_tables([Table("Sheet1", schedule(IDS[:2]))], "")
    many = analysis.analyze_tables(sheet(), "")
    assert 0 < few.confidence < 0.4 < many.confidence
    empty = analysis.analyze_tables([Table("Info", [[Cell("My Open 2026")]])], "")
    assert empty.match_count == 0 and empty.confidence == 0


def test_acronym_rules():
    assert analysis.detect_acronym("4 Digit World Cup (4WC) 2026") == "4WC"
    assert analysis.detect_acronym("OWC 2026 Open Tournament") == "OWC"
    assert analysis.detect_acronym("Corsace Closed Invitational") == "CCI"
    assert analysis.detect_acronym("Corsace") == ""
    assert analysis.detect_acronym("Test Open 2026 (2v2 Tournament)") == ""        # numbers are not initials
    assert analysis.suggest_slug(analysis.analyze_tables(sheet(title="4 Digit World Cup (4WC) 2026"), ""), set()) == "4wc-2026"


# ---------- reading sources -----------------------------------------------------------------------------
def test_sheet_links_are_found_in_a_page_with_name_hints():
    html = """
    <p>Test Open 2026: <a href="https://docs.google.com/spreadsheets/d/AAA/edit#gid=0">Spreadsheet</a></p>
    <p><a href="https://www.google.com/url?q=https%3A%2F%2Fdocs.google.com%2Fspreadsheets%2Fd%2FBBB%2Fedit&sa=D">Winter Cup 2026</a></p>
    <p><a href="https://docs.google.com/spreadsheets/d/AAA/edit?usp=sharing">again</a> <a href="https://osu.ppy.sh/home">osu</a></p>
    <pre>mirror: https://docs.google.com/spreadsheets/d/CCC/edit</pre>"""
    refs = {r.key: r for r in refs_from_html(html)}
    assert set(refs) == {"gsheet:AAA", "gsheet:BBB", "gsheet:CCC"}
    assert refs["gsheet:AAA"].hint == "Test Open 2026" and refs["gsheet:BBB"].hint == "Winter Cup 2026" and refs["gsheet:CCC"].hint == ""


def test_an_index_sheet_gives_the_row_text_as_the_hint():
    tables = [Table("List", [[Cell("Summer Open 2026"), Cell("sheet", link="https://docs.google.com/spreadsheets/d/XYZ/edit")]])]
    [r] = refs_from_tables(tables)
    assert r.key == "gsheet:XYZ" and r.hint == "Summer Open 2026"
    assert sheet_key("https://docs.google.com/spreadsheets/d/e/2PACX-1/pub?output=xlsx") == "gsheet-pub:2PACX-1"


def test_source_urls_must_be_public_web_pages(tmp_path):
    conn = connect(tmp_path / "d.db")
    for bad in ("file:///etc/passwd", "http://localhost:8001/x", "http://127.0.0.1/x", "http://10.0.0.5/x"):
        with pytest.raises(ValueError):
            discovery.add_source(conn, "bad", "page", bad)
    with pytest.raises(ValueError):
        discovery.add_source(conn, "bad", "sheet", "https://example.com/x")
    sid = discovery.add_source(conn, "Forum", "page", "https://osu.ppy.sh/community/forums/topics/1")
    with pytest.raises(ValueError):
        discovery.add_source(conn, "Forum again", "page", "https://osu.ppy.sh/community/forums/topics/1")
    assert sid == 1


# ---------- scanning -------------------------------------------------------------------------------------
@pytest.fixture()
def env(tmp_path):
    db = tmp_path / "scout.db"
    conn = connect(db)
    sid = discovery.add_source(conn, "Forum thread", "page", "https://forum.example/thread")
    conn.close()
    return db, sid


def test_scan_creates_candidates_without_publishing_anything(env):
    db, sid = env
    f = FakeFetcher(pages={"https://forum.example/thread": [ref(1, "Test Open 2026"), ref(2, "Empty Cup 2026"), ref(3, "Private Cup")]},
                    sheets={ref(1).url: sheet(), ref(2).url: [Table("Info", [[Cell("Empty Cup 2026")]])],
                            ref(3).url: PermissionError("Google refused the export")})
    stats = discovery.scan_source(db, sid, f)
    assert (stats["created"], stats["no_links"], stats["errors"]) == (1, 1, 1)
    conn = connect(db)
    [c] = discovery.list_candidates(conn)
    assert c["status"] == "pending" and c["name"] == "Test Open 2026" and c["match_count"] == len(IDS)
    assert c["detected_format"] == "team" and c["detected_client"] == "stable" and c["confidence"] > 0.8
    assert c["source_url"] == ref(1).url and c["discovered_at"].endswith("Z") and c["reasons"]
    assert conn.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0] == 0           # nothing published


def test_scanning_twice_downloads_nothing_new_and_never_duplicates(env):
    db, sid = env
    f = FakeFetcher(pages={"https://forum.example/thread": [ref(1, "Test Open 2026"), ref(2, "Empty Cup")]},
                    sheets={ref(1).url: sheet(), ref(2).url: [Table("x", [[Cell("nothing")]])]})
    discovery.scan_source(db, sid, f)
    assert len(f.downloads) == 2
    again = discovery.scan_source(db, sid, f)
    assert len(f.downloads) == 2 and again["skipped"] == 2 and again["created"] == 0
    conn = connect(db)
    assert conn.execute("SELECT COUNT(*) FROM discovery_candidates").fetchone()[0] == 1
    # a day later the pending candidate and the empty sheet are looked at again; still exactly one candidate
    later = repo.utcnow() + discovery.RETRY_AFTER["candidate"] + discovery.RETRY_AFTER["no_links"]
    stats = discovery.scan_source(db, sid, f, now=later)
    assert stats["updated"] == 1 and stats["created"] == 0
    assert conn.execute("SELECT COUNT(*) FROM discovery_candidates").fetchone()[0] == 1


def test_tournaments_we_already_have_are_not_candidates(env, tmp_path):
    db, sid = env
    conn = connect(db)
    tid = repo.upsert_tournament(conn, "have", "Have Open")
    repo.set_source(conn, tid, ref(1).url + "?usp=sharing", "google_sheet")
    # a second sheet that is a copy of another tournament's lobbies
    t2 = repo.upsert_tournament(conn, "copycat-origin", "Origin")
    repo.add_match_links(conn, t2, ingest.discover_all.__globals__["extract_links_from_text"]("\n".join(mp(i) for i in IDS)))
    f = FakeFetcher(pages={"https://forum.example/thread": [ref(1, "Have Open"), ref(2, "Copy of Origin"), ref(3, "Other Cup 2026")]},
                    sheets={ref(2).url: sheet(), ref(3).url: sheet(ids=[999000 + i for i in range(10)])})
    stats = discovery.scan_source(db, sid, f)
    assert stats["known"] == 1 and stats["duplicate"] == 1 and stats["created"] == 1
    assert ref(1).url not in f.downloads                                       # known sheets are not even downloaded
    pending = discovery.list_candidates(conn, "pending")
    assert [c["name"] for c in pending] == ["Other Cup 2026"]
    [dup] = discovery.list_candidates(conn, "duplicate")
    assert dup["duplicate_of"] == "copycat-origin" and any("already in" in r for r in dup["reasons"])


def test_a_scan_is_bounded_and_resumes_where_it_left_off(env):
    db, sid = env
    refs = [ref(n, f"Cup {n} 2026") for n in range(1, 6)]
    f = FakeFetcher(pages={"https://forum.example/thread": refs}, sheets={r.url: sheet(title=f"Cup {n} 2026 Open") for n, r in enumerate(refs, 1)})
    first = discovery.scan_source(db, sid, f, max_analyses=2)
    assert first["created"] == 2 and first["deferred"] == 3
    second = discovery.scan_source(db, sid, f, max_analyses=10)
    assert second["created"] == 3 and second["skipped"] == 2 and len(f.downloads) == 5


def test_an_unreachable_source_is_recorded_not_raised(env):
    db, sid = env
    class Down(FakeFetcher):
        def page_refs(self, url):
            raise ConnectionError("no route")
    stats = discovery.scan_source(db, sid, Down())
    assert "no route" in stats["error"]
    conn = connect(db)
    s = discovery.list_sources(conn)[0]
    assert s["last_status"] == "error" and "no route" in s["last_error"]


def test_due_sources_follow_their_interval(env):
    db, sid = env
    conn = connect(db)
    assert [s["id"] for s in discovery.due_sources(conn)] == [sid]
    f = FakeFetcher(pages={"https://forum.example/thread": []})
    discovery.scan_source(db, sid, f)
    assert discovery.due_sources(conn) == []
    assert [s["id"] for s in discovery.due_sources(conn, repo.utcnow() + discovery.timedelta(hours=7))] == [sid]
    discovery.set_source_enabled(conn, sid, False)
    assert discovery.due_sources(conn, repo.utcnow() + discovery.timedelta(days=2)) == []


# ---------- the queue -> the import job ---------------------------------------------------------------------
@pytest.fixture()
def queued(env, monkeypatch):
    db, sid = env
    real = ingest.discover_all
    ids = IDS[:12]

    def fake_discover_all(location, kind=None):
        import tempfile
        p = Path(tempfile.gettempdir()) / "disc-sheet.txt"
        p.write_text("\n".join([f"{ROUNDS[i]}\n{mp(i)}" for i in ids]))
        _, links, slots = real(str(p), "text")
        return "google_sheet", links, slots

    monkeypatch.setattr(ingest, "discover_all", fake_discover_all)
    f = FakeFetcher(pages={"https://forum.example/thread": [ref(1, "Test Open 2026")]}, sheets={ref(1).url: sheet(ids=ids)})
    discovery.scan_source(db, sid, f)
    conn = connect(db)
    [c] = discovery.list_candidates(conn)
    return db, c, ft.FakeClient(PAYLOADS)


def test_approving_a_candidate_runs_the_normal_import_and_starts_tracking(queued):
    db, c, client = queued
    reg = JobRegistry(db)
    res = discovery.approve_candidate(db, c["id"], reg, lambda: client, mode="inline")
    assert res["job"].phase == "done" and res["job"].request.origin == "discovery"
    conn = connect(db)
    t = repo.get_tournament(conn, res["candidate"]["tournament_slug"])
    assert t["name"] == "Test Open 2026" and t["format"] == "team" and t["source_url"] == c["source_url"] and t["auto_update"] == 1
    assert conn.execute("SELECT COUNT(*) FROM tournament_matches WHERE status = 'imported'").fetchone()[0] == 12 and client.calls == 12
    # approving again is harmless: same job, no new tournament, no new downloads
    again = discovery.approve_candidate(db, c["id"], reg, lambda: client, mode="inline")
    assert again["job"].id == res["job"].id and client.calls == 12
    assert conn.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0] == 1
    assert discovery.list_candidates(conn) == []                                          # left the pending queue
    [row] = discovery.list_candidates(conn, "approved")
    assert row["job"]["phase"] == "done"
    with pytest.raises(ValueError):
        discovery.set_candidate_status(conn, c["id"], "bogus")


def test_approval_can_correct_the_detected_fields_and_slug_clashes_are_resolved(queued):
    db, c, client = queued
    conn = connect(db)
    repo.upsert_tournament(conn, c["suggested_slug"], "Something else")
    res = discovery.approve_candidate(db, c["id"], JobRegistry(db), lambda: client, mode="queue",
                                      overrides={"name": "Test Open 2026 Fixed", "format": "1v1"})
    assert res["candidate"]["tournament_slug"] == c["suggested_slug"] + "-2" and res["job"].request.format == "1v1"
    assert res["job"].phase == "queued"                                  # queued: the scheduler (or a step call) runs it


def test_an_explicit_slug_that_is_taken_is_refused(queued):
    db, c, client = queued
    conn = connect(db)
    repo.upsert_tournament(conn, "taken", "Other")
    with pytest.raises(ValueError, match="already used"):
        discovery.approve_candidate(db, c["id"], JobRegistry(db), lambda: client, mode="queue", overrides={"slug": "taken"})
    with pytest.raises(ValueError, match="(?i)slug"):
        discovery.approve_candidate(db, c["id"], JobRegistry(db), lambda: client, mode="queue", overrides={"slug": "Bad Slug"})
    assert conn.execute("SELECT status FROM discovery_candidates").fetchone()[0] == "pending"        # a refused approval changes nothing
    with pytest.raises(KeyError):
        discovery.approve_candidate(db, 9999, JobRegistry(db))


def test_ignored_candidates_stay_ignored_until_restored(queued):
    db, c, client = queued
    conn = connect(db)
    assert discovery.set_candidate_status(conn, c["id"], "ignored")
    assert not discovery.set_candidate_status(conn, c["id"], "ignored")
    f = FakeFetcher(pages={"https://forum.example/thread": [ref(1, "Test Open 2026")]}, sheets={ref(1).url: sheet()})
    later = repo.utcnow() + discovery.timedelta(days=3)
    discovery.scan_source(db, 1, f, now=later)
    assert f.downloads == [] and discovery.list_candidates(conn) == []
    assert discovery.set_candidate_status(conn, c["id"], "pending")
    assert len(discovery.list_candidates(conn)) == 1


def test_auto_approval_is_off_by_default_and_respects_the_threshold(queued):
    db, c, client = queued
    reg = JobRegistry(db)
    assert discovery.auto_approve(db, reg, lambda: client) == []                           # threshold 0: never
    assert discovery.auto_approve(db, reg, lambda: client, threshold=0.99) == []           # not confident enough
    assert c["confidence"] < 0.99
    assert discovery.auto_approve(db, reg, lambda: client, threshold=0.5) == [c["suggested_slug"]]
    conn = connect(db)
    assert [x["status"] for x in discovery.list_candidates(conn, "all")] == ["approved"]


def test_the_scheduled_tick_scans_due_sources_but_publishes_nothing(env):
    db, sid = env
    f = FakeFetcher(pages={"https://forum.example/thread": [ref(1, "Test Open 2026")]}, sheets={ref(1).url: sheet()})
    out = discovery.run_due_sources(db, fetcher=f)
    assert out["scanned"][0]["created"] == 1 and out["auto_approved"] == []
    conn = connect(db)
    assert conn.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0] == 0
    assert updater.tick(db, None, budget=5, discovery=False)["discovery"] is None


# ---------- admin API -----------------------------------------------------------------------------------
def test_admin_queue_api(queued, monkeypatch):
    db, c, client = queued
    monkeypatch.setattr(server, "settings", dataclasses.replace(server.settings, osu_client_id="x", osu_client_secret="y"))
    app = create_app(db, client_factory=lambda: client, admin_token="owner", imports_mode="public", import_mode="step")
    api = TestClient(app)
    owner = {"x-admin-token": "owner"}
    assert api.get("/api/admin/discovery/candidates").status_code == 401
    assert api.get("/api/admin/discovery/candidates", headers={"x-admin-token": "nope"}).status_code == 401
    rows = api.get("/api/admin/discovery/candidates", headers=owner).json()
    assert [r["name"] for r in rows] == ["Test Open 2026"] and rows[0]["match_count"] == 12
    assert api.get("/api/admin/discovery/candidates?status=bogus", headers=owner).status_code == 422

    assert api.post("/api/admin/discovery/sources", json={"name": "Wiki", "kind": "page", "url": "http://localhost/x"}, headers=owner).status_code == 422
    made = api.post("/api/admin/discovery/sources", json={"name": "Wiki", "kind": "page", "url": "https://wiki.example/t"}, headers=owner)
    assert made.status_code == 201 and len(api.get("/api/admin/discovery/sources", headers=owner).json()) == 2
    assert api.post(f"/api/admin/discovery/sources/{made.json()['id']}/enabled", json={"enabled": False}, headers=owner).status_code == 200

    # approve starts the import job; in step mode the owner's browser advances it
    r = api.post(f"/api/admin/discovery/candidates/{c['id']}/approve", json={}, headers=owner)
    assert r.status_code == 202 and r.json()["job"]["origin"] == "discovery" and r.json()["job"]["mode"] == "step"
    job = r.json()["job"]
    for _ in range(10):
        job = api.post(f"/api/imports/{job['id']}/step", headers=owner).json()
        if job["phase"] in ("done", "error"):
            break
    assert job["phase"] == "done", job
    assert api.get("/api/tournaments/" + r.json()["candidate"]["tournament_slug"]).json()["tracking"]["matches_tracked"] == 12
    assert api.post("/api/admin/discovery/candidates/9999/approve", json={}, headers=owner).status_code == 404
    assert api.post(f"/api/admin/discovery/candidates/{c['id']}/ignore", headers=owner).status_code == 404       # already approved
