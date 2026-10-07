"""Replay detection, formats/teams, the web API and the importer job (no network, no real DB)."""
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scout import ingest  # noqa: E402
from scout.analytics import service  # noqa: E402
from scout.analytics.report import build_report, leaderboard, search_players  # noqa: E402
from scout.db import connect, repo  # noqa: E402
from scout.importer import ImportRequest, JobRegistry  # noqa: E402
from scout.server import create_app  # noqa: E402

import fake_tournament as ft  # noqa: E402


# ---------- helpers ---------------------------------------------------------
def _score(uid, score, team="red"):
    return {"user_id": uid, "score": score, "accuracy": 0.95, "max_combo": 500, "mods": ["NF"],
            "statistics": {"count_300": 1, "count_100": 0, "count_50": 0, "count_miss": 0},
            "passed": True, "match": {"slot": 0, "team": team, "pass": True}}


def _game(gid, beatmap, minute, scores, ended=True, team_type="head-to-head"):
    return {"id": gid, "detail": {"type": "other"}, "game": {
        "id": gid, "beatmap_id": beatmap, "mods": ["NF"], "scoring_type": "scorev2", "team_type": team_type,
        "start_time": f"2027-01-01T10:{minute:02d}:00Z", "end_time": f"2027-01-01T10:{minute:02d}:50Z" if ended else None,
        "beatmap": {"id": beatmap, "beatmapset_id": beatmap * 10, "version": "x", "difficulty_rating": 6,
                    "beatmapset": {"artist": "A", "title": f"T{beatmap}"}},
        "scores": scores}}


def _payload(match_id, name, games, users=(1, 2, 3, 4)):
    return {"match": {"id": match_id, "name": name, "start_time": "2027-01-01T10:00:00Z", "end_time": None},
            "events": games, "first_event_id": 1, "latest_event_id": 2,
            "users": [{"id": u, "username": f"p{u}", "country_code": "VN", "country": {"code": "VN", "name": "Vietnam"}}
                      for u in users]}


def _load(tmp_path, payloads: dict[int, dict], fmt="1v1", rnd="Qualifiers"):
    conn = connect(tmp_path / "r.db")
    tid = repo.upsert_tournament(conn, "t", "T", "T", fmt=fmt)
    src = tmp_path / "links.txt"
    src.write_text(rnd + "\n" + "\n".join(f"https://osu.ppy.sh/community/matches/{m}" for m in payloads))
    ingest.discover(conn, tid, str(src))
    ingest.fetch(conn, tid, ft.FakeClient(payloads), progress=lambda *_: None)
    return conn, tid


def _reasons(conn):
    return {r["osu_game_id"]: r["exclude_reason"] for r in conn.execute("SELECT osu_game_id, exclude_reason FROM match_games")}


# ---------- replay logic ----------------------------------------------------
def test_abort_then_immediate_replay_keeps_only_the_replay(tmp_path):
    four = lambda base: [_score(u, base + u * 1000) for u in (1, 2, 3, 4)]  # noqa: E731
    games = [_game(1, 100, 0, four(500000), ended=False),     # aborted
             _game(2, 100, 5, four(600000)),                  # remake, same players, same map: counts
             _game(3, 200, 10, four(700000))]
    conn, _ = _load(tmp_path, {10: _payload(10, "X: (a) vs (b)", games)})
    r = _reasons(conn)
    assert r[1] is not None                                  # the aborted first attempt is excluded
    assert r[2] is None and r[3] is None


def test_whole_pool_played_twice_in_one_lobby_is_not_a_replay(tmp_path):
    """A qualifier lobby that runs the same pool for two sessions: both sessions are real performances."""
    four = lambda base: [_score(u, base + u * 1000) for u in (1, 2, 3, 4)]  # noqa: E731
    games = [_game(i, 100 + (i % 3), i, four(500000 + i * 1000)) for i in range(1, 7)]  # maps 101,102,100,101,102,100
    conn, _ = _load(tmp_path, {10: _payload(10, "X: (Qualifiers) vs (Lobby Egypt)", games)})
    assert set(_reasons(conn).values()) == {None}


def test_same_beatmap_in_other_lobbies_is_never_excluded(tmp_path):
    four = lambda base: [_score(u, base + u * 1000) for u in (1, 2, 3, 4)]  # noqa: E731
    payloads = {m: _payload(m, f"X: (Qualifiers) vs (Lobby {m})", [_game(m * 10, 100, 1, four(500000 + m))])
                for m in (11, 12, 13)}
    conn, _ = _load(tmp_path, payloads)
    assert set(_reasons(conn).values()) == {None}


def test_replay_needs_overlapping_players(tmp_path):
    """Back-to-back games on one map by completely different players are two sessions, not a remake."""
    games = [_game(1, 100, 0, [_score(1, 500000), _score(2, 400000)]),
             _game(2, 100, 5, [_score(3, 450000), _score(4, 420000)])]
    conn, _ = _load(tmp_path, {10: _payload(10, "X: (Qualifiers) vs (Lobby A)", games)})
    assert set(_reasons(conn).values()) == {None}


def test_empty_lobby_does_not_affect_analytics(tmp_path):
    four = lambda base: [_score(u, base + u * 1000) for u in (1, 2, 3, 4)]  # noqa: E731
    payloads = {10: _payload(10, "X: (a) vs (b)", [_game(1, 100, 1, four(500000)), _game(2, 101, 2, four(600000))]),
                11: _payload(11, "X: (c) vs (d)", [])}
    conn, _ = _load(tmp_path, payloads, rnd="Group Stage")
    rep = build_report(conn, "t")
    assert rep["summary"]["matches"] == 1 and rep["summary"]["empty_matches"] == 1


# ---------- formats + teams ---------------------------------------------------
@pytest.fixture()
def team_db(tmp_path):
    payloads, rounds = ft.build()
    conn = connect(tmp_path / "t.db")
    tid = repo.upsert_tournament(conn, "fake", "Fake Open", "FAKE", fmt="team")
    src = tmp_path / "links.txt"
    lines = []
    for rnd in ("Group Stage", "Semifinals", "Grand Finals"):
        lines.append(rnd)
        lines += [f"https://osu.ppy.sh/community/matches/{m}" for m, r in rounds.items() if r == rnd]
    src.write_text("\n".join(lines))
    ingest.discover(conn, tid, str(src))
    ingest.fetch(conn, tid, ft.FakeClient(payloads), progress=lambda *_: None)
    return tmp_path / "t.db", conn


def test_format_is_stored_and_validated(tmp_path):
    conn = connect(tmp_path / "f.db")
    repo.upsert_tournament(conn, "a", fmt="1v1")
    repo.upsert_tournament(conn, "b", fmt="team")
    assert repo.get_tournament(conn, "a")["format"] == "1v1" and repo.get_tournament(conn, "b")["format"] == "team"
    with pytest.raises(ValueError):
        repo.upsert_tournament(conn, "c", fmt="battle-royale")


def test_one_v_one_has_no_teams(tmp_path):
    four = lambda base: [_score(u, base + u * 1000) for u in (1, 2)]  # noqa: E731
    conn, _ = _load(tmp_path, {10: _payload(10, "X: (p1) vs (p2)", [_game(1, 100, 1, four(500000)), _game(2, 101, 2, four(450000))])},
                    rnd="Group Stage")
    rep = build_report(conn, "t")
    assert rep["tournament"]["format"] == "1v1" and not rep["tournament"]["has_teams"]
    assert rep["teams"] == [] and rep["team_pages"] == {}
    assert rep["mvp"] is None or rep["mvp"]["username"].startswith("p")


def test_team_membership_is_tournament_scoped(team_db):
    path, conn = team_db
    rows = conn.execute("SELECT t.name, m.user_id FROM team_memberships m JOIN tournament_teams t ON t.id = m.team_id").fetchall()
    by_team = {}
    for r in rows:
        by_team.setdefault(r["name"], set()).add(r["user_id"])
    assert by_team == {"Team A": {1, 2}, "Team B": {3, 4}, "Team C": {5, 6}, "Team D": {7, 8}}
    # same player, different tournament, different team: global player row is untouched
    other = repo.upsert_tournament(conn, "other", "Other", fmt="team")
    conn.execute("INSERT INTO tournament_teams (tournament_id, slug, name) VALUES (?, 'zeta', 'Zeta')", (other,))
    zeta = conn.execute("SELECT id FROM tournament_teams WHERE slug = 'zeta'").fetchone()[0]
    conn.execute("INSERT INTO team_memberships (tournament_id, team_id, user_id) VALUES (?, ?, 1)", (other, zeta))
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM team_memberships WHERE user_id = 1").fetchone()[0] == 2


def test_team_report_and_leaderboards(team_db):
    _, conn = team_db
    rep = build_report(conn, "fake")
    assert rep["tournament"]["has_teams"] and rep["summary"]["teams"] == 4
    assert [t["rank"] for t in rep["teams"]] == [1, 2, 3, 4]
    top = rep["team_pages"][rep["teams"][0]["slug"]]
    assert {r["username"] for r in top["roster"]} <= {"Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel"}
    assert top["matches"] and all(m["sides"][0]["slug"] == top["slug"] for m in top["matches"])
    alpha = rep["players"]["alpha"]
    assert alpha["team"]["name"] == "Team A" and [t["username"] for t in alpha["teammates"]] == ["Bravo"]
    assert alpha["mod_ratings"]["DT"]["rank"] >= 1 and alpha["by_round"][0]["rank_of"] == 8

    gs = leaderboard(rep, "round", "GS")
    assert gs["rows"][0]["rank"] == 1 and {r["key"] for r in []} == set()
    assert [r["rank"] for r in leaderboard(rep, "mod", "DT")["rows"]] == list(range(1, 9))
    assert leaderboard(rep, "maps")["rows"][0]["maps"] >= leaderboard(rep, "maps")["rows"][-1]["maps"]
    with pytest.raises(KeyError):
        leaderboard(rep, "round", "RO128")
    with pytest.raises(ValueError):
        leaderboard(rep, "vibes")


def test_search_is_scoped_to_tournament(team_db):
    _, conn = team_db
    rep = build_report(conn, "fake")
    assert [h["username"] for h in search_players(rep, "ALP")] == ["Alpha"]
    assert [h["username"] for h in search_players(rep, "3")] == ["Charlie"]   # osu! user id
    assert search_players(rep, "nobody-here") == []
    assert search_players(rep, "") == []


# ---------- web API -----------------------------------------------------------
@pytest.fixture()
def client(team_db):
    path, conn = team_db
    conn.close()
    service.invalidate()
    return TestClient(create_app(path, client_factory=lambda: None, threaded_imports=False))


def test_api_tournament_pages(client):
    assert client.get("/api/health").status_code == 200
    lst = client.get("/api/tournaments").json()
    assert lst[0]["slug"] == "fake" and lst[0]["format"] == "team" and lst[0]["teams"] == 4
    ov = client.get("/api/tournaments/fake").json()
    assert ov["mvp"]["username"] == "Alpha" and ov["mod_leaders"] and ov["top_teams"]
    lb = client.get("/api/tournaments/fake/leaderboard", params={"mode": "mod", "key": "DT"}).json()
    assert lb["rows"][0]["rank"] == 1 and lb["options"]["mods"]
    perf = client.get("/api/tournaments/fake/leaderboard", params={"mode": "performance"}).json()
    default = client.get("/api/tournaments/fake/leaderboard").json()
    assert perf["mode"] == "performance" and default["mode"] == "tournament"
    assert {"confidence", "performance_rating", "tournament_rating"} <= set(default["rows"][0])
    assert client.get("/api/tournaments/fake/leaderboard", params={"q": "bra"}).json()["rows"][0]["username"] == "Bravo"
    assert client.get("/api/tournaments/fake/leaderboard", params={"mode": "round", "key": "ZZ"}).status_code == 422
    assert client.get("/api/tournaments/fake/search", params={"q": "gol"}).json()[0]["username"] == "Golf"
    assert client.get("/api/tournaments/fake/players/alpha").json()["player"]["rank"] == 1
    assert client.get("/api/tournaments/fake/players/nope").status_code == 404
    teams = client.get("/api/tournaments/fake/teams").json()["teams"]
    assert len(teams) == 4
    assert client.get(f"/api/tournaments/fake/teams/{teams[0]['slug']}").json()["team"]["roster"]
    ms = client.get("/api/tournaments/fake/matches").json()["matches"]
    detail = client.get(f"/api/tournaments/fake/matches/{ms[0]['osu_match_id']}").json()
    assert detail["games"] and detail["games"][0]["scores"]
    assert client.get("/api/tournaments/missing").status_code == 404


def test_api_never_exposes_secrets(client):
    for path in ("/api/health", "/api/tournaments", "/api/tournaments/fake"):
        assert "secret" not in client.get(path).text.lower().replace("osu_credentials_configured", "")


def test_import_job_flow(tmp_path, monkeypatch):
    payloads, rounds = ft.build()
    sheet = tmp_path / "sheet.txt"
    sheet.write_text("Group Stage\n" + "\n".join(
        f"https://osu.ppy.sh/community/matches/{m}" for m, r in rounds.items() if r == "Group Stage"))
    # the importer is hard-wired to Google Sheets; feed it the local file through discover
    from scout import importer
    real = importer.ingest.discover
    monkeypatch.setattr(importer.ingest, "discover", lambda conn, tid, loc, kind=None, **kw: real(conn, tid, str(sheet)))
    db = tmp_path / "imp.db"
    reg = JobRegistry()
    req = ImportRequest(name="Fake", acronym="FK", slug="fake-imp", format="team",
                        sheet_url="https://docs.google.com/spreadsheets/d/abc/edit")
    job = reg.start(db, req, client_factory=lambda: ft.FakeClient(payloads), threaded=False)
    assert job.phase == "done", job.error
    n = sum(1 for r in rounds.values() if r == "Group Stage")
    assert job.found == job.total == job.done == n and job.failed == 0
    conn = connect(db)
    assert repo.get_tournament(conn, "fake-imp")["format"] == "team"
    assert build_report(conn, "fake-imp")["summary"]["teams"] == 4


def test_import_validation_and_missing_credentials(tmp_path, monkeypatch):
    for bad in (dict(slug="Bad Slug"), dict(sheet_url="https://example.com/x"), dict(format="ffa"), dict(name=" ")):
        args = dict(name="N", acronym="N", slug="ok", sheet_url="https://docs.google.com/spreadsheets/d/a/edit", format="1v1")
        args.update(bad)
        with pytest.raises(ValueError):
            ImportRequest(**args).validate()
    from scout import server
    monkeypatch.setattr(server.settings.__class__, "osu_client_id", "", raising=False)
    monkeypatch.setattr(server, "settings", type("S", (), {"osu_client_id": "", "osu_client_secret": "", "db_path": tmp_path / "x.db", "admin_token": "",
                                                "imports_mode": "auto", "allowed_origins": "", "enable_docs": False})())
    c = TestClient(create_app(tmp_path / "x.db"))
    r = c.post("/api/imports", json={"name": "N", "acronym": "N", "slug": "ok", "format": "1v1",
                                     "sheet_url": "https://docs.google.com/spreadsheets/d/a/edit"})
    assert r.status_code == 500 and "OSU_CLIENT" in r.json()["detail"]
    r = c.post("/api/imports", json={"name": "N", "acronym": "N", "slug": "ok", "format": "ffa",
                                     "sheet_url": "https://docs.google.com/spreadsheets/d/a/edit"})
    assert r.status_code == 422


def test_failed_import_leaves_no_ghost_tournament(tmp_path, monkeypatch):
    from scout import importer
    empty = tmp_path / "empty.txt"
    empty.write_text("nothing here")
    real = importer.ingest.discover
    monkeypatch.setattr(importer.ingest, "discover", lambda conn, tid, loc, kind=None, **kw: real(conn, tid, str(empty)))
    db = tmp_path / "g.db"
    job = JobRegistry().start(db, ImportRequest(name="N", acronym="N", slug="ghost", format="1v1",
                                                sheet_url="https://docs.google.com/spreadsheets/d/a/edit"),
                              client_factory=lambda: ft.FakeClient({}), threaded=False)
    assert job.phase == "error" and "No osu! multiplayer links" in job.error
    assert repo.get_tournament(connect(db), "ghost") is None


def test_qualified_players_rank_above_eliminated_ones(tmp_path):
    from scout.analytics.report import leaderboard
    # qualifier: players 3 and 4 clearly out-score 1 and 2 on every map
    qual = [_game(i, 100 + i, i, [_score(1, 300000), _score(2, 280000), _score(3, 900000), _score(4, 850000)])
            for i in range(1, 9)]
    # bracket: only players 1 and 2 qualified
    semi = [_game(50 + i, 200 + i, i, [_score(1, 500000 + i * 1000), _score(2, 480000)]) for i in range(1, 5)]
    conn = connect(tmp_path / "q.db")
    tid = repo.upsert_tournament(conn, "t", "T", fmt="1v1")
    src = tmp_path / "links.txt"
    src.write_text("Qualifiers\nhttps://osu.ppy.sh/community/matches/10\n"
                   "Semifinals\nhttps://osu.ppy.sh/community/matches/11\n")
    ingest.discover(conn, tid, str(src))
    ingest.fetch(conn, tid, ft.FakeClient({10: _payload(10, "X: (Qualifiers) vs (Lobby A)", qual),
                                           11: _payload(11, "X: (p1) vs (p2)", semi)}), progress=lambda *_: None)
    rep = build_report(conn, "t")
    names = [r["username"] for r in rep["rankings"]]
    assert set(names[:2]) == {"p1", "p2"} and set(names[2:]) == {"p3", "p4"}   # the line, whatever the ratings
    assert [r["qualified"] for r in rep["rankings"]] == [True, True, False, False]
    perf = {r["username"]: r["performance_rating"] for r in rep["rankings"]}
    assert perf["p3"] > perf["p1"]                                              # eliminated players can still out-perform
    assert leaderboard(rep)["qualified_cutoff"] == 2
    assert leaderboard(rep, "performance")["qualified_cutoff"] is None          # pure performance has no line
    assert [r["rank"] for r in rep["rankings"]] == [1, 2, 3, 4]


# ---------- import protection -------------------------------------------------
IMPORT_BODY = {"name": "N", "acronym": "N", "slug": "ok", "format": "1v1",
               "sheet_url": "https://docs.google.com/spreadsheets/d/a/edit"}


def _protected(tmp_path, **kw):
    return TestClient(create_app(tmp_path / "p.db", client_factory=lambda: None, threaded_imports=False, **kw))


def test_imports_need_the_admin_token_when_one_is_set(tmp_path):
    c = _protected(tmp_path, admin_token="s3cret")
    assert c.get("/api/health").json()["imports"] == "token"
    assert c.post("/api/imports", json=IMPORT_BODY).status_code == 401
    assert c.post("/api/imports", json=IMPORT_BODY, headers={"X-Admin-Token": "wrong"}).status_code == 401
    # right token gets past the gate (the import itself then fails on the fake sheet, which is fine)
    assert c.post("/api/imports", json=IMPORT_BODY, headers={"X-Admin-Token": "s3cret"}).status_code == 202


def test_imports_can_be_switched_off(tmp_path):
    c = _protected(tmp_path, admin_token="s3cret", imports_mode="off")
    assert c.get("/api/health").json()["imports"] == "off"
    assert c.post("/api/imports", json=IMPORT_BODY, headers={"X-Admin-Token": "s3cret"}).status_code == 403


def test_only_one_import_runs_at_a_time(tmp_path):
    from scout.importer import ImportJob
    c = _protected(tmp_path, admin_token="")
    assert c.get("/api/health").json()["imports"] == "open"
    busy = ImportJob(id="busy", request=ImportRequest(**IMPORT_BODY), phase="importing")
    c.app.state.jobs.jobs["busy"] = busy
    assert c.post("/api/imports", json={**IMPORT_BODY, "slug": "other"}).status_code == 429
    busy.phase = "done"
    assert c.post("/api/imports", json={**IMPORT_BODY, "slug": "other"}).status_code == 202


def test_docs_are_hidden_by_default(tmp_path):
    c = _protected(tmp_path)
    assert c.get("/api/docs").status_code == 404 and c.get("/api/openapi.json").status_code == 404
