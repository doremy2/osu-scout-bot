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


def test_model_endpoint_exposes_live_constants(client):
    m = client.get("/api/model", params={"slug": "fake"}).json()
    assert m["rating"]["scale_below"] > m["rating"]["scale"] and m["rounds"][0]["code"] == "Q"
    assert m["tournament"]["confidence_k"] > 0 and m["awards"]["min_maps_overall"] > 0
    assert client.get("/api/model").json()["tournament"] is None
    assert client.get("/api/model", params={"slug": "nope"}).status_code == 404


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
    reg = JobRegistry(db)
    req = ImportRequest(name="Fake", acronym="FK", slug="fake-imp", format="team",
                        sheet_url="https://docs.google.com/spreadsheets/d/abc/edit")
    job = reg.start(req, client_factory=lambda: ft.FakeClient(payloads), mode="inline")
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
                                                "imports_mode": "auto", "import_mode": "", "turso_url": "", "allowed_origins": "", "enable_docs": False,
                                                "readonly": False})())
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
    job = JobRegistry(db).start(ImportRequest(name="N", acronym="N", slug="ghost", format="1v1",
                                              sheet_url="https://docs.google.com/spreadsheets/d/a/edit"),
                                client_factory=lambda: ft.FakeClient({}), mode="inline")
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
    c = _protected(tmp_path, admin_token="")
    assert c.get("/api/health").json()["imports"] == "open"
    store = c.app.state.jobs.store
    busy = store.create(ImportRequest(**IMPORT_BODY))
    busy.phase = "importing"
    store.save(busy)
    assert c.post("/api/imports", json={**IMPORT_BODY, "slug": "other"}).status_code == 429
    busy.phase = "done"
    store.save(busy)
    assert c.post("/api/imports", json={**IMPORT_BODY, "slug": "other"}).status_code == 202


def test_docs_are_hidden_by_default(tmp_path):
    c = _protected(tmp_path)
    assert c.get("/api/docs").status_code == 404 and c.get("/api/openapi.json").status_code == 404


# ---------- draft simulator ------------------------------------------------------
def test_match_win_probability_basics():
    from scout.analytics.draft import match_win_probability, build_sequence, DraftConfig
    assert abs(match_win_probability([0.5] * 9, 9) - 0.5) < 1e-9
    assert match_win_probability([0.9] * 9, 9) > 0.99 and match_win_probability([0.1] * 9, 9) < 0.01
    assert match_win_probability([1.0] * 5, 9) == 1.0
    assert abs(match_win_probability([], 9, 0.5) - 0.5) < 1e-9
    seq = build_sequence(DraftConfig(best_of=9, bans_per_side=2, first_ban="A", first_pick="B"), 20, True)
    assert [x["side"] for x in seq[:4]] == ["A", "B", "A", "B"] and seq[4] == {"type": "pick", "side": "B"}
    assert sum(x["type"] == "pick" for x in seq) == 8
    # a small pool shrinks the number of picks instead of failing
    assert sum(x["type"] == "pick" for x in build_sequence(DraftConfig(best_of=13), 10, False)) == 10 - 4


def test_draft_api_runs_a_full_draft(client):
    setup = client.get("/api/tournaments/fake/draft/setup").json()
    assert setup["side_kind"] == "team" and len(setup["sides"]) == 4
    rnd = max(setup["rounds"], key=lambda r: r["maps"])["round"]
    a, b = setup["sides"][0]["slug"], setup["sides"][1]["slug"]
    body = {"a": a, "b": b, "round": rnd, "bans_per_side": 1, "best_of": 5}
    taken: list[int] = []
    for _ in range(40):
        d = client.post("/api/tournaments/fake/draft/advice", json={**body, "taken": taken}).json()
        assert all(0 < m["p_a"] < 1 for m in d["maps"]) and 0 < d["match_win_a"] < 1
        if d["done"]:
            break
        assert d["next"]["side"] in ("A", "B") and d["suggestions"]
        taken.append(d["suggestions"][0]["beatmap_id"])          # follow the advice
    assert d["done"] and len(taken) == len(d["sequence"]) == 2 + 4
    assert [m["taken"]["type"] for m in sorted((m for m in d["maps"] if "taken" in m), key=lambda m: m["taken"]["order"])]         == ["ban", "ban", "pick", "pick", "pick", "pick"]
    # swapping the sides mirrors the odds
    r = client.post("/api/tournaments/fake/draft/advice", json={**body, "a": b, "b": a, "taken": []}).json()
    first = client.post("/api/tournaments/fake/draft/advice", json={**body, "taken": []}).json()
    m1 = {m["beatmap_id"]: m["p_a"] for m in first["maps"]}
    assert all(abs(m["p_a"] + m1[m["beatmap_id"]] - 1) < 0.05 for m in r["maps"])
    assert client.post("/api/tournaments/fake/draft/advice", json={**body, "b": a}).status_code == 422
    assert client.post("/api/tournaments/fake/draft/advice", json={**body, "a": "nope"}).status_code == 404


def test_draft_pool_includes_maps_nobody_has_played(team_db):
    """The pool comes from the sheet's slot list, so unplayed maps (and rounds with no matches yet) still appear, in slot order."""
    from scout.analytics.draft import pool_for_round, round_pools
    from scout.analytics.report import build_analysis
    path, conn = team_db
    tid = repo.get_tournament(conn, "fake")["id"]
    played = conn.execute("""SELECT DISTINCT g.beatmap_id FROM match_games g JOIN tournament_matches m ON m.id = g.match_id
                             WHERE m.round = 'GS' AND g.beatmap_id < 9000 ORDER BY g.beatmap_id LIMIT 3""").fetchall()
    ids = [r[0] for r in played]
    slots = [
        {"round": "GS", "slot": "NM1", "beatmap_id": ids[0], "beatmapset_id": None, "position": 0, "label": None, "star_rating": None},
        {"round": "GS", "slot": "NM2", "beatmap_id": None, "beatmapset_id": 321, "position": 1,
         "label": "Some Artist - Unplayed Song [Hard]", "star_rating": 6.5},
        {"round": "GS", "slot": "HD1", "beatmap_id": ids[1], "beatmapset_id": None, "position": 2, "label": None, "star_rating": None},
        {"round": "GS", "slot": "TB", "beatmap_id": ids[2], "beatmapset_id": None, "position": 3, "label": None, "star_rating": None},
        {"round": "F", "slot": "NM1", "beatmap_id": None, "beatmapset_id": 1, "position": 0, "label": "A - B [C]", "star_rating": 5.0},
        {"round": "F", "slot": "NM2", "beatmap_id": None, "beatmapset_id": 2, "position": 1, "label": "D - E [F]", "star_rating": 6.0},
    ]
    repo.save_pool_slots(conn, tid, slots)
    an = build_analysis(conn, "fake")
    pool = pool_for_round(an, "GS")
    assert [m["slot"] for m in pool] == ["NM1", "NM2", "HD1", "TB"]
    unplayed = pool[1]
    assert (unplayed["artist"], unplayed["title"], unplayed["version"]) == ("Some Artist", "Unplayed Song", "Hard")
    assert unplayed["plays"] == 0 and unplayed["beatmap_id"] < 0 and unplayed["beatmapset_id"] == 321
    assert pool[3]["mod"] == "TB"
    assert any(r["round"] == "F" and r["maps"] == 2 for r in round_pools(an))     # a round nobody has played yet


# ---------- Vercel / read-only hosting -------------------------------------------------
def test_writable_copy_of_a_readonly_snapshot(tmp_path):
    from scout.db import writable_copy
    src = tmp_path / "snap.db"
    conn = connect(src)
    conn.close()
    dest = writable_copy(src, name="scout-test-copy.db")
    assert dest != src and dest.exists()
    c2 = connect(dest)                      # opens, migrates and writes without touching the original
    c2.execute("INSERT INTO tournaments (slug, name) VALUES ('x', 'X')")
    c2.commit()
    assert connect(src).execute("SELECT COUNT(*) FROM tournaments").fetchone()[0] == 0


def test_vercel_entrypoint_serves_the_snapshot_and_refuses_imports():
    import subprocess
    code = ";".join([
        "from fastapi.testclient import TestClient",
        "import scout_api",
        "c = TestClient(scout_api.app)",
        "assert c.get('/api/health').json()['imports'] == 'off'",
        "assert len(c.get('/api/tournaments').json()) >= 1",
        "r = c.post('/api/imports', json={'name': 'N', 'acronym': 'N', 'slug': 'ok', 'format': '1v1',"
        " 'sheet_url': 'https://docs.google.com/spreadsheets/d/a/edit'})",
        "assert r.status_code == 403, r.text",
        "print('ok')",
    ])
    env = {k: v for k, v in __import__("os").environ.items() if not k.startswith("SCOUT_")}
    env["VERCEL"] = "1"
    out = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).resolve().parent.parent, env=env,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0 and "ok" in out.stdout, out.stderr[-800:]


def test_vercel_json_is_consistent_with_the_code():
    import importlib
    import json
    cfg = json.loads((Path(__file__).resolve().parent.parent / "vercel.json").read_text(encoding="utf-8"))
    services = cfg["services"]
    assert set(services) == {"web", "scout_api"}
    # only web is public: the catch-all rewrite targets it and nothing targets scout_api
    assert [r["destination"]["service"] for r in cfg["rewrites"]] == ["web"]
    # web reaches scout_api through exactly one binding, into the env var the web app reads
    assert services["web"]["bindings"] == [{"type": "service", "service": "scout_api", "format": "url", "env": "SCOUT_API_URL"}]
    assert "bindings" not in services["scout_api"]
    # the FastAPI entrypoint "module:attr" resolves to an ASGI app
    module, attr = services["scout_api"]["entrypoint"].split(":")
    root = str(Path(__file__).resolve().parent.parent)
    sys.path.insert(0, root)
    assert hasattr(importlib.import_module(module), attr)
    # build/runtime keys must live inside services, not at the top level
    assert not {"framework", "buildCommand", "installCommand", "functions", "outputDirectory"} & set(cfg)


# ---------- stepped imports (serverless): the browser drives the job with short requests -------------
def test_stepped_import_runs_to_completion_in_slices(tmp_path, monkeypatch):
    import dataclasses

    from scout import importer
    payloads, rounds = ft.build()
    sheet = tmp_path / "sheet.txt"
    sheet.write_text("Group Stage\n" + "\n".join(
        f"https://osu.ppy.sh/community/matches/{m}" for m, r in rounds.items() if r == "Group Stage"))
    real = importer.ingest.discover
    monkeypatch.setattr(importer.ingest, "discover", lambda conn, tid, loc, kind=None, **kw: real(conn, tid, str(sheet)))
    monkeypatch.setattr(importer, "settings", dataclasses.replace(importer.settings, step_budget=0.0))   # one lobby per step
    app = create_app(tmp_path / "s.db", client_factory=lambda: ft.FakeClient(payloads), import_mode="step",
                     admin_token="invite")
    c = TestClient(app)
    body = {"name": "Stepped", "acronym": "ST", "slug": "stepped", "format": "team",
            "sheet_url": "https://docs.google.com/spreadsheets/d/abc/edit"}
    assert c.post("/api/imports", json=body).status_code == 401                      # invite code required
    started = c.post("/api/imports", json=body, headers={"X-Admin-Token": "invite"})
    assert started.status_code == 202 and started.json()["mode"] == "step" and started.json()["phase"] == "queued"
    job_id = started.json()["id"]
    assert c.post(f"/api/imports/{job_id}/step").status_code == 401                  # steps are protected too

    seen, last = [], None
    for _ in range(60):
        last = c.post(f"/api/imports/{job_id}/step", headers={"X-Admin-Token": "invite"}).json()
        seen.append((last["phase"], last["done"]))
        if last["phase"] in ("done", "error"):
            break
    assert last["phase"] == "done", last
    n = sum(1 for r in rounds.values() if r == "Group Stage")
    assert last["total"] == last["done"] == n
    assert len([s for s in seen if s[0] == "importing"]) >= 2                        # genuinely spread over several requests
    assert c.get(f"/api/imports/{job_id}").json()["phase"] == "done"                 # status readable by anyone with the id
    assert c.get("/api/tournaments/stepped").status_code == 200
    # a second import can start now that the first is finished
    assert c.post("/api/imports", json={**body, "slug": "stepped-2"}, headers={"X-Admin-Token": "invite"}).status_code == 202


def test_seeding_copies_a_database_into_a_libsql_target(team_db, tmp_path):
    from scout.seed import copy_database
    path, conn = team_db
    conn.close()
    dst = connect(tmp_path / "target.db", backend="libsql")
    counts = copy_database(path, dst, progress=lambda *_: None)
    assert counts["tournaments"] == 1 and counts["game_scores"] > 100 and counts["tournament_teams"] == 4
    src_report = build_report(connect(path), "fake")
    dst_report = build_report(dst, "fake")
    assert [r["username"] for r in dst_report["rankings"]] == [r["username"] for r in src_report["rankings"]]
    assert [t["slug"] for t in dst_report["teams"]] == [t["slug"] for t in src_report["teams"]]
    copy_database(path, dst, progress=lambda *_: None)          # re-running replaces rows instead of duplicating them
    assert dst.execute("SELECT COUNT(*) FROM game_scores").fetchone()[0] == counts["game_scores"]
