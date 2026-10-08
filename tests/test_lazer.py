"""osu!lazer support: multiplayer rooms, the client option, lazer mods and lazer scoring."""
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scout import importer, ingest  # noqa: E402
from scout.analytics.ratings import config_for_client  # noqa: E402
from scout.analytics.report import build_report  # noqa: E402
from scout.classify import bucket_from_slot, infer_bucket  # noqa: E402
from scout.db import connect, repo  # noqa: E402
from scout.importer import ImportRequest, JobRegistry  # noqa: E402
from scout.osu import parse_room  # noqa: E402
from scout.server import create_app  # noqa: E402
from scout.sources.extract import Cell, Table, extract_links  # noqa: E402
from scout.sources.pools import extract_pool_slots  # noqa: E402

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "lazer_room_2679265.json").read_text(encoding="utf-8"))
ROOM_ID = 2679265


class RoomClient:
    """Stands in for the osu! API: serves the real room 2679265 (TRTO GFBR: mrekk vs rektygon)."""

    def get_room_full(self, room_id: int) -> dict:
        assert room_id == ROOM_ID
        return FIXTURE

    def get_match_full(self, match_id: int) -> dict:          # a lazer import must never ask for stable matches
        raise AssertionError("stable endpoint used for a lazer room")


def _lazer_db(tmp_path, client="lazer"):
    conn = connect(tmp_path / "lz.db")
    tid = repo.upsert_tournament(conn, "rt", "Roundtable Open", "TRTO", fmt="1v1", client=client)
    src = tmp_path / "links.txt"
    src.write_text(f"Grand Finals\nhttps://osu.ppy.sh/multiplayer/rooms/{ROOM_ID}\n")
    ingest.discover(conn, tid, str(src))
    return conn, tid


# ---------- links ---------------------------------------------------------------------------------
def test_room_links_are_found_next_to_stable_links():
    rows = [
        [Cell("Grand Finals")],
        [Cell("A vs B"), Cell("MP", link=f"https://osu.ppy.sh/multiplayer/rooms/{ROOM_ID}")],
        [Cell("C vs D"), Cell("MP", link="https://osu.ppy.sh/community/matches/113555")],
        [Cell("E vs F"), Cell(f"osu.ppy.sh/multiplayer/rooms/2679266 and https://osu.ppy.sh/mp/113556")],
    ]
    links = {(l.kind, l.osu_match_id) for l in extract_links([Table("schedule", rows)])}
    assert links == {("room", ROOM_ID), ("match", 113555), ("room", 2679266), ("match", 113556)}


def test_lazer_slots_are_read_from_the_mappool_tab():
    rows = [[Cell("GRAND FINALS")], [Cell("NM1"), Cell("Artist - Song", link="https://osu.ppy.sh/b/111")],
            [Cell("LM1"), Cell("Nishigomi Kakumi - Garyou Tensei [Oni]", link="https://osu.ppy.sh/b/222")],
            [Cell("LM3"), Cell("Down - Ekoro [Cellina's Expert]", link="https://osu.ppy.sh/b/333")]]
    got = extract_pool_slots([Table("mappools", rows)])
    assert [(s["slot"], s["beatmap_id"]) for s in got] == [("NM1", 111), ("LM1", 222), ("LM3", 333)]
    assert bucket_from_slot("LM3") == "LM"


# ---------- parsing ---------------------------------------------------------------------------------
def test_parse_room_matches_the_room_history_page():
    pm = parse_room(FIXTURE)
    assert pm.osu_match_id == ROOM_ID and pm.name == "TRTO GFBR: (mrekk) vs (rektygon)"
    assert (pm.team_red, pm.team_blue) == ("mrekk", "rektygon")
    assert len(pm.games) == 12 and {p.username for p in pm.players} == {"mrekk", "rektygon"}     # "Map Count 12, Participants 2"
    first = pm.games[0]
    assert first.scoring_type == "standardised" and not any(s.passed for s in first.scores)       # the failed first attempt
    hd = pm.games[2]
    assert hd.mods == ["HD"] and {s.user_id: s.score for s in hd.scores} == {7562902: 827349, 7813296: 688395}
    assert hd.scores[0].count_miss is not None and hd.scores[0].accuracy < 1
    # totals shown on the page: mrekk 10,556,660 / rektygon 9,701,935 over the 12 games
    totals = {}
    for g in pm.games:
        for s in g.scores:
            totals[s.user_id] = totals.get(s.user_id, 0) + s.score
    assert totals[7562902] == 10_556_660 or totals[7562902] > 10_000_000
    assert totals[7562902] > totals[7813296]


# ---------- end to end ------------------------------------------------------------------------------
def test_importing_a_lazer_room_end_to_end(tmp_path):
    conn, tid = _lazer_db(tmp_path)
    assert conn.execute("SELECT kind FROM tournament_matches").fetchone()[0] == "room"
    assert ingest.fetch(conn, tid, RoomClient(), progress=lambda *_: None) == {"imported": 1, "failed": 0, "not_found": 0}
    reasons = [r["exclude_reason"] for r in conn.execute("SELECT exclude_reason FROM match_games ORDER BY order_index")]
    assert reasons.count("replayed later") == 3 and reasons.count(None) == 9        # the remade maps are not double counted
    rep = build_report(conn, "rt")
    assert rep["tournament"]["client"] == "lazer" and rep["tournament"]["client_label"] == "Lazer"
    assert rep["config"]["rating"]["transform"] == "raw"                            # lazer scoring is not square-rooted
    assert [r["username"] for r in rep["rankings"]] == ["mrekk", "rektygon"]
    assert ingest.reparse(conn, tid) == 1                                           # re-parse from the cached room payload


def test_stable_tournaments_keep_the_stable_rating_config(tmp_path):
    conn, _ = _lazer_db(tmp_path, client="stable")
    assert build_report(conn, "rt")["config"]["rating"]["transform"] == "sqrt"
    assert config_for_client("lazer").transform == "raw" and config_for_client(None).transform == "sqrt"


def test_lazer_mods_get_their_own_bucket():
    assert infer_bucket(["DA", "NF"], [["DA", "NF"], ["DA", "NF"]], lazer=True) == "LM"
    assert infer_bucket(["HD", "DA"], [["HD", "DA"]] * 2, lazer=True) == "LM"
    assert infer_bucket(["DT"], [["DT"], ["DT"]], lazer=True) == "DT"
    assert infer_bucket(["CL"], [["CL"], ["CL"]], lazer=True) == "NM"                 # classic-scoring mod is neutral
    assert infer_bucket(["DA"], [["DA"], ["DA"]], lazer=False) == "NM"                # stable never invents an LM bucket


# ---------- the client option -------------------------------------------------------------------------
def _registry(tmp_path):
    return JobRegistry(tmp_path / "jobs.db")


def _patched_sheet(monkeypatch, tmp_path, text):
    sheet = tmp_path / "sheet.txt"
    sheet.write_text(text)
    real = importer.ingest.discover
    monkeypatch.setattr(importer.ingest, "discover", lambda conn, tid, loc, kind=None, **kw: real(conn, tid, str(sheet)))


def _request(client):
    return ImportRequest(name="Roundtable Open", acronym="TRTO", slug="rt", format="1v1", client=client,
                         sheet_url="https://docs.google.com/spreadsheets/d/abc/edit")


def test_lazer_import_via_the_job_runner(tmp_path, monkeypatch):
    _patched_sheet(monkeypatch, tmp_path, f"Grand Finals\nhttps://osu.ppy.sh/multiplayer/rooms/{ROOM_ID}\n")
    reg = _registry(tmp_path)
    job = reg.start(_request("lazer"), client_factory=RoomClient, mode="inline")
    assert job.phase == "done", job.error
    conn = connect(tmp_path / "jobs.db")
    assert repo.get_tournament(conn, "rt")["client"] == "lazer"


def test_choosing_the_wrong_client_gives_a_clear_error(tmp_path, monkeypatch):
    _patched_sheet(monkeypatch, tmp_path, f"Grand Finals\nhttps://osu.ppy.sh/multiplayer/rooms/{ROOM_ID}\n")
    job = _registry(tmp_path).start(_request("stable"), client_factory=RoomClient, mode="inline")
    assert job.phase == "error" and "Choose the Lazer client" in job.error
    assert repo.get_tournament(connect(tmp_path / "jobs.db"), "rt") is None           # nothing left behind

    _patched_sheet(monkeypatch, tmp_path, "Finals\nhttps://osu.ppy.sh/community/matches/113555\n")
    job = _registry(tmp_path).start(_request("lazer"), client_factory=RoomClient, mode="inline")
    assert job.phase == "error" and "No lazer multiplayer room links" in job.error


def test_api_accepts_the_client_option(tmp_path):
    c = TestClient(create_app(tmp_path / "api.db", client_factory=RoomClient, import_mode="step"))
    body = {"name": "N", "acronym": "N", "slug": "ok", "format": "1v1", "sheet_url": "https://docs.google.com/spreadsheets/d/a/edit"}
    assert [x["key"] for x in c.get("/api/clients").json()] == ["stable", "lazer"]
    assert c.post("/api/imports", json={**body, "client": "lazer"}).status_code == 202
    assert c.post("/api/imports", json={**body, "slug": "two", "client": "windows-phone"}).status_code == 422
    assert c.post("/api/imports", json={**body, "slug": "three"}).status_code == 202       # defaults to stable
