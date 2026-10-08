"""Command line: python -m scout <command> ...

Typical flow:
  python -m scout import vrso-2027 "https://docs.google.com/spreadsheets/d/..." --name "VRSO 2027" --acronym VRSO --format team
  python -m scout pool vrso-2027 data/pools/vrso-2027.csv        (optional, improves mod/warmup detection)
  python -m scout report vrso-2027
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

from . import ingest
from .analytics import build_report
from .config import settings
from .db import connect, repo
from .clients import CLIENTS
from .formats import FORMATS
from .models import MatchLink
from .rounds import round_name
from .sources import extract_links_from_text


def _client():
    from .osu import OsuClient
    return OsuClient(settings.osu_client_id, settings.osu_client_secret, settings.osu_min_interval)


def _tid(conn, slug: str) -> int:
    t = repo.get_tournament(conn, slug)
    if not t:
        sys.exit(f"Unknown tournament '{slug}'. Create it with: python -m scout create {slug}")
    return t["id"]


def _upsert(conn, a) -> int:
    """Create/update the tournament. A brand-new tournament must say what format it is."""
    if repo.get_tournament(conn, a.slug) is None and not a.format:
        sys.exit("A new tournament needs --format 1v1 or --format team.")
    return repo.upsert_tournament(conn, a.slug, a.name, a.acronym, a.warmups, a.format, a.client)


def cmd_create(conn, a):
    tid = _upsert(conn, a)
    print(f"Tournament '{a.slug}' ready (id {tid}).")


def cmd_discover(conn, a):
    tid = _upsert(conn, a)
    found, new = ingest.discover(conn, tid, a.source, kind=a.kind, round_override=a.round)
    print(f"Found {found} MP links ({new} new) in {a.source}")
    unrounded = conn.execute(
        "SELECT COUNT(*) FROM tournament_matches WHERE tournament_id = ? AND round IS NULL", (tid,)).fetchone()[0]
    if unrounded:
        print(f"  {unrounded} matches have no round yet — re-run with --round, or use `set-round`.")
    return tid


def cmd_add(conn, a):
    tid = repo.upsert_tournament(conn, a.slug)
    links = extract_links_from_text("\n".join(a.links))
    links += [MatchLink(int(x)) for x in a.links if x.isdigit()]
    new = repo.add_match_links(conn, tid, links, round_override=a.round)
    print(f"Added {new} new matches.")


def cmd_fetch(conn, a):
    tid = _tid(conn, a.slug)
    counts = ingest.fetch(conn, tid, _client(), retry_failed=a.retry_failed, use_cache=not a.refetch)
    print(f"Done: {counts}")


def cmd_import(conn, a):
    a.slug = a.slug
    cmd_discover(conn, a)
    a.retry_failed, a.refetch = False, False
    cmd_fetch(conn, a)
    cmd_status(conn, a)


def cmd_status(conn, a):
    tid = _tid(conn, a.slug)
    rows = conn.execute(
        "SELECT status, COUNT(*) n FROM tournament_matches WHERE tournament_id = ? GROUP BY status", (tid,)).fetchall()
    print("Matches:", ", ".join(f"{r['status']}={r['n']}" for r in rows) or "none")
    for r in conn.execute(
            "SELECT round, COUNT(*) n FROM tournament_matches WHERE tournament_id = ? GROUP BY round ORDER BY round", (tid,)):
        print(f"  {round_name(r['round']) if r['round'] else '(no round)'}: {r['n']}")
    g = conn.execute(
        """SELECT SUM(g.excluded = 0) used, SUM(g.excluded) excl FROM match_games g
           JOIN tournament_matches m ON m.id = g.match_id WHERE m.tournament_id = ?""", (tid,)).fetchone()
    print(f"Games: {g['used'] or 0} used, {g['excl'] or 0} excluded")
    for r in conn.execute(
            """SELECT g.exclude_reason, COUNT(*) n FROM match_games g JOIN tournament_matches m ON m.id = g.match_id
               WHERE m.tournament_id = ? AND g.excluded = 1 GROUP BY g.exclude_reason""", (tid,)):
        print(f"  excluded — {r['exclude_reason']}: {r['n']}")
    for r in conn.execute(
            "SELECT osu_match_id, error FROM tournament_matches WHERE tournament_id = ? AND status IN ('failed','not_found')", (tid,)):
        print(f"  ! {r['osu_match_id']}: {r['error']}")


def cmd_pool(conn, a):
    tid = _tid(conn, a.slug)
    with open(a.csv, encoding="utf-8-sig", newline="") as f:
        rows = [{k.strip().lower(): (v or "").strip() for k, v in r.items()} for r in csv.DictReader(f)]
    n = repo.load_mappool(conn, tid, rows)
    print(f"Loaded {n} mappool entries. Reclassifying games: {ingest.finalize(conn, tid)}")


def cmd_set_round(conn, a):
    tid = _tid(conn, a.slug)
    repo.set_match_round(conn, tid, a.match_id, a.round)
    print(f"{a.match_id} -> {a.round}")


def cmd_reparse(conn, a):
    tid = _tid(conn, a.slug)
    print(f"Re-parsed {ingest.reparse(conn, tid)} matches from cache.")


def cmd_recompute(conn, a):
    tid = _tid(conn, a.slug)
    print(f"Reclassified games + teams: {ingest.finalize(conn, tid)}")
    cmd_status(conn, a)


def cmd_serve(conn, a):
    from .server import run
    run(port=a.port)


def cmd_dev(conn, a):
    """API (8001) + Next.js dev server (3000) together."""
    import subprocess
    web = Path(__file__).resolve().parent.parent / "web"
    npm = "npm.cmd" if sys.platform == "win32" else "npm"
    node = subprocess.Popen([npm, "run", "dev"], cwd=web)
    try:
        from .server import run
        print("\n  osu! scout  ->  http://localhost:3000\n")
        run(port=a.port)
    finally:
        if sys.platform == "win32":   # npm.cmd spawns node; kill the whole tree
            subprocess.run(["taskkill", "/T", "/F", "/PID", str(node.pid)], capture_output=True)
        else:
            node.terminate()


def cmd_report(conn, a):
    rep = build_report(conn, a.slug)
    if a.json:
        Path(a.json).parent.mkdir(parents=True, exist_ok=True)
        Path(a.json).write_text(json.dumps(rep, indent=2, default=list), encoding="utf-8")
        print(f"Wrote {a.json}")
    if a.player:
        page = rep["players"].get(repo.slugify(a.player))
        if not page:
            sys.exit(f"No player '{a.player}' in this tournament")
        print_player(page)
        return
    print_report(rep, a.top)


def print_report(rep: dict, top: int) -> None:
    t, s = rep["tournament"], rep["summary"]
    print(f"\n== {t['name']} ({t['start_date']} → {t['end_date']}) ==")
    print(f"{s['matches']} matches · {s['games']} maps · {s['players']} players · mods: {' '.join(s['mods'])}")
    print("\nAwards")
    for aw in rep["awards"]:
        w = aw["winner"]
        print(f"  {aw['title']:<34} {w['username'] + '  ' + str(w['value']) if w else '— (nobody met: ' + aw['rule'] + ')'}")
    mods = s["mods"]
    print(f"\nRankings (top {top})")
    print(f"  {'#':>3} {'player':<18}{'rating':>7}{'maps':>6}  " + "".join(f"{m:>6}" for m in mods) + "   map WR  matches")
    for r in rep["rankings"][:top]:
        mr = "".join(f"{r['mod_ratings'][m]['rating']:>6.2f}" if m in r["mod_ratings"] else f"{'-':>6}" for m in mods)
        wr = f"{100 * r['map_winrate']:.0f}%" if r["map_winrate"] is not None else "-"
        print(f"  {r['rank']:>3} {r['username'][:17]:<18}{r['rating']:>7.2f}{r['maps_played']:>6}  {mr}   {wr:>6}  "
              f"{r['match_record'][0]}-{r['match_record'][1]}")


def print_player(p: dict) -> None:
    print(f"\n{p['username']} ({p['country'] or '?'})")
    print(f"Overall Tournament Rating: {p['rating']:.2f}   (raw {p['rating_raw']:.2f})")
    print(f"Tournament Rank: #{p['rank']} / {p['rank_of']}")
    for m, v in p["mod_ratings"].items():
        print(f"  {m}: {v['rating']:.2f}  ({v['maps']} maps)")
    print(f"Maps Played: {p['maps_played']}")
    print(f"Average Score: {p['avg_score']:,}" if p["avg_score"] else "Average Score: -")
    print(f"Average Accuracy: {100 * p['avg_accuracy']:.2f}%" if p["avg_accuracy"] else "Average Accuracy: -")
    wr = f"{100 * p['map_winrate']:.1f}%" if p["map_winrate"] is not None else "-"
    print(f"Map Win Rate: {wr}  ({p['map_record'][0]}-{p['map_record'][1]})")
    print(f"Match Record: {p['match_record'][0]}-{p['match_record'][1]}")
    if p.get("best_performance"):
        b = p["best_performance"]
        print(f"Best Performance: {b['score']:,} on {b['map']} ({b['mod']}, {round_name(b['round'])}, z {b['z']:+.2f})")
    print("Performance by round:")
    for r in p["by_round"]:
        print(f"  {r['round_name']:<15} {r['rating']:.2f}  ({r['maps']} maps)")
    print("Match history:")
    for h in p["match_history"]:
        print(f"  {round_name(h['round']):<15} {h['result'] or '?'} {h['score'] or '':<6} {h['name']}")


def cmd_update(conn, a):
    """Rescan tournament sources and import only the lobbies that are new."""
    from . import updater
    from .importer import JobStore
    if a.slug:
        res = updater.check_tournament(a.db, a.slug)
        print(f"{a.slug}: {res['state']} - {res.get('new', 0)} new, {res.get('pending', 0)} waiting" + (f" ({res['error']})" if res.get("error") else ""))
        if res.get("job"):
            import time
            job = updater.drive(JobStore(a.db), res["job"], _client, time.monotonic() + a.budget)
            print(f"  import job {job.phase}: {job.done}/{job.total}")
        return
    out = updater.tick(a.db, _client, budget=a.budget, discovery=not a.no_discovery)
    for r in out["resumed"]:
        print(f"resumed {r['slug']}: {r['phase']}")
    for r in out["checked"]:
        print(f"{r['slug']}: {r['state']} - {r.get('new', 0)} new, {r.get('pending', 0)} waiting")
    if not out["resumed"] and not out["checked"]:
        print("Nothing is due.")
    if out["discovery"]:
        for sc in out["discovery"]["scanned"]:
            print(f"discovery source {sc['source_id']}: {sc['created']} new candidates, {sc['known']} known")


def cmd_source(conn, a):
    from . import discovery
    if a.action == "add":
        sid = discovery.add_source(conn, a.name, a.kind, a.url, a.every)
        print(f"Source {sid} added: {a.name}")
    elif a.action == "list":
        for s in discovery.list_sources(conn):
            print(f"{s['id']:>3} {'on ' if s['enabled'] else 'off'} {s['kind']:<5} {s['name']:<30} last {s['last_scanned_at'] or 'never'} "
                  f"({s['last_status'] or '-'}, {s['last_found']} sheets)  {s['url']}")
    else:
        ids = [a.id] if a.id else [s["id"] for s in discovery.list_sources(conn) if s["enabled"]]
        for sid in ids:
            r = discovery.scan_source(a.db, sid)
            print(f"source {sid}: {r.get('error') or ''}{r['created']} new, {r['updated']} updated, {r['known']} known, "
                  f"{r['duplicate']} duplicate, {r['no_links']} without links, {r['errors']} errors, {r['deferred']} deferred")


def cmd_candidates(conn, a):
    from . import discovery
    rows = discovery.list_candidates(conn, a.status)
    for c in rows:
        print(f"#{c['id']:<4} {c['confidence']:.2f} {c['status']:<9} {c['detected_format']:<4} {c['detected_client']:<6} "
              f"{c['match_count']:>4} MP  {c['name']}  [{c['suggested_slug']}]  {c['source_url']}")
        if a.why:
            for r in c["reasons"]:
                print(f"        {r}")
    if not rows:
        print("The queue is empty.")


def cmd_approve(conn, a):
    from . import discovery
    from .importer import JobRegistry
    res = discovery.approve_candidate(a.db, a.id, JobRegistry(a.db), _client, mode="inline")
    job = res["job"]
    print(f"{res['candidate']['tournament_slug']}: import {job.phase}" + (f" - {job.error}" if job.error else ""))


def cmd_ignore(conn, a):
    from . import discovery
    print("Ignored." if discovery.set_candidate_status(conn, a.id, "ignored") else "No pending candidate with that id.")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="scout", description="osu! tournament ingestion + analytics")
    ap.add_argument("--db", default=str(settings.db_path))
    sub = ap.add_subparsers(dest="cmd", required=True)

    def tourney_opts(p):
        p.add_argument("--name")
        p.add_argument("--acronym")
        p.add_argument("--warmups", type=int, help="games to skip per lobby when no mappool is loaded")
        p.add_argument("--format", choices=sorted(FORMATS), help="tournament format (required for a new tournament)")
        p.add_argument("--client", choices=sorted(CLIENTS), help="osu! client: stable (default) or lazer (multiplayer rooms)")

    p = sub.add_parser("create"); p.add_argument("slug"); tourney_opts(p); p.set_defaults(fn=cmd_create)
    for nm, fn in (("discover", cmd_discover), ("import", cmd_import)):
        p = sub.add_parser(nm, help="scan a sheet/CSV/XLSX/text file for MP links" + (" and fetch them" if nm == "import" else ""))
        p.add_argument("slug"); p.add_argument("source")
        p.add_argument("--kind", choices=["google_sheet", "csv", "xlsx", "text"])
        p.add_argument("--round", help="force this round for every link in the source")
        tourney_opts(p); p.set_defaults(fn=fn)
    p = sub.add_parser("add", help="add MP links/ids by hand"); p.add_argument("slug"); p.add_argument("links", nargs="+")
    p.add_argument("--round"); p.set_defaults(fn=cmd_add)
    p = sub.add_parser("fetch"); p.add_argument("slug")
    p.add_argument("--retry-failed", action="store_true"); p.add_argument("--refetch", action="store_true")
    p.set_defaults(fn=cmd_fetch)
    p = sub.add_parser("status"); p.add_argument("slug"); p.set_defaults(fn=cmd_status)
    p = sub.add_parser("pool", help="load mappool CSV: beatmap_id,slot[,round]"); p.add_argument("slug"); p.add_argument("csv")
    p.set_defaults(fn=cmd_pool)
    p = sub.add_parser("set-round"); p.add_argument("slug"); p.add_argument("match_id", type=int); p.add_argument("round")
    p.set_defaults(fn=cmd_set_round)
    p = sub.add_parser("reparse", help="rebuild games/scores from cached API data"); p.add_argument("slug")
    p.set_defaults(fn=cmd_reparse)
    p = sub.add_parser("recompute", help="re-run classification + team detection (no API calls)")
    p.add_argument("slug"); p.set_defaults(fn=cmd_recompute)
    p = sub.add_parser("update", help="rescan tournament sources and import only the new lobbies (all that are due, or one slug)")
    p.add_argument("slug", nargs="?"); p.add_argument("--budget", type=float, default=600, help="seconds of work")
    p.add_argument("--no-discovery", action="store_true"); p.set_defaults(fn=cmd_update)
    p = sub.add_parser("source", help="discovery sources: add | list | scan")
    p.add_argument("action", choices=["add", "list", "scan"]); p.add_argument("--name"); p.add_argument("--url")
    p.add_argument("--kind", choices=["page", "sheet"], default="page"); p.add_argument("--every", type=int, default=360, help="minutes between scans")
    p.add_argument("--id", type=int); p.set_defaults(fn=cmd_source)
    p = sub.add_parser("candidates", help="the discovery review queue"); p.add_argument("--status", default="pending")
    p.add_argument("--why", action="store_true", help="show the confidence reasons"); p.set_defaults(fn=cmd_candidates)
    p = sub.add_parser("approve", help="approve a candidate: imports it"); p.add_argument("id", type=int); p.set_defaults(fn=cmd_approve)
    p = sub.add_parser("ignore", help="ignore a candidate"); p.add_argument("id", type=int); p.set_defaults(fn=cmd_ignore)
    p = sub.add_parser("serve", help="start the web API on 127.0.0.1:8001"); p.add_argument("--port", type=int, default=8001)
    p.set_defaults(fn=cmd_serve)
    p = sub.add_parser("dev", help="start API + website (http://localhost:3000)"); p.add_argument("--port", type=int, default=8001)
    p.set_defaults(fn=cmd_dev)
    p = sub.add_parser("report"); p.add_argument("slug"); p.add_argument("--json"); p.add_argument("--player")
    p.add_argument("--top", type=int, default=20); p.set_defaults(fn=cmd_report)

    a = ap.parse_args(argv)
    conn = connect(a.db)
    try:
        a.fn(conn, a)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
