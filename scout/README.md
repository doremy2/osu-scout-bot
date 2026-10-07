# scout/ — tournament ingestion + analytics engine

The new core of osu! scout: give it a tournament source, it finds every MP link,
pulls full lobbies from the osu! API, stores them in one schema, then calculates
ratings, mod leaderboards, awards and player pages.

It lives next to the old bot and does **not** touch `storage.py`, `analysis.py`,
or `data/osu_scout.db`. Its database is `data/scout.db` (override with `SCOUT_DB`).

## Setup

```
pip install requests openpyxl
```
`.env` already has `OSU_CLIENT_ID` / `OSU_CLIENT_SECRET` — that's all it needs.

## Workflow

```bash
# 1. Scan a source and import every lobby (Google Sheet must be "anyone with link can view")
python -m scout import vrso-2027 "https://docs.google.com/spreadsheets/d/<id>/edit" --name "VRSO 2027" --acronym VRSO --format team

#    other sources: a downloaded .xlsx, a .csv, or a .txt of pasted links
python -m scout discover vrso-2027 data/sources/vrso_ro16.csv --round "RO16"
python -m scout add vrso-2027 https://osu.ppy.sh/community/matches/123456 --round "Grand Finals"
python -m scout fetch vrso-2027            # resumable; only fetches 'pending'

# 2. Check what came in
python -m scout status vrso-2027

# 3. (Recommended) load the mappool -> exact mod slots + warmups excluded
python -m scout pool vrso-2027 data/pools/vrso-2027.csv     # columns: beatmap_id,slot[,round]
#    no pool? skip the first N games of each lobby instead:
python -m scout create vrso-2027 --warmups 2

# 4. Fix a round the sheet didn't label
python -m scout set-round vrso-2027 123456 "Quarterfinals"

# 5. Results
python -m scout report vrso-2027
python -m scout report vrso-2027 --player hyrn
python -m scout report vrso-2027 --json data/reports/vrso-2027.json   # for the website / bot
```

## Layout

| Module | Job |
|---|---|
| `sources/` | sheet / XLSX / CSV / text → `MatchLink`s. Reads cell hyperlinks and `=HYPERLINK()` formulas (most sheets hide the URL behind "MP Link" text). Round hint = row text → nearest header row above → tab name. |
| `osu/` | API v2 client (client-credentials, 1 req/s, retries, event pagination) + parser (handles legacy and lazer score formats). |
| `db/` | `schema.sql` + `repo.py`. Only place that writes. Raw API responses are cached (compressed) so you can re-parse without refetching. |
| `ingest.py` | `discover` → `fetch` → `finalize`. Matches have a status (`pending/imported/failed/not_found`), so an interrupted import just continues. |
| `classify.py` | Per game: mod bucket (pool slot, else inferred — mixed player mods = FM), and exclusions: warmups, aborts, replayed maps, <2 scores. Re-runnable. |
| `analytics/` | `dataset` (only SQL reader) → `ratings` (z-scores, outcomes, player stats) → `report` (rankings, mod boards, awards, teams, matches, player pages). |

**Adding a source later** (forum post, tournament site): write a reader that returns
`Table`s or text and register it in `sources/__init__.py`. Nothing downstream changes.

**Changing the rating algorithm**: replace `normalize_scores()` in `analytics/ratings.py`
or tweak `RatingConfig`. Data never needs re-importing.

## Rating v1

For each score: `z = (score − mean) / std` against **everyone who played that beatmap
in the tournament** (falls back to the lobby if fewer than 6 scores), clipped to ±3.
Uses accuracy/combo instead when the lobby's win condition was accuracy/combo.

For any slice (overall, a mod, a round):

- `z_mean = Σ(w·z) / Σw`, with w = round weight (Q 0.90 … GF 1.20) — a finals map counts slightly more, it can't outweigh the event
- `z_adj = Σ(w·z) / (Σw + 3)` — shrinkage: few maps get pulled toward average
- `rating = 7.0 + 1.5 · z_adj` (≈ 7 average, ≈ 9+ dominant). `rating_raw` uses `z_mean`.

Awards also need minimum samples (`AwardConfig`): 8 maps overall, 4 maps for a mod
award, 3 maps in finals, 6 team games for carry.

**Known v1 limitations** (all intentional for now): no opponent-strength adjustment
(map pool normalization is still confounded by who played the map), no map
difficulty term beyond normalization, FM/HD detection without a mappool is a guess,
team names come from the lobby title (`ACR: (Team A) vs (Team B)`).


## Tournament format, teams and the website

- `--format 1v1|team` is **required** for a new tournament (stored on `tournaments.format`; the web import form asks for it too).
  Formats are registered in `formats.py`, so adding another is one entry there.
- Team tournaments get tournament-scoped `tournament_teams` / `team_memberships` (derived in `teams.py` from lobby titles,
  `Lobby <Team>` qualifier labels, then country). A player's team is never stored on the global `players` row.
- `python -m scout recompute <slug>` re-runs classification + team detection from the DB (no API calls).
- Replay rule (`classify.py`): a game is a replay only if the **same beatmap is played again straight away in the same lobby by
  overlapping players** and a later attempt completed. The same map in other lobbies, or a lobby re-running its whole pool, is never excluded.

```bash
python -m scout dev      # API on 127.0.0.1:8001 + website on http://localhost:3000
python -m scout serve    # API only (what Next.js proxies /api/scout/* to)
python -m pytest         # backend tests
```

The browser never sees SQLite or `OSU_CLIENT_SECRET`: Next.js proxies `/api/scout/*` to `server.py`, which owns the DB and runs imports.
