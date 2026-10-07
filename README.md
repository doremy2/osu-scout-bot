# osu! scout

Tournament analytics and scouting for osu!. Paste a tournament's Google Sheet, and scout pulls every
multiplayer lobby from the osu! API and turns it into player ratings, mod rankings, team/country
profiles, match pages and awards.

```
Google Sheet → scout importer → data/scout.db → analytics → FastAPI (scout.server) → Next.js site
```

The browser never touches SQLite or your osu! credentials: Next.js proxies `/api/scout/*` to the local
Python service, which owns the database and runs imports.

## Quick start

Requirements: Python 3.11+, Node 20+, an osu! OAuth app (<https://osu.ppy.sh/home/account/edit#oauth>).

```bash
pip install -r requirements.txt
cd web && npm install && cd ..
```

Create `.env` in the project root:

```
OSU_CLIENT_ID=...
OSU_CLIENT_SECRET=...
```

Run everything:

```bash
python -m scout dev
```

Then open <http://localhost:3000>. (`python -m scout serve` runs only the API on `127.0.0.1:8001`.)

## Using the site

- **Import**: `/import` takes a name, acronym, slug, Google Sheet URL and a required format
  (`1v1` or `Team`). The sheet must be shared as "anyone with the link can view". Progress is shown live,
  then you're redirected to the report. Re-importing a slug only fetches new or failed matches.
- **Tournament page** `/tournaments/<slug>`: Overview, Players, Teams (team format only), Leaderboards
  (overall / by round / by mod / consistency / maps played), Matches, Awards, plus in-tournament player search.
- **Profiles**: players (`/players/<name>`), teams or countries (`/teams/<team>`) and individual matches,
  all cross-linked.

## Command line

```bash
python -m scout import my-cup "<sheet url>" --name "My Cup" --acronym MC --format team
python -m scout status my-cup
python -m scout recompute my-cup        # reclassify games + teams, no API calls
python -m scout reparse my-cup          # rebuild from cached API responses
python -m scout pool my-cup pool.csv    # optional mappool: beatmap_id,slot[,round]
python -m scout report my-cup --player someone
```

See [`scout/README.md`](scout/README.md) for the full workflow.

## How ratings work

Every score is normalized against everyone who played the same beatmap in that tournament, then averaged
per player (with small-sample shrinkage and small round weights) into overall, per-mod and per-round
ratings. 7.00 is the field average; each standard deviation adds 1.5. Team rating pools its players' scores.
All of this lives in `scout/analytics/`; the frontend only displays it.

Replays: a game is excluded as a replay only when the same beatmap is played again straight away in the
same lobby by overlapping players. The same map in other lobbies, or a lobby running its pool twice, is kept.

## Layout

| Path | What |
|---|---|
| `scout/` | ingestion, database, analytics, FastAPI service (the new system) |
| `web/app/(scout)/` | the tournament site; `web/app/legacy/` is the old OWC leaderboard |
| `tests/` | `python -m pytest` |
| `data/scout.db` | tournament database (local, not committed) |
| top-level `*.py` | the original Discord bot and OWC ranking pipeline, untouched |

## Tests

```bash
python -m pytest
cd web && npx tsc --noEmit
```
