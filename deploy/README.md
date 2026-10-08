# Deploying osu! scout

Two processes make up the site, and they must run on the same machine:

| Process | What | Listens on |
|---|---|---|
| `scout-api` | Python service (`python -m scout serve`): SQLite, ratings, imports, the osu! credentials | `127.0.0.1:8001` (never public) |
| `scout-web` | Next.js site; proxies `/api/scout/*` to the API | `127.0.0.1:3000` |
| Caddy | HTTPS + public entry point | `:80` / `:443` |

Only Caddy is exposed to the internet. Frontend-only hosts (Netlify, a plain Vercel Next.js project) will not work, because the API owns the database; Vercel needs route C (snapshot, or a hosted Turso database for live imports).

Pick one route: **A. systemd on a VPS** (simplest), **B. Docker Compose**, or **C. Vercel** (read-only snapshot, see the end).

---

## Before you go public

1. **Set `SCOUT_ADMIN_TOKEN`.** Imports spend your osu! API quota and write to the database. With a token set, `POST /api/imports` returns 401 without the `X-Admin-Token` header, and the import page shows a token field. Use a long random value: `openssl rand -hex 32`. Set `SCOUT_IMPORTS=off` to disable web imports entirely and import with the CLI instead.
2. Only one import can run at a time (extra requests get 429).
3. The API's `/api/docs` is off unless `SCOUT_DOCS=1`.
4. Keep port 8001 closed in the firewall (the API binds to 127.0.0.1 by default).
5. Put your real domain in the Caddyfile.

---

## A. VPS with systemd (Ubuntu/Debian)

```bash
# 1. packages
sudo apt update && sudo apt install -y python3 python3-venv git sqlite3 caddy
# Node 20+ (see https://nodejs.org or use nvm)

# 2. code + user
sudo useradd --system --create-home --shell /usr/sbin/nologin scout
sudo git clone https://github.com/doremy2/osu-scout-bot.git /opt/osu-scout
sudo chown -R scout:scout /opt/osu-scout

# 3. config (secrets live here, not in git)
sudo install -m 600 -o scout -g scout /opt/osu-scout/deploy/scout.env.example /opt/osu-scout/.env
sudoedit /opt/osu-scout/.env          # fill in OSU_CLIENT_ID, OSU_CLIENT_SECRET, SCOUT_ADMIN_TOKEN

# 4. your existing data (the database is not in git)
#    from your PC:  scp data/scout.db you@server:/tmp/scout.db
sudo -u scout mkdir -p /opt/osu-scout/data
sudo mv /tmp/scout.db /opt/osu-scout/data/scout.db && sudo chown scout:scout /opt/osu-scout/data/scout.db

# 5. build + install services
sudo -u scout bash /opt/osu-scout/deploy/deploy.sh
sudo cp /opt/osu-scout/deploy/scout-api.service /opt/osu-scout/deploy/scout-web.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now scout-api scout-web

# 6. HTTPS
sudo cp /opt/osu-scout/deploy/Caddyfile /etc/caddy/Caddyfile   # edit the domain first
sudo systemctl reload caddy
```

Point your domain's DNS A record at the server's IP before step 6 so Caddy can get a certificate.

**Updating:** `sudo -u scout bash /opt/osu-scout/deploy/deploy.sh && sudo systemctl restart scout-api scout-web`

**Logs:** `journalctl -u scout-api -f` and `journalctl -u scout-web -f`

**Backups** (the database is the only copy of your imports):

```bash
sudo -u scout crontab -e
# daily at 04:15, keep 14 days
15 4 * * * /opt/osu-scout/deploy/backup.sh
```

---

## B. Docker Compose

```bash
cp deploy/scout.env.example .env && nano .env     # credentials + SCOUT_ADMIN_TOKEN
# put your domain in deploy/Caddyfile.docker
docker compose up -d --build
# seed with your existing database (first run only):
docker compose cp data/scout.db api:/data/scout.db && docker compose restart api
```

Data lives in the `scoutdata` volume; back it up with
`docker compose exec api sqlite3 /data/scout.db ".backup /data/backup.db"` and copy that file out.

---

## Importing on a public site

- **Web:** go to `/import`, enter the admin token, and paste the sheet.
- **CLI on the server:** `sudo -u scout /opt/osu-scout/venv/bin/python -m scout import <slug> <sheet-url> --format team`
- Or set `SCOUT_IMPORTS=off` and only ever use the CLI.

## Tuning notes

- One small VPS (1 vCPU / 1 GB) is plenty: reports are cached in memory per tournament.
- The osu! API is called at most once per second (`OSU_MIN_INTERVAL`).
- For rate limiting beyond this (many anonymous visitors), add Caddy's `rate_limit` plugin or Cloudflare in front.


---

## C. Vercel (multi-service project)

`vercel.json` defines two services: `web` (Next.js, public at `/`) and `scout_api` (FastAPI from `scout_api:app`, internal only).
`web` gets the API's internal address through a service binding, injected as `SCOUT_API_URL`; the browser only ever calls
`/api/scout/*`, which the web app proxies at runtime (`web/app/api/scout/[...path]/route.ts`).

Vercel functions have no persistent disk and cannot run background work, so the API has two modes, picked by the environment:

| Mode | When | Data | Imports |
|---|---|---|---|
| **Snapshot** | `TURSO_DATABASE_URL` not set | read-only `deploy/scout.db` bundled with the deploy | off (the Import button is greyed out) |
| **Hosted database** | `TURSO_DATABASE_URL` set | your Turso database (SQLite-compatible) | live, **open to anyone** (with limits) |

### Turn on live imports

1. **Create the database** (free tier is plenty). With the Turso CLI:
   ```bash
   turso auth login
   turso db create osu-scout
   turso db show osu-scout --url          # libsql://osu-scout-<you>.turso.io
   turso db tokens create osu-scout       # the auth token
   ```
2. **Copy your current data into it** (once; `pip install libsql` first):
   ```bash
   TURSO_DATABASE_URL=libsql://... TURSO_AUTH_TOKEN=... python scripts/seed_turso.py
   ```
3. **Add environment variables** in Vercel (Project → Settings → Environment Variables), for Production and Preview:

   | Variable | Value |
   |---|---|
   | `TURSO_DATABASE_URL` | the `libsql://...` URL |
   | `TURSO_AUTH_TOKEN` | the token from step 1 |
   | `OSU_CLIENT_ID`, `OSU_CLIENT_SECRET` | your osu! OAuth app |
   | `SCOUT_ADMIN_TOKEN` | your **owner key** (`openssl rand -hex 24`): bypasses the limits, may re-import or delete tournaments |
   | `SCOUT_IMPORTS_PER_VISITOR` / `SCOUT_IMPORTS_PER_DAY` | optional caps per 24 h; default 0 = unlimited |
   | `SCOUT_MAX_MATCHES` | optional, default 300: refuse bigger sheets |
   | `SCOUT_IMPORTS` | optional: `token` = invite-only (holders of the key), `off` = no web imports |

4. **Redeploy** (`vercel deploy --prod`). The Import button becomes active for everyone.

How an import runs on Vercel: the browser starts a job (stored in the database) and then keeps asking the server to do the
next ~40-second slice of work (scan the sheet, fetch lobbies at 1 per second, calculate ratings), showing live progress.
Several imports can run at once (the osu! API allows about 60 requests per minute in total, so many large imports at the same time will each go slower). If a request drops the browser simply asks again; the job continues where it stopped.

**Because anyone can import, the site protects itself:**
- No import rate limit by default, and imports can run at the same time. To cap them, set `SCOUT_IMPORTS_PER_VISITOR` / `SCOUT_IMPORTS_PER_DAY` (visitors are told apart by a salted hash of their IP, never the address itself).
- Only **new** tournaments: a visitor can't re-import or overwrite an existing slug (the owner key can).
- Google-Sheets links only, at most `SCOUT_MAX_MATCHES` lobbies, length limits on names.
- Every import spends your osu! API quota (about one request per match). If it ever gets abused, turn the caps on without a code change.

**Moderation** (needs `SCOUT_ADMIN_TOKEN`): remove a bad tournament with
`curl -X DELETE https://<your-site>/api/scout/tournaments/<slug> -H "X-Admin-Token: <key>"`.
To re-import an existing tournament (e.g. the sheet gained new matches), tick "Site owner?" on the Import page and enter the key.
Switch to invite-only any time with `SCOUT_IMPORTS=token` (the key is then the invite code), or off with `SCOUT_IMPORTS=off`.

### Snapshot mode (no database)

Import locally, run `python scripts/make_deploy_db.py`, commit `deploy/scout.db`, and redeploy. No secrets needed.

**Steps:** import the repo into Vercel as a project (the services come from `vercel.json`), or use the CLI
(`vercel link`, `vercel deploy`, `vercel deploy --prod`). `vercel dev -L` runs both services locally.
