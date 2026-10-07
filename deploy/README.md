# Deploying osu! scout

Two processes make up the site, and they must run on the same machine:

| Process | What | Listens on |
|---|---|---|
| `scout-api` | Python service (`python -m scout serve`): SQLite, ratings, imports, the osu! credentials | `127.0.0.1:8001` (never public) |
| `scout-web` | Next.js site; proxies `/api/scout/*` to the API | `127.0.0.1:3000` |
| Caddy | HTTPS + public entry point | `:80` / `:443` |

Only Caddy is exposed to the internet. Frontend-only hosts (Netlify, a plain Vercel Next.js project) will not work, because the API owns the database; Vercel works only in the read-only snapshot setup of route C.

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

`vercel.json` defines two services: `web` (Next.js, public at `/`) and `scout_api` (FastAPI from `scout_api.py`, internal only).
`web` gets the API's internal address through a service binding, injected as `SCOUT_API_URL`; the browser only ever calls
`/api/scout/*`, which the web app proxies at runtime (`web/app/api/scout/[...path]/route.ts`).

**Serverless limits that shape this setup**
- The deployed filesystem is read-only and functions are short-lived, so the API serves a bundled **snapshot** of the database
  (`deploy/scout.db`) and **web imports are off**. To add or update tournaments: import locally, run
  `python scripts/make_deploy_db.py`, commit `deploy/scout.db`, and redeploy.
- No secrets are needed on Vercel (the osu! credentials are only used when importing, which happens locally).
- The old Discord bot is not deployed.

**Steps:** import the repo into Vercel as a project with the *Services* preset (or `vercel` / `vercel dev` with the CLI), keep the
framework preset from `vercel.json`, and deploy. `vercel dev` runs both services together locally.
