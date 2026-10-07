"""Vercel entrypoint for the scouting API (service `scout_api`, internal; only the web service calls it).

Two ways to run it, chosen by the environment:

* TURSO_DATABASE_URL set (+ TURSO_AUTH_TOKEN): the data lives in a hosted Turso database and anyone can import
  new tournaments from the website (per-visitor and daily limits apply), driven in short steps by the browser.
  SCOUT_ADMIN_TOKEN, if set, is the owner's key: it bypasses the limits, may re-import existing tournaments and
  may delete tournaments. SCOUT_IMPORTS=token restricts importing to holders of that key instead.
* Not set: a bundled read-only snapshot (deploy/scout.db, refreshed with scripts/make_deploy_db.py) is served
  and web imports are switched off.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent

if os.environ.get("TURSO_DATABASE_URL"):
    os.environ.setdefault("SCOUT_IMPORT_MODE", "step")      # nothing may run in the background on serverless
    os.environ.setdefault("SCOUT_IMPORTS", "public")        # anyone can add tournaments, within limits
else:
    os.environ.setdefault("SCOUT_IMPORTS", "off")
    os.environ.setdefault("SCOUT_READONLY", "1")
os.environ.setdefault("SCOUT_DB", str(ROOT / "deploy" / "scout.db"))

from scout.server import create_app  # noqa: E402  (after the environment is set)

app = create_app()
