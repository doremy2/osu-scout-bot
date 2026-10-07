"""Vercel entrypoint for the scouting API (service `scout_api`, internal; only the web service calls it).

The deployed filesystem is read-only and functions are short-lived, so this serves a bundled snapshot of the
database (deploy/scout.db, refreshed with scripts/make_deploy_db.py) and web imports are switched off.
Add or update tournaments locally, rebuild the snapshot, and redeploy.
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.environ.setdefault("SCOUT_IMPORTS", "off")
os.environ.setdefault("SCOUT_READONLY", "1")
os.environ.setdefault("SCOUT_DB", str(ROOT / "deploy" / "scout.db"))

from scout.server import create_app  # noqa: E402  (after the environment is set)

app = create_app()
