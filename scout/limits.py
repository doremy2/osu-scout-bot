"""Abuse limits for imports that anyone may start (no invite code).

Every import spends the owner's osu! API quota and writes to the shared database, so a public site needs a budget:
a few imports per visitor per day and a ceiling for the whole site. Visitors are told apart by a salted hash of
their IP address (the address itself is never stored). The log lives in the database so every server instance
shares it.
"""
from __future__ import annotations

import hashlib
import sqlite3

from .config import settings

WINDOW = "-1 day"


def client_key(ip: str | None) -> str:
    salt = settings.admin_token or "osu-scout"
    return hashlib.sha256(f"{salt}|{ip or 'unknown'}".encode()).hexdigest()[:24]


def check(conn: sqlite3.Connection, key: str) -> str | None:
    """Return a human-readable reason if this visitor (or the whole site) is over budget, else None."""
    per_visitor = conn.execute(
        "SELECT COUNT(*) FROM import_log WHERE client = ? AND created_at > datetime('now', ?)", (key, WINDOW)).fetchone()[0]
    if per_visitor >= settings.public_imports_per_visitor:
        return (f"You've reached the limit of {settings.public_imports_per_visitor} imports per day. "
                "Please try again tomorrow.")
    site = conn.execute("SELECT COUNT(*) FROM import_log WHERE created_at > datetime('now', ?)", (WINDOW,)).fetchone()[0]
    if site >= settings.public_imports_per_day:
        return "The site has reached its daily import limit. Please try again tomorrow."
    return None


def record(conn: sqlite3.Connection, key: str, slug: str) -> None:
    conn.execute("INSERT INTO import_log (client, slug) VALUES (?, ?)", (key, slug))
    conn.commit()
