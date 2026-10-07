"""SQLite access. One connection helper + repository functions; nothing else knows SQL."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 2
_SCHEMA = Path(__file__).with_name("schema.sql")


def connect(db_path: str | Path) -> sqlite3.Connection:
    db_path = Path(db_path)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL") if str(db_path) != ":memory:" else None
    init_schema(conn)
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    try:   # fast path: already current, so concurrent readers never take a write lock
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        if row is not None and row["version"] >= SCHEMA_VERSION:
            return
    except sqlite3.OperationalError:
        pass
    conn.executescript(_SCHEMA.read_text(encoding="utf-8"))
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
    _migrate(conn)
    conn.commit()


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _add_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    if column not in _columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _migrate(conn: sqlite3.Connection) -> None:
    """Idempotent upgrades for databases created by an older schema version."""
    # v2: tournament format, teams on matches, country name on players
    _add_column(conn, "tournaments", "format", "TEXT NOT NULL DEFAULT '1v1'")
    _add_column(conn, "tournament_matches", "team_red_id", "INTEGER REFERENCES tournament_teams(id)")
    _add_column(conn, "tournament_matches", "team_blue_id", "INTEGER REFERENCES tournament_teams(id)")
    _add_column(conn, "players", "country_name", "TEXT")
    conn.execute("UPDATE schema_version SET version = ? WHERE version < ?", (SCHEMA_VERSION, SCHEMA_VERSION))
