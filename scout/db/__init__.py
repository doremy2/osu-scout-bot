"""SQLite access. One connection helper + repository functions; nothing else knows SQL."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_VERSION = 8
_SCHEMA = Path(__file__).with_name("schema.sql")


def writable_copy(src: str | Path, name: str = "scout-readonly.db") -> Path:
    """Serverless hosts (Vercel) ship the code read-only. Copy the bundled snapshot to the temp dir once per
    instance so SQLite can open it normally (WAL, schema checks) without touching the deployed files."""
    import shutil
    import tempfile
    dest = Path(tempfile.gettempdir()) / name
    src = Path(src)
    if not dest.exists() or dest.stat().st_mtime < src.stat().st_mtime or dest.stat().st_size != src.stat().st_size:
        shutil.copyfile(src, dest)
    return dest


def connect(db_path: str | Path, backend: str | None = None):
    """sqlite3 file by default; a libsql embedded replica of the hosted database when TURSO_DATABASE_URL is set;
    backend="libsql" forces the libsql wrapper over a plain local file (used by the tests)."""
    import os

    from ..config import settings
    from .remote import open_remote

    if backend is None:
        backend = "turso" if settings.turso_url else ("libsql" if os.environ.get("SCOUT_BACKEND") == "libsql" else "sqlite")
    if backend == "turso":
        conn = open_remote(settings.replica_path, settings.turso_url, settings.turso_token)
        conn.sync(min_interval=3.0)          # other instances' imports show up within a few seconds
        init_schema(conn)
        return conn
    if backend == "libsql":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = open_remote(str(db_path))
        init_schema(conn)
        return conn
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
    except Exception:   # noqa: BLE001 - no schema_version table yet (sqlite3.OperationalError / libsql error)
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
    # v7: lazer support: which client a tournament used, and whether a lobby is a stable match or a lazer room
    _add_column(conn, "tournaments", "client", "TEXT NOT NULL DEFAULT 'stable'")
    _add_column(conn, "tournament_matches", "kind", "TEXT NOT NULL DEFAULT 'match'")
    # v8: automatic updating. Where a tournament came from, whether to keep re-checking it, and what the last check saw.
    fresh_tracking = "source_url" not in _columns(conn, "tournaments")
    _add_column(conn, "tournaments", "source_url", "TEXT")
    _add_column(conn, "tournaments", "source_type", "TEXT")
    _add_column(conn, "tournaments", "auto_update", "INTEGER NOT NULL DEFAULT 0")
    _add_column(conn, "tournaments", "last_checked_at", "TEXT")
    _add_column(conn, "tournaments", "last_source_hash", "TEXT")
    _add_column(conn, "tournaments", "last_changed_at", "TEXT")
    _add_column(conn, "tournaments", "last_new_matches", "INTEGER NOT NULL DEFAULT 0")
    _add_column(conn, "tournaments", "last_check_error", "TEXT")
    _add_column(conn, "tournaments", "check_failures", "INTEGER NOT NULL DEFAULT 0")
    _add_column(conn, "import_jobs", "origin", "TEXT NOT NULL DEFAULT 'import'")      # import | update | discovery
    _add_column(conn, "tournament_matches", "attempts", "INTEGER NOT NULL DEFAULT 0")  # failed/not-found fetches so far
    if fresh_tracking:      # tournaments imported before v8 get the source they were scanned from, once
        conn.execute(
            """UPDATE tournaments SET
                 source_url = (SELECT s.location FROM tournament_sources s
                               WHERE s.tournament_id = tournaments.id AND s.kind = 'google_sheet' ORDER BY s.id LIMIT 1),
                 source_type = 'google_sheet', auto_update = 1
               WHERE EXISTS (SELECT 1 FROM tournament_sources s WHERE s.tournament_id = tournaments.id AND s.kind = 'google_sheet')""")
    # v4: sheet text for pool slots (maps nobody has played yet)
    if _columns(conn, "pool_slots"):
        _add_column(conn, "pool_slots", "label", "TEXT")
        _add_column(conn, "pool_slots", "star_rating", "REAL")
    conn.execute("UPDATE schema_version SET version = ? WHERE version < ?", (SCHEMA_VERSION, SCHEMA_VERSION))
