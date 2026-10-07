"""Copy a local scout.db into another database (the hosted Turso one) so the live site starts with your data."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .db.batch import run_batch

# parents before children (foreign keys)
TABLES = ["tournaments", "tournament_sources", "players", "beatmaps", "mappool", "pool_slots", "tournament_teams",
          "tournament_matches", "match_games", "game_scores", "tournament_players", "team_memberships"]


def copy_database(src_path: str | Path, dst, chunk: int = 200, progress=print) -> dict[str, int]:
    """Copy every tournament table (not the raw API cache or job history). `dst` is any scout connection."""
    src = sqlite3.connect(str(src_path))
    src.row_factory = sqlite3.Row
    counts: dict[str, int] = {}
    try:
        for table in TABLES:
            cols = [r["name"] for r in src.execute(f"PRAGMA table_info({table})")]
            if not cols:
                continue
            have = {r["name"] for r in dst.execute(f"PRAGMA table_info({table})")}
            cols = [c for c in cols if c in have]
            sql = f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})"
            rows = [tuple(r[c] for c in cols) for r in src.execute(f"SELECT {', '.join(cols)} FROM {table}")]
            for i in range(0, len(rows), chunk):
                run_batch(dst, [(sql, row) for row in rows[i:i + chunk]], chunk=chunk)
            counts[table] = len(rows)
            progress(f"  {table}: {len(rows)} rows")
    finally:
        src.close()
    return counts
