"""Build deploy/scout.db: a compact snapshot of data/scout.db for read-only hosting (Vercel).

Drops the raw osu! API cache (only needed to re-parse locally), vacuums, and checks the result opens.
Run after importing or changing tournaments:  python scripts/make_deploy_db.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scout.config import settings  # noqa: E402


def main() -> None:
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else settings.db_path
    dest = ROOT / "deploy" / "scout.db"
    dest.parent.mkdir(exist_ok=True)
    if dest.exists():
        dest.unlink()
    source = sqlite3.connect(src)
    target = sqlite3.connect(dest)
    source.backup(target)                       # consistent copy even if the source is open elsewhere
    source.close()
    target.execute("DELETE FROM raw_match_cache")
    target.execute("PRAGMA journal_mode = DELETE")
    target.commit()
    target.execute("VACUUM")
    n = target.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0]
    target.close()
    print(f"wrote {dest} ({dest.stat().st_size / 1e6:.1f} MB, {n} tournaments)")


if __name__ == "__main__":
    main()
