"""One-time: copy data/scout.db into your hosted Turso database.

    TURSO_DATABASE_URL=libsql://<db>.turso.io TURSO_AUTH_TOKEN=<token> python scripts/seed_turso.py
    python scripts/seed_turso.py --source other.db          # a different local database
    python scripts/seed_turso.py --force                    # the target already has tournaments: overwrite matching rows

Safe to re-run (rows are replaced by id), but it never deletes anything from the target.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scout.config import settings  # noqa: E402
from scout.db import connect  # noqa: E402
from scout.seed import copy_database  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default=str(settings.db_path))
    ap.add_argument("--libsql-file", help="copy into a plain local libsql file instead of Turso (for testing)")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()

    if a.libsql_file:
        dst = connect(a.libsql_file, backend="libsql")
    elif settings.turso_url:
        dst = connect(a.source, backend="turso")
    else:
        sys.exit("Set TURSO_DATABASE_URL and TURSO_AUTH_TOKEN (or pass --libsql-file for a local test).")

    existing = dst.execute("SELECT COUNT(*) FROM tournaments").fetchone()[0]
    if existing and not a.force:
        sys.exit(f"The target already has {existing} tournaments. Re-run with --force to overwrite matching rows.")
    print(f"Copying {a.source} ...")
    counts = copy_database(a.source, dst)
    dst.sync()
    print("Done:", ", ".join(f"{t}={n}" for t, n in counts.items()))


if __name__ == "__main__":
    main()
