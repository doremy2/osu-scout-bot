"""Settings, read from environment variables (or a .env file next to the project)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    env = PROJECT_ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path(os.environ.get("SCOUT_DB", PROJECT_ROOT / "data" / "scout.db"))
    osu_client_id: str = os.environ.get("OSU_CLIENT_ID", "")
    osu_client_secret: str = os.environ.get("OSU_CLIENT_SECRET", "")
    # osu! asks for <= 60 req/min; one request per second keeps us well under.
    osu_min_interval: float = float(os.environ.get("OSU_MIN_INTERVAL", "1.0"))
    # --- public deployment ------------------------------------------------
    # Who may start imports (they spend osu! API quota and write to the DB):
    #   SCOUT_ADMIN_TOKEN set  -> imports need that token (X-Admin-Token header)
    #   SCOUT_IMPORTS=off      -> imports disabled entirely
    #   SCOUT_IMPORTS=public   -> anyone may import NEW tournaments, within limits; the admin token (if set) is the owner key
    #   neither                -> open (fine for local use only)
    admin_token: str = os.environ.get("SCOUT_ADMIN_TOKEN", "")
    imports_mode: str = os.environ.get("SCOUT_IMPORTS", "auto").lower()
    # Serve a bundled, read-only snapshot (serverless hosts). Set automatically on Vercel.
    readonly: bool = os.environ.get("SCOUT_READONLY", "").lower() in ("1", "true", "yes") or bool(os.environ.get("VERCEL"))
    # --- hosted database (Turso / libsql) -----------------------------------
    # With TURSO_DATABASE_URL set, data lives in the hosted database and a local replica file serves reads.
    turso_url: str = os.environ.get("TURSO_DATABASE_URL", "")
    turso_token: str = os.environ.get("TURSO_AUTH_TOKEN", "")
    replica_path: str = os.environ.get("SCOUT_REPLICA", str(Path(os.environ.get("TMPDIR") or os.environ.get("TEMP") or "/tmp") / "scout-replica.db"))
    # How imports run: "thread" = a background thread does it all (local / VPS);
    # "step" = the browser drives it with short requests (serverless, where nothing may run in the background).
    import_mode: str = os.environ.get("SCOUT_IMPORT_MODE", "").lower()
    step_budget: float = float(os.environ.get("SCOUT_STEP_BUDGET", "40"))      # seconds of work per step request
    max_matches: int = int(os.environ.get("SCOUT_MAX_MATCHES", "300"))         # refuse sheets with more lobbies than this
    # Budget for imports anyone may start (SCOUT_IMPORTS=public)
    public_imports_per_visitor: int = int(os.environ.get("SCOUT_IMPORTS_PER_VISITOR", "2"))   # per visitor per 24 h
    public_imports_per_day: int = int(os.environ.get("SCOUT_IMPORTS_PER_DAY", "20"))          # whole site per 24 h
    host: str = os.environ.get("SCOUT_HOST", "127.0.0.1")
    port: int = int(os.environ.get("SCOUT_PORT", "8001"))
    allowed_origins: str = os.environ.get("SCOUT_ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
    enable_docs: bool = os.environ.get("SCOUT_DOCS", "").lower() in ("1", "true", "yes")


settings = Settings()
