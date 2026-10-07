"""sqlite3-compatible wrapper around libsql (Turso), so the rest of the code does not care which database it uses.

libsql's Python client mirrors sqlite3 but has no `row_factory`, so rows come back as tuples. This wrapper adds
name-indexed rows, `with conn:` transactions and thread safety (one shared connection, one lock).

Deployment shape: an *embedded replica*. Reads hit a local copy (fast, so the analytics' many small queries are
fine); writes are forwarded to the hosted primary; `sync()` pulls other instances' writes. With no `sync_url`
it is just a local libsql file, which is how the tests exercise this module without any network.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Iterable


class Row:
    """Like sqlite3.Row: index by position or column name, iterable, convertible with dict()."""
    __slots__ = ("_keys", "_vals")

    def __init__(self, keys: tuple[str, ...], vals: tuple):
        self._keys = keys
        self._vals = vals

    def keys(self) -> list[str]:
        return list(self._keys)

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                return self._vals[self._keys.index(key)]
            except ValueError:
                raise IndexError(f"No item with that key: {key}") from None
        return self._vals[key]

    def __iter__(self):
        return iter(self._vals)

    def __len__(self) -> int:
        return len(self._vals)

    def __eq__(self, other) -> bool:
        return isinstance(other, Row) and other._keys == self._keys and other._vals == self._vals

    def __repr__(self) -> str:
        return f"Row({dict(zip(self._keys, self._vals))})"


class Cursor:
    def __init__(self, rows: list[Row], description, lastrowid, rowcount):
        self._rows = rows
        self._i = 0
        self.description = description
        self.lastrowid = lastrowid
        self.rowcount = rowcount

    def fetchone(self):
        if self._i >= len(self._rows):
            return None
        self._i += 1
        return self._rows[self._i - 1]

    def fetchall(self):
        rest = self._rows[self._i:]
        self._i = len(self._rows)
        return rest

    def fetchmany(self, size: int = 1):
        out = self._rows[self._i:self._i + size]
        self._i += len(out)
        return out

    def __iter__(self):
        return iter(self.fetchall())


class Connection:
    is_remote = True

    def __init__(self, raw, lock: threading.RLock, state: dict):
        self._raw = raw
        self._lock = lock
        self._state = state          # shared: {"last_sync": float}
        self.row_factory = None      # accepted for sqlite3 compatibility; rows are always Row

    # -- statements ---------------------------------------------------------
    def execute(self, sql: str, params: Iterable[Any] = ()) -> Cursor:
        with self._lock:
            cur = self._raw.execute(sql, tuple(params))
            desc = cur.description
            if desc:
                keys = tuple(d[0] for d in desc)
                rows = [Row(keys, tuple(r)) for r in cur.fetchall()]
            else:
                rows = []
            return Cursor(rows, desc, getattr(cur, "lastrowid", None), getattr(cur, "rowcount", -1))

    def executemany(self, sql: str, seq: Iterable[Iterable[Any]]) -> None:
        with self._lock:
            self._raw.executemany(sql, [tuple(p) for p in seq])

    def executescript(self, script: str) -> None:
        with self._lock:
            self._raw.executescript(script)

    # -- transactions -------------------------------------------------------
    def commit(self) -> None:
        with self._lock:
            self._raw.commit()

    def rollback(self) -> None:
        with self._lock:
            self._raw.rollback()

    def __enter__(self):
        self._lock.acquire()          # held for the whole transaction so no other thread interleaves
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._raw.commit()
            else:
                self._raw.rollback()
        finally:
            self._lock.release()
        return False

    def close(self) -> None:
        """The underlying connection is shared for the life of the process; closing one handle is a no-op."""

    # -- replication --------------------------------------------------------
    def sync(self, min_interval: float = 0.0) -> None:
        """Pull remote changes into the local replica (no-op for a plain local file)."""
        now = time.monotonic()
        if now - self._state["last_sync"] < min_interval:
            return
        with self._lock:
            try:
                self._raw.sync()
            except Exception:  # noqa: BLE001 - a local-only libsql file has nothing to sync
                pass
            self._state["last_sync"] = time.monotonic()


_shared: dict[tuple, tuple[Any, threading.RLock, dict]] = {}
_shared_lock = threading.Lock()


def open_remote(path: str, sync_url: str | None = None, auth_token: str | None = None) -> Connection:
    """One process-wide libsql connection per (path, url); every call returns a cheap handle onto it."""
    import libsql

    key = (path, sync_url)
    with _shared_lock:
        if key not in _shared:
            kwargs = {"sync_url": sync_url, "auth_token": auth_token} if sync_url else {}
            raw = libsql.connect(path, **kwargs)
            _shared[key] = (raw, threading.RLock(), {"last_sync": 0.0})
            if sync_url:
                try:
                    raw.sync()
                except Exception:  # noqa: BLE001
                    pass
                _shared[key][2]["last_sync"] = time.monotonic()
        raw, lock, state = _shared[key]
    return Connection(raw, lock, state)
