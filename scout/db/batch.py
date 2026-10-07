"""Run many write statements as one unit.

Against a local sqlite file this is just a transaction. Against the hosted database every statement is a network
round trip, so the statements are rendered into one script and sent in a single request per chunk.
"""
from __future__ import annotations

import uuid
from typing import Any, Iterable

Statement = tuple[str, tuple]


def literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, (bytes, bytearray)):
        return "X'" + bytes(value).hex() + "'"
    return "'" + str(value).replace("'", "''") + "'"


def render(sql: str, params: Iterable[Any]) -> str:
    """Substitute ? placeholders with SQL literals (placeholders inside quoted strings are left alone)."""
    values = list(params)
    out: list[str] = []
    quote = ""
    i = 0
    for ch in sql:
        if quote:
            if ch == quote:
                quote = ""
        elif ch in ("'", '"'):
            quote = ch
        elif ch == "?":
            out.append(literal(values[i]))
            i += 1
            continue
        out.append(ch)
    if i != len(values):
        raise ValueError(f"{len(values)} parameters for {i} placeholders in: {sql[:80]}")
    return "".join(out)


def run_batch(conn, statements: list[Statement], chunk: int = 300) -> None:
    if not statements:
        return
    if not getattr(conn, "is_remote", False):
        with conn:
            for sql, params in statements:
                conn.execute(sql, params)
        return
    conn.commit()                     # close any implicit transaction a caller left open: BEGIN cannot nest
    for start in range(0, len(statements), chunk):
        part = statements[start:start + chunk]
        token = uuid.uuid4().hex
        body = ";\n".join(render(sql, params) for sql, params in part)
        # libsql's executescript does not raise when a statement fails, so the script ends with a marker row
        # that is checked afterwards: no marker means it stopped early, and everything is rolled back.
        conn.executescript(
            f"BEGIN;\n{body};\nINSERT OR REPLACE INTO _batch_marker (id, token) VALUES (1, '{token}');\nCOMMIT;")
        conn.sync()                   # replica reads must see the script's own marker row (read-your-writes)
        row = conn.execute("SELECT token FROM _batch_marker WHERE id = 1").fetchone()
        if row is None or row[0] != token:
            conn.rollback()
            raise RuntimeError("A batched write did not complete and was rolled back.")
