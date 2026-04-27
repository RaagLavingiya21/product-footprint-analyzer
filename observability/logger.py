"""Observability logger: records every LLM call to llm_logs in db/footprints.db.

Never raises — all writes are wrapped in try/except so a logging failure
never breaks the calling tool.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "db" / "footprints.db"

_DDL = """
CREATE TABLE IF NOT EXISTS llm_logs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp        TEXT    NOT NULL DEFAULT (datetime('now')),
    app_name         TEXT    NOT NULL,
    tool_name        TEXT    NOT NULL,
    model            TEXT    NOT NULL,
    tokens_in        INTEGER,
    tokens_out       INTEGER,
    latency_seconds  REAL,
    rag_used         INTEGER NOT NULL DEFAULT 0,
    rag_queries      TEXT,
    error            TEXT,
    session_id       TEXT
)
"""

_INSERT = """
INSERT INTO llm_logs
    (app_name, tool_name, model, tokens_in, tokens_out, latency_seconds,
     rag_used, rag_queries, error, session_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def _ensure_table(db_path: Path = DB_PATH) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(_DDL)


def log_llm_call(
    app_name: str,
    tool_name: str,
    model: str,
    tokens_in: int | None,
    tokens_out: int | None,
    latency_seconds: float | None,
    rag_used: bool,
    rag_queries: str | None = None,
    error: str | None = None,
    session_id: str | None = None,
    db_path: Path = DB_PATH,
) -> None:
    """Write one LLM call record. Silently swallows all exceptions."""
    try:
        _ensure_table(db_path)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                _INSERT,
                (
                    app_name,
                    tool_name,
                    model,
                    tokens_in,
                    tokens_out,
                    latency_seconds,
                    1 if rag_used else 0,
                    rag_queries,
                    error,
                    session_id,
                ),
            )
    except Exception:
        pass
