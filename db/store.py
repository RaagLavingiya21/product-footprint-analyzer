"""SQLite persistence for product footprint analyses.

No Streamlit imports — callable from any Python context.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from calc.footprint import FootprintResult, LineItem

DB_PATH = Path(__file__).parent / "footprints.db"

_DDL = """
CREATE TABLE IF NOT EXISTS products (
    product_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name    TEXT    NOT NULL,
    analysis_date   TEXT    NOT NULL,
    total_kg_co2e   REAL    NOT NULL,
    matched_items   INTEGER NOT NULL,
    flagged_items   INTEGER NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'approved',
    flagged_comment TEXT
);

CREATE TABLE IF NOT EXISTS line_items (
    item_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id     INTEGER NOT NULL REFERENCES products(product_id),
    component      TEXT,
    material       TEXT,
    spend_usd      REAL,
    matched_sector TEXT,
    emission_factor REAL,
    ef_source      TEXT,
    kg_co2e        REAL,
    share_pct      REAL,
    flag_status    TEXT    NOT NULL
);
"""


@dataclass
class AnalysisSummary:
    product_id: int
    product_name: str
    analysis_date: str
    total_kg_co2e: float
    matched_items: int
    flagged_items: int


def init_db(db_path: Path = DB_PATH) -> None:
    """Create tables if they don't exist. Safe to call on every app start."""
    with sqlite3.connect(db_path) as conn:
        conn.executescript(_DDL)
        _migrate_db(conn)


def _migrate_db(conn: sqlite3.Connection) -> None:
    """Add columns introduced after the initial schema. Idempotent."""
    for col, definition in [
        ("status", "TEXT NOT NULL DEFAULT 'approved'"),
        ("flagged_comment", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {definition}")
        except sqlite3.OperationalError:
            pass  # column already exists


def save_analysis(
    product_name: str,
    result: FootprintResult,
    analysis_date: date | None = None,
    db_path: Path = DB_PATH,
    status: str = "approved",
    flagged_comment: str | None = None,
) -> int:
    """Persist a footprint result. Returns the new product_id.

    Args:
        status:          "approved" or "flagged".
        flagged_comment: Human-entered concern note; stored when status="flagged".
    """
    if analysis_date is None:
        analysis_date = date.today()

    init_db(db_path)

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO products
                (product_name, analysis_date, total_kg_co2e, matched_items, flagged_items,
                 status, flagged_comment)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                product_name.strip(),
                analysis_date.isoformat(),
                round(result.total_kg_co2e, 6),
                result.matched_count,
                result.flagged_count,
                status,
                flagged_comment.strip() if flagged_comment else None,
            ),
        )
        product_id = cursor.lastrowid

        conn.executemany(
            """
            INSERT INTO line_items
                (product_id, component, material, spend_usd, matched_sector,
                 emission_factor, ef_source, kg_co2e, share_pct, flag_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [_line_item_row(product_id, li) for li in result.line_items],
        )

    return product_id


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _line_item_row(product_id: int, li: LineItem) -> tuple:
    flags = []
    if li.is_flagged_by_parser:
        flags.append("parser_flagged")
    if li.is_low_confidence:
        flags.append("low_confidence")
    if li.is_no_ef_match:
        flags.append("unmatched")
    flag_status = "|".join(flags) if flags else "ok"

    return (
        product_id,
        li.component,
        li.material,
        li.spend_usd,
        li.sector_name or None,
        round(li.ef_kg_co2e_per_usd, 6) if li.ef_kg_co2e_per_usd else None,
        li.ef_source or None,
        round(li.kg_co2e, 6) if li.is_matched else None,
        round(li.share_pct, 4) if li.is_matched else None,
        flag_status,
    )
