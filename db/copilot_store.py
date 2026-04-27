"""SQLite schema and CRUD for the Supplier Engagement Copilot.

Three new tables in db/footprints.db:
  suppliers   — contact directory (pre-populated with dummy data)
  engagements — one row per supplier engagement, tracks full lifecycle
  audit_log   — append-only event log, exportable for inventory audits

No Streamlit imports — callable from any Python context.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from db.store import DB_PATH

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name  TEXT    NOT NULL UNIQUE,
    contact_name   TEXT,
    contact_email  TEXT
);

CREATE TABLE IF NOT EXISTS engagements (
    engagement_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_name         TEXT    NOT NULL,
    product_name          TEXT    NOT NULL,
    component_name        TEXT,
    material              TEXT,
    kg_co2e               REAL,
    share_pct             REAL,
    status                TEXT    NOT NULL DEFAULT 'open',
    email_draft           TEXT,
    email_sent            TEXT,
    response_received     TEXT,
    routing_decision      TEXT,
    decision_rationale    TEXT,
    ghg_protocol_citation TEXT,
    next_step             TEXT,
    created_at            TEXT    NOT NULL DEFAULT (datetime('now')),
    last_action_date      TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    log_id                INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp             TEXT    NOT NULL DEFAULT (datetime('now')),
    event                 TEXT    NOT NULL,
    workflow              TEXT    NOT NULL,
    model                 TEXT,
    supplier_name         TEXT,
    product_name          TEXT,
    component_name        TEXT,
    email_sent            TEXT,
    response_received     TEXT,
    routing_decision      TEXT,
    decision_rationale    TEXT,
    ghg_protocol_citation TEXT,
    data_collected        TEXT,
    status                TEXT
);
"""

# ---------------------------------------------------------------------------
# Seed data — six realistic consumer-goods suppliers
# ---------------------------------------------------------------------------

_SEED_SUPPLIERS = [
    ("FiberTex Global",        "Sarah Chen",     "sarah.chen@fibertex.example.com"),
    ("PolyNova Materials",     "Raj Patel",       "raj.patel@polynova.example.com"),
    ("ChemDyes International", "Maria Santos",    "maria.santos@chemdyes.example.com"),
    ("PackRight Solutions",    "Tom Eriksson",    "tom.eriksson@packright.example.com"),
    ("MetalWorks Industries",  "Aisha Okonkwo",  "aisha.okonkwo@metalworks.example.com"),
    ("SilicaSoft Technologies","James Liu",       "james.liu@silicasoft.example.com"),
]


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Supplier:
    supplier_id: int
    supplier_name: str
    contact_name: str | None
    contact_email: str | None


@dataclass
class Engagement:
    engagement_id: int
    supplier_name: str
    product_name: str
    component_name: str | None
    material: str | None
    kg_co2e: float | None
    share_pct: float | None
    status: str
    email_draft: str | None
    email_sent: str | None
    response_received: str | None
    routing_decision: str | None
    decision_rationale: str | None
    ghg_protocol_citation: str | None
    next_step: str | None
    created_at: str
    last_action_date: str | None


@dataclass
class AuditEntry:
    log_id: int
    timestamp: str
    event: str
    workflow: str
    model: str | None
    supplier_name: str | None
    product_name: str | None
    component_name: str | None
    email_sent: str | None
    response_received: str | None
    routing_decision: str | None
    decision_rationale: str | None
    ghg_protocol_citation: str | None
    data_collected: str | None
    status: str | None


# ---------------------------------------------------------------------------
# Init + seed
# ---------------------------------------------------------------------------

def init_copilot_db(db_path: Path = DB_PATH) -> None:
    """Create tables and seed suppliers if the table is empty. Safe to call repeatedly."""
    with sqlite3.connect(db_path) as conn:
        conn.executescript(_DDL)
        count = conn.execute("SELECT COUNT(*) FROM suppliers").fetchone()[0]
        if count == 0:
            conn.executemany(
                "INSERT INTO suppliers (supplier_name, contact_name, contact_email) VALUES (?, ?, ?)",
                _SEED_SUPPLIERS,
            )


# ---------------------------------------------------------------------------
# Suppliers
# ---------------------------------------------------------------------------

def get_all_suppliers(db_path: Path = DB_PATH) -> list[Supplier]:
    init_copilot_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM suppliers ORDER BY supplier_name"
        ).fetchall()
    return [Supplier(**dict(r)) for r in rows]


def get_supplier_by_name(name: str, db_path: Path = DB_PATH) -> Supplier | None:
    init_copilot_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM suppliers WHERE supplier_name = ? COLLATE NOCASE",
            (name,),
        ).fetchone()
    return Supplier(**dict(row)) if row else None


# ---------------------------------------------------------------------------
# Engagements
# ---------------------------------------------------------------------------

def create_engagement(
    supplier_name: str,
    product_name: str,
    component_name: str | None,
    material: str | None,
    kg_co2e: float | None,
    share_pct: float | None,
    email_draft: str | None = None,
    db_path: Path = DB_PATH,
) -> int:
    """Insert a new engagement row and return its engagement_id."""
    init_copilot_db(db_path)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO engagements
                (supplier_name, product_name, component_name, material,
                 kg_co2e, share_pct, email_draft, status, created_at, last_action_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'open', datetime('now'), datetime('now'))
            """,
            (supplier_name, product_name, component_name, material, kg_co2e, share_pct, email_draft),
        )
    return cursor.lastrowid


def update_engagement(
    engagement_id: int,
    db_path: Path = DB_PATH,
    **fields,
) -> None:
    """Update any subset of engagement fields by keyword argument."""
    allowed = {
        "status", "email_draft", "email_sent", "response_received",
        "routing_decision", "decision_rationale", "ghg_protocol_citation",
        "next_step",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    updates["last_action_date"] = datetime.utcnow().isoformat(sep=" ", timespec="seconds")
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [engagement_id]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            f"UPDATE engagements SET {set_clause} WHERE engagement_id = ?",
            values,
        )


def get_engagement(engagement_id: int, db_path: Path = DB_PATH) -> Engagement | None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM engagements WHERE engagement_id = ?", (engagement_id,)
        ).fetchone()
    return Engagement(**dict(row)) if row else None


def get_engagements_for_product(
    product_name: str, db_path: Path = DB_PATH
) -> list[Engagement]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM engagements WHERE product_name = ? ORDER BY share_pct DESC",
            (product_name,),
        ).fetchall()
    return [Engagement(**dict(r)) for r in rows]


def get_all_engagements(db_path: Path = DB_PATH) -> list[Engagement]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM engagements ORDER BY created_at DESC"
        ).fetchall()
    return [Engagement(**dict(r)) for r in rows]


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

def append_audit_log(
    event: str,
    workflow: str,
    supplier_name: str | None = None,
    product_name: str | None = None,
    component_name: str | None = None,
    model: str | None = None,
    email_sent: str | None = None,
    response_received: str | None = None,
    routing_decision: str | None = None,
    decision_rationale: str | None = None,
    ghg_protocol_citation: str | None = None,
    data_collected: str | None = None,
    status: str | None = None,
    db_path: Path = DB_PATH,
) -> None:
    """Append one row to the audit log. Raises on failure (caller handles display)."""
    init_copilot_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO audit_log
                (event, workflow, model, supplier_name, product_name, component_name,
                 email_sent, response_received, routing_decision, decision_rationale,
                 ghg_protocol_citation, data_collected, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event, workflow, model, supplier_name, product_name, component_name,
                email_sent, response_received, routing_decision, decision_rationale,
                ghg_protocol_citation, data_collected, status,
            ),
        )


def get_audit_log(
    supplier_name: str | None = None,
    product_name: str | None = None,
    db_path: Path = DB_PATH,
) -> list[AuditEntry]:
    """Return audit log rows, optionally filtered by supplier or product."""
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if supplier_name and product_name:
            rows = conn.execute(
                "SELECT * FROM audit_log WHERE supplier_name = ? AND product_name = ? ORDER BY timestamp DESC",
                (supplier_name, product_name),
            ).fetchall()
        elif supplier_name:
            rows = conn.execute(
                "SELECT * FROM audit_log WHERE supplier_name = ? ORDER BY timestamp DESC",
                (supplier_name,),
            ).fetchall()
        elif product_name:
            rows = conn.execute(
                "SELECT * FROM audit_log WHERE product_name = ? ORDER BY timestamp DESC",
                (product_name,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY timestamp DESC"
            ).fetchall()
    return [AuditEntry(**dict(r)) for r in rows]
