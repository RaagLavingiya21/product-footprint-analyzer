"""Read-only queries for the conversational advisor and gap analyzer.

No Streamlit imports — callable from any Python context.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from db.store import DB_PATH


def get_all_products(db_path: Path = DB_PATH) -> list[dict]:
    """Return all rows from the products table."""
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT product_id, product_name, analysis_date, total_kg_co2e, matched_items, flagged_items "
            "FROM products ORDER BY analysis_date DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_product_by_name(name: str, db_path: Path = DB_PATH) -> dict | None:
    """Return a product's summary row and its line items by product name.

    If multiple products share the same name, returns the most recently saved one.
    Returns None if no match is found or the DB does not exist.
    """
    if not db_path.exists():
        return None
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT product_id, product_name, analysis_date, total_kg_co2e, matched_items, flagged_items "
            "FROM products WHERE product_name = ? ORDER BY analysis_date DESC LIMIT 1",
            (name,),
        ).fetchone()
    if row is None:
        return None
    product = dict(row)
    product["line_items"] = get_product_line_items(product["product_id"], db_path)
    return product


def get_product_line_items(product_id: int, db_path: Path = DB_PATH) -> list[dict]:
    """Return all line items for a product."""
    if not db_path.exists():
        return []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT component, material, spend_usd, matched_sector, emission_factor, "
            "ef_source, kg_co2e, share_pct, flag_status "
            "FROM line_items WHERE product_id = ? ORDER BY share_pct DESC NULLS LAST",
            (product_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def build_llm_context(db_path: Path = DB_PATH) -> str:
    """Build a text summary of all saved analyses for the LLM system prompt."""
    products = get_all_products(db_path)

    if not products:
        return "No product analyses have been saved yet."

    lines: list[str] = ["## Saved Product Footprint Analyses\n"]

    for p in products:
        lines.append(
            f"### Product: {p['product_name']} (ID: {p['product_id']})\n"
            f"- Analysis date: {p['analysis_date']}\n"
            f"- Total footprint: {p['total_kg_co2e']:.4f} kg CO₂e\n"
            f"- Matched line items: {p['matched_items']}\n"
            f"- Flagged line items: {p['flagged_items']}\n"
        )

        items = get_product_line_items(p["product_id"], db_path)
        if items:
            lines.append("#### Line items (sorted by share, highest first):\n")
            for li in items:
                component = li["component"] or "—"
                material = li["material"] or "—"
                spend = f"${li['spend_usd']:.2f}" if li["spend_usd"] is not None else "—"
                sector = li["matched_sector"] or "unmatched"
                ef = f"{li['emission_factor']:.6f}" if li["emission_factor"] is not None else "—"
                kg = f"{li['kg_co2e']:.4f}" if li["kg_co2e"] is not None else "—"
                share = f"{li['share_pct']:.1f}%" if li["share_pct"] is not None else "—"
                flag = li["flag_status"]
                lines.append(
                    f"- {component} / {material}: spend={spend}, sector={sector}, "
                    f"EF={ef} kgCO₂e/USD, footprint={kg} kg CO₂e, share={share}, status={flag}"
                )
            lines.append("")

    return "\n".join(lines)
