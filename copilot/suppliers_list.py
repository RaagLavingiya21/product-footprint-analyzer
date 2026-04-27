"""Workflow 1: Build a ranked list of supplier engagement candidates for a product.

Pure DB query — no LLM call. Matches line items to the suppliers contact table by
case-insensitive substring lookup on component name.

v1 known limitation: line_items has no supplier_id foreign key. Matching is
best-effort on component name. v2 will replace this with an explicit supplier_id
column in line_items.
"""

from __future__ import annotations

from pathlib import Path

from db.copilot_store import get_engagements_for_product, get_supplier_by_name, init_copilot_db
from db.reader import get_all_products, get_product_line_items
from db.store import DB_PATH
from copilot.models import EngagementCandidate, SuppliersListResult


def _match_supplier(component: str | None, material: str | None, db_path: Path) -> tuple:
    """Try to match a component or material name to the suppliers table.

    Returns (supplier_name, contact_name, contact_email, contact_found).
    Tries component first, then material, then falls back to component name.
    """
    for candidate_name in filter(None, [component, material]):
        supplier = get_supplier_by_name(candidate_name, db_path)
        if supplier:
            return supplier.supplier_name, supplier.contact_name, supplier.contact_email, True

        # Substring match: check if any supplier name contains the component word or vice versa
        from db.copilot_store import get_all_suppliers
        for s in get_all_suppliers(db_path):
            name_lower = s.supplier_name.lower()
            comp_lower = candidate_name.lower()
            if comp_lower in name_lower or name_lower in comp_lower:
                return s.supplier_name, s.contact_name, s.contact_email, True

    label = component or material or "Unknown"
    return label, None, None, False


def run(
    product_name: str,
    top_n: int = 5,
    db_path: Path = DB_PATH,
) -> SuppliersListResult:
    """Return the top-N highest-emitting engagement candidates for a product.

    Args:
        product_name: Exact or case-insensitive product name from the products table.
        top_n:        Maximum number of candidates to return (sorted by kg_co2e descending).
        db_path:      SQLite database path.
    """
    init_copilot_db(db_path)

    # Find product_id by case-insensitive name match
    all_products = get_all_products(db_path)
    product = next(
        (p for p in all_products if p["product_name"].lower() == product_name.lower()),
        None,
    )
    if product is None:
        available = ", ".join(p["product_name"] for p in all_products) or "none"
        return SuppliersListResult(
            candidates=[],
            product_name=product_name,
            error=f"Product '{product_name}' not found. Available: {available}",
        )

    product_id = product["product_id"]
    canonical_name = product["product_name"]

    # Line items are already sorted by share_pct DESC; filter to matched-only rows
    line_items = [
        li for li in get_product_line_items(product_id, db_path)
        if li["kg_co2e"] is not None
    ][:top_n]

    if not line_items:
        return SuppliersListResult(
            candidates=[],
            product_name=canonical_name,
            error="No matched line items found for this product.",
        )

    # Build a quick lookup of existing engagements keyed by (supplier_name, component)
    existing = {
        (e.supplier_name.lower(), (e.component_name or "").lower()): e
        for e in get_engagements_for_product(canonical_name, db_path)
    }

    candidates: list[EngagementCandidate] = []
    for li in line_items:
        supplier_name, contact_name, contact_email, contact_found = _match_supplier(
            li["component"], li["material"], db_path
        )

        # Check whether this component is already being engaged
        key = (supplier_name.lower(), (li["component"] or "").lower())
        existing_eng = existing.get(key)

        candidates.append(
            EngagementCandidate(
                supplier_name=supplier_name,
                component=li["component"],
                material=li["material"],
                kg_co2e=li["kg_co2e"],
                share_pct=li["share_pct"],
                contact_found=contact_found,
                contact_name=contact_name,
                contact_email=contact_email,
                existing_engagement_id=existing_eng.engagement_id if existing_eng else None,
                engagement_status=existing_eng.status if existing_eng else "new",
            )
        )

    return SuppliersListResult(candidates=candidates, product_name=canonical_name)
