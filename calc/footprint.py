"""Emission footprint calculation, aggregation, and hotspot identification.

Formula (per CLAUDE.md eval invariant):
    kg_co2e = spend_usd × ef_kg_co2e_per_usd

A row is included in the total only when both spend_usd and ef_kg_co2e_per_usd are available.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from factors.ef_lookup import EFMatch
from parsing.bom_parser import BOMRow, ParsedBOM


@dataclass
class LineItem:
    row_index: int
    component: str | None
    material: str | None
    quantity: float | None
    spend_usd: float | None
    weight_kg: float | None
    supplier: str | None
    country_of_origin: str | None
    # EF fields
    sector_name: str
    sector_code: str
    ef_kg_co2e_per_usd: float
    ef_source: str
    ef_confidence: float
    # Calculated
    kg_co2e: float
    share_pct: float  # filled in after total is known
    # Status
    is_matched: bool
    is_low_confidence: bool
    is_no_ef_match: bool
    is_flagged_by_parser: bool  # has any parsing flag


@dataclass
class FootprintResult:
    product_name: str
    total_kg_co2e: float
    line_items: list[LineItem]
    matched_count: int    # rows with a valid EF and spend_usd
    flagged_count: int    # rows with any flag (parser or EF)
    unmatched_count: int  # rows skipped from total (no spend_usd or no EF)
    completeness_pct: float  # matched / total rows × 100

    @property
    def has_any_results(self) -> bool:
        return self.total_kg_co2e > 0

    @property
    def hotspots(self) -> list[LineItem]:
        """Line items sorted descending by kg_co2e (only matched rows)."""
        return sorted(
            [li for li in self.line_items if li.is_matched],
            key=lambda li: li.kg_co2e,
            reverse=True,
        )


def calculate_footprint(
    bom: ParsedBOM,
    ef_matches: list[EFMatch | None],
) -> FootprintResult:
    """Calculate product carbon footprint from parsed BOM and EF matches.

    Args:
        bom:        Parsed BOM (output of parse_bom_csv).
        ef_matches: One EFMatch (or None) per BOMRow, in the same order as bom.rows.

    Returns:
        FootprintResult with per-row line items and aggregate totals.
    """
    if len(ef_matches) != len(bom.rows):
        raise ValueError(
            f"ef_matches length ({len(ef_matches)}) must equal bom.rows length ({len(bom.rows)})"
        )

    line_items: list[LineItem] = []
    running_total = 0.0

    for row, ef in zip(bom.rows, ef_matches):
        line_item = _build_line_item(row, ef)
        if line_item.is_matched:
            running_total += line_item.kg_co2e
        line_items.append(line_item)

    # Fill share_pct now that total is known
    for li in line_items:
        if running_total > 0 and li.is_matched:
            li.share_pct = (li.kg_co2e / running_total) * 100
        else:
            li.share_pct = 0.0

    # Verify invariant: sum of matched line items == total (within float tolerance)
    computed_sum = sum(li.kg_co2e for li in line_items if li.is_matched)
    assert math.isclose(computed_sum, running_total, rel_tol=1e-9), (
        f"Invariant violation: sum of line items ({computed_sum}) != total ({running_total})"
    )

    matched = sum(1 for li in line_items if li.is_matched)
    flagged = sum(1 for li in line_items if li.is_flagged_by_parser or li.is_low_confidence or li.is_no_ef_match)
    unmatched = sum(1 for li in line_items if not li.is_matched)
    total_rows = len(line_items)
    completeness = (matched / total_rows * 100) if total_rows > 0 else 0.0

    return FootprintResult(
        product_name=bom.product_name,
        total_kg_co2e=running_total,
        line_items=line_items,
        matched_count=matched,
        flagged_count=flagged,
        unmatched_count=unmatched,
        completeness_pct=completeness,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_line_item(row: BOMRow, ef: EFMatch | None) -> LineItem:
    has_spend = row.spend_usd is not None and row.spend_usd > 0
    has_ef = ef is not None and not ef.is_no_match and ef.ef_kg_co2e_per_usd > 0

    is_matched = has_spend and has_ef
    kg_co2e = (row.spend_usd * ef.ef_kg_co2e_per_usd) if is_matched else 0.0  # type: ignore[operator]

    return LineItem(
        row_index=row.row_index,
        component=row.component,
        material=row.material,
        quantity=row.quantity,
        spend_usd=row.spend_usd,
        weight_kg=row.weight_kg,
        supplier=row.supplier,
        country_of_origin=row.country_of_origin,
        sector_name=ef.sector_name if ef else "",
        sector_code=ef.sector_code if ef else "",
        ef_kg_co2e_per_usd=ef.ef_kg_co2e_per_usd if ef else 0.0,
        ef_source=ef.source_citation if ef else "",
        ef_confidence=ef.confidence_score if ef else 0.0,
        kg_co2e=kg_co2e,
        share_pct=0.0,  # filled in by caller after total is known
        is_matched=is_matched,
        is_low_confidence=ef.is_low_confidence if ef else False,
        is_no_ef_match=(ef is None or ef.is_no_match),
        is_flagged_by_parser=len(row.flags) > 0,
    )
