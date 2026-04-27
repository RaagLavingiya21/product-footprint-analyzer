"""Tests for parsing/bom_parser.py — covers all three BOM examples from the Spec."""

import io
import textwrap

import pytest

from parsing.bom_parser import parse_bom_csv


def _csv(text: str) -> bytes:
    return textwrap.dedent(text).strip().encode("utf-8")


# ---------------------------------------------------------------------------
# Example 1: Clean cotton T-shirt — no flags expected
# ---------------------------------------------------------------------------

CLEAN_TSHIRT_CSV = _csv("""
    component,material,quantity,spend_usd,weight_kg,supplier,country_of_origin
    body,cotton fabric,1,10,0.15,ABCD,India
    thread,nylon,1,1,0.005,ABCD,India
    label,nylon,1,1,0.001,ABCD,India
    dye,reactive dye,1,1,0.009,ABCD,India
    Packaging,LDPE,1,1,0.008,ABCD,China
""")


def test_clean_bom_no_flags():
    result = parse_bom_csv(CLEAN_TSHIRT_CSV, "T-shirt Clean")
    assert result.is_valid
    assert result.file_errors == []
    assert len(result.rows) == 5
    assert result.all_flags == [], f"Expected no flags, got: {result.all_flags}"


def test_clean_bom_values_parsed():
    result = parse_bom_csv(CLEAN_TSHIRT_CSV, "T-shirt Clean")
    body = result.rows[0]
    assert body.component == "body"
    assert body.material == "cotton fabric"
    assert body.quantity == 1.0
    assert body.spend_usd == 10.0
    assert body.weight_kg == 0.15
    assert body.supplier == "ABCD"
    assert body.country_of_origin == "India"


# ---------------------------------------------------------------------------
# Example 2: Messy cotton T-shirt
# ---------------------------------------------------------------------------

MESSY_TSHIRT_CSV = _csv("""
    component,material,quantity,spend_usd,weight_kg,supplier,country_of_origin
    body,,1,10,0.15,ABCD,India
    thread,nylon,1,,0.001,ABCD,India
    label,nylon,1,1,0.001,ABCD,India
    dye,reactive dye,1,1,0.009,ABCD,India
    Packaging,Plastic,1,1,0.008,ABCD,China
    Packaging,LDPE,1,1,0.008,ABCD,China
    Packaging,LDPE,1,1,0.008,ABCD,India
""")


def test_messy_bom_row1_missing_material():
    result = parse_bom_csv(MESSY_TSHIRT_CSV, "T-shirt Messy")
    body_flags = [f for f in result.rows[0].flags if f.field == "material" and f.flag_type == "missing"]
    assert len(body_flags) == 1, "Row 1 (body) should be flagged for missing material"


def test_messy_bom_row2_missing_spend_usd():
    result = parse_bom_csv(MESSY_TSHIRT_CSV, "T-shirt Messy")
    thread_flags = [f for f in result.rows[1].flags if f.field == "spend_usd" and f.flag_type == "missing"]
    assert len(thread_flags) == 1, "Row 2 (thread) should be flagged for missing spend_usd"


def test_messy_bom_quantity_not_flagged_when_missing():
    """quantity missing should NOT trigger a flag per updated CLAUDE.md."""
    csv_data = _csv("""
        component,material,quantity,spend_usd,weight_kg
        body,cotton,,10,0.15
    """)
    result = parse_bom_csv(csv_data, "Test")
    qty_missing_flags = [f for f in result.all_flags if f.field == "quantity" and f.flag_type == "missing"]
    assert qty_missing_flags == [], "Missing quantity should NOT be flagged"


def test_messy_bom_rows_6_7_duplicates():
    """Rows 6 and 7 (Packaging/LDPE/0.008/ABCD — China and India) share the same
    component/material/weight/supplier key and must be flagged as duplicates.
    Row 5 (Packaging/Plastic) has a different material so is NOT a duplicate."""
    result = parse_bom_csv(MESSY_TSHIRT_CSV, "T-shirt Messy")
    # rows[5] = Packaging/LDPE/ABCD/China, rows[6] = Packaging/LDPE/ABCD/India
    dup_flags_r6 = [f for f in result.rows[5].flags if f.flag_type == "duplicate"]
    dup_flags_r7 = [f for f in result.rows[6].flags if f.flag_type == "duplicate"]
    assert len(dup_flags_r6) >= 1, "Row 6 (Packaging/LDPE/China) should be flagged as duplicate"
    assert len(dup_flags_r7) >= 1, "Row 7 (Packaging/LDPE/India) should be flagged as duplicate"
    # Row 5 (Packaging/Plastic) is a different material — not a duplicate
    assert result.rows[4].flags == [], "Row 5 (Packaging/Plastic) should have no flags"


def test_messy_bom_clean_rows_have_no_flags():
    result = parse_bom_csv(MESSY_TSHIRT_CSV, "T-shirt Messy")
    # Row 3 (label) and row 4 (dye) should have no flags
    assert result.rows[2].flags == [], "Row 3 (label) should have no flags"
    assert result.rows[3].flags == [], "Row 4 (dye) should have no flags"


# ---------------------------------------------------------------------------
# Example 3: Water bottle edge cases
# ---------------------------------------------------------------------------

WATER_BOTTLE_CSV = _csv("""
    component,material,quantity,spend_usd,weight_kg,supplier,country_of_origin
    body,stainless steel,1,10,0.3,ABCD,India
    Lid,SS304,1,3,0.07,abc,India
    Gasket/Seal,Silicone,1,1,0.0008,cvb,India
    Insulation,aerogel,1,2,0.0001,dfgh,India
    Antimicrobial treatment,tritan copolyester,1,2,0.0001,bnm,India
    packaging,cardboard,1,1,0.050,ert,china
    packaging,coating,1,1,0.005,ert,china
""")


def test_water_bottle_parsed():
    result = parse_bom_csv(WATER_BOTTLE_CSV, "Water Bottle")
    assert result.is_valid
    assert len(result.rows) == 7


def test_water_bottle_stainless_steel_duplicate_flag():
    """Rows 1 and 2 (stainless steel / SS304, same supplier India) should be flagged as duplicates
    only if they share the same key — they don't (different material strings), so no dup flag here.
    Duplicate detection uses exact material match; semantic similarity is handled by factors module."""
    result = parse_bom_csv(WATER_BOTTLE_CSV, "Water Bottle")
    # Exact duplicate detection: body/stainless steel != Lid/SS304 — no dup flag from parser
    dup_flags = [f for f in result.all_flags if f.flag_type == "duplicate"]
    assert dup_flags == [], "Parser should not flag rows with different material strings as duplicates"


# ---------------------------------------------------------------------------
# File-level failure modes
# ---------------------------------------------------------------------------


def test_empty_file_error():
    result = parse_bom_csv(_csv("component,material,quantity,spend_usd"), "Empty")
    assert not result.is_valid
    assert any("empty" in e.lower() for e in result.file_errors)


def test_missing_required_column():
    csv_data = _csv("""
        component,material,quantity
        body,cotton,1
    """)
    result = parse_bom_csv(csv_data, "Missing col")
    assert not result.is_valid
    assert any("spend_usd" in e for e in result.file_errors)


def test_too_many_rows():
    header = "component,material,quantity,spend_usd\n"
    rows = "body,cotton,1,10\n" * 501
    csv_data = (header + rows).encode("utf-8")
    result = parse_bom_csv(csv_data, "Big BOM")
    assert not result.is_valid
    assert any("500" in e for e in result.file_errors)


def test_corrupt_file():
    result = parse_bom_csv(b"\xff\xfe invalid bytes \x00\x01", "Corrupt")
    assert not result.is_valid
    assert len(result.file_errors) > 0


def test_oversized_file():
    header = b"component,material,quantity,spend_usd\n"
    big_row = b"body,cotton,1,10\n" * 10000
    data = header + big_row  # well over 5 MB
    # Pad to exceed limit
    data = data + b"x" * (5 * 1024 * 1024)
    result = parse_bom_csv(data, "Big file")
    assert not result.is_valid
    assert any("5 MB" in e or "5mb" in e.lower() for e in result.file_errors)


# ---------------------------------------------------------------------------
# Imperial unit conversion
# ---------------------------------------------------------------------------


def test_weight_lb_converted_to_kg():
    csv_data = _csv("""
        component,material,quantity,spend_usd,weight_kg
        body,cotton,1,10,0.33lb
    """)
    result = parse_bom_csv(csv_data)
    row = result.rows[0]
    assert row.weight_kg is not None
    assert abs(row.weight_kg - 0.33 * 0.453592) < 1e-5
    conv_flags = [f for f in row.flags if f.flag_type == "formatting_fixed"]
    assert len(conv_flags) == 1


def test_weight_oz_converted_to_kg():
    csv_data = _csv("""
        component,material,quantity,spend_usd,weight_kg
        body,cotton,1,10,5oz
    """)
    result = parse_bom_csv(csv_data)
    row = result.rows[0]
    assert row.weight_kg is not None
    assert abs(row.weight_kg - 5 * 0.0283495) < 1e-5


def test_weight_g_converted_to_kg():
    csv_data = _csv("""
        component,material,quantity,spend_usd,weight_kg
        body,cotton,1,10,150g
    """)
    result = parse_bom_csv(csv_data)
    row = result.rows[0]
    assert row.weight_kg is not None
    assert abs(row.weight_kg - 0.15) < 1e-5


# ---------------------------------------------------------------------------
# Anomalous quantity (z-score outlier)
# ---------------------------------------------------------------------------


def test_anomalous_quantity_flagged():
    """One row with quantity=10000 among rows with quantity=1 should be flagged."""
    csv_data = _csv("""
        component,material,quantity,spend_usd
        a,cotton,1,5
        b,nylon,1,5
        c,LDPE,1,5
        d,steel,1,5
        e,glass,10000,5
    """)
    result = parse_bom_csv(csv_data)
    outlier_flags = [
        f for f in result.all_flags
        if f.field == "quantity" and f.flag_type == "anomalous"
    ]
    assert len(outlier_flags) >= 1, "Extreme quantity outlier should be flagged"


def test_anomalous_spend_flagged():
    csv_data = _csv("""
        component,material,quantity,spend_usd
        a,cotton,1,5
        b,nylon,1,5
        c,LDPE,1,5
        d,steel,1,5
        e,glass,1,999999
    """)
    result = parse_bom_csv(csv_data)
    outlier_flags = [
        f for f in result.all_flags
        if f.field == "spend_usd" and f.flag_type == "anomalous"
    ]
    assert len(outlier_flags) >= 1, "Extreme spend_usd outlier should be flagged"


# ---------------------------------------------------------------------------
# Eval invariants
# ---------------------------------------------------------------------------


def test_all_rows_present_in_output():
    """Every input row must appear in ParsedBOM.rows regardless of flags."""
    result = parse_bom_csv(MESSY_TSHIRT_CSV, "T-shirt Messy")
    assert len(result.rows) == 7


def test_same_input_same_output():
    """Determinism: parsing the same bytes twice yields identical results."""
    r1 = parse_bom_csv(CLEAN_TSHIRT_CSV, "T-shirt")
    r2 = parse_bom_csv(CLEAN_TSHIRT_CSV, "T-shirt")
    assert len(r1.rows) == len(r2.rows)
    for a, b in zip(r1.rows, r2.rows):
        assert a.spend_usd == b.spend_usd
        assert a.material == b.material
