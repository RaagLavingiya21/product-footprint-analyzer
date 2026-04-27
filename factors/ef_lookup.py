"""Emission factor lookup from the Open CEDA 2025 database.

Lookup strategy (in order):
1. Exact match (case-insensitive) against material_mapping entries → confidence 100
2. Fuzzy match against material_mapping material names via rapidfuzz
3. Fuzzy match against all CEDA sector names
Confidence thresholds: ≥80 = match, 60–79 = low-confidence flag, <60 = no match.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from rapidfuzz import process as fuzz_process
from rapidfuzz.distance import JaroWinkler

from factors.material_mapping import material_mapping

_DATA_PATH = Path(__file__).parent.parent / "data" / "Open CEDA 2025 (updated 2025-11-12).xlsx"
_SHEET = "GHG_t_Raw"

CONFIDENCE_MATCH = 80
CONFIDENCE_LOW = 60
FALLBACK_COUNTRY = "USA"


@dataclass
class EFMatch:
    material_input: str
    sector_name: str
    sector_code: str
    ef_kg_co2e_per_usd: float
    country_used: str
    confidence_score: float  # 0–100
    is_low_confidence: bool
    is_no_match: bool
    source_citation: str
    suggested_alternatives: list[str]  # populated when is_no_match or is_low_confidence


# ---------------------------------------------------------------------------
# Data loading — cached so the Excel file is read only once per process
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _load_ceda() -> tuple[list[str], list[str], dict[str, dict[str, float]]]:
    """Return (sector_names, sector_codes, ef_index).

    ef_index: {sector_code: {country_code: ef_value}}
    """
    df = pd.read_excel(_DATA_PATH, sheet_name=_SHEET, header=None)

    sector_names: list[str] = [
        str(v) for v in df.iloc[2, 3:].tolist() if pd.notna(v)
    ]
    sector_codes: list[str] = [
        str(v) for v in df.iloc[3, 3:].tolist() if pd.notna(v)
    ]

    country_codes: list[str] = [str(v) for v in df.iloc[4:, 0].tolist()]
    ef_values = df.iloc[4:, 3:].values  # numpy array, rows=countries, cols=sectors

    ef_index: dict[str, dict[str, float]] = {}
    for s_idx, code in enumerate(sector_codes):
        ef_index[code] = {}
        for c_idx, cc in enumerate(country_codes):
            val = ef_values[c_idx, s_idx]
            if pd.notna(val):
                ef_index[code][cc] = float(val)

    return sector_names, sector_codes, ef_index


@functools.lru_cache(maxsize=1)
def _country_name_to_code() -> dict[str, str]:
    """Return {lowercase_country_name: country_code} for fuzzy country resolution."""
    df = pd.read_excel(_DATA_PATH, sheet_name=_SHEET, header=None)
    codes = df.iloc[4:, 0].tolist()
    names = df.iloc[4:, 1].tolist()
    return {str(n).lower(): str(c) for n, c in zip(names, codes) if pd.notna(n) and pd.notna(c)}


@functools.lru_cache(maxsize=1)
def _mapping_index() -> dict[str, dict]:
    """Return {lowercase_material_name: mapping_entry}."""
    return {entry["material"].lower(): entry for entry in material_mapping}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def lookup_ef(material: str, country: str | None = None) -> EFMatch:
    """Look up an emission factor for a BOM material.

    Args:
        material: Material name from the BOM row.
        country:  Country of origin (optional); used to select country-specific EF.

    Returns:
        EFMatch with ef_kg_co2e_per_usd populated if a match is found,
        or is_no_match=True if no suitable sector was identified.
    """
    sector_names, sector_codes, ef_index = _load_ceda()
    country_code = _resolve_country(country)

    sector_code, sector_name, confidence, alternatives = _find_sector(material, sector_names, sector_codes)

    if sector_code is None:
        return EFMatch(
            material_input=material,
            sector_name="",
            sector_code="",
            ef_kg_co2e_per_usd=0.0,
            country_used=country_code,
            confidence_score=0.0,
            is_low_confidence=False,
            is_no_match=True,
            source_citation="",
            suggested_alternatives=alternatives,
        )

    ef_value = _get_ef(ef_index, sector_code, country_code)
    is_low = confidence < CONFIDENCE_MATCH

    return EFMatch(
        material_input=material,
        sector_name=sector_name,
        sector_code=sector_code,
        ef_kg_co2e_per_usd=ef_value,
        country_used=country_code,
        confidence_score=confidence,
        is_low_confidence=is_low,
        is_no_match=False,
        source_citation=f"Open CEDA 2025, {sector_name}, {country_code}",
        suggested_alternatives=alternatives if is_low else [],
    )


def get_all_sector_names() -> list[str]:
    """Return all CEDA sector names (useful for UI suggestions)."""
    sector_names, _, _ = _load_ceda()
    return sector_names


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_country(country: str | None) -> str:
    """Map a country name or code string to a 3-letter CEDA country code."""
    if not country:
        return FALLBACK_COUNTRY

    name_map = _country_name_to_code()
    key = country.strip().lower()

    # Direct match by name
    if key in name_map:
        return name_map[key]

    # Match by 3-letter ISO code (case-insensitive)
    upper = country.strip().upper()
    all_codes = set(name_map.values())
    if upper in all_codes:
        return upper

    # Fuzzy fallback on country name
    result = fuzz_process.extractOne(
        key, name_map.keys(), scorer=JaroWinkler.similarity, score_cutoff=0.85
    )
    if result:
        return name_map[result[0]]

    return FALLBACK_COUNTRY


def _find_sector(
    material: str,
    sector_names: list[str],
    sector_codes: list[str],
) -> tuple[str | None, str, float, list[str]]:
    """Return (sector_code, sector_name, confidence, alternatives).

    Tries curated mapping first, then falls back to fuzzy sector-name search.
    """
    material_lower = material.strip().lower()
    mapping_idx = _mapping_index()

    # 1. Exact match in curated mapping
    if material_lower in mapping_idx:
        entry = mapping_idx[material_lower]
        return entry["sector_code"], entry["sector"], 100.0, []

    # 2. Fuzzy match against curated mapping material names
    map_result = fuzz_process.extractOne(
        material_lower,
        mapping_idx.keys(),
        scorer=JaroWinkler.similarity,
        score_cutoff=CONFIDENCE_LOW / 100,
    )
    if map_result:
        matched_material, raw_score, _ = map_result
        confidence = raw_score * 100
        entry = mapping_idx[matched_material]
        alternatives = _top_sector_alternatives(material_lower, sector_names, sector_codes, n=3)
        return entry["sector_code"], entry["sector"], confidence, alternatives

    # 3. Fuzzy match directly against all CEDA sector names
    sector_result = fuzz_process.extractOne(
        material_lower,
        [n.lower() for n in sector_names],
        scorer=JaroWinkler.similarity,
        score_cutoff=CONFIDENCE_LOW / 100,
    )
    if sector_result:
        matched_name_lower, raw_score, idx = sector_result
        confidence = raw_score * 100
        alternatives = _top_sector_alternatives(material_lower, sector_names, sector_codes, n=3)
        return sector_codes[idx], sector_names[idx], confidence, alternatives

    # 4. No match — return top suggestions for human review
    alternatives = _top_sector_alternatives(material_lower, sector_names, sector_codes, n=3)
    return None, "", 0.0, alternatives


def _top_sector_alternatives(
    material_lower: str,
    sector_names: list[str],
    sector_codes: list[str],
    n: int = 3,
) -> list[str]:
    """Return top-n sector name suggestions for human review."""
    results = fuzz_process.extract(
        material_lower,
        [name.lower() for name in sector_names],
        scorer=JaroWinkler.similarity,
        limit=n,
    )
    return [sector_names[r[2]] for r in results]


def _get_ef(ef_index: dict[str, dict[str, float]], sector_code: str, country_code: str) -> float:
    """Return the EF for a sector/country, falling back to USA if country not available."""
    sector_efs = ef_index.get(sector_code, {})
    if country_code in sector_efs:
        return sector_efs[country_code]
    if FALLBACK_COUNTRY in sector_efs:
        return sector_efs[FALLBACK_COUNTRY]
    # Last resort: return mean across available countries
    values = [v for v in sector_efs.values() if v > 0]
    return sum(values) / len(values) if values else 0.0
