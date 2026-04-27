"""MCP server exposing three tools from the Product Carbon Footprint Analyzer.

Tools:
  get_emission_factor — look up the CEDA emission factor for a material
  classify_spend      — calculate kg CO2e from a spend amount
  lookup_disclosure   — check public emissions disclosures for a supplier
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the project root is on sys.path when run directly
_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from mcp.server.fastmcp import FastMCP

from factors.ef_lookup import lookup_ef

mcp = FastMCP("product-footprint-analyzer")

# ---------------------------------------------------------------------------
# Disclosure table (v1: hardcoded; v2 will pull from suppliers DB)
# ---------------------------------------------------------------------------

_DISCLOSURE_TABLE: dict[str, dict] = {
    "rajput textiles": {
        "has_disclosure": True,
        "disclosure_source": "CDP 2023, score B",
        "sbti_target": False,
        "notes": "India-based textile supplier, CDP reporter",
    },
    "jiangsu thread co": {
        "has_disclosure": False,
        "disclosure_source": None,
        "sbti_target": False,
        "notes": "No public disclosure found",
    },
    "chemdyes international": {
        "has_disclosure": True,
        "disclosure_source": "Sustainability Report 2023",
        "sbti_target": True,
        "notes": "Germany-based, SBTi target set",
    },
    "ldpe packaging co": {
        "has_disclosure": False,
        "disclosure_source": None,
        "sbti_target": False,
        "notes": "No public disclosure found",
    },
    "nordic steel": {
        "has_disclosure": True,
        "disclosure_source": "CDP 2023, score A",
        "sbti_target": True,
        "notes": "Sweden-based, SBTi committed",
    },
    "cotton mills ltd": {
        "has_disclosure": False,
        "disclosure_source": None,
        "sbti_target": False,
        "notes": "No public disclosure found",
    },
}

_NOT_FOUND_DISCLOSURE = {
    "has_disclosure": False,
    "disclosure_source": None,
    "sbti_target": False,
    "notes": "Supplier not found in disclosure database",
}


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def get_emission_factor(material: str, country: str = "global") -> dict:
    """Look up the emission factor for a material from the CEDA database.

    Args:
        material: Material name (e.g. "cotton", "polyester", "steel").
        country:  Country of origin — use "global" for the global average (default).

    Returns:
        emission_factor: kg CO2e per USD spend.
        ef_unit:         Unit string ("kg CO2e per USD").
        source:          CEDA sector and country used.
        confidence:      "high" (≥80), "low" (60–79), or "no_match".
    """
    try:
        resolved_country = None if country.strip().lower() == "global" else country
        match = lookup_ef(material, resolved_country)
    except Exception as exc:
        return {"error": f"Emission factor lookup failed: {exc}"}

    if match.is_no_match:
        confidence = "no_match"
    elif match.is_low_confidence:
        confidence = "low"
    else:
        confidence = "high"

    return {
        "emission_factor": match.ef_kg_co2e_per_usd,
        "ef_unit": "kg CO2e per USD",
        "source": match.source_citation,
        "confidence": confidence,
    }


@mcp.tool()
def classify_spend(material: str, spend_usd: float) -> dict:
    """Calculate the carbon impact of a spend amount for a given material.

    Args:
        material:   Material name (e.g. "cotton", "nylon").
        spend_usd:  Spend amount in USD.

    Returns:
        kg_co2e:      Estimated greenhouse gas emissions in kg CO2e.
        ceda_sector:  CEDA sector name matched to this material.
        source:       Citation for the emission factor used.
    """
    try:
        match = lookup_ef(material, None)
    except Exception as exc:
        return {"error": f"Emission factor lookup failed: {exc}"}

    if match.is_no_match:
        return {
            "error": (
                f"No emission factor found for '{material}'. "
                f"Suggested alternatives: {', '.join(match.suggested_alternatives) or 'none'}."
            )
        }

    kg_co2e = match.ef_kg_co2e_per_usd * spend_usd

    return {
        "kg_co2e": round(kg_co2e, 6),
        "ceda_sector": match.sector_name,
        "source": match.source_citation,
    }


@mcp.tool()
def lookup_disclosure(supplier_name: str, country: str) -> dict:
    """Check if a supplier has public emissions disclosures.

    Args:
        supplier_name: Name of the supplier.
        country:       Country the supplier operates in.

    Returns:
        has_disclosure:    True if a public disclosure was found.
        disclosure_source: Name/description of the disclosure, or null.
        sbti_target:       True if the supplier has set an SBTi target.
        notes:             Additional context.
    """
    key = supplier_name.strip().lower()
    return _DISCLOSURE_TABLE.get(key, _NOT_FOUND_DISCLOSURE)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run(transport="stdio")
