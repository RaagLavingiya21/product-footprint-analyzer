# CLAUDE.md — Product Carbon Footprint Analyzer

## Project Purpose
This project is to create a tool which will be used by sustainability analysts at consumer goods companies to estimate product-level Scope 3 footprints from messy BOM data so they can identify hotspots and prioritize supplier engagement. The tool fetches and parses the material data from uploaded bill of materials, fixes or flags messy or incomplete bill of material data, fetches emission factor from external emission factors database, calculates products emission footprint based on the BOM and emission factors. This enables the user to understand product's total emission, emission hotspots, and design decarbonization strategies to reduce the carbon footprint of product. 


## User Persona
This tool will be used by a sustainability analyst or business analyst. The company size - 500 to5,000 employees. 
- Users will have some business and data analysis, sustainability, financial, legal, and accounting background, but won't have a technical and coding knowledge. 
- Users require the tool results to be auditable, the methodology to be standard and reliable
- The user will use the outcome of the tool to Understand the total emission of the product, and the breakdown of emission from product components and drive decision making around:
- where the emission hotspots are
- which components are the most emitting
- how they can reduce the emission from the overall product as well as from components
- how they can choose different suppliers or different materials or different quantities or different methods to generate the same product in a lower-carbon-emission way

## Domain Context

- **BOM**: Bill of Materials(BOM) is a list of all the components, the material used in components, the quantity and weights of the material. It's the recipe of product with all the ingridients
- **Emission factor**: Emission factor is an estimate of greenhouse gases released from a specific activity. It can be measured in different units, like ton of CO2 per kg of a material or ton of CO2 per kWh of electricity. 
- **Activity data**: a quantitative measure of a company’s operational activities that generate greenhouse gas emissions, such as fuel consumption, energy use, or materials purchased

- **GWP**: It is global warming potential. It is a measure of the warming effect a gas has over a certain time period. Generally GWP100(warming potenital over 100 year time period) is used and is normalized in CO2e(CO2 equivalence). 

- **Cradle-to-gate**: It is one of the boundary condition for life cycle analysis (LCA). It means from raw material extraction through manufacturing, up to the point the product leaves the factory gate. It is different from Cradle to Grave, which also includes use of the product and end of life, and Gate to Gate, which only includes manufacturing. 

- **Scope 3 Category 1**: is Purchased Goods and Services — emissions from the production of goods and services a company buys.

- **Primary vs Secondary data**: Primary data is firsthand, supplier-specific, or facility-level data (e.g., energy bills, direct emissions) from a company’s value chain. Secondary data is industry-average data (e.g., databases, literature) used when primary data is unavailable

- **Hotspot** : A hotspot is a material, process, or supplier that contributes a disproportionately large share of a product's footprint, making it a priority for reduction efforts.
(and so on)

## Architecture

Four modules, strict dependency direction:

- `parsing/` — BOM ingestion, normalization, unit standardization
- `factors/` — emission factor lookup from external databases (ecoinvent, USEEIO)
- `calc/` — emission calculations, aggregation, hotspot identification
- `app.py` — Streamlit UI, user interaction, result display

**Dependency rules:**
- `app.py` imports from `calc/`, `factors/`, `parsing/`. Nothing imports from `app.py`.
- `calc/` imports from `factors/` and `parsing/`. 
- `factors/` imports from `parsing/`. 
- `parsing/` imports from nothing internal.

**Hard constraints:**
- No Streamlit calls outside `app.py`
- No calculations inside `app.py`, `factors/`, or `parsing/`
- No emission factor lookups inside `app.py` or `calc/`
- `calc/`, `factors/`, and `parsing/` must be runnable from a plain Python script with no UI dependency


## Decision Rules for Ambiguous Inputs
Bullet list. One bullet per case:
- Missing spend_usd → flag for human review
- Missing component or material → flag for human review
- Formatting discrepancy → fix it
- Ambiguous material → suggest nearest matches,flag for review
- A unit is in imperial (lb, oz) instead of metric - convert to metric
- Two rows look like duplicates → proceed but flag
- Supplier data contradicts previous submission → flag with explanation
- Low-confidence emission factor match → proceed but flag with confidence score
 

## Non-Goals
- It does not prepare a regulatory or compliance report. 
- It does not produce a decarbonization plan.  
- This tool does not replace an LCA practitioner's judgment for certification-grade assessments (e.g., EPDs, ISO 14067 conformance).


## Eval Invariants
- Total footprint must equal the sum of the individual footprint of all the material line items. 
- total kg of co2e = spend_usd x emission factor
- Every emission factor must have a source citation
- Every number in the output must have a traceable source. 
- every emission factor's activity unit must match the activity unit in the BOM row it's applied to.
- Unit mismatches must be resolved by conversion
- Same input must produce the same output in terms of total footprint. 
- Unmatched items must must be flagged for human review. 
- Confidence below threshold must must be flagged to human as low confidence. 


## Coding Conventions
- use python 3.13 for parsing, emission factor lookup and Calculation logic layers. use Ruff python formatter
- Use Streamlit for presentation layer(app.py). 
- All the units should be encoding of what is reprenets - eg. total_kg_CO2e rather than just total
- Use Pytest for unit tests

## When to Ask the User
Missing quantity → flag for human review
- Missing component or material → flag for human review
- Anomolus quanity → flag for human review
- Ambiguous material → suggest nearest matches,
  flag for review
- No direct match for emission factors in EEIO or EcoInvent database -> suggest nearest matches, flag for review



