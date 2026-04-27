# Product Footprint Analyzer — MCP Server

Exposes three tools from the Product Carbon Footprint Analyzer codebase via the Model Context Protocol (MCP), so Claude Code or other MCP clients can call them directly.

---

## Registering with Claude Code

Add the following to your Claude Code MCP config (`~/.claude/mcp_config.json` or via `claude mcp add`):

```json
{
  "mcpServers": {
    "product-footprint-analyzer": {
      "command": "python3",
      "args": ["/path/to/product-footprint-analyzer/mcp_server/server.py"],
      "env": {}
    }
  }
}
```

Replace `/path/to/product-footprint-analyzer` with the absolute path to this project.

Or register it from the terminal:

```bash
claude mcp add product-footprint-analyzer python3 /path/to/product-footprint-analyzer/mcp_server/server.py
```

---

## Tools

### `get_emission_factor`

Look up the CEDA emission factor for a material.

| Parameter | Type   | Required | Description |
|-----------|--------|----------|-------------|
| material  | string | Yes      | Material name (e.g. "cotton", "polyester") |
| country   | string | No       | Country of origin; defaults to `"global"` |

**Returns:**

| Field            | Type   | Description |
|------------------|--------|-------------|
| emission_factor  | float  | kg CO2e per USD spend |
| ef_unit          | string | `"kg CO2e per USD"` |
| source           | string | CEDA sector + country used |
| confidence       | string | `"high"`, `"low"`, or `"no_match"` |

**Example:**
```
get_emission_factor(material="cotton", country="India")
→ { "emission_factor": 0.42, "ef_unit": "kg CO2e per USD", "source": "Open CEDA 2025, Textile mills, IND", "confidence": "high" }
```

---

### `classify_spend`

Calculate estimated kg CO2e from a USD spend amount for a material.

| Parameter | Type   | Required | Description |
|-----------|--------|----------|-------------|
| material  | string | Yes      | Material name |
| spend_usd | float  | Yes      | Spend amount in USD |

**Returns:**

| Field        | Type   | Description |
|--------------|--------|-------------|
| kg_co2e      | float  | Estimated GHG emissions |
| ceda_sector  | string | CEDA sector matched to this material |
| source       | string | Citation for the emission factor |

**Example:**
```
classify_spend(material="steel", spend_usd=5000)
→ { "kg_co2e": 2150.0, "ceda_sector": "Iron and steel", "source": "Open CEDA 2025, Iron and steel, USA" }
```

---

### `lookup_disclosure`

Check if a supplier has public emissions disclosures.

| Parameter     | Type   | Required | Description |
|---------------|--------|----------|-------------|
| supplier_name | string | Yes      | Supplier name |
| country       | string | Yes      | Country the supplier operates in |

**Returns:**

| Field             | Type         | Description |
|-------------------|--------------|-------------|
| has_disclosure    | bool         | True if a public disclosure was found |
| disclosure_source | string\|null | Disclosure name/description |
| sbti_target       | bool         | True if supplier has an SBTi target |
| notes             | string       | Additional context |

**Example:**
```
lookup_disclosure(supplier_name="Nordic Steel", country="Sweden")
→ { "has_disclosure": true, "disclosure_source": "CDP 2023, score A", "sbti_target": true, "notes": "Sweden-based, SBTi committed" }
```

Covered suppliers (v1): Rajput Textiles, Jiangsu Thread Co, ChemDyes International, LDPE Packaging Co, Nordic Steel, Cotton Mills Ltd.

---

## Running Locally

```bash
pip install mcp
python3 mcp_server/server.py
```

The server starts in stdio mode and waits for MCP messages on stdin. You won't see any output — that is expected. Use Ctrl+C to stop.
