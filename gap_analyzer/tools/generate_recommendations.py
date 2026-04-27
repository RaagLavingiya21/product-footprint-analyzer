"""Tool 4: Generate prioritised recommendations to close Scope 3 data gaps.

Receives results from Tools 1 and 2 and produces a ranked action list grounded
in GHG Protocol data collection and calculation guidance.
"""

from __future__ import annotations

import json
import re
import time

import anthropic
from dotenv import load_dotenv

from gap_analyzer.models import CompanyProfile, ToolResult
from observability.logger import log_llm_call
from rag.retriever import IndexNotBuiltError, retrieve

load_dotenv()

_MODEL = "claude-sonnet-4-6"
_TOOL_NAME = "generate_recommendations"

_SYSTEM_PREAMBLE = """\
You are a Scope 3 GHG reporting specialist generating a prioritised action plan \
to help a company close data gaps and build a complete Scope 3 inventory.

Relevant GHG Protocol guidance on data collection, calculation methods, and \
data quality improvement is provided below. Ground every recommendation in \
this guidance and cite specific sections.

Rules:
- Focus recommendations on the HIGH and MEDIUM significance categories from the \
  materiality assessment. Include LOW only if it is low effort.
- For each recommendation: specify the action, which category it addresses, \
  the effort level (High / Medium / Low), the expected outcome, and cite the \
  GHG Protocol section that supports this approach.
- Prioritise by: significance score (descending) × inverse effort (Low effort first \
  within same significance tier).
- Do NOT recommend specific suppliers or software products.
- Do NOT fabricate guidance — cite only sections provided below.
- Format your response in two parts:

PART 1 — Numbered action list:
For each recommendation write:
**Priority N — [Category #: Category Name]**
- **Action:** What the company should do
- **Effort:** High / Medium / Low
- **Expected outcome:** What this achieves for their Scope 3 inventory
- **GHG Protocol basis:** Citation

PART 2 — JSON block:
```json
{
  "recommendations": [
    {
      "priority": 1,
      "category_num": 1,
      "category_name": "Purchased goods and services",
      "action": "...",
      "effort": "Medium",
      "outcome": "...",
      "citation": "..."
    }
  ]
}
```

GHG Protocol guidance:
"""

_JSON_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)
_N_RAG_RESULTS = 4


def _build_rag_context() -> tuple[str, list[str]]:
    queries = [
        "scope 3 data collection recommendations suppliers",
        "improving scope 3 data quality primary data",
        "scope 3 reduction targets engagement",
    ]

    seen: set[str] = set()
    chunks = []
    citations = []

    for query in queries:
        try:
            results = retrieve(query, n_results=_N_RAG_RESULTS)
        except IndexNotBuiltError:
            raise
        for r in results:
            if r.source_citation not in seen:
                seen.add(r.source_citation)
                chunks.append(r)
                citations.append(r.source_citation)

    context = "\n---\n".join(f"[{r.source_citation}]\n{r.text}\n" for r in chunks)
    return context, citations


def _format_materiality_context(previous_results: dict[str, ToolResult]) -> str:
    """Summarise Tool 1 and Tool 2 outputs for the LLM prompt."""
    lines: list[str] = []

    tool1 = previous_results.get("assess_reporting_requirements")
    if tool1 is not None and tool1.structured is not None:
        applicable = tool1.structured.get("applicable", [])
        uncertain = tool1.structured.get("uncertain", [])
        if applicable or uncertain:
            lines.append("Applicable categories (from reporting requirements assessment):")
            for cat in applicable:
                lines.append(f"  - Category {cat['category_num']}: {cat['category_name']}")
            for cat in uncertain:
                lines.append(f"  - Category {cat['category_num']}: {cat['category_name']} (uncertain)")

    tool2 = previous_results.get("assess_materiality")
    if tool2 is not None and tool2.structured is not None:
        ranked = tool2.structured.get("ranked", [])
        if ranked:
            lines.append("\nMateriality ranking (from materiality assessment, high to low):")
            for cat in sorted(ranked, key=lambda x: x.get("score", 0), reverse=True):
                lines.append(
                    f"  - Category {cat['category_num']}: {cat['category_name']} "
                    f"— {cat.get('significance', '?')} (score {cat.get('score', '?')}) "
                    f"— Data needed: {cat.get('data_needed', 'unknown')}"
                )
        else:
            lines.append("\nMateriality assessment completed but no ranked categories returned — "
                         "prioritise by typical emissions significance for this sector.")

    if not lines:
        return "No prior tool results available — provide general recommendations for all 15 categories."

    return "\n".join(lines)


def run(
    company_profile: CompanyProfile,
    previous_results: dict[str, ToolResult] | None = None,
    session_id: str | None = None,
) -> ToolResult:
    """Generate prioritised recommendations to close Scope 3 data gaps."""
    previous_results = previous_results or {}

    # DEBUG: inspect what previous_results contains at runtime
    print("=== Tool 4 DEBUG: previous_results keys ===", list(previous_results.keys()))
    tool2_debug = previous_results.get("assess_materiality")
    print("=== Tool 4 DEBUG: previous_results.get('assess_materiality') ===", tool2_debug)
    if tool2_debug is not None:
        print("=== Tool 4 DEBUG: tool2.structured ===", tool2_debug.structured)
        print("=== Tool 4 DEBUG: tool2.error ===", tool2_debug.error)

    try:
        rag_context, citations = _build_rag_context()
    except IndexNotBuiltError:
        return ToolResult(
            tool_name=_TOOL_NAME, content="", structured={}, citations=[],
            error="RAG index not built. Run `python -m rag.ingest` first.",
        )

    prior_context = _format_materiality_context(previous_results)
    system_content = _SYSTEM_PREAMBLE + rag_context
    client = anthropic.Anthropic()
    _rag_queries = [
        "scope 3 data collection recommendations suppliers",
        "improving scope 3 data quality primary data",
        "scope 3 reduction targets engagement",
    ]

    t0 = time.perf_counter()
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=4000,
            system=[{"type": "text", "text": system_content, "cache_control": {"type": "ephemeral"}}],
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Based on the GHG Protocol guidance and the prior analysis results below, "
                        "generate a prioritised action plan for this company to close their "
                        "Scope 3 data gaps:\n\n"
                        + company_profile.as_text()
                        + "\n\n"
                        + prior_context
                    ),
                }
            ],
        )
    except anthropic.RateLimitError:
        log_llm_call(
            app_name="gap_analyzer", tool_name=_TOOL_NAME, model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=True, rag_queries="|".join(_rag_queries),
            error="RateLimitError", session_id=session_id,
        )
        return ToolResult(
            tool_name=_TOOL_NAME, content="", structured={}, citations=citations,
            error="Rate limit reached. Please wait a moment and try again.",
        )
    except anthropic.APIConnectionError:
        log_llm_call(
            app_name="gap_analyzer", tool_name=_TOOL_NAME, model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=True, rag_queries="|".join(_rag_queries),
            error="APIConnectionError", session_id=session_id,
        )
        return ToolResult(
            tool_name=_TOOL_NAME, content="", structured={}, citations=citations,
            error="Could not connect to the AI service. Check your internet connection.",
        )
    except anthropic.APIStatusError as exc:
        log_llm_call(
            app_name="gap_analyzer", tool_name=_TOOL_NAME, model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=True, rag_queries="|".join(_rag_queries),
            error=f"APIStatusError({exc.status_code})", session_id=session_id,
        )
        return ToolResult(
            tool_name=_TOOL_NAME, content="", structured={}, citations=citations,
            error=f"AI service error ({exc.status_code}): {exc.message}",
        )

    log_llm_call(
        app_name="gap_analyzer", tool_name=_TOOL_NAME, model=_MODEL,
        tokens_in=response.usage.input_tokens, tokens_out=response.usage.output_tokens,
        latency_seconds=time.perf_counter() - t0,
        rag_used=True, rag_queries="|".join(_rag_queries), session_id=session_id,
    )

    full_text = response.content[0].text if response.content else ""

    structured: dict = {"recommendations": []}
    json_match = _JSON_RE.search(full_text)
    if json_match:
        try:
            structured = json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    display_content = _JSON_RE.sub("", full_text).strip()

    return ToolResult(
        tool_name=_TOOL_NAME,
        content=display_content,
        structured=structured,
        citations=citations,
    )
