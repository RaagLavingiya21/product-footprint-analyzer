"""Tool 1: Assess which Scope 3 categories are applicable for the company.

Grounds the LLM in RAG-retrieved GHG Protocol sections.
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
_TOOL_NAME = "assess_reporting_requirements"

_SYSTEM_PREAMBLE = """\
You are a Scope 3 GHG reporting specialist helping a company determine which \
of the 15 Scope 3 categories under the GHG Protocol Corporate Value Chain \
(Scope 3) Standard apply to them.

Relevant GHG Protocol guidance is provided below. Use it to ground your assessment. \
Cite specific sections for every determination.

Rules:
- Assess all 15 categories. For each, determine: Applicable / Not Applicable / Uncertain.
- Base applicability on the company's sector, size, geography, and products.
- For Uncertain: explain what additional information would resolve it.
- Do NOT fabricate guidance — only cite sections provided below.
- Format your response in two parts:

PART 1 — Markdown table:
| Category # | Category Name | Applicability | Rationale | Citation |
|------------|---------------|---------------|-----------|----------|
(one row per category)

PART 2 — JSON block (used by downstream tools):
```json
{
  "applicable": [{"category_num": 1, "category_name": "...", "rationale": "..."}],
  "not_applicable": [{"category_num": 2, "category_name": "...", "rationale": "..."}],
  "uncertain": [{"category_num": 3, "category_name": "...", "rationale": "..."}]
}
```

GHG Protocol guidance:
"""

_JSON_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)
_N_RAG_RESULTS = 4


def _build_rag_context(sector: str) -> tuple[str, list[str]]:
    """Run 3 targeted queries and return (formatted_context, deduplicated_citations)."""
    queries = [
        f"scope 3 category applicability requirements {sector}",
        "scope 3 boundary requirements shall minimum",
        "minimum boundaries scope 3 categories overview table",
    ]

    seen_ids: set[str] = set()
    chunks = []
    citations = []

    for query in queries:
        try:
            results = retrieve(query, n_results=_N_RAG_RESULTS)
        except IndexNotBuiltError:
            raise

        for r in results:
            chunk_key = r.source_citation
            if chunk_key not in seen_ids:
                seen_ids.add(chunk_key)
                chunks.append(r)
                citations.append(r.source_citation)

    context_parts = []
    for r in chunks:
        context_parts.append(
            f"[{r.source_citation}]\n{r.text}\n"
        )

    return "\n---\n".join(context_parts), citations


def run(
    company_profile: CompanyProfile,
    previous_results: dict[str, ToolResult] | None = None,
    session_id: str | None = None,
) -> ToolResult:
    """Assess applicable Scope 3 categories for the given company profile."""
    if not company_profile.sector.strip() or not company_profile.products.strip():
        return ToolResult(
            tool_name=_TOOL_NAME,
            content="",
            structured={},
            citations=[],
            error="Incomplete company profile: sector and products are required.",
        )

    try:
        rag_context, citations = _build_rag_context(company_profile.sector)
    except IndexNotBuiltError:
        return ToolResult(
            tool_name=_TOOL_NAME,
            content="",
            structured={},
            citations=[],
            error="RAG index not built. Run `python -m rag.ingest` first.",
        )

    system_content = _SYSTEM_PREAMBLE + rag_context
    client = anthropic.Anthropic()
    _rag_queries = [
        f"scope 3 category applicability requirements {company_profile.sector}",
        "scope 3 boundary requirements shall minimum",
        "minimum boundaries scope 3 categories overview table",
    ]

    t0 = time.perf_counter()
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=3000,
            system=[
                {
                    "type": "text",
                    "text": system_content,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Based on the GHG Protocol guidance provided, assess which Scope 3 "
                        "categories apply to this company:\n\n"
                        + company_profile.as_text()
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

    # Parse structured JSON block
    structured: dict = {"applicable": [], "not_applicable": [], "uncertain": []}
    json_match = _JSON_RE.search(full_text)
    if json_match:
        try:
            structured = json.loads(json_match.group(1))
        except json.JSONDecodeError:
            pass

    # Content shown to user: strip the JSON block, keep the markdown table + any prose
    display_content = _JSON_RE.sub("", full_text).strip()

    return ToolResult(
        tool_name=_TOOL_NAME,
        content=display_content,
        structured=structured,
        citations=citations,
    )
