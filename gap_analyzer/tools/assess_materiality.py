"""Tool 2: Rank applicable Scope 3 categories by materiality.

Receives the reporting requirements result from Tool 1 and produces a
prioritised list with significance scores grounded in GHG Protocol guidance.

Two-call design:
  Call 1 — generate the markdown table (narrative output, no JSON pressure).
  Call 2 — extract structured JSON from that table (focused, short output).
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
_TOOL_NAME = "assess_materiality"

_SYSTEM_TABLE = """\
You are a Scope 3 GHG reporting specialist helping a company prioritise which \
Scope 3 categories to focus on based on likely emissions significance.

Relevant GHG Protocol guidance is provided below. Use it to ground every \
significance determination. Cite specific sections.

Rules:
- Only assess the categories passed in as applicable or uncertain from Tool 1. \
  Do not re-assess categories already marked Not Applicable.
- Assign each category a significance level: High / Medium / Low.
- Base significance on: typical emissions magnitude for the sector, data \
  availability, and the GHG Protocol's own materiality guidance.
- For each category explain: why it is significant, what drives the emissions, \
  and what data would be needed to quantify it.
- Do NOT fabricate data or benchmarks not present in the guidance below.

Return a single markdown table with these columns:
| Category # | Category Name | Significance | Score (3/2/1) | Rationale | Data needed | Citation |
|------------|---------------|--------------|---------------|-----------|-------------|----------|
(High=3, Medium=2, Low=1; one row per applicable/uncertain category)

GHG Protocol guidance:
"""

_EXTRACT_PROMPT = """\
From the materiality assessment above, extract only the categories ranked \
High or Medium significance into this JSON format. Include maximum 8 categories. \
Be concise in rationale field — one sentence maximum:

```json
{
  "ranked": [
    {
      "category_num": 1,
      "category_name": "Purchased goods and services",
      "significance": "High",
      "score": 3,
      "rationale": "one sentence"
    }
  ]
}
```

Return only the JSON block, nothing else.
"""

_JSON_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)
_N_RAG_RESULTS = 4


def _build_rag_context() -> tuple[str, list[str]]:
    queries = [
        "materiality assessment scope 3 categories criteria",
        "significant scope 3 categories identification",
        "scope 3 category prioritization relevance",
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


def _format_applicable_categories(previous_results: dict[str, ToolResult]) -> str:
    """Extract applicable + uncertain categories from Tool 1's structured output."""
    _FALLBACK = (
        "No reporting requirements data available from the prior step. "
        "Assess materiality for all 15 standard Scope 3 categories."
    )

    tool1 = previous_results.get("assess_reporting_requirements")
    if tool1 is None or tool1.structured is None:
        return _FALLBACK

    applicable = tool1.structured.get("applicable", [])
    uncertain = tool1.structured.get("uncertain", [])

    if not applicable and not uncertain:
        return _FALLBACK

    lines = ["Categories to assess for materiality (from reporting requirements assessment):\n"]
    for cat in applicable:
        lines.append(
            f"- Category {cat['category_num']}: {cat['category_name']} "
            f"(Applicable) — {cat.get('rationale', '')}"
        )
    for cat in uncertain:
        lines.append(
            f"- Category {cat['category_num']}: {cat['category_name']} "
            f"(Uncertain — needs more information) — {cat.get('rationale', '')}"
        )
    return "\n".join(lines)


def _extract_structured(
    client: anthropic.Anthropic,
    markdown_table: str,
    session_id: str | None = None,
) -> dict:
    """Call 2: extract top High/Medium categories as JSON from the markdown table."""
    t0 = time.perf_counter()
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=800,
            messages=[
                {"role": "user", "content": markdown_table + "\n\n" + _EXTRACT_PROMPT}
            ],
        )
        log_llm_call(
            app_name="gap_analyzer", tool_name=f"{_TOOL_NAME}_extract", model=_MODEL,
            tokens_in=response.usage.input_tokens, tokens_out=response.usage.output_tokens,
            latency_seconds=time.perf_counter() - t0,
            rag_used=False, session_id=session_id,
        )
        text = response.content[0].text if response.content else ""
        print("=== Tool 2 DEBUG: extraction call raw response ===")
        print(text[:1500])
        json_match = _JSON_RE.search(text)
        if json_match:
            return json.loads(json_match.group(1))
        print("=== Tool 2 DEBUG: extraction call returned no JSON block ===")
    except json.JSONDecodeError as exc:
        print(f"=== Tool 2 DEBUG: extraction JSONDecodeError: {exc} ===")
        log_llm_call(
            app_name="gap_analyzer", tool_name=f"{_TOOL_NAME}_extract", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=False, error=str(exc), session_id=session_id,
        )
    except anthropic.APIError as exc:
        print(f"=== Tool 2 DEBUG: extraction API error: {exc} ===")
        log_llm_call(
            app_name="gap_analyzer", tool_name=f"{_TOOL_NAME}_extract", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=False, error=str(exc), session_id=session_id,
        )
    return {"ranked": []}


def run(
    company_profile: CompanyProfile,
    previous_results: dict[str, ToolResult] | None = None,
    session_id: str | None = None,
) -> ToolResult:
    """Rank applicable Scope 3 categories by materiality significance."""
    previous_results = previous_results or {}

    try:
        rag_context, citations = _build_rag_context()
    except IndexNotBuiltError:
        return ToolResult(
            tool_name=_TOOL_NAME, content="", structured={}, citations=[],
            error="RAG index not built. Run `python -m rag.ingest` first.",
        )

    categories_context = _format_applicable_categories(previous_results)
    system_content = _SYSTEM_TABLE + rag_context
    client = anthropic.Anthropic()
    _rag_queries = [
        "materiality assessment scope 3 categories criteria",
        "significant scope 3 categories identification",
        "scope 3 category prioritization relevance",
    ]

    # Call 1: generate the markdown table
    t0 = time.perf_counter()
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=3000,
            system=[{"type": "text", "text": system_content, "cache_control": {"type": "ephemeral"}}],
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Based on the GHG Protocol guidance provided, rank the following "
                        "Scope 3 categories by materiality for this company:\n\n"
                        + company_profile.as_text()
                        + "\n\n"
                        + categories_context
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

    markdown_table = response.content[0].text if response.content else ""
    print("=== Tool 2 DEBUG: table call response length ===", len(markdown_table))
    print("=== Tool 2 DEBUG: table call response (last 500 chars) ===")
    print(markdown_table[-500:])

    # Call 2: extract structured JSON from the table
    structured = _extract_structured(client, markdown_table, session_id=session_id)
    print("=== Tool 2 DEBUG: final structured ===", structured)

    return ToolResult(
        tool_name=_TOOL_NAME,
        content=markdown_table,
        structured=structured,
        citations=citations,
    )
