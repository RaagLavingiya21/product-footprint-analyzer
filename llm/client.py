"""Claude-powered conversational advisor for saved footprint analyses.

No Streamlit imports — callable from any Python context.

Routing:
  data     → SQLite context only
  guidance → GHG Protocol RAG context only
  both     → SQLite + RAG context combined
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field

import anthropic
from dotenv import load_dotenv

from llm.router import route_question
from observability.logger import log_llm_call
from rag.retriever import IndexNotBuiltError, retrieve

load_dotenv()

_MODEL = "claude-sonnet-4-6"

_SYSTEM_DATA = """\
You are a carbon footprint advisor helping sustainability analysts at consumer goods companies \
understand their product footprints. You have access to the user's saved product analyses below.

Rules:
- Only answer questions about data that exists in the saved analyses provided.
- For every numeric claim you make, cite the product name and the specific line item or total.
- Do not fabricate numbers, emission factors, or comparisons not present in the data.
- Do not recommend specific suppliers, investment decisions, or compliance actions.
- When asked about reduction strategies, describe general approaches (material substitution, \
lower-EF sector alternatives) grounded in the data, but do not prescribe specific business decisions.
- If the user asks about something not in the data, say so clearly.

Methodology reminder (for your reference when explaining numbers):
- kg CO₂e = spend_usd × emission_factor (kgCO₂e/USD)
- Emission factors are from Open CEDA 2025 (USEEIO spend-based model), Scope 3 Category 1, cradle-to-gate.

"""

_SYSTEM_GUIDANCE = """\
You are a carbon footprint advisor helping sustainability analysts at consumer goods companies \
understand GHG reporting requirements and methodology. You have access to relevant sections of \
the GHG Protocol Corporate Value Chain (Scope 3) Standard below.

Rules:
- Ground every answer in the GHG Protocol guidance provided. Cite specific sections.
- Do NOT fabricate guidance or standards not present in the excerpts below.
- Do not recommend specific suppliers, investment decisions, or compliance actions.
- If the guidance excerpts do not cover the question, say so clearly rather than guessing.

"""

_SYSTEM_BOTH = """\
You are a carbon footprint advisor helping sustainability analysts at consumer goods companies \
understand their product footprints and GHG reporting requirements. You have access to the \
user's saved product analyses and relevant sections of the GHG Protocol Scope 3 Standard below.

Rules:
- For factual claims about product data: cite the product name and specific line item or total.
- For GHG Protocol guidance: cite the specific section from the standard.
- Do not fabricate numbers, emission factors, or guidance not present in the sources below.
- Do not recommend specific suppliers, investment decisions, or compliance actions.
- When asked about reduction strategies, ground your answer in both the product data (hotspots, \
  materials) and the GHG Protocol approach, but do not prescribe specific business decisions.
- If either source doesn't cover part of the question, say so clearly.

Methodology reminder:
- kg CO₂e = spend_usd × emission_factor (kgCO₂e/USD)
- Emission factors are from Open CEDA 2025 (USEEIO spend-based model), Scope 3 Category 1, cradle-to-gate.

"""

_NUMBER_RE = re.compile(r"\d+\.?\d*")
_N_RAG_RESULTS = 4

_QUERY_EXPANSION_SYSTEM = """\
You are a search query writer for a GHG Protocol Scope 3 Standard retrieval system.

Given a user question, rewrite it into 2-3 short retrieval queries using precise GHG Protocol \
terminology. The queries will be embedded and matched against the Scope 3 Standard document — \
use the language of the standard, not the user's phrasing.

Return only a JSON array of strings, nothing else.
Example output: ["significant scope 3 categories identification", \
"scope 3 category prioritization relevance threshold", \
"identifying material scope 3 emissions minimum boundaries"]
"""

_QUERIES_RE = re.compile(r'\[.*?\]', re.DOTALL)


@dataclass
class AdvisorResponse:
    content: str
    has_data_reference: bool
    citations: list[str] = field(default_factory=list)
    error: str | None = None


def _expand_queries(
    user_message: str,
    client: anthropic.Anthropic,
    session_id: str | None = None,
) -> list[str]:
    """Rewrite user question into 2-3 GHG Protocol retrieval queries.

    Falls back to the raw user message as a single query on any failure.
    """
    t0 = time.perf_counter()
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=150,
            system=_QUERY_EXPANSION_SYSTEM,
            messages=[{"role": "user", "content": user_message}],
        )
        latency = time.perf_counter() - t0
        log_llm_call(
            app_name="advisor",
            tool_name="query_expander",
            model=_MODEL,
            tokens_in=response.usage.input_tokens,
            tokens_out=response.usage.output_tokens,
            latency_seconds=latency,
            rag_used=False,
            session_id=session_id,
        )
        text = response.content[0].text if response.content else ""
        match = _QUERIES_RE.search(text)
        if match:
            import json
            queries = json.loads(match.group(0))
            if isinstance(queries, list) and queries:
                return [q for q in queries if isinstance(q, str)]
    except Exception as exc:
        log_llm_call(
            app_name="advisor",
            tool_name="query_expander",
            model=_MODEL,
            tokens_in=None,
            tokens_out=None,
            latency_seconds=time.perf_counter() - t0,
            rag_used=False,
            error=str(exc),
            session_id=session_id,
        )
    return [user_message]


def _build_rag_context(
    user_message: str,
    client: anthropic.Anthropic,
    session_id: str | None = None,
) -> tuple[str, list[str], list[str]]:
    """Expand query, retrieve GHG Protocol chunks, deduplicate.

    Returns (formatted_context, citations, expanded_queries).
    """
    queries = _expand_queries(user_message, client, session_id)

    seen: set[str] = set()
    chunks = []

    for query in queries:
        results = retrieve(query, n_results=_N_RAG_RESULTS)
        for r in results:
            if r.source_citation not in seen:
                seen.add(r.source_citation)
                chunks.append(r)

    context = "\n---\n".join(f"[{r.source_citation}]\n{r.text}\n" for r in chunks)
    citations = [r.source_citation for r in chunks]
    return context, citations, queries


def ask_advisor(
    user_message: str,
    conversation_history: list[dict],
    db_context: str,
    session_id: str | None = None,
) -> AdvisorResponse:
    """Send a user message and return an AdvisorResponse.

    Args:
        user_message: The latest user message.
        conversation_history: Prior turns as [{"role": "user"|"assistant", "content": str}, ...].
            Does NOT include the current user_message yet.
        db_context: Formatted DB context string from db.reader.build_llm_context().
        session_id: Optional UUID to group related calls in llm_logs.
    """
    client = anthropic.Anthropic()

    # Step 1: classify the question (router logs its own call)
    route = route_question(user_message, session_id=session_id)

    # Step 2: fetch RAG context when needed
    rag_context = ""
    citations: list[str] = []
    rag_queries: list[str] = []
    rag_unavailable_note = ""

    if route in ("guidance", "both"):
        try:
            rag_context, citations, rag_queries = _build_rag_context(
                user_message, client, session_id
            )
        except IndexNotBuiltError:
            rag_unavailable_note = (
                "\n\n(Note: GHG Protocol reference index is not available. "
                "Run `python -m rag.ingest` to build it.)"
            )
            route = "data"

    # Step 3: build system content based on route
    if route == "data":
        system_content = _SYSTEM_DATA + db_context
    elif route == "guidance":
        system_content = _SYSTEM_GUIDANCE + "## GHG Protocol Guidance\n\n" + rag_context
    else:  # both
        system_content = (
            _SYSTEM_BOTH
            + "## Saved Product Footprint Analyses\n\n"
            + db_context
            + "\n\n## GHG Protocol Guidance\n\n"
            + rag_context
        )

    messages = [*conversation_history, {"role": "user", "content": user_message}]

    # Step 4: main advisor LLM call
    t0 = time.perf_counter()
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=2048,
            system=[
                {
                    "type": "text",
                    "text": system_content,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=messages,
        )
    except anthropic.RateLimitError as exc:
        log_llm_call(
            app_name="advisor", tool_name="chat", model=_MODEL,
            tokens_in=None, tokens_out=None,
            latency_seconds=time.perf_counter() - t0,
            rag_used=bool(rag_queries),
            rag_queries="|".join(rag_queries) if rag_queries else None,
            error="RateLimitError",
            session_id=session_id,
        )
        return AdvisorResponse(
            content="", has_data_reference=False,
            error="Rate limit reached. Please wait a moment and try again.",
        )
    except anthropic.APIConnectionError as exc:
        log_llm_call(
            app_name="advisor", tool_name="chat", model=_MODEL,
            tokens_in=None, tokens_out=None,
            latency_seconds=time.perf_counter() - t0,
            rag_used=bool(rag_queries),
            rag_queries="|".join(rag_queries) if rag_queries else None,
            error="APIConnectionError",
            session_id=session_id,
        )
        return AdvisorResponse(
            content="", has_data_reference=False,
            error="Could not connect to the AI service. Check your internet connection and try again.",
        )
    except anthropic.APIStatusError as exc:
        log_llm_call(
            app_name="advisor", tool_name="chat", model=_MODEL,
            tokens_in=None, tokens_out=None,
            latency_seconds=time.perf_counter() - t0,
            rag_used=bool(rag_queries),
            rag_queries="|".join(rag_queries) if rag_queries else None,
            error=f"APIStatusError({exc.status_code})",
            session_id=session_id,
        )
        return AdvisorResponse(
            content="", has_data_reference=False,
            error=f"AI service error ({exc.status_code}): {exc.message}",
        )

    latency = time.perf_counter() - t0
    log_llm_call(
        app_name="advisor",
        tool_name="chat",
        model=_MODEL,
        tokens_in=response.usage.input_tokens,
        tokens_out=response.usage.output_tokens,
        latency_seconds=latency,
        rag_used=bool(rag_queries),
        rag_queries="|".join(rag_queries) if rag_queries else None,
        session_id=session_id,
    )

    text = response.content[0].text if response.content else ""
    text += rag_unavailable_note

    return AdvisorResponse(
        content=text,
        has_data_reference=bool(_NUMBER_RE.search(text)),
        citations=citations,
    )
