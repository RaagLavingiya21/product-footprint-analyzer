"""Workflow 2: Draft a supplier data request email grounded in GHG Protocol guidance.

Makes one LLM call with RAG-retrieved context from the GHG Protocol Scope 3 Standard.
Returns an EmailDraft the user can review and edit before sending.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

import anthropic
from dotenv import load_dotenv

from copilot.models import EmailDraft, EmailDraftResult, EngagementCandidate
from db.store import DB_PATH
from observability.logger import log_llm_call
from rag.retriever import IndexNotBuiltError, retrieve

load_dotenv()

_MODEL = "claude-sonnet-4-6"
_N_RAG_RESULTS = 4

_RAG_QUERIES = [
    "scope 3 supplier primary data collection request methodology",
    "purchased goods services supplier emission factor activity data",
    "scope 3 data quality primary secondary supplier engagement",
]

_SYSTEM_PROMPT = """\
You are a GHG Protocol specialist helping a sustainability analyst draft a professional \
supplier data request email for Scope 3 Category 1 (Purchased Goods & Services) inventory.

Relevant GHG Protocol guidance is provided below. Use it to ground the email.

Rules:
- Cite the specific GHG Protocol section that requires or recommends primary supplier data.
- Specify the exact data fields the supplier should provide: activity data (quantity, units), \
  emission factor or product carbon footprint, methodology used, system boundary.
- Set a 14-day response deadline.
- Professional, concise tone — no jargon the supplier won't understand.
- Do NOT fabricate guidance not present in the excerpts below.

Format your response in two parts:

PART 1 — Email body (plain text only, no subject line, no JSON):
Write the complete email body. Start with a greeting. End with a signature placeholder "[Your Name]".

PART 2 — JSON block:
```json
{"subject": "...", "ghg_protocol_basis": "one sentence citing the GHG Protocol section that supports this data request"}
```

GHG Protocol guidance:
"""

_JSON_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)


def _build_rag_context() -> tuple[str, list[str], list[str]]:
    """Return (formatted_context, citations, queries_used)."""
    seen: set[str] = set()
    chunks = []

    for query in _RAG_QUERIES:
        try:
            results = retrieve(query, n_results=_N_RAG_RESULTS)
        except IndexNotBuiltError:
            raise
        for r in results:
            if r.source_citation not in seen:
                seen.add(r.source_citation)
                chunks.append(r)

    context = "\n---\n".join(f"[{r.source_citation}]\n{r.text}\n" for r in chunks)
    citations = [r.source_citation for r in chunks]
    return context, citations, _RAG_QUERIES


def _user_message(candidate: EngagementCandidate, product_name: str) -> str:
    lines = [
        f"Draft a supplier data request email for the following engagement:\n",
        f"- Product: {product_name}",
        f"- Component: {candidate.component or 'N/A'}",
        f"- Material: {candidate.material or 'N/A'}",
        f"- Estimated footprint contribution: "
        f"{candidate.kg_co2e:.4f} kg CO₂e ({candidate.share_pct:.1f}% of product total)"
        if candidate.kg_co2e is not None else "- Estimated footprint contribution: unknown",
    ]

    if candidate.contact_found and candidate.contact_name:
        lines.append(f"- Supplier contact: {candidate.contact_name} at {candidate.supplier_name}")
    else:
        lines.append(
            f"- Supplier: {candidate.supplier_name} "
            f"(contact information not found — leave greeting generic)"
        )

    return "\n".join(lines)


def run(
    candidate: EngagementCandidate,
    product_name: str,
    session_id: str | None = None,
    db_path: Path = DB_PATH,
) -> EmailDraftResult:
    """Draft a GHG Protocol-grounded data request email for one supplier engagement candidate."""
    try:
        rag_context, citations, rag_queries = _build_rag_context()
    except IndexNotBuiltError:
        return EmailDraftResult(
            draft=None,
            citations=[],
            error="RAG index not built. Run `python -m rag.ingest` first.",
        )

    system_content = _SYSTEM_PROMPT + rag_context
    client = anthropic.Anthropic()
    t0 = time.perf_counter()

    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=1500,
            system=[{"type": "text", "text": system_content, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": _user_message(candidate, product_name)}],
        )
    except anthropic.RateLimitError:
        log_llm_call(
            app_name="copilot", tool_name="draft_email", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=True, rag_queries="|".join(rag_queries),
            error="RateLimitError", session_id=session_id,
        )
        return EmailDraftResult(
            draft=None, citations=citations,
            error="LLM call failed: rate limit reached. Please wait and try again.",
        )
    except anthropic.APIConnectionError:
        log_llm_call(
            app_name="copilot", tool_name="draft_email", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=True, rag_queries="|".join(rag_queries),
            error="APIConnectionError", session_id=session_id,
        )
        return EmailDraftResult(
            draft=None, citations=citations,
            error="LLM call failed: could not connect to the AI service.",
        )
    except anthropic.APIStatusError as exc:
        log_llm_call(
            app_name="copilot", tool_name="draft_email", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=True, rag_queries="|".join(rag_queries),
            error=f"APIStatusError({exc.status_code})", session_id=session_id,
        )
        return EmailDraftResult(
            draft=None, citations=citations,
            error=f"LLM call failed ({exc.status_code}): {exc.message}",
        )

    latency = time.perf_counter() - t0
    log_llm_call(
        app_name="copilot", tool_name="draft_email", model=_MODEL,
        tokens_in=response.usage.input_tokens, tokens_out=response.usage.output_tokens,
        latency_seconds=latency,
        rag_used=True, rag_queries="|".join(rag_queries), session_id=session_id,
    )

    full_text = response.content[0].text if response.content else ""

    # Extract JSON block for subject + ghg_protocol_basis
    subject = f"Scope 3 Emissions Data Request — {candidate.component or candidate.supplier_name}"
    ghg_protocol_basis = "GHG Protocol Corporate Value Chain (Scope 3) Standard, Chapter 7."

    json_match = _JSON_RE.search(full_text)
    if json_match:
        try:
            meta = json.loads(json_match.group(1))
            subject = meta.get("subject", subject)
            ghg_protocol_basis = meta.get("ghg_protocol_basis", ghg_protocol_basis)
        except json.JSONDecodeError:
            pass

    # Body is everything before the JSON block
    body = _JSON_RE.sub("", full_text).strip()
    # Strip PART labels if the model included them
    body = re.sub(r"^PART\s+1\s*[—–-]\s*Email body[^\n]*\n?", "", body, flags=re.IGNORECASE).strip()

    to = candidate.contact_email or ""

    return EmailDraftResult(
        draft=EmailDraft(to=to, subject=subject, body=body, ghg_protocol_basis=ghg_protocol_basis),
        citations=citations,
    )
