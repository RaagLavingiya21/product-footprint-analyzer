"""Lightweight LLM router: classifies user questions into data / guidance / both."""

from __future__ import annotations

import re
import time

import anthropic
from dotenv import load_dotenv

from observability.logger import log_llm_call

load_dotenv()

_MODEL = "claude-sonnet-4-6"

_ROUTER_SYSTEM = """\
Classify the user question into exactly one of three routing buckets.
Return only a JSON object — no explanation, no preamble.

data     — answerable from saved product footprint data alone
           Examples:
             "What is my highest emitting product?"
             "What are the hotspots for my cotton t-shirt?"
             "Compare emissions between product X and Y"
             "Which supplier has the highest emissions?"

guidance — answerable from GHG Protocol standard alone (no product data needed)
           Examples:
             "What are the reporting requirements for Scope 3 Category 4?"
             "What does the GHG Protocol say about LCA boundaries?"
             "What does the GHG Protocol say about materiality assessment?"

both     — requires product data AND GHG Protocol guidance
           Examples:
             "How do I reduce emissions for product X?"
             "How do I report this product's emissions?"
             "What data do I need for product X's emission reporting?"
             "How do I engage suppliers to reduce emissions for product X?"

Return exactly one of:
{"route": "data"}
{"route": "guidance"}
{"route": "both"}
"""

_ROUTE_RE = re.compile(r'"route"\s*:\s*"(data|guidance|both)"')


def route_question(
    user_message: str,
    session_id: str | None = None,
) -> str:
    """Classify a user question and return 'data', 'guidance', or 'both'.

    Defaults to 'both' on any API error or parse failure so all context is
    available and no information is lost.
    """
    client = anthropic.Anthropic()
    t0 = time.perf_counter()
    error_msg: str | None = None
    tokens_in = tokens_out = None

    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=50,
            system=_ROUTER_SYSTEM,
            messages=[{"role": "user", "content": user_message}],
        )
        tokens_in = response.usage.input_tokens
        tokens_out = response.usage.output_tokens
    except anthropic.APIError as exc:
        error_msg = str(exc)
        log_llm_call(
            app_name="advisor",
            tool_name="router",
            model=_MODEL,
            tokens_in=None,
            tokens_out=None,
            latency_seconds=time.perf_counter() - t0,
            rag_used=False,
            error=error_msg,
            session_id=session_id,
        )
        return "both"

    latency = time.perf_counter() - t0
    text = response.content[0].text if response.content else ""
    match = _ROUTE_RE.search(text)
    route = match.group(1) if match else "both"

    log_llm_call(
        app_name="advisor",
        tool_name="router",
        model=_MODEL,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        latency_seconds=latency,
        rag_used=False,
        session_id=session_id,
    )
    return route
