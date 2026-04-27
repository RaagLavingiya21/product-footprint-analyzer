"""Workflow 4: Route a parsed supplier response to the correct next action.

Step 1 — pure Python rules determine the action (deterministic, no LLM).
Step 2 — lightweight LLM call generates a one-sentence rationale and GHG Protocol citation.
"""

from __future__ import annotations

import json
import re
import time

import anthropic
from dotenv import load_dotenv

from copilot.models import ParsedResponse, RoutingDecision, RoutingResult
from observability.logger import log_llm_call

load_dotenv()

_MODEL = "claude-sonnet-4-6"

_SYSTEM_RATIONALE = """\
You are a GHG Protocol specialist reviewing a supplier engagement routing decision.

Given the routing action and the parsed supplier response summary, write:
1. A single sentence explaining WHY this action is the correct next step.
2. The most relevant GHG Protocol Scope 3 Standard section citation (if applicable), \
   or null if no specific section applies.

Return ONLY this JSON object:
{"rationale": "one sentence", "ghg_protocol_citation": "Section X.Y: Title" or null}
"""

_JSON_RE = re.compile(r'\{[^{}]*"rationale"[^{}]*\}', re.DOTALL)

# Actions
STORE_DATA = "store_data"
DRAFT_FOLLOW_UP = "draft_follow_up"
FLAG_FOR_HUMAN = "flag_for_human_review"
ESCALATE = "escalate"


def _determine_action(parsed: ParsedResponse, days_since_contact: int) -> str:
    """Pure rule engine — maps parsed response fields to a routing action."""
    issues = set(parsed.issues_identified)

    # Issues that always escalate to human review
    if "anomalous_values" in issues:
        return FLAG_FOR_HUMAN
    if "dispute" in issues:
        return FLAG_FOR_HUMAN
    if "wrong_contact" in issues:
        return FLAG_FOR_HUMAN

    # Issues that trigger automated follow-up
    if "wrong_facility" in issues:
        return DRAFT_FOLLOW_UP

    # Clean complete submission
    if parsed.completeness_score == "complete" and not issues:
        return STORE_DATA

    # No response within threshold
    if parsed.response_type == "no_response" and days_since_contact >= 14:
        return DRAFT_FOLLOW_UP

    # Partial or missing data
    if parsed.completeness_score in ("partial", "none"):
        return DRAFT_FOLLOW_UP

    # Supplier asking questions — send follow-up with answers
    if parsed.response_type == "question":
        return DRAFT_FOLLOW_UP

    # Ambiguous — default to human review so nothing is lost
    return FLAG_FOR_HUMAN


def _build_rationale_prompt(
    action: str,
    parsed: ParsedResponse,
    supplier_name: str,
    component: str | None,
) -> str:
    return (
        f"Routing action decided: {action}\n\n"
        f"Supplier: {supplier_name}\n"
        f"Component: {component or 'N/A'}\n"
        f"Response type: {parsed.response_type}\n"
        f"Completeness: {parsed.completeness_score}\n"
        f"Issues identified: {', '.join(parsed.issues_identified) or 'none'}\n"
        f"Data provided: {parsed.data_provided}\n\n"
        "Write the rationale and citation JSON."
    )


def run(
    parsed: ParsedResponse,
    supplier_name: str,
    component: str | None = None,
    days_since_contact: int = 0,
    session_id: str | None = None,
) -> RoutingResult:
    """Determine routing action and generate LLM rationale.

    Args:
        parsed:              Structured output from parse_response.run().
        supplier_name:       Name of the supplier for context.
        component:           Component/material for context.
        days_since_contact:  Days elapsed since the last email was sent (used for no-response rule).
        session_id:          Optional UUID for observability grouping.
    """
    action = _determine_action(parsed, days_since_contact)

    # LLM call for rationale and citation
    client = anthropic.Anthropic()
    t0 = time.perf_counter()

    fallback_rationale = (
        f"Routing to '{action}' based on response type '{parsed.response_type}' "
        f"and completeness '{parsed.completeness_score}'."
    )

    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=200,
            system=_SYSTEM_RATIONALE,
            messages=[{
                "role": "user",
                "content": _build_rationale_prompt(action, parsed, supplier_name, component),
            }],
        )
    except anthropic.APIError as exc:
        log_llm_call(
            app_name="copilot", tool_name="exception_router", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=False, error=str(exc), session_id=session_id,
        )
        return RoutingResult(
            decision=RoutingDecision(
                action=action,
                rationale=fallback_rationale,
                ghg_protocol_citation=None,
            )
        )

    latency = time.perf_counter() - t0
    log_llm_call(
        app_name="copilot", tool_name="exception_router", model=_MODEL,
        tokens_in=response.usage.input_tokens, tokens_out=response.usage.output_tokens,
        latency_seconds=latency, rag_used=False, session_id=session_id,
    )

    text = response.content[0].text if response.content else ""
    rationale = fallback_rationale
    citation: str | None = None

    match = _JSON_RE.search(text)
    if match:
        try:
            data = json.loads(match.group(0))
            rationale = data.get("rationale", fallback_rationale)
            citation = data.get("ghg_protocol_citation") or None
        except json.JSONDecodeError:
            pass

    return RoutingResult(
        decision=RoutingDecision(
            action=action,
            rationale=rationale,
            ghg_protocol_citation=citation,
        )
    )
