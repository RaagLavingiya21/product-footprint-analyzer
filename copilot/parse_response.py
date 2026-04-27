"""Workflow 3: Parse a supplier's email response into a structured object.

Two-call pattern:
  Call 1 — parse response into structured JSON (max_tokens=800).
  Call 2 — focused extraction fallback if JSON is absent or malformed (max_tokens=400).
"""

from __future__ import annotations

import json
import re
import time

import anthropic
from dotenv import load_dotenv

from copilot.models import ParsedResponse, ParseResponseResult
from observability.logger import log_llm_call

load_dotenv()

_MODEL = "claude-sonnet-4-6"

_VALID_RESPONSE_TYPES = {"data_submission", "question", "pushback", "no_response", "partial"}
_VALID_ISSUES = {"wrong_facility", "anomalous_values", "dispute", "missing_fields", "wrong_contact"}
_VALID_COMPLETENESS = {"complete", "partial", "none"}

_SYSTEM_PARSE = """\
You are a supplier engagement analyst parsing a supplier's email response to a Scope 3 \
emissions data request.

Analyse the email and classify it according to these definitions:

response_type:
  data_submission — supplier provided emission data (even if incomplete)
  question        — supplier is asking clarifying questions before providing data
  pushback        — supplier disputes the request, the methodology, or declines to share
  no_response     — email is empty, out-of-office, or clearly not a substantive reply
  partial         — supplier provided some data but explicitly acknowledges gaps

issues_identified (list, may be empty):
  wrong_facility   — data is for a different facility or region than requested
  anomalous_values — numbers are implausibly high or low for the material/component
  dispute          — supplier contests your emission factor estimates or methodology
  missing_fields   — required data fields (activity data, EF, methodology, boundary) are absent
  wrong_contact    — supplier redirected to a different contact without providing data

completeness_score:
  complete — all required fields present: activity data, emission factor, methodology, boundary
  partial  — some fields present, some missing
  none     — no usable emission data provided

Rules:
- Be conservative: if in doubt about completeness, score lower.
- data_provided should summarise in plain English what the email actually contains.
- Do NOT fabricate data fields not present in the email.

REQUIRED: End your response with this JSON block — do not omit it:
```json
{
  "response_type": "...",
  "data_provided": "...",
  "issues_identified": [],
  "completeness_score": "..."
}
```
"""

_EXTRACT_PROMPT = """\
From the analysis above, extract the structured classification into this JSON format. \
Return ONLY the JSON block, nothing else:

```json
{
  "response_type": "data_submission or question or pushback or no_response or partial",
  "data_provided": "one sentence describing what the email contains",
  "issues_identified": ["list of issue codes or empty array"],
  "completeness_score": "complete or partial or none"
}
```
"""

_JSON_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)


def _parse_structured(text: str) -> ParsedResponse | None:
    """Extract and validate ParsedResponse from an LLM response containing a JSON block."""
    json_match = _JSON_RE.search(text)
    if not json_match:
        return None
    try:
        data = json.loads(json_match.group(1))
    except json.JSONDecodeError:
        return None

    response_type = data.get("response_type", "")
    completeness = data.get("completeness_score", "")
    issues = [i for i in data.get("issues_identified", []) if isinstance(i, str)]

    if response_type not in _VALID_RESPONSE_TYPES:
        response_type = "partial"
    if completeness not in _VALID_COMPLETENESS:
        completeness = "none"

    return ParsedResponse(
        response_type=response_type,
        data_provided=str(data.get("data_provided", "")),
        issues_identified=issues,
        completeness_score=completeness,
        raw_llm_output=text,
    )


def _fallback_extract(
    client: anthropic.Anthropic,
    prior_text: str,
    session_id: str | None,
) -> ParsedResponse | None:
    """Call 2: focused extraction if Call 1 produced no valid JSON."""
    t0 = time.perf_counter()
    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=400,
            messages=[{"role": "user", "content": prior_text + "\n\n" + _EXTRACT_PROMPT}],
        )
        text = response.content[0].text if response.content else ""
        log_llm_call(
            app_name="copilot", tool_name="parse_response_extract", model=_MODEL,
            tokens_in=response.usage.input_tokens, tokens_out=response.usage.output_tokens,
            latency_seconds=time.perf_counter() - t0,
            rag_used=False, session_id=session_id,
        )
        return _parse_structured(text)
    except Exception as exc:
        log_llm_call(
            app_name="copilot", tool_name="parse_response_extract", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=False, error=str(exc), session_id=session_id,
        )
        return None


def run(
    response_text: str,
    supplier_name: str,
    component: str | None = None,
    session_id: str | None = None,
) -> ParseResponseResult:
    """Parse a supplier email response into a structured ParsedResponse.

    Args:
        response_text: Full text of the supplier's email reply.
        supplier_name: Name of the supplier (for context in the prompt).
        component:     Component/material that was being requested (for context).
        session_id:    Optional UUID for observability grouping.
    """
    if not response_text.strip():
        return ParseResponseResult(
            parsed=ParsedResponse(
                response_type="no_response",
                data_provided="No response text provided.",
                issues_identified=[],
                completeness_score="none",
            )
        )

    context_header = (
        f"Supplier: {supplier_name}\n"
        + (f"Component requested: {component}\n" if component else "")
        + "\nSupplier email response:\n"
        + response_text
    )

    client = anthropic.Anthropic()
    t0 = time.perf_counter()

    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=800,
            system=_SYSTEM_PARSE,
            messages=[{"role": "user", "content": context_header}],
        )
    except anthropic.RateLimitError:
        log_llm_call(
            app_name="copilot", tool_name="parse_response", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=False, error="RateLimitError", session_id=session_id,
        )
        return ParseResponseResult(
            parsed=None,
            error="LLM call failed: rate limit reached. Please wait and try again.",
        )
    except anthropic.APIConnectionError:
        log_llm_call(
            app_name="copilot", tool_name="parse_response", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=False, error="APIConnectionError", session_id=session_id,
        )
        return ParseResponseResult(
            parsed=None,
            error="LLM call failed: could not connect to the AI service.",
        )
    except anthropic.APIStatusError as exc:
        log_llm_call(
            app_name="copilot", tool_name="parse_response", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=False, error=f"APIStatusError({exc.status_code})", session_id=session_id,
        )
        return ParseResponseResult(
            parsed=None,
            error=f"LLM call failed ({exc.status_code}): {exc.message}",
        )

    latency = time.perf_counter() - t0
    full_text = response.content[0].text if response.content else ""

    log_llm_call(
        app_name="copilot", tool_name="parse_response", model=_MODEL,
        tokens_in=response.usage.input_tokens, tokens_out=response.usage.output_tokens,
        latency_seconds=latency, rag_used=False, session_id=session_id,
    )

    parsed = _parse_structured(full_text)

    if parsed is None:
        # Call 2: focused extraction fallback
        parsed = _fallback_extract(client, full_text, session_id)

    if parsed is None:
        return ParseResponseResult(
            parsed=None,
            error=(
                "Could not determine the exception type from the supplier response. "
                "Please review the response manually and classify it."
            ),
        )

    return ParseResponseResult(parsed=parsed)
