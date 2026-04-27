"""LLM-powered planner: takes a company profile and returns an ordered execution plan."""

from __future__ import annotations

import json
import re
import time

import anthropic
from dotenv import load_dotenv

from gap_analyzer.models import CompanyProfile, Plan, PlanStep
from observability.logger import log_llm_call

load_dotenv()

_MODEL = "claude-sonnet-4-6"

_SYSTEM_PROMPT = """\
You are a planning agent for a Scope 3 GHG emissions gap analyzer tool.

Given a company profile, produce an ordered execution plan for the gap analysis. \
The plan is executed by a separate executor that calls each tool in sequence, \
pausing at checkpoints for user confirmation.

Active tools (v1):
1. assess_reporting_requirements — determines which of the 15 Scope 3 categories \
apply to this company based on GHG Protocol guidance
2. assess_materiality — ranks applicable categories by estimated emissions significance \
and data availability; receives Tool 1 output
3. generate_recommendations — produces a prioritised action plan to close data gaps; \
receives Tool 1 and Tool 2 outputs

(Tool: analyze_data_gaps is reserved for v2 and must NOT be included in the plan.)

Rules:
- Always plan exactly three steps in this order: assess_reporting_requirements, \
assess_materiality, generate_recommendations. Each has a checkpoint after it.
- Provide a rationale for each step that is specific to this company's sector and products.
- Return your plan as a JSON block (inside ```json ... ```) with this schema:
  {"steps": [{"step_num": 1, "tool_name": "assess_reporting_requirements", \
"rationale": "...", "has_checkpoint_after": true}, ...]}
- After the JSON block, write 2–3 sentences of plain-English reasoning \
explaining why this plan makes sense for this specific company.
"""

_DEFAULT_PLAN = Plan(
    steps=[
        PlanStep(
            step_num=1,
            tool_name="assess_reporting_requirements",
            rationale="Establish which Scope 3 categories apply before further analysis.",
            has_checkpoint_after=True,
        ),
        PlanStep(
            step_num=2,
            tool_name="assess_materiality",
            rationale="Rank applicable categories by significance to focus effort on the highest-impact areas.",
            has_checkpoint_after=True,
        ),
        PlanStep(
            step_num=3,
            tool_name="generate_recommendations",
            rationale="Produce a prioritised action plan grounded in materiality scores and GHG Protocol data collection guidance.",
            has_checkpoint_after=True,
        ),
    ],
    raw_plan_text="Default plan (planner unavailable).",
)

_JSON_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)


def generate_plan(
    company_profile: CompanyProfile,
    session_id: str | None = None,
) -> Plan:
    """Call the LLM to produce an execution plan for this company profile."""
    client = anthropic.Anthropic()
    t0 = time.perf_counter()

    try:
        response = client.messages.create(
            model=_MODEL,
            max_tokens=1024,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[
                {"role": "user", "content": company_profile.as_text()}
            ],
        )
    except (anthropic.RateLimitError, anthropic.APIConnectionError, anthropic.APIStatusError) as exc:
        log_llm_call(
            app_name="gap_analyzer", tool_name="planner", model=_MODEL,
            tokens_in=None, tokens_out=None, latency_seconds=time.perf_counter() - t0,
            rag_used=False, error=str(exc), session_id=session_id,
        )
        return Plan(
            steps=_DEFAULT_PLAN.steps,
            raw_plan_text=f"Planner unavailable ({exc}). Running default plan.",
        )

    log_llm_call(
        app_name="gap_analyzer", tool_name="planner", model=_MODEL,
        tokens_in=response.usage.input_tokens, tokens_out=response.usage.output_tokens,
        latency_seconds=time.perf_counter() - t0,
        rag_used=False, session_id=session_id,
    )

    full_text = response.content[0].text if response.content else ""

    json_match = _JSON_RE.search(full_text)
    if not json_match:
        return Plan(steps=_DEFAULT_PLAN.steps, raw_plan_text=full_text or _DEFAULT_PLAN.raw_plan_text)

    try:
        data = json.loads(json_match.group(1))
        steps = [
            PlanStep(
                step_num=s["step_num"],
                tool_name=s["tool_name"],
                rationale=s["rationale"],
                has_checkpoint_after=s.get("has_checkpoint_after", True),
            )
            for s in data.get("steps", [])
        ]
        if not steps:
            steps = _DEFAULT_PLAN.steps
    except (json.JSONDecodeError, KeyError):
        steps = _DEFAULT_PLAN.steps

    # Strip the JSON block from display text; keep only prose reasoning
    prose = _JSON_RE.sub("", full_text).strip()
    return Plan(steps=steps, raw_plan_text=prose or full_text)
