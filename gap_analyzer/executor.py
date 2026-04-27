"""Executor: routes a plan step to the correct tool and enforces the loop guard."""

from __future__ import annotations

from gap_analyzer.models import CompanyProfile, PlanStep, ToolResult
from gap_analyzer.tools import (
    assess_reporting_requirements,
    assess_materiality,
    analyze_data_gaps,
    generate_recommendations,
)

_TOOL_REGISTRY = {
    "assess_reporting_requirements": assess_reporting_requirements.run,
    "assess_materiality": assess_materiality.run,
    "analyze_data_gaps": analyze_data_gaps.run,
    "generate_recommendations": generate_recommendations.run,
}

_MAX_CALLS_PER_TOOL = 3


def execute_step(
    step: PlanStep,
    company_profile: CompanyProfile,
    previous_results: dict[str, ToolResult],
    call_counts: dict[str, int],
    session_id: str | None = None,
) -> ToolResult:
    """Execute a single plan step.

    Args:
        step: The plan step to execute.
        company_profile: Company profile from the form.
        previous_results: Results from earlier tool calls (tool_name -> ToolResult).
        call_counts: Mutable dict tracking how many times each tool has been called.
            Caller is responsible for persisting this in session state.
    """
    tool_name = step.tool_name
    tool_fn = _TOOL_REGISTRY.get(tool_name)

    if tool_fn is None:
        return ToolResult(
            tool_name=tool_name,
            content=f"Unknown tool: `{tool_name}`.",
            structured={},
            citations=[],
            error=f"unknown_tool:{tool_name}",
        )

    count = call_counts.get(tool_name, 0)
    if count >= _MAX_CALLS_PER_TOOL:
        return ToolResult(
            tool_name=tool_name,
            content=(
                f"Tool `{tool_name}` has been called {count} times without completing. "
                "Stopping to prevent an infinite loop. Please restart the analysis."
            ),
            structured={},
            citations=[],
            error="infinite_loop_guard",
        )

    call_counts[tool_name] = count + 1

    return tool_fn(company_profile, previous_results, session_id=session_id)
