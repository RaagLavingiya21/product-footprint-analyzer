"""Tool 3: Analyze data gaps against material categories. (v2 — stub)"""

from __future__ import annotations

from gap_analyzer.models import CompanyProfile, ToolResult

_TOOL_NAME = "analyze_data_gaps"


def run(
    company_profile: CompanyProfile,
    previous_results: dict | None = None,
    session_id: str | None = None,
) -> ToolResult:
    return ToolResult(
        tool_name=_TOOL_NAME,
        content="Data gap analysis is not yet available in v1.",
        structured={},
        citations=[],
        error="not_implemented",
    )
