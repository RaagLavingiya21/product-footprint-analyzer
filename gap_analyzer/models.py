"""Shared dataclasses for the Scope 3 Gap Analyzer agent."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CompanyProfile:
    name: str
    size: str        # e.g. "500–5,000 employees"
    sector: str      # e.g. "apparel manufacturing"
    geography: str   # e.g. "United States"
    products: str    # free-text description of products/services

    def as_text(self) -> str:
        return (
            f"Company name: {self.name}\n"
            f"Size: {self.size}\n"
            f"Sector: {self.sector}\n"
            f"Geography: {self.geography}\n"
            f"Products / services: {self.products}"
        )


@dataclass
class PlanStep:
    step_num: int
    tool_name: str          # one of the four registered tool names
    rationale: str
    has_checkpoint_after: bool


@dataclass
class Plan:
    steps: list[PlanStep]
    raw_plan_text: str      # LLM reasoning displayed in the UI


@dataclass
class ToolResult:
    tool_name: str
    content: str            # markdown shown to the user at checkpoint
    structured: dict        # machine-readable payload forwarded to next tool
    citations: list[str]    # source_citation strings from RAG
    error: str | None = None
