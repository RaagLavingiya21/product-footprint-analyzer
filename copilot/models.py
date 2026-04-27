"""Shared dataclasses for the Supplier Engagement Copilot workflows."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class EngagementCandidate:
    supplier_name: str          # from suppliers table, or component name if no match
    component: str | None
    material: str | None
    kg_co2e: float | None
    share_pct: float | None
    contact_found: bool         # True if matched in suppliers table
    contact_name: str | None
    contact_email: str | None
    existing_engagement_id: int | None
    engagement_status: str      # "new" or existing status from engagements table


@dataclass
class SuppliersListResult:
    candidates: list[EngagementCandidate]
    product_name: str
    error: str | None = None


@dataclass
class EmailDraft:
    to: str                     # contact_email or "" if contact not found
    subject: str
    body: str
    ghg_protocol_basis: str     # one-line citation shown in UI


@dataclass
class EmailDraftResult:
    draft: EmailDraft | None
    citations: list[str]        # RAG source_citation strings
    error: str | None = None


@dataclass
class ParsedResponse:
    response_type: str              # data_submission / question / pushback / no_response / partial
    data_provided: str
    issues_identified: list[str]    # wrong_facility / anomalous_values / dispute / missing_fields / wrong_contact
    completeness_score: str         # complete / partial / none
    raw_llm_output: str = ""


@dataclass
class ParseResponseResult:
    parsed: ParsedResponse | None
    error: str | None = None


@dataclass
class RoutingDecision:
    action: str                     # store_data / draft_follow_up / flag_for_human_review / escalate
    rationale: str
    ghg_protocol_citation: str | None


@dataclass
class RoutingResult:
    decision: RoutingDecision | None
    error: str | None = None
