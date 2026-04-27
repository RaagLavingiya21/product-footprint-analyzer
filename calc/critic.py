"""Critic-reviser loop: validates a FootprintResult and flags or corrects issues.

Three checks (no LLM calls — pure Python):
  1. Math integrity   — total_kg_co2e must equal sum of matched line items (±0.1%)
  2. Anomalous share  — any line item with share_pct > 95% is flagged for review
  3. Missing emission — any row with spend_usd > 0 but no kg_co2e is flagged and noted
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from calc.footprint import FootprintResult

_ANOMALOUS_SHARE_THRESHOLD = 95.0
_MATH_TOLERANCE_REL = 0.001  # 0.1%


@dataclass
class CriticFinding:
    check: str       # "math_mismatch" | "anomalous_share" | "missing_emission"
    severity: str    # "corrected" | "warning"
    message: str
    row_index: int | None = None


@dataclass
class CriticReport:
    findings: list[CriticFinding] = field(default_factory=list)
    total_was_corrected: bool = False
    original_total: float | None = None

    @property
    def has_findings(self) -> bool:
        return bool(self.findings)

    @property
    def correction_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "corrected")

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "warning")


def run_critic(result: FootprintResult) -> tuple[FootprintResult, CriticReport]:
    """Validate and auto-correct a FootprintResult.

    Mutates result in-place when a math error is detected and corrected.
    Returns the (possibly corrected) result and a CriticReport.
    """
    report = CriticReport()

    # ── Check 1: math integrity ───────────────────────────────────────────────
    computed_sum = sum(li.kg_co2e for li in result.line_items if li.is_matched)
    if result.total_kg_co2e > 0 and not math.isclose(
        computed_sum, result.total_kg_co2e, rel_tol=_MATH_TOLERANCE_REL
    ):
        original = result.total_kg_co2e
        result.total_kg_co2e = computed_sum
        for li in result.line_items:
            if li.is_matched and computed_sum > 0:
                li.share_pct = (li.kg_co2e / computed_sum) * 100
        report.total_was_corrected = True
        report.original_total = original
        report.findings.append(CriticFinding(
            check="math_mismatch",
            severity="corrected",
            message=(
                f"Total recalculated: {original:.6f} → {computed_sum:.6f} kg CO₂e "
                "(sum of line items differed from reported total by >0.1%)."
            ),
        ))

    # ── Check 2: anomalous share ──────────────────────────────────────────────
    for li in result.line_items:
        if li.is_matched and li.share_pct > _ANOMALOUS_SHARE_THRESHOLD:
            report.findings.append(CriticFinding(
                check="anomalous_share",
                severity="warning",
                message=(
                    f"This line item ({li.component or '—'} / {li.material or '—'}) "
                    f"represents {li.share_pct:.1f}% of total — verify this is correct."
                ),
                row_index=li.row_index,
            ))

    # ── Check 3: spend with no emission ──────────────────────────────────────
    for li in result.line_items:
        if li.spend_usd and li.spend_usd > 0 and (not li.is_matched or li.kg_co2e == 0):
            report.findings.append(CriticFinding(
                check="missing_emission",
                severity="warning",
                message=(
                    f"Row {li.row_index + 1} ({li.component or '—'} / {li.material or '—'}): "
                    f"spend_usd={li.spend_usd:.2f} but no emission calculated — "
                    "excluded from total."
                ),
                row_index=li.row_index,
            ))

    return result, report
