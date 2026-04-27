"""Product Carbon Footprint Analyzer — Streamlit UI.

Five-phase state machine with three human-in-the-loop checkpoints:
  upload      → file upload
  bom_review  → Checkpoint 1: review cleaned BOM before factor matching
  ef_review   → Checkpoint 2: review matched factors before calculating
  calc_review → Checkpoint 3: critic findings + results, approve or flag concern
  saved       → confirmation screen
"""

import uuid
from datetime import date

import pandas as pd
import streamlit as st

from calc.critic import CriticReport, run_critic
from calc.footprint import FootprintResult, calculate_footprint
from db.store import save_analysis
from factors.ef_lookup import EFMatch, lookup_ef
from parsing.bom_parser import ParsedBOM, parse_bom_csv

st.set_page_config(
    page_title="Product Carbon Footprint Analyzer",
    page_icon="🌿",
    layout="wide",
)

# ── Constants ─────────────────────────────────────────────────────────────────

_FLAG_SEVERITY_COLOR = {"error": "🔴", "warning": "🟡"}
_CONFIDENCE_LOW_THRESHOLD = 80

# ── Session state ─────────────────────────────────────────────────────────────

_STATE_DEFAULTS: dict = {
    "fp_phase": "upload",
    "fp_file_key": None,          # uploaded filename; changing it resets pipeline
    "fp_bom": None,               # ParsedBOM
    "fp_ef_matches": None,        # list[EFMatch | None]
    "fp_ef_warnings": [],         # list[str]
    "fp_result": None,            # FootprintResult
    "fp_critic_report": None,     # CriticReport
    "fp_session_id": None,
    "fp_flag_mode": False,        # True once user clicks "Flag a concern"
    "fp_saved_product_id": None,
}

for _k, _v in _STATE_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

if st.session_state["fp_session_id"] is None:
    st.session_state["fp_session_id"] = str(uuid.uuid4())


def _reset() -> None:
    for k, v in _STATE_DEFAULTS.items():
        st.session_state[k] = v
    st.session_state["fp_session_id"] = str(uuid.uuid4())
    st.rerun()


# ── Pipeline stage functions ──────────────────────────────────────────────────

def _parse_bom_stage(uploaded) -> ParsedBOM:
    raw_bytes = uploaded.read()
    product_name = uploaded.name.removesuffix(".csv").replace("_", " ").title()
    return parse_bom_csv(raw_bytes, product_name)


def _ef_lookup_stage(bom: ParsedBOM) -> tuple[list[EFMatch | None], list[str]]:
    ef_matches: list[EFMatch | None] = []
    ef_warnings: list[str] = []
    for row in bom.rows:
        if row.material:
            ef = lookup_ef(row.material, row.country_of_origin)
            ef_matches.append(ef)
            if ef.is_no_match:
                ef_warnings.append(
                    f"Row {row.row_index + 1} ({row.material!r}): no emission factor match. "
                    f"Suggestions: {', '.join(ef.suggested_alternatives) or 'none'}."
                )
            elif ef.is_low_confidence:
                ef_warnings.append(
                    f"Row {row.row_index + 1} ({row.material!r}): low-confidence match to "
                    f"'{ef.sector_name}' ({ef.confidence_score:.0f}%). "
                    f"Suggestions: {', '.join(ef.suggested_alternatives) or 'none'}."
                )
        else:
            ef_matches.append(None)
    return ef_matches, ef_warnings


# ── DataFrame helpers ─────────────────────────────────────────────────────────

def _bom_dataframe(bom: ParsedBOM) -> pd.DataFrame:
    flagged = bom.flagged_row_indices
    rows = []
    for row in bom.rows:
        rows.append({
            "Row": row.row_index + 1,
            "Component": row.component or "—",
            "Material": row.material or "—",
            "Quantity": row.quantity,
            "Spend (USD)": row.spend_usd,
            "Weight (kg)": row.weight_kg,
            "Supplier": row.supplier or "—",
            "Country": row.country_of_origin or "—",
            "Flags": "⚠️" if row.row_index in flagged else "✅",
        })
    return pd.DataFrame(rows)


def _flags_dataframe(bom: ParsedBOM) -> pd.DataFrame:
    rows = []
    for flag in bom.all_flags:
        bom_row = next((r for r in bom.rows if r.row_index == flag.row_index), None)
        rows.append({
            "Row": flag.row_index + 1,
            "Component": bom_row.component if bom_row else "—",
            "Material": bom_row.material if bom_row else "—",
            "Field": flag.field,
            "Issue": flag.message,
            "Severity": _FLAG_SEVERITY_COLOR.get(flag.severity, "") + " " + flag.severity,
        })
    return pd.DataFrame(rows)


def _ef_matches_dataframe(bom: ParsedBOM, ef_matches: list[EFMatch | None]) -> pd.DataFrame:
    rows = []
    for row, ef in zip(bom.rows, ef_matches):
        if ef is None:
            rows.append({
                "Row": row.row_index + 1,
                "Component": row.component or "—",
                "Material": row.material or "—",
                "Matched Sector": "—",
                "EF (kg CO₂e/USD)": None,
                "Confidence (%)": None,
                "Source": "—",
                "Status": "⚫ No material",
            })
        elif ef.is_no_match:
            rows.append({
                "Row": row.row_index + 1,
                "Component": row.component or "—",
                "Material": row.material or "—",
                "Matched Sector": "No match",
                "EF (kg CO₂e/USD)": None,
                "Confidence (%)": None,
                "Source": "—",
                "Status": "🚫 Unmatched",
            })
        else:
            rows.append({
                "Row": row.row_index + 1,
                "Component": row.component or "—",
                "Material": row.material or "—",
                "Matched Sector": ef.sector_name,
                "EF (kg CO₂e/USD)": round(ef.ef_kg_co2e_per_usd, 4),
                "Confidence (%)": round(ef.confidence_score, 0),
                "Source": ef.source_citation,
                "Status": "⚠️ Low confidence" if ef.is_low_confidence else "✅ Matched",
            })
    return pd.DataFrame(rows)


def _results_dataframe(result: FootprintResult) -> pd.DataFrame:
    rows = []
    for li in result.line_items:
        rows.append({
            "Component": li.component or "—",
            "Material": li.material or "—",
            "Spend (USD)": li.spend_usd,
            "Matched Sector": li.sector_name or "—",
            "EF (kg CO₂e/USD)": round(li.ef_kg_co2e_per_usd, 4) if li.ef_kg_co2e_per_usd else None,
            "EF Source": li.ef_source or "—",
            "EF Confidence (%)": round(li.ef_confidence, 0) if li.ef_confidence else None,
            "kg CO₂e": round(li.kg_co2e, 4) if li.is_matched else None,
            "Share (%)": round(li.share_pct, 1) if li.is_matched else None,
            "Status": (
                "✅ Matched" if li.is_matched and not li.is_low_confidence
                else ("⚠️ Low Confidence" if li.is_low_confidence else "🚫 Unmatched")
            ),
        })
    return pd.DataFrame(rows)


def _download_csv(result: FootprintResult) -> bytes:
    return _results_dataframe(result).to_csv(index=False).encode("utf-8")


# ── Page header ───────────────────────────────────────────────────────────────

st.title("🌿 Product Carbon Footprint Analyzer")
st.caption(
    "Estimate Scope 3 Category 1 (Purchased Goods & Services) emissions from a Bill of Materials. "
    "Emission factors sourced from Open CEDA 2025 (spend-based, kg CO₂e/USD)."
)

_PHASE_LABELS = {
    "upload": "1 — Upload",
    "bom_review": "2 — Review BOM",
    "ef_review": "3 — Review factors",
    "calc_review": "4 — Review results",
    "saved": "5 — Saved",
}

with st.sidebar:
    st.markdown(f"**Step:** {_PHASE_LABELS.get(st.session_state['fp_phase'], '—')}")
    st.divider()
    if st.button("↩ Start over"):
        _reset()

st.divider()

phase = st.session_state["fp_phase"]


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE: upload
# ═══════════════════════════════════════════════════════════════════════════════

if phase == "upload":
    uploaded = st.file_uploader(
        "Upload your Bill of Materials (CSV)",
        type=["csv"],
        help="Required columns: component, material, quantity, spend_usd. "
             "Optional: weight_kg, supplier, country_of_origin.",
    )

    if not uploaded:
        with st.expander("Expected CSV format", expanded=False):
            st.markdown(
                "| component | material | quantity | spend_usd | weight_kg | supplier | country_of_origin |\n"
                "|-----------|----------|----------|-----------|-----------|----------|-------------------|\n"
                "| body | cotton fabric | 1 | 10 | 0.15 | ABCD | India |"
            )
        st.stop()

    if uploaded.name != st.session_state["fp_file_key"]:
        with st.spinner("Parsing BOM…"):
            bom = _parse_bom_stage(uploaded)

        if bom.file_errors:
            for err in bom.file_errors:
                st.error(f"**File error:** {err}")
            st.stop()

        st.session_state["fp_bom"] = bom
        st.session_state["fp_file_key"] = uploaded.name
        st.session_state["fp_phase"] = "bom_review"
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE: bom_review — Checkpoint 1
# ═══════════════════════════════════════════════════════════════════════════════

elif phase == "bom_review":
    bom: ParsedBOM = st.session_state["fp_bom"]

    st.subheader("Checkpoint 1 — Review cleaned BOM")
    st.caption(
        f"**{bom.product_name}** · {len(bom.rows)} rows · "
        f"{len(bom.flagged_row_indices)} flagged"
    )

    st.dataframe(_bom_dataframe(bom), use_container_width=True, hide_index=True)

    if bom.all_flags:
        with st.expander(
            f"⚠️ {len(bom.all_flags)} parsing flag(s) — review before continuing",
            expanded=True,
        ):
            st.dataframe(_flags_dataframe(bom), use_container_width=True, hide_index=True)

    st.divider()
    col_confirm, col_cancel = st.columns([2, 1])

    with col_cancel:
        if st.button("← Upload a different file"):
            _reset()

    with col_confirm:
        if st.button("Confirm BOM — run factor matching →", type="primary"):
            with st.spinner("Looking up emission factors…"):
                ef_matches, ef_warnings = _ef_lookup_stage(bom)
            st.session_state["fp_ef_matches"] = ef_matches
            st.session_state["fp_ef_warnings"] = ef_warnings
            st.session_state["fp_phase"] = "ef_review"
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE: ef_review — Checkpoint 2
# ═══════════════════════════════════════════════════════════════════════════════

elif phase == "ef_review":
    bom: ParsedBOM = st.session_state["fp_bom"]
    ef_matches: list = st.session_state["fp_ef_matches"]
    ef_warnings: list[str] = st.session_state["fp_ef_warnings"]

    matched_count = sum(1 for ef in ef_matches if ef and not ef.is_no_match)
    unmatched_count = sum(1 for ef in ef_matches if ef is None or ef.is_no_match)
    low_conf_count = sum(1 for ef in ef_matches if ef and ef.is_low_confidence)

    st.subheader("Checkpoint 2 — Review matched emission factors")
    st.caption(
        f"**{bom.product_name}** · {matched_count} matched · "
        f"{low_conf_count} low-confidence · {unmatched_count} unmatched"
    )

    st.dataframe(
        _ef_matches_dataframe(bom, ef_matches),
        use_container_width=True,
        hide_index=True,
        column_config={
            "EF (kg CO₂e/USD)": st.column_config.NumberColumn(format="%.4f"),
            "Confidence (%)": st.column_config.NumberColumn(format="%.0f"),
        },
    )

    if ef_warnings:
        with st.expander(
            f"⚠️ {len(ef_warnings)} emission factor flag(s) — review before continuing",
            expanded=True,
        ):
            for w in ef_warnings:
                st.warning(w)

    st.divider()
    col_confirm, col_back = st.columns([2, 1])

    with col_back:
        if st.button("← Back to BOM review"):
            st.session_state["fp_phase"] = "bom_review"
            st.rerun()

    with col_confirm:
        if st.button("Confirm factors — calculate footprint →", type="primary"):
            with st.spinner("Calculating footprint and running critic checks…"):
                result = calculate_footprint(bom, ef_matches)
                result, critic_report = run_critic(result)
            st.session_state["fp_result"] = result
            st.session_state["fp_critic_report"] = critic_report
            st.session_state["fp_phase"] = "calc_review"
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE: calc_review — Checkpoint 3
# ═══════════════════════════════════════════════════════════════════════════════

elif phase == "calc_review":
    bom: ParsedBOM = st.session_state["fp_bom"]
    result: FootprintResult = st.session_state["fp_result"]
    critic_report: CriticReport = st.session_state["fp_critic_report"]

    st.subheader("Checkpoint 3 — Review results")

    # ── Summary metrics ──
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Product", result.product_name)
    with col2:
        st.metric("Total Footprint", f"{result.total_kg_co2e:.2f} kg CO₂e")
    with col3:
        st.metric("Matched rows", f"{result.matched_count} / {len(result.line_items)}")
    with col4:
        st.metric("Flagged rows", result.flagged_count)

    if result.completeness_pct < 100:
        st.warning(
            f"**Partial result:** {result.completeness_pct:.0f}% of rows included. "
            f"{result.unmatched_count} row(s) excluded due to missing spend_usd or no EF match."
        )

    # ── Critic findings ──
    st.divider()
    st.subheader("🔍 Critic review")

    if not critic_report.has_findings:
        st.success("✅ All critic checks passed — no issues found.")
    else:
        if critic_report.correction_count:
            st.info(
                f"**{critic_report.correction_count} auto-correction(s) applied** "
                "before showing results below."
            )
        for finding in critic_report.findings:
            row_label = f" · Row {finding.row_index + 1}" if finding.row_index is not None else ""
            if finding.severity == "corrected":
                st.info(f"**Auto-corrected{row_label}:** {finding.message}")
            else:
                st.warning(f"**{finding.check.replace('_', ' ').title()}{row_label}:** {finding.message}")

    # ── Hotspot chart ──
    if result.has_any_results:
        st.divider()
        st.subheader("Emission hotspots")
        hotspot_data = pd.DataFrame([
            {
                "Component / Material": f"{li.component} – {li.material}",
                "kg CO₂e": round(li.kg_co2e, 4),
            }
            for li in result.hotspots
        ])
        st.bar_chart(hotspot_data.set_index("Component / Material"), horizontal=True)

    # ── Line item breakdown ──
    st.subheader("Line-item breakdown")
    st.caption(
        "Methodology: kg CO₂e = spend_usd × emission factor (kg CO₂e/USD). "
        "Source: Open CEDA 2025. Boundary: Cradle-to-gate, Scope 3 Category 1."
    )
    st.dataframe(
        _results_dataframe(result),
        use_container_width=True,
        hide_index=True,
        column_config={
            "EF (kg CO₂e/USD)": st.column_config.NumberColumn(format="%.4f"),
            "kg CO₂e": st.column_config.NumberColumn(format="%.4f"),
            "Share (%)": st.column_config.NumberColumn(format="%.1f"),
            "EF Confidence (%)": st.column_config.NumberColumn(format="%.0f"),
        },
    )

    st.download_button(
        label="⬇️ Download results as CSV",
        data=_download_csv(result),
        file_name=f"{result.product_name.replace(' ', '_')}_footprint.csv",
        mime="text/csv",
    )

    # ── Approve / Flag ──
    st.divider()
    st.subheader("Save analysis")

    product_name_input = st.text_input(
        "Product name",
        value=bom.product_name,
        help="Edit before saving.",
    )

    if not st.session_state["fp_flag_mode"]:
        col_approve, col_flag = st.columns(2)
        with col_approve:
            if st.button("✅ Approve and save", type="primary"):
                if not product_name_input.strip():
                    st.error("Product name cannot be empty.")
                else:
                    product_id = save_analysis(
                        product_name_input, result,
                        analysis_date=date.today(),
                        status="approved",
                    )
                    st.session_state["fp_saved_product_id"] = product_id
                    st.session_state["fp_phase"] = "saved"
                    st.rerun()
        with col_flag:
            if st.button("⚠️ Flag a concern before saving"):
                st.session_state["fp_flag_mode"] = True
                st.rerun()
    else:
        st.warning(
            "Describe your concern below. The analysis will be saved with status **flagged** "
            "and your comment will be stored in the audit record."
        )
        concern = st.text_area(
            "Concern / reason for flagging",
            placeholder="e.g. EF match for 'nylon thread' looks incorrect — should verify against supplier data.",
            height=120,
        )
        col_save, col_cancel = st.columns(2)
        with col_save:
            if st.button("Save as flagged", type="secondary"):
                if not product_name_input.strip():
                    st.error("Product name cannot be empty.")
                elif not concern.strip():
                    st.error("Please describe your concern before saving.")
                else:
                    product_id = save_analysis(
                        product_name_input, result,
                        analysis_date=date.today(),
                        status="flagged",
                        flagged_comment=concern,
                    )
                    st.session_state["fp_saved_product_id"] = product_id
                    st.session_state["fp_phase"] = "saved"
                    st.rerun()
        with col_cancel:
            if st.button("← Cancel flag"):
                st.session_state["fp_flag_mode"] = False
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE: saved
# ═══════════════════════════════════════════════════════════════════════════════

elif phase == "saved":
    result: FootprintResult = st.session_state["fp_result"]
    product_id = st.session_state["fp_saved_product_id"]

    st.success(f"✅ Analysis saved — Product ID: **{product_id}**")
    st.markdown(
        f"**{result.product_name}** · "
        f"{result.total_kg_co2e:.4f} kg CO₂e total · "
        f"{result.matched_count} matched rows"
    )

    if st.button("↩ Analyze another product", type="primary"):
        _reset()

# ── Methodology note (always visible) ─────────────────────────────────────────
with st.expander("Methodology & limitations", expanded=False):
    st.markdown(
        """
        **Approach:** Spend-based emission factors from Open CEDA 2025 (US Environmentally Extended Input-Output model).
        Each BOM row's footprint = `spend_usd × emission_factor (kg CO₂e/USD)`.

        **Scope:** Cradle-to-gate, Scope 3 Category 1 (Purchased Goods & Services). Does not include product use or end-of-life.

        **Data quality:** This tool uses secondary (industry-average) data. For certification-grade assessments
        (ISO 14067, EPDs), primary supplier data and a qualified LCA practitioner are required.

        **Confidence scores:** ≥80% = direct match; 60–79% = low confidence (flagged); <60% = no match (excluded from total).

        **Critic checks:** After calculation, an automated critic verifies math integrity, flags anomalous concentration
        (>95% share), and identifies rows with spend but no emission factor.
        """
    )
