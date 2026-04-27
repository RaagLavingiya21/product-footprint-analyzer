"""Supplier Engagement Copilot — Streamlit page.

Four-phase state machine:
  selection    → supplier table + checkboxes
  email_review → editable email drafts per supplier
  response     → paste supplier replies, trigger parse + route
  status       → engagement summary + full audit log + CSV export
"""

from __future__ import annotations

import uuid

import pandas as pd
import streamlit as st

from copilot.draft_email import run as draft_email
from copilot.exception_router import run as route_exception
from copilot.models import EngagementCandidate
from copilot.parse_response import run as parse_response
from copilot.suppliers_list import run as get_suppliers_list
from db.copilot_store import (
    append_audit_log,
    create_engagement,
    get_audit_log,
    init_copilot_db,
    update_engagement,
)
from db.reader import get_all_products

st.set_page_config(
    page_title="Supplier Engagement Copilot",
    page_icon="✉️",
    layout="wide",
)

st.title("✉️ Supplier Engagement Copilot")
st.caption(
    "Engage high-emitting suppliers to collect primary emissions data "
    "in accordance with the GHG Protocol Scope 3 Standard."
)

init_copilot_db()

# ── Session state ─────────────────────────────────────────────────────────────

_STATE_DEFAULTS: dict = {
    "sc_phase": "selection",
    "sc_product_name": None,
    "sc_suppliers_result": None,
    "sc_selected_names": [],          # list[str] — supplier names selected in Phase 1
    "sc_email_drafts": {},            # supplier_name -> EmailDraftResult
    "sc_engagement_ids": {},          # supplier_name -> engagement_id
    "sc_process_results": {},         # supplier_name -> {"parsed": ..., "routing": ...}
    "sc_session_id": None,
}

for _k, _v in _STATE_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

if st.session_state["sc_session_id"] is None:
    st.session_state["sc_session_id"] = str(uuid.uuid4())


def _reset() -> None:
    for k, v in _STATE_DEFAULTS.items():
        st.session_state[k] = v
    st.session_state["sc_session_id"] = str(uuid.uuid4())
    st.rerun()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(f"**Phase:** `{st.session_state['sc_phase']}`")
    if st.button("↩ Start over"):
        _reset()


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — Supplier Selection
# ═══════════════════════════════════════════════════════════════════════════════

if st.session_state["sc_phase"] == "selection":
    st.subheader("Phase 1 — Select suppliers to engage")

    products = get_all_products()
    if not products:
        st.warning(
            "No saved product analyses found. "
            "Upload a BOM on the main page and save an analysis first."
        )
        st.stop()

    product_names = [p["product_name"] for p in products]
    selected_product = st.selectbox("Select product", product_names)

    if st.button("Load suppliers", type="primary"):
        with st.spinner("Loading top suppliers…"):
            result = get_suppliers_list(selected_product, top_n=10)
        st.session_state["sc_product_name"] = selected_product
        st.session_state["sc_suppliers_result"] = result

    result = st.session_state["sc_suppliers_result"]

    if result is not None:
        if result.error:
            st.error(result.error)
        elif not result.candidates:
            st.info("No matched line items found for this product.")
        else:
            st.markdown(f"**Top suppliers for {result.product_name}** — select which to engage:")

            # Warn if any contacts are missing
            missing_contacts = [c for c in result.candidates if not c.contact_found]
            if missing_contacts:
                st.warning(
                    f"{len(missing_contacts)} supplier(s) have no contact info in the database: "
                    + ", ".join(c.supplier_name for c in missing_contacts)
                    + ". You can still draft emails but will need to add contact details manually."
                )

            # Display table + checkboxes
            selected_names: list[str] = []
            for idx, c in enumerate(result.candidates):
                col_check, col_supplier, col_component, col_co2e, col_share, col_status, col_contact = st.columns(
                    [0.5, 2, 2, 1.5, 1.2, 1.5, 1.2]
                )
                checked = col_check.checkbox(
                    label="",
                    key=f"sel_{idx}_{c.supplier_name}_{c.component}",
                    value=c.supplier_name in st.session_state["sc_selected_names"],
                )
                col_supplier.markdown(f"**{c.supplier_name}**")
                col_component.markdown(c.component or "—")
                col_co2e.markdown(
                    f"{c.kg_co2e:.4f}" if c.kg_co2e is not None else "—"
                )
                col_share.markdown(
                    f"{c.share_pct:.1f}%" if c.share_pct is not None else "—"
                )
                status_badge = (
                    "🟢 new" if c.engagement_status == "new"
                    else f"🔵 {c.engagement_status}"
                )
                col_status.markdown(status_badge)
                col_contact.markdown("✅" if c.contact_found else "⚠️ missing")

                if checked:
                    selected_names.append(c.supplier_name)

            st.caption(
                "Columns: supplier · component · kg CO₂e · share % · status · contact"
            )

            if selected_names:
                st.markdown(f"**{len(selected_names)} supplier(s) selected.**")
                if st.button("Draft emails →", type="primary"):
                    st.session_state["sc_selected_names"] = selected_names
                    st.session_state["sc_email_drafts"] = {}
                    st.session_state["sc_phase"] = "email_review"
                    st.rerun()
            else:
                st.info("Select at least one supplier to continue.")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — Email Review
# ═══════════════════════════════════════════════════════════════════════════════

elif st.session_state["sc_phase"] == "email_review":
    product_name: str = st.session_state["sc_product_name"]
    result = st.session_state["sc_suppliers_result"]
    selected_names: list[str] = st.session_state["sc_selected_names"]
    session_id: str = st.session_state["sc_session_id"]

    # Build a lookup of candidates by supplier_name
    candidate_map: dict[str, EngagementCandidate] = {
        c.supplier_name: c for c in result.candidates
    }

    st.subheader("Phase 2 — Review email drafts")
    st.caption(
        f"Drafting emails for {len(selected_names)} supplier(s) for **{product_name}**. "
        "Edit the body before confirming."
    )

    # Generate any missing drafts
    drafts: dict = st.session_state["sc_email_drafts"]
    for name in selected_names:
        if name not in drafts:
            candidate = candidate_map[name]
            with st.spinner(f"Drafting email for {name}…"):
                draft_result = draft_email(candidate, product_name, session_id=session_id)
            drafts[name] = draft_result
    st.session_state["sc_email_drafts"] = drafts

    # Render one tab per supplier
    tabs = st.tabs(selected_names)
    all_ready = True

    for tab, name in zip(tabs, selected_names):
        with tab:
            draft_result = drafts.get(name)
            candidate = candidate_map.get(name)

            if draft_result is None or draft_result.error:
                err = draft_result.error if draft_result else "Unknown error"
                st.error(f"LLM call failed: {err} — please try again.")
                all_ready = False
                continue

            draft = draft_result.draft

            if not candidate.contact_found:
                st.warning(
                    "⚠️ No contact info found for this supplier — "
                    "add the recipient email address manually."
                )

            col_to, col_subj = st.columns([2, 3])
            with col_to:
                st.text_input(
                    "To",
                    value=draft.to,
                    key=f"to_{name}",
                    placeholder="supplier@example.com",
                )
            with col_subj:
                st.text_input("Subject", value=draft.subject, key=f"subj_{name}")

            st.text_area(
                "Email body (editable)",
                value=draft.body,
                key=f"body_{name}",
                height=350,
            )

            st.caption(f"**GHG Protocol basis:** {draft.ghg_protocol_basis}")

            if draft_result.citations:
                with st.expander(
                    f"Sources ({len(draft_result.citations)} GHG Protocol references)",
                    expanded=False,
                ):
                    for cite in draft_result.citations:
                        st.markdown(f"- {cite}")

    st.divider()

    col_confirm, col_back = st.columns([2, 1])
    with col_back:
        if st.button("← Back to selection"):
            st.session_state["sc_phase"] = "selection"
            st.rerun()

    with col_confirm:
        if all_ready:
            if st.button("Confirm and proceed →", type="primary"):
                # Save engagements and log to audit_log
                engagement_ids: dict[str, int] = {}
                audit_errors: list[str] = []

                for name in selected_names:
                    candidate = candidate_map[name]
                    body_key = f"body_{name}"
                    final_body = st.session_state.get(body_key, drafts[name].draft.body)

                    eng_id = create_engagement(
                        supplier_name=name,
                        product_name=product_name,
                        component_name=candidate.component,
                        material=candidate.material,
                        kg_co2e=candidate.kg_co2e,
                        share_pct=candidate.share_pct,
                        email_draft=final_body,
                    )
                    engagement_ids[name] = eng_id

                    try:
                        append_audit_log(
                            event="email_drafted",
                            workflow="draft_email",
                            supplier_name=name,
                            product_name=product_name,
                            component_name=candidate.component,
                            email_sent=final_body,
                            status="open",
                        )
                    except Exception as exc:
                        audit_errors.append(
                            f"Audit log entry failed for {name}: {exc}. "
                            "Please try again or check the database."
                        )

                for err in audit_errors:
                    st.error(err)

                if not audit_errors:
                    st.session_state["sc_engagement_ids"] = engagement_ids
                    st.session_state["sc_phase"] = "response"
                    st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — Response Input
# ═══════════════════════════════════════════════════════════════════════════════

elif st.session_state["sc_phase"] == "response":
    product_name: str = st.session_state["sc_product_name"]
    selected_names: list[str] = st.session_state["sc_selected_names"]
    engagement_ids: dict = st.session_state["sc_engagement_ids"]
    drafts: dict = st.session_state["sc_email_drafts"]
    session_id: str = st.session_state["sc_session_id"]
    result = st.session_state["sc_suppliers_result"]
    candidate_map = {c.supplier_name: c for c in result.candidates}
    process_results: dict = st.session_state["sc_process_results"]

    st.subheader("Phase 3 — Paste supplier responses")
    st.caption(
        "Paste each supplier's email reply below. "
        "Click **Process response** to classify and route it."
    )

    tabs = st.tabs(selected_names)

    for tab, name in zip(tabs, selected_names):
        with tab:
            candidate = candidate_map.get(name)
            eng_id = engagement_ids.get(name)
            draft_body = drafts.get(name, {})
            sent_text = (
                draft_body.draft.body
                if draft_body and draft_body.draft
                else st.session_state.get(f"body_{name}", "")
            )

            with st.expander("Email sent", expanded=False):
                st.markdown(sent_text)

            already_processed = name in process_results

            if already_processed:
                pr = process_results[name]
                parsed = pr["parsed"]
                routing = pr["routing"]

                st.success("Response processed.")
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**Response type:** `{parsed.response_type}`")
                    st.markdown(f"**Completeness:** `{parsed.completeness_score}`")
                    st.markdown(
                        f"**Issues:** {', '.join(parsed.issues_identified) or 'none'}"
                    )
                    st.markdown(f"**Data provided:** {parsed.data_provided}")
                with col2:
                    action_color = {
                        "store_data": "🟢",
                        "draft_follow_up": "🟡",
                        "flag_for_human_review": "🔴",
                        "escalate": "🔴",
                    }.get(routing.action, "⚪")
                    st.markdown(f"**Action:** {action_color} `{routing.action}`")
                    st.markdown(f"**Rationale:** {routing.rationale}")
                    if routing.ghg_protocol_citation:
                        st.caption(f"Citation: {routing.ghg_protocol_citation}")
            else:
                response_text = st.text_area(
                    "Supplier response (paste here)",
                    key=f"resp_{name}",
                    height=200,
                    placeholder="Paste the supplier's reply email here…",
                )

                if st.button(f"Process response", key=f"proc_{name}", type="primary"):
                    if not response_text.strip():
                        st.warning("Paste the supplier's response before processing.")
                    else:
                        with st.spinner("Parsing response…"):
                            parse_result = parse_response(
                                response_text=response_text,
                                supplier_name=name,
                                component=candidate.component if candidate else None,
                                session_id=session_id,
                            )

                        if parse_result.error:
                            st.error(parse_result.error)
                        else:
                            with st.spinner("Routing decision…"):
                                from datetime import datetime, timezone
                                # Compute days since engagement was created
                                eng = None
                                try:
                                    from db.copilot_store import get_engagement
                                    eng = get_engagement(eng_id)
                                except Exception:
                                    pass
                                days = 0
                                if eng and eng.created_at:
                                    try:
                                        created = datetime.fromisoformat(eng.created_at)
                                        days = (datetime.now() - created).days
                                    except Exception:
                                        pass

                                route_result = route_exception(
                                    parsed=parse_result.parsed,
                                    supplier_name=name,
                                    component=candidate.component if candidate else None,
                                    days_since_contact=days,
                                    session_id=session_id,
                                )

                            decision = route_result.decision

                            # Update engagement record
                            if eng_id:
                                new_status = {
                                    "store_data": "closed",
                                    "draft_follow_up": "follow-up",
                                    "flag_for_human_review": "flagged",
                                    "escalate": "flagged",
                                }.get(decision.action if decision else "", "flagged")

                                update_engagement(
                                    eng_id,
                                    status=new_status,
                                    response_received=response_text,
                                    routing_decision=decision.action if decision else None,
                                    decision_rationale=decision.rationale if decision else None,
                                    ghg_protocol_citation=decision.ghg_protocol_citation if decision else None,
                                    next_step=decision.action if decision else None,
                                )

                            # Audit log
                            try:
                                append_audit_log(
                                    event="response_parsed_and_routed",
                                    workflow="parse_response+exception_router",
                                    supplier_name=name,
                                    product_name=product_name,
                                    component_name=candidate.component if candidate else None,
                                    email_sent=sent_text,
                                    response_received=response_text,
                                    routing_decision=decision.action if decision else None,
                                    decision_rationale=decision.rationale if decision else None,
                                    ghg_protocol_citation=decision.ghg_protocol_citation if decision else None,
                                    status=new_status,
                                )
                            except Exception as exc:
                                st.error(
                                    f"Audit log entry failed for {name}: {exc}. "
                                    "Please try again or check the database."
                                )

                            # Store in session state
                            updated = dict(st.session_state["sc_process_results"])
                            updated[name] = {
                                "parsed": parse_result.parsed,
                                "routing": decision,
                            }
                            st.session_state["sc_process_results"] = updated
                            st.rerun()

    st.divider()

    all_processed = all(name in process_results for name in selected_names)
    if all_processed:
        if st.button("View engagement status →", type="primary"):
            st.session_state["sc_phase"] = "status"
            st.rerun()
    else:
        remaining = sum(1 for n in selected_names if n not in process_results)
        st.info(f"{remaining} supplier response(s) still to process.")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — Engagement Status + Audit Log
# ═══════════════════════════════════════════════════════════════════════════════

elif st.session_state["sc_phase"] == "status":
    product_name: str = st.session_state["sc_product_name"]
    selected_names: list[str] = st.session_state["sc_selected_names"]
    process_results: dict = st.session_state["sc_process_results"]

    st.subheader("Phase 4 — Engagement Status")
    st.caption(f"Product: **{product_name}**")

    # Engagement status table
    status_rows = []
    for name in selected_names:
        pr = process_results.get(name, {})
        parsed = pr.get("parsed")
        routing = pr.get("routing")
        status_rows.append({
            "Supplier": name,
            "Status": routing.action if routing else "—",
            "Response type": parsed.response_type if parsed else "—",
            "Completeness": parsed.completeness_score if parsed else "—",
            "Issues": ", ".join(parsed.issues_identified) if parsed and parsed.issues_identified else "none",
            "Rationale": routing.rationale if routing else "—",
            "GHG Protocol citation": routing.ghg_protocol_citation or "—" if routing else "—",
        })

    status_df = pd.DataFrame(status_rows)

    def _action_color(val: str) -> str:
        colors = {
            "store_data": "background-color: #d4edda",
            "draft_follow_up": "background-color: #fff3cd",
            "flag_for_human_review": "background-color: #f8d7da",
            "escalate": "background-color: #f8d7da",
        }
        return colors.get(val, "")

    st.dataframe(
        status_df.style.map(_action_color, subset=["Status"]),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    # Full audit log
    st.subheader("Audit Log")
    try:
        log_entries = get_audit_log(product_name=product_name)
    except Exception as exc:
        st.error(f"Could not load audit log: {exc}")
        log_entries = []

    if log_entries:
        log_rows = [
            {
                "Timestamp": e.timestamp,
                "Event": e.event,
                "Workflow": e.workflow,
                "Supplier": e.supplier_name or "—",
                "Component": e.component_name or "—",
                "Routing decision": e.routing_decision or "—",
                "Rationale": e.decision_rationale or "—",
                "GHG citation": e.ghg_protocol_citation or "—",
                "Status": e.status or "—",
            }
            for e in log_entries
        ]
        log_df = pd.DataFrame(log_rows)
        st.dataframe(log_df, use_container_width=True, hide_index=True)

        csv_bytes = log_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Export audit log as CSV",
            data=csv_bytes,
            file_name=f"{product_name.replace(' ', '_')}_supplier_engagement_audit.csv",
            mime="text/csv",
        )
    else:
        st.info("No audit log entries found for this product.")

    st.divider()

    if st.button("↩ Start new engagement", type="secondary"):
        _reset()
