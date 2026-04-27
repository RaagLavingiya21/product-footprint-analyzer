"""Scope 3 Gap Analyzer — Streamlit page."""

from __future__ import annotations

import uuid

import streamlit as st

from gap_analyzer import CompanyProfile, execute_step, generate_plan

st.set_page_config(
    page_title="Scope 3 Gap Analyzer",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 Scope 3 Gap Analyzer")
st.caption(
    "Assess your Scope 3 reporting requirements and data readiness against the "
    "GHG Protocol Corporate Value Chain (Scope 3) Standard."
)

# ── Session state init ────────────────────────────────────────────────────────

_STATE_DEFAULTS = {
    "ga_profile": None,
    "ga_plan": None,
    "ga_current_step": 0,
    "ga_results": {},
    "ga_call_counts": {},
    "ga_phase": "form",
    "ga_session_id": None,
}
for key, default in _STATE_DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = default


def _reset():
    for key, default in _STATE_DEFAULTS.items():
        st.session_state[key] = default
    st.rerun()


# ── Sidebar reset ─────────────────────────────────────────────────────────────

with st.sidebar:
    if st.button("↩ Start over"):
        _reset()

# ── Phase: form ───────────────────────────────────────────────────────────────

if st.session_state["ga_phase"] == "form":
    st.subheader("Company profile")
    st.caption("We use this to determine which Scope 3 categories apply to your business.")

    with st.form("company_profile_form"):
        name = st.text_input("Company name", placeholder="e.g. ACME Apparel")
        size = st.selectbox(
            "Company size",
            ["Under 500 employees", "500–5,000 employees", "5,000–50,000 employees", "50,000+ employees"],
        )
        sector = st.text_input("Sector / industry", placeholder="e.g. apparel manufacturing")
        geography = st.text_input("Primary geography", placeholder="e.g. United States")
        products = st.text_area(
            "Products / services",
            placeholder="Describe what your company produces or sells",
            height=100,
        )
        submitted = st.form_submit_button("Analyse", type="primary")

    if submitted:
        if not sector.strip() or not products.strip():
            st.error("Please fill in **sector** and **products / services** to continue.")
        else:
            st.session_state["ga_profile"] = CompanyProfile(
                name=name.strip() or "Unknown company",
                size=size,
                sector=sector.strip(),
                geography=geography.strip() or "Not specified",
                products=products.strip(),
            )
            st.session_state["ga_session_id"] = str(uuid.uuid4())
            st.session_state["ga_phase"] = "planning"
            st.rerun()

# ── Phase: planning ───────────────────────────────────────────────────────────

elif st.session_state["ga_phase"] == "planning":
    profile: CompanyProfile = st.session_state["ga_profile"]

    with st.expander("Company profile", expanded=False):
        st.markdown(profile.as_text())

    if st.session_state["ga_plan"] is None:
        with st.spinner("Generating execution plan…"):
            plan = generate_plan(profile, session_id=st.session_state["ga_session_id"])
        st.session_state["ga_plan"] = plan

    plan = st.session_state["ga_plan"]

    st.subheader("Execution plan")
    st.markdown(plan.raw_plan_text)

    st.markdown("**Steps:**")
    for step in plan.steps:
        st.markdown(
            f"**{step.step_num}.** `{step.tool_name}` — {step.rationale}"
            + (" *(checkpoint after)*" if step.has_checkpoint_after else "")
        )

    if st.button("Start analysis →", type="primary"):
        st.session_state["ga_current_step"] = 0
        st.session_state["ga_phase"] = "executing"
        st.rerun()

# ── Phase: executing ──────────────────────────────────────────────────────────

elif st.session_state["ga_phase"] == "executing":
    profile: CompanyProfile = st.session_state["ga_profile"]
    plan = st.session_state["ga_plan"]
    idx: int = st.session_state["ga_current_step"]

    if idx >= len(plan.steps):
        st.session_state["ga_phase"] = "done"
        st.rerun()

    step = plan.steps[idx]

    with st.spinner(f"Running: {step.tool_name.replace('_', ' ')}…"):
        result = execute_step(
            step=step,
            company_profile=profile,
            previous_results=st.session_state["ga_results"],
            call_counts=st.session_state["ga_call_counts"],
            session_id=st.session_state["ga_session_id"],
        )

    updated_results = dict(st.session_state["ga_results"])
    updated_results[step.tool_name] = result
    st.session_state["ga_results"] = updated_results

    if result.error == "infinite_loop_guard":
        st.error(result.content)
        st.stop()
    elif result.error and result.error != "not_implemented":
        st.error(f"**Tool error:** {result.error}")
        st.stop()
    elif step.has_checkpoint_after:
        st.session_state["ga_phase"] = "checkpoint"
        st.rerun()
    else:
        st.session_state["ga_current_step"] = idx + 1
        st.rerun()

# ── Phase: checkpoint ─────────────────────────────────────────────────────────

elif st.session_state["ga_phase"] == "checkpoint":
    profile: CompanyProfile = st.session_state["ga_profile"]
    plan = st.session_state["ga_plan"]
    idx: int = st.session_state["ga_current_step"]
    step = plan.steps[idx]
    result = st.session_state["ga_results"].get(step.tool_name)

    checkpoint_num = sum(1 for s in plan.steps[: idx + 1] if s.has_checkpoint_after)
    st.subheader(f"Checkpoint {checkpoint_num}: {step.tool_name.replace('_', ' ').title()}")

    if result:
        if result.error == "not_implemented":
            st.info(result.content)
        elif result.content:
            st.markdown(result.content)

        if result.citations:
            with st.expander(f"Sources ({len(result.citations)} GHG Protocol references)", expanded=False):
                for cite in result.citations:
                    st.markdown(f"- {cite}")

    st.divider()
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Looks good — continue →", type="primary"):
            st.session_state["ga_current_step"] = idx + 1
            next_idx = idx + 1
            if next_idx >= len(plan.steps):
                st.session_state["ga_phase"] = "done"
            else:
                st.session_state["ga_phase"] = "executing"
            st.rerun()
    with col2:
        if st.button("Stop here"):
            st.session_state["ga_phase"] = "done"
            st.rerun()

# ── Phase: done ───────────────────────────────────────────────────────────────

elif st.session_state["ga_phase"] == "done":
    profile: CompanyProfile = st.session_state["ga_profile"]
    results: dict = st.session_state["ga_results"]

    st.subheader("Gap Analysis Report")
    st.caption(f"Generated for: **{profile.name}** | {profile.sector} | {profile.geography}")

    # Compile full report as markdown for download
    report_lines = [
        f"# Scope 3 Gap Analysis Report\n",
        f"**Company:** {profile.name}  ",
        f"**Sector:** {profile.sector}  ",
        f"**Geography:** {profile.geography}  ",
        f"**Size:** {profile.size}  ",
        f"**Products / services:** {profile.products}\n",
        "---\n",
    ]

    for tool_name, result in results.items():
        section_title = tool_name.replace("_", " ").title()
        st.subheader(section_title)
        if result.error == "not_implemented":
            st.info(result.content)
            report_lines.append(f"## {section_title}\n\n_{result.content}_\n")
        elif result.error:
            st.error(result.error)
            report_lines.append(f"## {section_title}\n\n**Error:** {result.error}\n")
        else:
            st.markdown(result.content)
            report_lines.append(f"## {section_title}\n\n{result.content}\n")

        if result.citations:
            with st.expander("Sources", expanded=False):
                for cite in result.citations:
                    st.markdown(f"- {cite}")
            report_lines.append(
                "### Sources\n" + "\n".join(f"- {c}" for c in result.citations) + "\n"
            )

    st.divider()
    st.download_button(
        label="⬇️ Download report as Markdown",
        data="\n".join(report_lines).encode("utf-8"),
        file_name=f"{profile.name.replace(' ', '_')}_scope3_gap_analysis.md",
        mime="text/markdown",
    )
