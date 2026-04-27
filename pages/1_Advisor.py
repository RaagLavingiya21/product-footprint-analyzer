"""Conversational Advisor — Streamlit page."""

import uuid

import streamlit as st

from db.reader import build_llm_context, get_all_products
from llm.client import ask_advisor

st.set_page_config(
    page_title="Footprint Advisor",
    page_icon="💬",
    layout="wide",
)

st.title("💬 Carbon Footprint Advisor")
st.caption(
    "Ask questions about your saved product footprint analyses. "
    "All answers are grounded in your saved data."
)

# --- Check DB has data ---
products = get_all_products()
if not products:
    st.warning(
        "No saved analyses found. Upload a BOM on the main page and save an analysis first."
    )
    st.stop()

with st.expander(f"Loaded analyses ({len(products)} product(s))", expanded=False):
    for p in products:
        st.markdown(
            f"- **{p['product_name']}** (ID {p['product_id']}) — "
            f"{p['total_kg_co2e']:.4f} kg CO₂e — saved {p['analysis_date']}"
        )

st.divider()

# --- Session state ---
if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "advisor_session_id" not in st.session_state:
    st.session_state["advisor_session_id"] = str(uuid.uuid4())

# --- Render conversation history ---
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# --- Chat input ---
user_input = st.chat_input("Ask about your product footprints…")

if user_input:
    # Display user message immediately
    with st.chat_message("user"):
        st.markdown(user_input)

    # Build DB context fresh on every turn (catches new saves mid-session)
    db_context = build_llm_context()

    # Call advisor (history excludes the current message — ask_advisor appends it)
    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            response = ask_advisor(
                user_message=user_input,
                conversation_history=st.session_state["messages"],
                db_context=db_context,
                session_id=st.session_state["advisor_session_id"],
            )

        if response.error:
            st.error(response.error)
            # Don't append failed turns to history
        else:
            st.markdown(response.content)

            if response.citations:
                with st.expander(
                    f"Sources ({len(response.citations)} GHG Protocol references)",
                    expanded=False,
                ):
                    for cite in response.citations:
                        st.markdown(f"- {cite}")

            if not response.has_data_reference:
                st.caption(
                    "ℹ️ This response may not reference specific numbers from your data. "
                    "Ask a more specific question if you need data-backed details."
                )

            # Append both turns to history only on success
            st.session_state["messages"].append({"role": "user", "content": user_input})
            st.session_state["messages"].append(
                {"role": "assistant", "content": response.content}
            )

# --- Clear conversation ---
if st.session_state["messages"]:
    if st.button("🗑️ Clear conversation", type="secondary"):
        st.session_state["messages"] = []
        st.rerun()
