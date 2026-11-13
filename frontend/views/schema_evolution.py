"""
Schema Evolution View for SchemaGuard Dashboard (Phase 8).
"""

import streamlit as st
import pandas as pd
import json
from frontend.data_loader import load_schema_evolutions
from frontend.components.charts import create_schema_evolution_timeline_chart


def render_schema_evolution_view(db_path: str):
    """Renders schema detection, evolution history, and backward-compatible changes."""
    st.markdown("## 🧬 Schema Evolution & Compatibility History")
    st.markdown(
        "Audits schema modifications, type widening, column additions/removals, "
        "and breaking change rejection gates across all ingested Parquet batches."
    )

    evolutions_df = load_schema_evolutions(db_path)

    if evolutions_df.empty:
        st.info("No schema evolution records found in metadata catalog.")
        return

    # High Level KPIs
    total_audits = len(evolutions_df)
    evolved_batches = int((evolutions_df["is_evolved"] == True).sum())
    total_changes = int(evolutions_df["applied_changes_count"].sum())
    rejected_schemas = int((evolutions_df["evolution_status"] == "SCHEMA_REJECTED").sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Batches Audited", f"{total_audits:,}")
    c2.metric("Evolved Batches", f"{evolved_batches:,}")
    c3.metric("Total Changes Applied", f"{total_changes:,}")
    c4.metric("Breaking Rejections", f"{rejected_schemas:,}")

    st.markdown("---")

    # Chart: Evolution frequency
    fig_evol = create_schema_evolution_timeline_chart(evolutions_df)
    st.plotly_chart(fig_evol, use_container_width=True)

    # Detailed Table
    st.markdown("### 📜 Schema Evolution History")
    st.dataframe(
        evolutions_df[[
            "run_id", "batch_id", "table_name", "is_evolved", "evolution_status",
            "applied_changes_count", "rejection_reason"
        ]],
        column_config={
            "run_id": "Run Identifier",
            "batch_id": "Batch ID",
            "table_name": "Target Table",
            "is_evolved": "Schema Evolved?",
            "evolution_status": "Classification Status",
            "applied_changes_count": "Changes Count",
            "rejection_reason": "Rejection Gate Reason",
        },
        use_container_width=True,
        hide_index=True,
    )

    # Changes JSON Inspector
    st.markdown("---")
    st.markdown("### 🔬 Schema Change Inspector")

    evolved_rows = evolutions_df[evolutions_df["applied_changes_count"] > 0]
    if not evolved_rows.empty:
        batch_options = evolved_rows["batch_id"].unique().tolist()
        selected_batch = st.selectbox(
            "Select an Evolved Batch to Inspect Changes:",
            options=batch_options,
            index=0,
        )

        match = evolved_rows[evolved_rows["batch_id"] == selected_batch].iloc[0]
        st.markdown(f"**Table:** `{match['table_name']}` | **Run:** `{match['run_id']}`")

        raw_json = match.get("applied_changes_json")
        if raw_json:
            try:
                parsed = json.loads(raw_json)
                st.json(parsed)
            except Exception:
                st.code(raw_json, language="json")
        else:
            st.info("No detailed JSON changes payload recorded for this batch.")
    else:
        st.info("No batches with active schema evolution modifications recorded.")
