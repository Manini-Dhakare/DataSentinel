"""
Data Reconciliation View for SchemaGuard Dashboard (Phase 8).
"""

import streamlit as st
import pandas as pd
from frontend.data_loader import load_executive_summary
from frontend.components.charts import (
    create_reconciliation_waterfall_chart,
    create_record_accounting_distribution_chart,
)


def render_reconciliation_view(runs_df: pd.DataFrame, db_path: str):
    """Renders data reconciliation auditor, mathematical proof, and accounting waterfall."""
    st.markdown("## ⚖️ Data Reconciliation Auditor")
    st.markdown(
        "Verifies end-to-end data conservation across the entire pipeline lifecycle. "
        "Enforces the authoritative mathematical identity: **Total Input (I) = Valid Persisted (V) + Quarantined (Q) + Unaccounted (U)**."
    )

    summary = load_executive_summary(db_path)

    # Invariant Verification Banner
    total_i = summary["total_input_records"]
    total_v = summary["total_persisted_records"]
    total_q = summary["total_quarantined_records"]
    total_u = summary["total_unaccounted_records"]

    is_conserved = (total_i == (total_v + total_q + total_u))

    if is_conserved:
        st.success(
            f"✅ **Mathematical Record Conservation VERIFIED:** "
            f"**{total_i:,} (I)** = **{total_v:,} (V)** + **{total_q:,} (Q)** + **{total_u:,} (U)**. "
            f"Overall Reconciliation Health: **{summary['overall_reconciliation_rate_pct']}%**."
        )
    else:
        st.error(
            f"🚨 **Mathematical Record Discrepancy Detected:** "
            f"Input ({total_i:,}) != Valid ({total_v:,}) + Quarantined ({total_q:,}) + Unaccounted ({total_u:,})."
        )

    # Metric Row
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Input Records (I)", f"{total_i:,}")
    c2.metric("Valid Persisted (V)", f"{total_v:,}")
    c3.metric("Quarantined (Q)", f"{total_q:,}")
    c4.metric("Unaccounted / Rejected (U)", f"{total_u:,}")
    c5.metric("Reconciliation Rate", f"{summary['overall_reconciliation_rate_pct']}%")

    st.markdown("---")

    # Visualizations
    col_w, col_s = st.columns([1, 1])

    with col_w:
        fig_waterfall = create_reconciliation_waterfall_chart(
            total_input=total_i,
            valid_persisted=total_v,
            quarantined=total_q,
            unaccounted=total_u,
        )
        st.plotly_chart(fig_waterfall, use_container_width=True)

    with col_s:
        fig_stacked = create_record_accounting_distribution_chart(runs_df)
        st.plotly_chart(fig_stacked, use_container_width=True)

    st.markdown("---")

    # Run-by-Run Auditor Table
    st.markdown("### 🔍 Run-by-Run Reconciliation Audit Trail")
    if not runs_df.empty:
        audit_df = runs_df.copy()
        audit_df["conservation_check"] = (
            audit_df["total_records"] == (
                audit_df["valid_records"] + audit_df["invalid_records"] + audit_df["unaccounted_records"]
            )
        ).apply(lambda ok: "✅ VERIFIED" if ok else "❌ MISMATCH")

        st.dataframe(
            audit_df[[
                "run_id", "started_at", "status", "conservation_check",
                "total_records", "valid_records", "invalid_records",
                "unaccounted_records", "reconciliation_rate_pct"
            ]],
            column_config={
                "run_id": "Run Identifier",
                "started_at": st.column_config.DatetimeColumn("Execution Date", format="YYYY-MM-DD HH:mm:ss"),
                "status": "Pipeline Status",
                "conservation_check": "I = V + Q + U Invariant",
                "total_records": st.column_config.NumberColumn("Input (I)", format="%d"),
                "valid_records": st.column_config.NumberColumn("Persisted (V)", format="%d"),
                "invalid_records": st.column_config.NumberColumn("Quarantined (Q)", format="%d"),
                "unaccounted_records": st.column_config.NumberColumn("Unaccounted (U)", format="%d"),
                "reconciliation_rate_pct": st.column_config.NumberColumn("Reconciled %", format="%.2f%%"),
            },
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No runs available to reconcile.")
