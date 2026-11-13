"""
Executive Overview View for SchemaGuard Dashboard (Phase 8).
"""

import streamlit as st
import pandas as pd
from frontend.data_loader import load_executive_summary
from frontend.components.charts import (
    create_status_distribution_chart,
    create_execution_trends_chart,
)


def render_overview_view(runs_df: pd.DataFrame, db_path: str):
    """Renders the high-level executive overview KPI cards and distribution charts."""
    st.markdown("## 📊 Executive Overview")
    st.markdown(
        "Real-time operational health, lifecycle accounting, and execution efficiency for SchemaGuard."
    )

    summary = load_executive_summary(db_path)

    # Row 1: Pipeline Execution KPIs
    st.markdown("### 🚀 Pipeline Executions")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Executions", f"{summary['total_runs']:,}")
    c2.metric("Successful", f"{summary['successful_runs']:,}", delta=f"{summary['success_rate_pct']}%")
    c3.metric("Partial / Quarantined", f"{summary['partial_runs']:,}")
    c4.metric("Failed", f"{summary['failed_runs']:,}")
    c5.metric("Avg Latency", f"{summary['avg_duration_seconds']:.2f}s")

    st.markdown("---")

    # Row 2: Record-Level Conservation & Accounting KPIs
    st.markdown("### 📦 Record Lifecycle Accounting ($I = V + Q + U$)")
    r1, r2, r3, r4, r5, r6 = st.columns(6)
    r1.metric("Total Batches", f"{summary['total_batches_processed']:,}")
    r2.metric("Input Records (I)", f"{summary['total_input_records']:,}")
    r3.metric("Validated Records", f"{summary['total_valid_records']:,}")
    r4.metric("Durable Persisted (V)", f"{summary['total_persisted_records']:,}")
    r5.metric("Quarantined (Q)", f"{summary['total_quarantined_records']:,}")
    r6.metric("Unaccounted (U)", f"{summary['total_unaccounted_records']:,}", delta=f"{summary['overall_reconciliation_rate_pct']}% Rate")

    st.markdown("---")

    # Row 3: Charts
    col_chart_left, col_chart_right = st.columns([1, 2])
    with col_chart_left:
        fig_donut = create_status_distribution_chart(runs_df)
        st.plotly_chart(fig_donut, use_container_width=True)

    with col_chart_right:
        fig_trends = create_execution_trends_chart(runs_df)
        st.plotly_chart(fig_trends, use_container_width=True)

    # Row 4: Recent Executions Snapshot
    st.markdown("### 🕒 Recent Pipeline Runs")
    if not runs_df.empty:
        display_df = runs_df[[
            "run_id", "started_at", "status", "total_batches", "total_records",
            "valid_records", "invalid_records", "unaccounted_records", "duration_seconds"
        ]].head(10).copy()

        st.dataframe(
            display_df,
            column_config={
                "run_id": "Run Identifier",
                "started_at": st.column_config.DatetimeColumn("Execution Time", format="YYYY-MM-DD HH:mm:ss"),
                "status": "Pipeline Status",
                "total_batches": "Batches",
                "total_records": "Total Rows (I)",
                "valid_records": "Persisted (V)",
                "invalid_records": "Quarantined (Q)",
                "unaccounted_records": "Unaccounted (U)",
                "duration_seconds": st.column_config.NumberColumn("Duration", format="%.3f s"),
            },
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No pipeline executions logged in the metadata catalog.")
