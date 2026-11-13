"""
Pipeline Monitoring View for SchemaGuard Dashboard (Phase 8).
"""

import streamlit as st
import pandas as pd
from frontend.data_loader import load_batch_executions
from frontend.components.charts import (
    create_execution_trends_chart,
    create_batch_processing_summary_chart,
)


def render_monitoring_view(runs_df: pd.DataFrame, db_path: str):
    """Renders execution telemetry, failure inspection, and batch-level drill down."""
    st.markdown("## 🔍 Pipeline Monitoring")
    st.markdown(
        "Trace pipeline execution lineage, failure isolation, execution duration, and batch-level progression."
    )

    if runs_df.empty:
        st.info("No pipeline execution records found in metadata catalog.")
        return

    # Top Chart: Execution Durations
    fig_dur = create_execution_trends_chart(runs_df)
    st.plotly_chart(fig_dur, use_container_width=True)

    # Detailed Run Table
    st.markdown("### 📋 Execution History")
    st.dataframe(
        runs_df[[
            "run_id", "started_at", "completed_at", "duration_seconds", "status",
            "total_batches", "successful_batches", "partial_batches", "rejected_batches",
            "failed_batches", "total_records", "error_message"
        ]],
        column_config={
            "run_id": "Run ID",
            "started_at": st.column_config.DatetimeColumn("Started", format="YYYY-MM-DD HH:mm:ss"),
            "completed_at": st.column_config.DatetimeColumn("Completed", format="YYYY-MM-DD HH:mm:ss"),
            "duration_seconds": st.column_config.NumberColumn("Duration (s)", format="%.3f"),
            "status": "Status",
            "total_batches": "Batches",
            "successful_batches": "Success",
            "partial_batches": "Partial",
            "rejected_batches": "Rejected",
            "failed_batches": "Failed",
            "total_records": "Records",
            "error_message": "Error Description",
        },
        use_container_width=True,
        hide_index=True,
    )

    # Drill-Down Inspector
    st.markdown("---")
    st.markdown("### 🔎 Run & Batch Inspection Drill-Down")

    run_ids = runs_df["run_id"].unique().tolist()
    selected_run = st.selectbox("Select Pipeline Run to Inspect:", options=run_ids, index=0)

    if selected_run:
        run_record = runs_df[runs_df["run_id"] == selected_run].iloc[0]

        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric("Run ID", str(selected_run)[:16] + "...")
        col_b.metric("Execution Status", str(run_record["status"]))
        col_c.metric("Duration", f"{run_record['duration_seconds']:.3f} s")
        col_d.metric("Total Records Ingested", f"{int(run_record['total_records']):,}")

        if run_record["error_message"]:
            st.error(f"**Execution Error Message:** {run_record['error_message']}")

        # Load Batches for this Run
        batches_df = load_batch_executions(db_path, run_id=selected_run)
        if not batches_df.empty:
            st.markdown(f"#### Batches for Run `{selected_run}` ({len(batches_df)} batches)")

            fig_batch = create_batch_processing_summary_chart(batches_df)
            st.plotly_chart(fig_batch, use_container_width=True)

            st.dataframe(
                batches_df[[
                    "batch_id", "table_name", "status", "row_count", "valid_record_count",
                    "invalid_record_count", "quarantine_status", "source_file",
                    "processed_parquet_path", "quarantine_parquet_path", "error_message"
                ]],
                column_config={
                    "batch_id": "Batch ID",
                    "table_name": "Target Table",
                    "status": "Batch Status",
                    "row_count": "Total Rows",
                    "valid_record_count": "Valid Rows",
                    "invalid_record_count": "Quarantined Rows",
                    "quarantine_status": "Quarantine Status",
                    "source_file": "Source Filename",
                    "processed_parquet_path": "Output Storage Path",
                    "quarantine_parquet_path": "Quarantine Path",
                    "error_message": "Error Message",
                },
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info(f"No batch execution sidecars logged for run {selected_run}.")
