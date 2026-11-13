"""
Quarantine Monitoring View for SchemaGuard Dashboard (Phase 8).
"""

import streamlit as st
import pandas as pd
from frontend.data_loader import load_validation_metrics, load_batch_executions
from frontend.components.charts import create_quarantine_violations_chart


def render_quarantine_view(db_path: str):
    """Renders quarantine monitoring, failure categories breakdown, and invalid record sidecars."""
    st.markdown("## 🛑 Quarantine & Data Quality Monitoring")
    st.markdown(
        "Tracks records violating row-level data validation rules (referential integrity, "
        "duplicate primary keys, negative amounts, null constraints) and isolated into durable quarantine."
    )

    val_df = load_validation_metrics(db_path)
    batches_df = load_batch_executions(db_path)

    # Filter batches with quarantine routing
    quar_batches = batches_df[batches_df["invalid_record_count"] > 0].copy() if not batches_df.empty else pd.DataFrame()

    total_quarantined_records = int(val_df["invalid_records"].sum()) if not val_df.empty else 0
    total_quar_batches = len(quar_batches)
    total_clean_batches = len(batches_df[batches_df["invalid_record_count"] == 0]) if not batches_df.empty else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Quarantined Records", f"{total_quarantined_records:,}")
    c2.metric("Batches with Quarantine", f"{total_quar_batches:,}")
    c3.metric("Clean Batches (0 Quarantine)", f"{total_clean_batches:,}")

    st.markdown("---")

    # Chart: Failure Categories
    fig_cat = create_quarantine_violations_chart(val_df)
    st.plotly_chart(fig_cat, use_container_width=True)

    # Detailed Quarantine Batches Table
    st.markdown("### 📋 Quarantine Incident Summaries")
    if not quar_batches.empty:
        st.dataframe(
            quar_batches[[
                "run_id", "batch_id", "table_name", "row_count", "valid_record_count",
                "invalid_record_count", "quarantine_status", "quarantine_parquet_path",
                "error_message"
            ]],
            column_config={
                "run_id": "Run ID",
                "batch_id": "Batch ID",
                "table_name": "Target Table",
                "row_count": "Total Rows",
                "valid_record_count": "Valid (Persisted)",
                "invalid_record_count": "Quarantined Rows",
                "quarantine_status": "Quarantine Status",
                "quarantine_parquet_path": "Quarantine File Path",
                "error_message": "Error Description",
            },
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("🎉 No batches currently contain quarantined records!")

    # Raw Violation Metrics Table
    if not val_df.empty:
        st.markdown("### 🔬 Violation Counters Breakdown by Batch")
        st.dataframe(
            val_df[[
                "run_id", "batch_id", "table_name", "total_records", "valid_records",
                "invalid_records", "duplicate_violations", "foreign_key_violations",
                "range_violations", "null_violations", "malformed_violations"
            ]],
            column_config={
                "run_id": "Run Identifier",
                "batch_id": "Batch ID",
                "table_name": "Table",
                "total_records": "Total Rows",
                "valid_records": "Valid",
                "invalid_records": "Invalid",
                "duplicate_violations": "Duplicate PKs",
                "foreign_key_violations": "Invalid FKs",
                "range_violations": "Range / Amount",
                "null_violations": "Nulls",
                "malformed_violations": "Malformed",
            },
            use_container_width=True,
            hide_index=True,
        )
