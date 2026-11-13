"""
Idempotency & Reliability View for SchemaGuard Dashboard (Phase 8).
"""

import streamlit as st
import pandas as pd
from frontend.data_loader import load_idempotency_records, load_pipeline_runs


def render_reliability_view(db_path: str):
    """Renders idempotency registry, cryptographic fingerprints, and retry fault tolerance."""
    st.markdown("## 🛡️ Idempotency & Fault-Tolerance Reliability")
    st.markdown(
        "Monitors duplicate processing prevention, SHA-256 content-addressable input fingerprinting, "
        "legitimate reprocessing detection, and bounded retry policy telemetry."
    )

    idemp_df = load_idempotency_records(db_path)
    runs_df = load_pipeline_runs(db_path)

    # Key Metrics
    total_keys = len(idemp_df)
    committed_batches = int((idemp_df["status"] == "SUCCESS").sum()) if not idemp_df.empty else 0
    partial_batches = int((idemp_df["status"] == "PARTIAL_SUCCESS").sum()) if not idemp_df.empty else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Registered Idempotency Keys", f"{total_keys:,}")
    c2.metric("Committed Batches", f"{committed_batches:,}")
    c3.metric("Partial Batches", f"{partial_batches:,}")
    c4.metric("Configured Retry Policy", "Bounded (Max 3)")

    st.markdown("---")

    # Resilience Architecture Summary
    st.markdown("### ⚙️ Fault-Tolerance & Retry Policy Configuration")
    col_pol1, col_pol2 = st.columns(2)
    with col_pol1:
        st.markdown(
            """
            **Retry Strategy:**
            - **Max Retries:** 3 attempts
            - **Backoff Algorithm:** Exponential backoff ($t = d_0 \times b^k$)
            - **Initial Delay:** 0.05 seconds
            - **Max Delay:** 1.00 second
            - **Jitter:** Full randomized jitter to eliminate thundering herd
            """
        )
    with col_pol2:
        st.markdown(
            """
            **Error Classification Matrix:**
            - **Retryable Transient:** WinError 32 file locks, DuckDB transaction contention, I/O timeouts.
            - **Non-Retryable Deterministic:** Schema breaking changes, type mismatches, missing columns, corrupt headers.
            - **Atomicity:** Temporary staging (`.tmp`) with atomic swap (`os.replace`).
            """
        )

    st.markdown("---")

    # Idempotency Table
    st.markdown("### 🔑 Idempotency Registry & Content Fingerprints")
    if not idemp_df.empty:
        display_df = idemp_df.copy()
        # Truncate SHA256 for cleaner table display
        display_df["fingerprint_preview"] = display_df["source_fingerprint"].str[:16] + "..."

        st.dataframe(
            display_df[[
                "batch_id", "table_name", "status", "record_count",
                "source_file", "fingerprint_preview", "run_id", "registered_at",
                "processed_parquet_path"
            ]],
            column_config={
                "batch_id": "Batch ID",
                "table_name": "Table",
                "status": "Registration Status",
                "record_count": "Records Committed",
                "source_file": "Source Filename",
                "fingerprint_preview": "SHA-256 Fingerprint",
                "run_id": "Execution Run ID",
                "registered_at": st.column_config.DatetimeColumn("Registered UTC", format="YYYY-MM-DD HH:mm:ss"),
                "processed_parquet_path": "Durable Parquet Path",
            },
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No idempotency records currently registered in the catalog.")
