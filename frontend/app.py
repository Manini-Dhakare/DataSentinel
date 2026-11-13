"""
SchemaGuard Interactive Executive & Operational Dashboard (Phase 8).

Enterprise Streamlit Application featuring:
- Executive Overview (KPIs, Success Rates, Volume)
- Pipeline Monitoring (History, Duration, Errors, Batch Drill-Down)
- Data Reconciliation (I = V + Q + U Conservation Invariant, Waterfall Chart)
- Schema Evolution (Compatibility History, Breaking Change Gate, Changes JSON)
- Quarantine Monitoring (Data Quality Violations, Duplicate/FK Breakdown)
- Idempotency & Reliability (Content Fingerprints, Safe Reprocessing, Retries)
"""

import os
import sys
from datetime import datetime

# Ensure project root is available on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import pandas as pd

from frontend.data_loader import (
    load_pipeline_runs,
    filter_data,
    DEFAULT_DB_PATH,
)
from frontend.views.overview import render_overview_view
from frontend.views.monitoring import render_monitoring_view
from frontend.views.reconciliation import render_reconciliation_view
from frontend.views.schema_evolution import render_schema_evolution_view
from frontend.views.quarantine import render_quarantine_view
from frontend.views.reliability import render_reliability_view

# Page Config
st.set_page_config(
    page_title="SchemaGuard | Pipeline Operations & Reconciliation Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for polished enterprise look
st.markdown(
    """
    <style>
    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
    }
    div[data-testid="metric-container"] {
        background-color: rgba(240, 242, 246, 0.5);
        border: 1px solid rgba(200, 205, 215, 0.4);
        padding: 0.8rem 1rem;
        border-radius: 8px;
    }
    .stAlert {
        border-radius: 8px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def main():
    # Sidebar
    st.sidebar.title("🛡️ SchemaGuard")
    st.sidebar.caption("Parquet Evolution, Data Quality & Reconciliation")

    # Navigation Section
    section = st.sidebar.radio(
        "Navigation",
        options=[
            "Executive Overview",
            "Pipeline Monitoring",
            "Data Reconciliation",
            "Schema Evolution",
            "Quarantine Monitoring",
            "Idempotency & Reliability",
        ],
        index=0,
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown("### ⚙️ Operational Filters")

    # DB Path Config
    db_path = st.sidebar.text_input(
        "DuckDB Catalog Path:",
        value=DEFAULT_DB_PATH,
        help="Path to DuckDB operational metadata database.",
    )

    db_exists = os.path.isfile(db_path)
    if not db_exists:
        st.sidebar.warning(f"⚠️ DB file not found: `{db_path}`")
    else:
        st.sidebar.success("🟢 Metadata Catalog Connected")

    # Load runs for filtering
    runs_df = load_pipeline_runs(db_path) if db_exists else pd.DataFrame()

    # Status Filter
    status_options = ["All", "SUCCESS", "PARTIAL_SUCCESS", "FAILED"]
    selected_status = st.sidebar.multiselect(
        "Filter by Status:",
        options=status_options,
        default=["All"],
    )

    # Date Filter
    start_date = None
    end_date = None
    if not runs_df.empty and "started_at" in runs_df.columns and pd.api.types.is_datetime64_any_dtype(runs_df["started_at"]):
        min_date = runs_df["started_at"].min().date()
        max_date = runs_df["started_at"].max().date()
        date_range = st.sidebar.date_input(
            "Filter Date Range:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range

    # Refresh Button
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Refresh Catalog Data", use_container_width=True):
        st.rerun()

    st.sidebar.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Apply Filters
    filtered_runs = filter_data(
        runs_df,
        start_date=start_date,
        end_date=end_date,
        status_filter=selected_status,
    )

    # Empty Database Warning & Instructions
    if not db_exists or runs_df.empty:
        st.warning(
            "### ⚠️ No Pipeline Metadata Found\n\n"
            f"The metadata database at `{db_path}` does not exist or contains no pipeline execution runs yet.\n\n"
            "**To execute the pipeline and generate telemetry:**\n"
            "```bash\n"
            "python scripts/run_pipeline.py --input-dir data/generated --report-format both --metrics\n"
            "```"
        )
        return

    # Route to Views
    if section == "Executive Overview":
        render_overview_view(filtered_runs, db_path)
    elif section == "Pipeline Monitoring":
        render_monitoring_view(filtered_runs, db_path)
    elif section == "Data Reconciliation":
        render_reconciliation_view(filtered_runs, db_path)
    elif section == "Schema Evolution":
        render_schema_evolution_view(db_path)
    elif section == "Quarantine Monitoring":
        render_quarantine_view(db_path)
    elif section == "Idempotency & Reliability":
        render_reliability_view(db_path)


if __name__ == "__main__":
    main()
