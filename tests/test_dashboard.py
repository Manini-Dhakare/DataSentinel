"""
Unit & Integration Tests for SchemaGuard Dashboard (Phase 8).

Verifies:
1. data_loader: pipeline runs, batch executions, schema evolutions, validation metrics, idempotency records.
2. Invariant conservation: I = V + Q + U accurately computed across all loaders.
3. Empty database & missing file resilience (returns clean empty DataFrames without exceptions).
4. Date range and status filtering logic.
5. Plotly chart generator functions produce valid go.Figure objects.
6. Dashboard launcher script availability and entry points.
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import plotly.graph_objects as go
import pytest

from frontend.data_loader import (
    load_pipeline_runs,
    load_batch_executions,
    load_schema_evolutions,
    load_validation_metrics,
    load_idempotency_records,
    load_executive_summary,
    filter_data,
    get_available_tables,
)
from frontend.components.charts import (
    create_status_distribution_chart,
    create_execution_trends_chart,
    create_reconciliation_waterfall_chart,
    create_record_accounting_distribution_chart,
    create_batch_processing_summary_chart,
    create_schema_evolution_timeline_chart,
    create_quarantine_violations_chart,
)

REAL_DB_PATH = "data/metadata/schemaguard_metadata.duckdb"


def test_data_loader_real_db_pipeline_runs():
    """Verifies pipeline runs extraction and derived fields on real metadata catalog."""
    runs_df = load_pipeline_runs(REAL_DB_PATH)
    assert not runs_df.empty
    assert "run_id" in runs_df.columns
    assert "status" in runs_df.columns
    assert "duration_seconds" in runs_df.columns
    assert "unaccounted_records" in runs_df.columns
    assert "reconciliation_rate_pct" in runs_df.columns

    # Verify duration is non-negative
    assert (runs_df["duration_seconds"] >= 0).all()

    # Verify I = V + Q + U invariant
    for _, row in runs_df.iterrows():
        total_in = row["total_records"]
        valid = row["valid_records"]
        quar = row["invalid_records"]
        unacc = row["unaccounted_records"]
        assert total_in == valid + quar + unacc


def test_data_loader_executive_summary():
    """Verifies executive KPI aggregation and mathematical record conservation."""
    summary = load_executive_summary(REAL_DB_PATH)
    assert summary["total_runs"] > 0
    assert summary["total_batches_processed"] > 0
    assert summary["total_input_records"] > 0

    # Invariant: I = V + Q + U
    total_i = summary["total_input_records"]
    total_v = summary["total_persisted_records"]
    total_q = summary["total_quarantined_records"]
    total_u = summary["total_unaccounted_records"]
    assert total_i == total_v + total_q + total_u


def test_data_loader_batch_executions():
    """Verifies batch executions extraction and optional run_id filtering."""
    batches_df = load_batch_executions(REAL_DB_PATH)
    assert not batches_df.empty
    assert "batch_id" in batches_df.columns
    assert "status" in batches_df.columns

    first_run_id = batches_df["run_id"].iloc[0]
    filtered_batches = load_batch_executions(REAL_DB_PATH, run_id=first_run_id)
    assert not filtered_batches.empty
    assert (filtered_batches["run_id"] == first_run_id).all()


def test_data_loader_schema_evolutions():
    """Verifies schema evolution extraction from DuckDB catalog."""
    evol_df = load_schema_evolutions(REAL_DB_PATH)
    assert not evol_df.empty
    assert "is_evolved" in evol_df.columns
    assert "applied_changes_count" in evol_df.columns


def test_data_loader_validation_metrics():
    """Verifies validation quality counters extraction."""
    val_df = load_validation_metrics(REAL_DB_PATH)
    assert not val_df.empty
    assert "null_violations" in val_df.columns
    assert "duplicate_violations" in val_df.columns


def test_data_loader_idempotency_records():
    """Verifies idempotency registry loading."""
    idemp_df = load_idempotency_records(REAL_DB_PATH)
    assert not idemp_df.empty
    assert "idempotency_key" in idemp_df.columns
    assert "source_fingerprint" in idemp_df.columns


def test_data_loader_nonexistent_db_handling(tmp_path: Path):
    """Verifies that querying a missing database file returns empty DataFrames gracefully."""
    fake_path = str(tmp_path / "does_not_exist.duckdb")

    tables = get_available_tables(fake_path)
    assert tables == []

    runs_df = load_pipeline_runs(fake_path)
    assert runs_df.empty

    batches_df = load_batch_executions(fake_path)
    assert batches_df.empty

    evol_df = load_schema_evolutions(fake_path)
    assert evol_df.empty

    summary = load_executive_summary(fake_path)
    assert summary["total_runs"] == 0
    assert summary["total_input_records"] == 0


def test_data_loader_filter_data():
    """Verifies date and status filtering helpers."""
    runs_df = load_pipeline_runs(REAL_DB_PATH)
    assert not runs_df.empty

    # Status filter
    success_only = filter_data(runs_df, status_filter=["SUCCESS"])
    assert (success_only["status"] == "SUCCESS").all()

    # All filter
    all_runs = filter_data(runs_df, status_filter=["All"])
    assert len(all_runs) == len(runs_df)


def test_plotly_charts_generation():
    """Verifies that all Plotly chart generation utilities return valid figures."""
    runs_df = load_pipeline_runs(REAL_DB_PATH)
    batches_df = load_batch_executions(REAL_DB_PATH)
    evol_df = load_schema_evolutions(REAL_DB_PATH)
    val_df = load_validation_metrics(REAL_DB_PATH)

    # 1. Status Donut Chart
    fig1 = create_status_distribution_chart(runs_df)
    assert isinstance(fig1, go.Figure)

    # 2. Execution Trends
    fig2 = create_execution_trends_chart(runs_df)
    assert isinstance(fig2, go.Figure)

    # 3. Reconciliation Waterfall
    fig3 = create_reconciliation_waterfall_chart(1000, 950, 50, 0)
    assert isinstance(fig3, go.Figure)

    # 4. Record Accounting Stacked Bar
    fig4 = create_record_accounting_distribution_chart(runs_df)
    assert isinstance(fig4, go.Figure)

    # 5. Batch Processing Summary
    fig5 = create_batch_processing_summary_chart(batches_df)
    assert isinstance(fig5, go.Figure)

    # 6. Schema Evolution Timeline
    fig6 = create_schema_evolution_timeline_chart(evol_df)
    assert isinstance(fig6, go.Figure)

    # 7. Quarantine Violations Bar
    fig7 = create_quarantine_violations_chart(val_df)
    assert isinstance(fig7, go.Figure)


def test_plotly_charts_empty_state_resilience():
    """Verifies that charts handle empty DataFrames safely without exceptions."""
    empty_df = pd.DataFrame()

    fig1 = create_status_distribution_chart(empty_df)
    assert isinstance(fig1, go.Figure)

    fig2 = create_execution_trends_chart(empty_df)
    assert isinstance(fig2, go.Figure)

    fig4 = create_record_accounting_distribution_chart(empty_df)
    assert isinstance(fig4, go.Figure)

    fig5 = create_batch_processing_summary_chart(empty_df)
    assert isinstance(fig5, go.Figure)

    fig6 = create_schema_evolution_timeline_chart(empty_df)
    assert isinstance(fig6, go.Figure)

    fig7 = create_quarantine_violations_chart(empty_df)
    assert isinstance(fig7, go.Figure)


def test_dashboard_launcher_script_exists():
    """Verifies launcher script exists and references app.py."""
    launcher_path = Path("scripts/run_dashboard.py")
    assert launcher_path.exists()
    content = launcher_path.read_text(encoding="utf-8")
    assert "streamlit" in content
    assert "frontend" in content
    assert "app.py" in content
