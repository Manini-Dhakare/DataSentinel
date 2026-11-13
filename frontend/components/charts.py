"""
Plotly Visualization Component Library for SchemaGuard Dashboard (Phase 8).

Implements interactive, production-grade charts:
1. Pipeline Status Distribution (Donut chart).
2. Execution Duration & Volume Trends (Scatter & Bar timeline).
3. Record Reconciliation Waterfall (I = V + Q + U conservation proof).
4. Record Accounting Breakdown by Run (Stacked bars).
5. Batch Processing Breakdown (Multi-metric comparisons).
6. Schema Evolution Timeline (Change category tracking).
7. Quarantine Violations Distribution (Categorical failure breakdown).
"""

from typing import Dict, Any, Optional, List
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Consistent enterprise palette
COLOR_SUCCESS = "#22c55e"    # Green
COLOR_PARTIAL = "#eab308"    # Amber
COLOR_FAILED = "#ef4444"     # Red
COLOR_PRIMARY = "#3b82f6"    # Blue
COLOR_ACCENT = "#8b5cf6"     # Purple
COLOR_MUTED = "#64748b"      # Slate


def create_status_distribution_chart(runs_df: pd.DataFrame) -> go.Figure:
    """Generates a donut chart of pipeline execution outcomes."""
    if runs_df.empty or "status" not in runs_df.columns:
        fig = go.Figure()
        fig.update_layout(title="No Execution Data Available")
        return fig

    status_counts = runs_df["status"].value_counts().reset_index()
    status_counts.columns = ["status", "count"]

    color_map = {
        "SUCCESS": COLOR_SUCCESS,
        "PARTIAL_SUCCESS": COLOR_PARTIAL,
        "FAILED": COLOR_FAILED,
    }
    colors = [color_map.get(s, COLOR_MUTED) for s in status_counts["status"]]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=status_counts["status"],
                values=status_counts["count"],
                hole=0.55,
                marker=dict(colors=colors),
                textinfo="label+percent",
                hoverinfo="label+value+percent",
            )
        ]
    )
    fig.update_layout(
        title=dict(text="<b>Execution Status Distribution</b>", x=0.05),
        margin=dict(t=50, b=20, l=20, r=20),
        legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
        height=320,
    )
    return fig


def create_execution_trends_chart(runs_df: pd.DataFrame) -> go.Figure:
    """Generates execution duration trend over time."""
    if runs_df.empty or "started_at" not in runs_df.columns:
        fig = go.Figure()
        fig.update_layout(title="No Execution Data Available")
        return fig

    sorted_df = runs_df.sort_values("started_at").copy()
    sorted_df["run_label"] = sorted_df["run_id"].astype(str).str[:12] + "..."

    color_map = {
        "SUCCESS": COLOR_SUCCESS,
        "PARTIAL_SUCCESS": COLOR_PARTIAL,
        "FAILED": COLOR_FAILED,
    }
    colors = [color_map.get(s, COLOR_MUTED) for s in sorted_df["status"]]

    fig = go.Figure()

    # Duration bars
    fig.add_trace(
        go.Bar(
            x=sorted_df["run_label"],
            y=sorted_df["duration_seconds"],
            marker=dict(color=colors),
            name="Duration (s)",
            hovertemplate="<b>Run:</b> %{x}<br><b>Duration:</b> %{y:.3f}s<extra></extra>",
        )
    )

    # Average trend line
    if len(sorted_df) > 1:
        avg_dur = sorted_df["duration_seconds"].mean()
        fig.add_hline(
            y=avg_dur,
            line_dash="dot",
            line_color=COLOR_MUTED,
            annotation_text=f"Mean: {avg_dur:.2f}s",
            annotation_position="top right",
        )

    fig.update_layout(
        title=dict(text="<b>Pipeline Execution Duration & Health History</b>", x=0.05),
        xaxis_title="Pipeline Run Identifier",
        yaxis_title="Execution Duration (seconds)",
        margin=dict(t=50, b=40, l=40, r=20),
        height=320,
    )
    return fig


def create_reconciliation_waterfall_chart(
    total_input: int,
    valid_persisted: int,
    quarantined: int,
    unaccounted: int,
) -> go.Figure:
    """
    Renders a waterfall chart proving the mathematical conservation invariant:
    Input = Valid Persisted + Quarantined + Unaccounted.
    """
    fig = go.Figure(
        go.Waterfall(
            name="Records",
            orientation="v",
            measure=["relative", "relative", "relative", "total"],
            x=["Total Input (I)", "Quarantined (Q)", "Unaccounted (U)", "Valid Persisted (V)"],
            textposition="outside",
            text=[f"{total_input:,}", f"-{quarantined:,}", f"-{unaccounted:,}", f"{valid_persisted:,}"],
            y=[total_input, -quarantined, -unaccounted, valid_persisted],
            connector=dict(line=dict(color=COLOR_MUTED)),
            decreasing=dict(marker=dict(color=COLOR_PARTIAL)),
            increasing=dict(marker=dict(color=COLOR_PRIMARY)),
            totals=dict(marker=dict(color=COLOR_SUCCESS)),
        )
    )

    fig.update_layout(
        title=dict(text="<b>Record Conservation Waterfall Audit (I = V + Q + U)</b>", x=0.05),
        showlegend=False,
        yaxis_title="Record Count",
        margin=dict(t=50, b=30, l=40, r=20),
        height=350,
    )
    return fig


def create_record_accounting_distribution_chart(runs_df: pd.DataFrame) -> go.Figure:
    """Generates stacked bar chart comparing Valid vs Quarantined vs Unaccounted across runs."""
    if runs_df.empty:
        fig = go.Figure()
        fig.update_layout(title="No Execution Data Available")
        return fig

    sorted_df = runs_df.sort_values("started_at", ascending=False).head(15).copy()
    sorted_df["run_label"] = sorted_df["run_id"].astype(str).str[:12] + "..."

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            name="Valid Persisted (V)",
            x=sorted_df["run_label"],
            y=sorted_df["valid_records"],
            marker_color=COLOR_SUCCESS,
        )
    )
    fig.add_trace(
        go.Bar(
            name="Quarantined (Q)",
            x=sorted_df["run_label"],
            y=sorted_df["invalid_records"],
            marker_color=COLOR_PARTIAL,
        )
    )
    fig.add_trace(
        go.Bar(
            name="Unaccounted / Rejected (U)",
            x=sorted_df["run_label"],
            y=sorted_df["unaccounted_records"],
            marker_color=COLOR_FAILED,
        )
    )

    fig.update_layout(
        barmode="stack",
        title=dict(text="<b>Record Reconciliation Accounting Across Recent Runs</b>", x=0.05),
        xaxis_title="Run Identifier",
        yaxis_title="Record Volume",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
        margin=dict(t=50, b=60, l=40, r=20),
        height=350,
    )
    return fig


def create_batch_processing_summary_chart(batches_df: pd.DataFrame) -> go.Figure:
    """Generates batch volume inspection chart."""
    if batches_df.empty or "batch_id" not in batches_df.columns:
        fig = go.Figure()
        fig.update_layout(title="No Batch Data Available")
        return fig

    summary = (
        batches_df.groupby("batch_id")
        .agg(
            total_rows=("row_count", "sum"),
            valid_rows=("valid_record_count", "sum"),
            invalid_rows=("invalid_record_count", "sum"),
        )
        .reset_index()
    )

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            name="Valid Rows",
            x=summary["batch_id"],
            y=summary["valid_rows"],
            marker_color=COLOR_SUCCESS,
        )
    )
    fig.add_trace(
        go.Bar(
            name="Quarantined Rows",
            x=summary["batch_id"],
            y=summary["invalid_rows"],
            marker_color=COLOR_PARTIAL,
        )
    )

    fig.update_layout(
        barmode="group",
        title=dict(text="<b>Batch Processing Volumes & Quality Outcomes</b>", x=0.05),
        xaxis_title="Batch Identifier",
        yaxis_title="Row Count",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5),
        margin=dict(t=50, b=60, l=40, r=20),
        height=340,
    )
    return fig


def create_schema_evolution_timeline_chart(evolutions_df: pd.DataFrame) -> go.Figure:
    """Visualizes frequency of schema changes applied over batches."""
    if evolutions_df.empty or "batch_id" not in evolutions_df.columns:
        fig = go.Figure()
        fig.update_layout(title="No Schema Evolution Records Available")
        return fig

    evolved = evolutions_df[evolutions_df["is_evolved"] == True].copy()
    if evolved.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No schema evolutions detected (All schemas match baseline)",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color=COLOR_MUTED),
        )
        fig.update_layout(title="<b>Schema Evolution History</b>", height=300)
        return fig

    counts = evolved.groupby(["table_name", "batch_id"])["applied_changes_count"].sum().reset_index()

    fig = px.bar(
        counts,
        x="batch_id",
        y="applied_changes_count",
        color="table_name",
        title="<b>Cumulative Schema Evolution Changes by Batch</b>",
        labels={"applied_changes_count": "Applied Schema Changes", "batch_id": "Batch ID"},
        color_discrete_sequence=[COLOR_PRIMARY, COLOR_ACCENT],
    )
    fig.update_layout(
        margin=dict(t=50, b=40, l=40, r=20),
        height=320,
    )
    return fig


def create_quarantine_violations_chart(val_metrics_df: pd.DataFrame) -> go.Figure:
    """Aggregates and visualizes quarantine failure violation categories."""
    if val_metrics_df.empty:
        fig = go.Figure()
        fig.update_layout(title="No Validation Metrics Available")
        return fig

    categories = {
        "Duplicate Keys": int(val_metrics_df["duplicate_violations"].sum()) if "duplicate_violations" in val_metrics_df.columns else 0,
        "Foreign Key Violations": int(val_metrics_df["foreign_key_violations"].sum()) if "foreign_key_violations" in val_metrics_df.columns else 0,
        "Null Violations": int(val_metrics_df["null_violations"].sum()) if "null_violations" in val_metrics_df.columns else 0,
        "Range Violations": int(val_metrics_df["range_violations"].sum()) if "range_violations" in val_metrics_df.columns else 0,
        "Malformed Formats": int(val_metrics_df["malformed_violations"].sum()) if "malformed_violations" in val_metrics_df.columns else 0,
    }

    df_cat = pd.DataFrame([
        {"Violation Category": k, "Violation Count": v}
        for k, v in categories.items() if v > 0
    ])

    if df_cat.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="100% Clean Validation - Zero Violations Encountered",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=14, color=COLOR_SUCCESS),
        )
        fig.update_layout(title="<b>Data Validation Quality Health</b>", height=300)
        return fig

    df_cat = df_cat.sort_values("Violation Count", ascending=True)

    fig = go.Figure(
        go.Bar(
            x=df_cat["Violation Count"],
            y=df_cat["Violation Category"],
            orientation="h",
            marker=dict(color=COLOR_PARTIAL),
            text=df_cat["Violation Count"],
            textposition="auto",
        )
    )

    fig.update_layout(
        title=dict(text="<b>Quarantine Routing by Data Validation Failure Category</b>", x=0.05),
        xaxis_title="Violations Count",
        margin=dict(t=50, b=30, l=150, r=20),
        height=320,
    )
    return fig
