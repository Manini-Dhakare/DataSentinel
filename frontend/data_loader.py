"""
Data Loader & Metadata Access Layer for SchemaGuard Dashboard (Phase 8).

Directly interfaces with DuckDB metadata catalog in read-only mode to extract:
1. Executive pipeline KPIs and historical runs.
2. Batch execution telemetry and granular row-level outcomes.
3. Record conservation reconciliation metrics (enforcing I = V + Q + U).
4. Schema evolution history and applied change definitions.
5. Quarantine routing breakdown and data validation violation counts.
6. Idempotency registry records, SHA-256 fingerprints, and retry telemetry.

Provides resilient error handling, empty-state fallbacks, and date/status filtering.
"""

import os
import json
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
import pandas as pd
import duckdb

DEFAULT_DB_PATH = "data/metadata/schemaguard_metadata.duckdb"


def _safe_connect(db_path: str = DEFAULT_DB_PATH) -> Optional[duckdb.DuckDBPyConnection]:
    """Connects to DuckDB safely in read-only mode if the file exists."""
    if not os.path.exists(db_path):
        return None
    try:
        return duckdb.connect(db_path, read_only=True)
    except Exception:
        # Fallback to standard connect if read_only is locked or memory
        try:
            return duckdb.connect(db_path)
        except Exception:
            return None


def get_available_tables(db_path: str = DEFAULT_DB_PATH) -> List[str]:
    """Returns list of tables present in the database."""
    conn = _safe_connect(db_path)
    if not conn:
        return []
    try:
        res = conn.execute("SHOW TABLES").fetchall()
        return [r[0] for r in res]
    except Exception:
        return []
    finally:
        conn.close()


def load_pipeline_runs(db_path: str = DEFAULT_DB_PATH) -> pd.DataFrame:
    """Loads all pipeline runs with derived durations and accounting."""
    empty_cols = [
        "run_id", "started_at", "completed_at", "duration_seconds", "status",
        "total_batches", "successful_batches", "partial_batches", "rejected_batches",
        "failed_batches", "total_records", "valid_records", "invalid_records",
        "unaccounted_records", "reconciliation_rate_pct", "error_message"
    ]
    conn = _safe_connect(db_path)
    if not conn:
        return pd.DataFrame(columns=empty_cols)

    try:
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if "pipeline_runs" not in tables:
            return pd.DataFrame(columns=empty_cols)

        df = conn.execute("SELECT * FROM pipeline_runs ORDER BY started_at DESC").df()
        if df.empty:
            return pd.DataFrame(columns=empty_cols)

        df["started_at"] = pd.to_datetime(df["started_at"])
        df["completed_at"] = pd.to_datetime(df["completed_at"])

        # Compute duration
        df["duration_seconds"] = (df["completed_at"] - df["started_at"]).dt.total_seconds()
        df["duration_seconds"] = df["duration_seconds"].fillna(0.0).round(3)

        # Invariant accounting: I = V + Q + U  =>  U = I - (V + Q)
        total_in = df["total_records"].fillna(0)
        valid = df["valid_records"].fillna(0)
        quar = df["invalid_records"].fillna(0)
        accounted = valid + quar

        df["unaccounted_records"] = (total_in - accounted).clip(lower=0).astype(int)
        df["reconciliation_rate_pct"] = (
            accounted / total_in.replace(0, 1) * 100.0
        ).round(2)
        # For 0 input records, rate is 100%
        df.loc[total_in == 0, "reconciliation_rate_pct"] = 100.0

        return df
    except Exception:
        return pd.DataFrame(columns=empty_cols)
    finally:
        conn.close()


def load_batch_executions(db_path: str = DEFAULT_DB_PATH, run_id: Optional[str] = None) -> pd.DataFrame:
    """Loads batch execution records, optionally filtered by run_id."""
    empty_cols = [
        "run_id", "batch_id", "table_name", "source_file", "status",
        "row_count", "valid_record_count", "invalid_record_count",
        "processed_parquet_path", "quarantine_parquet_path", "quarantine_status",
        "error_message"
    ]
    conn = _safe_connect(db_path)
    if not conn:
        return pd.DataFrame(columns=empty_cols)

    try:
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if "batch_executions" not in tables:
            return pd.DataFrame(columns=empty_cols)

        query = "SELECT * FROM batch_executions"
        params = []
        if run_id:
            query += " WHERE run_id = ?"
            params.append(run_id)
        query += " ORDER BY run_id DESC, batch_id ASC"

        df = conn.execute(query, params).df()
        return df if not df.empty else pd.DataFrame(columns=empty_cols)
    except Exception:
        return pd.DataFrame(columns=empty_cols)
    finally:
        conn.close()


def load_schema_evolutions(db_path: str = DEFAULT_DB_PATH, run_id: Optional[str] = None) -> pd.DataFrame:
    """Loads schema evolution auditing records."""
    empty_cols = [
        "run_id", "batch_id", "table_name", "is_evolved", "evolution_status",
        "applied_changes_count", "applied_changes_json", "rejection_reason"
    ]
    conn = _safe_connect(db_path)
    if not conn:
        return pd.DataFrame(columns=empty_cols)

    try:
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if "schema_evolutions" not in tables:
            return pd.DataFrame(columns=empty_cols)

        query = "SELECT * FROM schema_evolutions"
        params = []
        if run_id:
            query += " WHERE run_id = ?"
            params.append(run_id)
        query += " ORDER BY run_id DESC, batch_id ASC"

        df = conn.execute(query, params).df()
        return df if not df.empty else pd.DataFrame(columns=empty_cols)
    except Exception:
        return pd.DataFrame(columns=empty_cols)
    finally:
        conn.close()


def load_validation_metrics(db_path: str = DEFAULT_DB_PATH, run_id: Optional[str] = None) -> pd.DataFrame:
    """Loads validation failure metric breakdowns."""
    empty_cols = [
        "run_id", "batch_id", "table_name", "total_records", "valid_records",
        "invalid_records", "null_violations", "duplicate_violations",
        "foreign_key_violations", "range_violations", "malformed_violations"
    ]
    conn = _safe_connect(db_path)
    if not conn:
        return pd.DataFrame(columns=empty_cols)

    try:
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if "validation_metrics" not in tables:
            return pd.DataFrame(columns=empty_cols)

        query = "SELECT * FROM validation_metrics"
        params = []
        if run_id:
            query += " WHERE run_id = ?"
            params.append(run_id)
        query += " ORDER BY run_id DESC, batch_id ASC"

        df = conn.execute(query, params).df()
        return df if not df.empty else pd.DataFrame(columns=empty_cols)
    except Exception:
        return pd.DataFrame(columns=empty_cols)
    finally:
        conn.close()


def load_idempotency_records(db_path: str = DEFAULT_DB_PATH) -> pd.DataFrame:
    """Loads registered idempotency keys and fingerprinting lineage."""
    empty_cols = [
        "idempotency_key", "table_name", "batch_id", "source_file",
        "source_fingerprint", "run_id", "status", "processed_parquet_path",
        "quarantine_parquet_path", "record_count", "registered_at"
    ]
    conn = _safe_connect(db_path)
    if not conn:
        return pd.DataFrame(columns=empty_cols)

    try:
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if "idempotency_records" not in tables:
            return pd.DataFrame(columns=empty_cols)

        df = conn.execute("SELECT * FROM idempotency_records ORDER BY registered_at DESC").df()
        if not df.empty:
            df["registered_at"] = pd.to_datetime(df["registered_at"])
            return df
        return pd.DataFrame(columns=empty_cols)
    except Exception:
        return pd.DataFrame(columns=empty_cols)
    finally:
        conn.close()


def load_executive_summary(db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    """Calculates authoritative aggregated executive KPIs across all pipeline history."""
    runs_df = load_pipeline_runs(db_path)
    if runs_df.empty:
        return {
            "total_runs": 0,
            "successful_runs": 0,
            "partial_runs": 0,
            "failed_runs": 0,
            "success_rate_pct": 0.0,
            "total_batches_processed": 0,
            "total_input_records": 0,
            "total_valid_records": 0,
            "total_persisted_records": 0,
            "total_quarantined_records": 0,
            "total_unaccounted_records": 0,
            "overall_reconciliation_rate_pct": 100.0,
            "avg_duration_seconds": 0.0,
        }

    total_runs = len(runs_df)
    success_runs = int((runs_df["status"] == "SUCCESS").sum())
    partial_runs = int((runs_df["status"] == "PARTIAL_SUCCESS").sum())
    failed_runs = int((runs_df["status"] == "FAILED").sum())

    total_batches = int(runs_df["total_batches"].sum())
    total_input = int(runs_df["total_records"].sum())
    total_valid = int(runs_df["valid_records"].sum())
    total_quar = int(runs_df["invalid_records"].sum())
    total_unacc = int(runs_df["unaccounted_records"].sum())

    # Calculate overall reconciliation rate: (valid + quar) / input
    recon_rate = (
        round(((total_valid + total_quar) / total_input) * 100.0, 2)
        if total_input > 0
        else 100.0
    )

    success_rate = round((success_runs / total_runs) * 100.0, 1) if total_runs > 0 else 0.0
    avg_dur = round(runs_df["duration_seconds"].mean(), 2) if not runs_df.empty else 0.0

    return {
        "total_runs": total_runs,
        "successful_runs": success_runs,
        "partial_runs": partial_runs,
        "failed_runs": failed_runs,
        "success_rate_pct": success_rate,
        "total_batches_processed": total_batches,
        "total_input_records": total_input,
        "total_valid_records": total_valid,
        "total_persisted_records": total_valid,  # Durably persisted matching valid records in SUCCESS/PARTIAL runs
        "total_quarantined_records": total_quar,
        "total_unaccounted_records": total_unacc,
        "overall_reconciliation_rate_pct": recon_rate,
        "avg_duration_seconds": avg_dur,
    }


def filter_data(
    df: pd.DataFrame,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    status_filter: Optional[List[str]] = None,
    date_col: str = "started_at",
) -> pd.DataFrame:
    """Applies date range and status filters safely."""
    if df.empty:
        return df

    filtered = df.copy()

    if status_filter and "status" in filtered.columns and "All" not in status_filter:
        filtered = filtered[filtered["status"].isin(status_filter)]

    if date_col in filtered.columns and pd.api.types.is_datetime64_any_dtype(filtered[date_col]):
        if start_date:
            ts_start = pd.to_datetime(start_date)
            # Make tz-naive for consistent comparison if needed
            if filtered[date_col].dt.tz is not None:
                filtered[date_col] = filtered[date_col].dt.tz_localize(None)
            filtered = filtered[filtered[date_col] >= ts_start]

        if end_date:
            ts_end = pd.to_datetime(end_date)
            if filtered[date_col].dt.tz is not None:
                filtered[date_col] = filtered[date_col].dt.tz_localize(None)
            filtered = filtered[filtered[date_col] <= ts_end]

    return filtered
