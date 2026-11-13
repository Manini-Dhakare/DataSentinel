"""
Phase 6.3 & 6.4 Tests: Execution History Persistence & Operational Monitoring.

Verifies:
1. MetadataCatalog execution history queries (get_pipeline_runs, get_batch_history, get_execution_summary).
2. OperationalMetrics contract enforcement and invariants.
3. MetricsCollector telemetry collection (reusing ReconciliationResult directly).
4. Machine-readable metrics report generation.
5. CLI execution with --metrics flag produces operational_metrics JSON report.
"""

import os
import json
import shutil
from pathlib import Path
import pytest

from src.storage.metadata_catalog import MetadataCatalog, CatalogStatus
from src.transformation.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineExecutionResult,
    PipelineExecutionStatus,
)
from src.reconciliation.reconciliation_engine import (
    ReconciliationEngine,
)
from src.monitoring.models import OperationalMetrics
from src.monitoring.metrics_collector import MetricsCollector
from scripts.run_pipeline import main


def test_metadata_catalog_execution_history(tmp_path: Path):
    db_file = str(tmp_path / "meta.duckdb")
    catalog = MetadataCatalog(db_path=db_file)

    # 1. Log two runs
    catalog.log_pipeline_run(
        run_id="run_101",
        started_at="2026-11-13T10:00:00Z",
        completed_at="2026-11-13T10:00:02Z",
        status="SUCCESS",
        total_batches=2,
        successful_batches=2,
        partial_batches=0,
        rejected_batches=0,
        failed_batches=0,
        total_records=200,
        total_valid_records=200,
        total_quarantined_records=0,
    )
    catalog.log_pipeline_run(
        run_id="run_102",
        started_at="2026-11-13T10:05:00Z",
        completed_at="2026-11-13T10:05:03Z",
        status="PARTIAL_SUCCESS",
        total_batches=2,
        successful_batches=1,
        partial_batches=1,
        rejected_batches=0,
        failed_batches=0,
        total_records=200,
        total_valid_records=190,
        total_quarantined_records=10,
    )

    # 2. Query history
    runs = catalog.get_pipeline_runs(limit=10)
    assert len(runs) == 2
    assert runs[0]["run_id"] == "run_102"  # ordered by started_at DESC

    latest = catalog.get_latest_pipeline_run()
    assert latest is not None
    assert latest["run_id"] == "run_102"

    summary = catalog.get_execution_summary()
    assert summary["total_runs"] == 2
    assert summary["successful_runs"] == 1
    assert summary["partial_runs"] == 1
    assert summary["failed_runs"] == 0
    assert summary["total_input_records"] == 400
    assert summary["total_valid_records"] == 390
    assert summary["total_quarantined_records"] == 10

    failed = catalog.get_failed_runs()
    assert len(failed) == 1
    assert failed[0]["run_id"] == "run_102"


def test_operational_metrics_collection_and_export(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"
    meta_db = str(tmp_path / "meta.duckdb")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=meta_db,
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="run_metrics_test")

    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)

    collector = MetricsCollector()
    metrics = collector.collect_metrics(
        pipeline_result=pipe_res,
        recon_result=recon_res,
        catalog=orchestrator.metadata_catalog,
        retry_count=0,
    )

    assert isinstance(metrics, OperationalMetrics)
    assert metrics.run_id == "run_metrics_test"
    assert metrics.pipeline_status == "SUCCESS"
    assert metrics.reconciliation_status == "RECONCILED"
    assert metrics.duration_seconds >= 0.0
    assert metrics.input_records == 1000
    assert metrics.validated_records == 1000
    assert metrics.persisted_records == 1000
    assert metrics.quarantined_records == 0
    assert metrics.unaccounted_records == 0
    assert metrics.reconciliation_rate_pct == 100.0
    assert metrics.total_batches == 1
    assert metrics.successful_batches == 1

    # Save metrics report
    report_file = collector.save_metrics_report(metrics, str(rep_dir))
    assert os.path.isfile(report_file)
    with open(report_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["run_id"] == "run_metrics_test"
    assert "record_accounting" in data
    assert data["record_accounting"]["persisted_records"] == 1000


def test_cli_runner_with_metrics_flag(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    exit_code = main([
        "--input-dir", str(landing),
        "--output-dir", str(proc_dir),
        "--report-dir", str(rep_dir),
        "--run-id", "cli_metrics_run",
        "--metrics",
    ])

    assert exit_code == 0
    metrics_file = rep_dir / "operational_metrics_cli_metrics_run.json"
    assert metrics_file.exists()
    with open(metrics_file, "r", encoding="utf-8") as f:
        d = json.load(f)
    assert d["run_id"] == "cli_metrics_run"
    assert d["pipeline_status"] == "SUCCESS"
