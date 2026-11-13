"""
End-to-End Workflow Integration Tests for SchemaGuard (Phase 5.3).

Verifies the complete pipeline lifecycle:
Input Parquet -> Discovery -> Schema Evolution -> Validation -> Quarantine ->
Storage -> Metadata Catalog -> Reconciliation -> Reports (JSON / Markdown)

Scenarios:
1. Clean Parquet ingestion with baseline schema.
2. Cumulative schema evolution across sequential batches (batches 1, 2, 3).
3. Data validation violations and durable quarantine (batch 6).
4. Breaking schema changes and safe rejection containment (batch 4).
5. Corrupted input file containment.
6. Storage collision / failure containment with subsequent batch continuity.
7. Empty landing directory handling.
"""

import os
import json
import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.transformation.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineExecutionStatus,
    BatchExecutionStatus,
    ArtifactType,
)
from src.reconciliation.reconciliation_engine import (
    ReconciliationEngine,
)
from src.reconciliation.models import (
    ReconciliationStatus,
)
from src.reconciliation.report_generator import (
    ReportGenerator,
)


def test_e2e_clean_parquet_pipeline(tmp_path: Path):
    """End-to-end execution of clean input batches."""
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="e2e_clean_run")

    assert pipe_res.status == PipelineExecutionStatus.SUCCESS
    assert pipe_res.total_batches == 2
    assert pipe_res.successful_batches == 2
    assert pipe_res.committed_batches == 2
    assert pipe_res.total_valid_records == pipe_res.total_records
    assert pipe_res.total_quarantined_records == 0

    # Reconcile
    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.RECONCILED
    assert recon_res.unaccounted_record_count == 0
    assert recon_res.missing_artifacts_count == 0
    assert recon_res.processed_artifacts_count == 2

    # Reports
    generator = ReportGenerator()
    json_path = generator.generate_json_report(pipe_res, recon_res, str(rep_dir))
    md_path = generator.generate_markdown_report(pipe_res, recon_res, str(rep_dir))

    assert os.path.isfile(json_path)
    assert os.path.isfile(md_path)


def test_e2e_cumulative_schema_evolution(tmp_path: Path):
    """End-to-end execution with backward-compatible schema changes (batches 1, 2, 3)."""
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")
    shutil.copyfile("data/generated/batch_003_add_sales_channel.parquet", landing / "batch_003_add_sales_channel.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    assert pipe_res.status == PipelineExecutionStatus.SUCCESS
    assert pipe_res.total_batches == 3
    assert pipe_res.successful_batches == 3
    assert pipe_res.committed_batches == 3

    # Check physical storage
    orders_dir = proc_dir / "orders"
    assert (orders_dir / "batch_001.parquet").exists()
    assert (orders_dir / "batch_002.parquet").exists()
    assert (orders_dir / "batch_003.parquet").exists()

    # Reconcile
    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)
    assert recon_res.status == ReconciliationStatus.RECONCILED
    assert recon_res.processed_artifacts_count == 3


def test_e2e_invalid_records_and_quarantine(tmp_path: Path):
    """End-to-end execution with invalid records routing to durable quarantine."""
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    quar_dir = tmp_path / "quarantine"

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_006_duplicates.parquet", landing / "batch_006_duplicates.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(quar_dir),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    assert pipe_res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert pipe_res.total_batches == 2
    assert pipe_res.successful_batches == 1
    assert pipe_res.partial_batches == 1
    assert pipe_res.total_quarantined_records == 50

    # Verify physical quarantine files exist
    quar_orders = quar_dir / "orders" / "batch_006"
    assert (quar_orders / "invalid_records.parquet").exists()
    assert (quar_orders / "quarantine_metadata.json").exists()

    # Reconcile
    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert recon_res.valid_record_count > 0
    assert recon_res.quarantined_record_count == 50
    assert recon_res.unaccounted_record_count == 0  # Conservation holds
    assert recon_res.quarantine_artifacts_count >= 2


def test_e2e_breaking_schema_rejection(tmp_path: Path):
    """End-to-end execution where breaking schema change is rejected without aborting pipeline."""
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_004_breaking_type.parquet", landing / "batch_004_breaking_type.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    assert pipe_res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert pipe_res.successful_batches == 1
    assert pipe_res.rejected_batches == 1

    # Reconcile
    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert recon_res.rejected_batches == 1
    assert recon_res.unaccounted_record_count == 1000  # Rejected rows tracked
    assert any(i.issue_code == "SCHEMA_REJECTED_RECORDS" for i in recon_res.issues)


def test_e2e_corrupted_input_file(tmp_path: Path):
    """End-to-end execution where a corrupt Parquet file in landing is safely contained."""
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "batch_099_corrupt.parquet").write_bytes(b"CORRUPT_NOT_PARQUET")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    assert pipe_res.status == PipelineExecutionStatus.FAILED
    assert pipe_res.failed_batches == 1
    assert len(pipe_res.errors) >= 1

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)
    assert recon_res.status == ReconciliationStatus.FAILED


def test_e2e_storage_failure_containment(tmp_path: Path):
    """End-to-end execution where a pre-existing corrupt file causes storage failure on batch 1, batch 2 succeeds."""
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_orders = tmp_path / "processed" / "orders"
    proc_orders.mkdir(parents=True)

    # Pre-populate corrupted destination for batch_001
    (proc_orders / "batch_001.parquet").write_bytes(b"CORRUPT_BYTES")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    assert pipe_res.total_batches == 2
    assert pipe_res.storage_failed_batches == 1
    assert pipe_res.committed_batches == 1
    assert pipe_res.status == PipelineExecutionStatus.PARTIAL_SUCCESS

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)
    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert any(i.issue_code == "STORAGE_FAILURE" for i in recon_res.issues)


def test_e2e_empty_input_directory(tmp_path: Path):
    """End-to-end execution on empty landing directory."""
    empty_landing = tmp_path / "empty_landing"
    empty_landing.mkdir()

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(empty_landing))

    assert pipe_res.status == PipelineExecutionStatus.SUCCESS
    assert pipe_res.total_batches == 0

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)
    assert recon_res.status == ReconciliationStatus.RECONCILED
    assert recon_res.total_batches == 0
    assert recon_res.input_record_count == 0
