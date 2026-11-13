"""
Phase 7: Comprehensive End-to-End Testing & Reconciliation Test Suite for SchemaGuard.

Integrates and validates all implemented capabilities across Phases 1 through 6:
1. Complete pipeline execution flow (discovery -> evolution -> validation -> quarantine -> storage -> catalog -> reconciliation).
2. Schema detection and cumulative evolution (batches 1, 2, 3).
3. Batch processing and data validation (Pydantic models, referential integrity).
4. Physical storage publication with technical audit columns and SHA-256 fingerprinting.
5. Quarantine persistence with SHA-256 fingerprinting and idempotency.
6. Metadata catalog logging and historical query telemetry.
7. Record conservation mathematical integrity: I = V + Q + U.
8. Physical artifact durability and existence verification.
9. Idempotency management, repeated execution detection, and legitimate reprocessing.
10. Transient error retry policies and safe bounded exhaustion.
11. CLI runner execution with --metrics and --reprocess flags.

Edge cases covered:
- Empty landing directory.
- Duplicate input batches in discovery.
- Changed source data (legitimate reprocessing).
- Batches with 100% invalid records.
- Batches with mixed valid and invalid records.
- Schema-breaking changes rejected while valid batches proceed.
- Corrupted Parquet files handled gracefully.
- Missing input files during batch processing.
- Storage failures and partial reconciliation.
- Metadata catalog failures and containment.
- Retry exhaustion on simulated transient failures.
- CLI end-to-end execution.
"""

import os
import json
import shutil
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.transformation.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineExecutionStatus,
    BatchExecutionStatus,
    ArtifactType,
    ErrorStage,
)
from src.reconciliation.reconciliation_engine import (
    ReconciliationEngine,
)
from src.reconciliation.models import (
    ReconciliationStatus,
    IssueSeverity,
)
from src.reconciliation.report_generator import (
    ReportGenerator,
)
from src.monitoring.metrics_collector import (
    MetricsCollector,
)
from src.storage.metadata_catalog import (
    MetadataCatalog,
    CatalogStatus,
)
from src.storage.storage_manager import (
    StorageStatus,
)
from src.orchestration.retry import (
    RetryPolicy,
    RetryExhaustedError,
)
from src.orchestration.idempotency import (
    IdempotencyStatus,
)
from scripts.run_pipeline import main


def _create_invalid_batch(source_path: str, dest_path: str, num_rows: int = 10):
    """Creates a Parquet batch where 100% of records fail data validation (negative amounts)."""
    os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
    table = pq.read_table(source_path).slice(0, num_rows)
    # Replace amount column with negative values to violate validation rule
    amount_idx = table.schema.get_field_index("amount")
    new_amounts = pa.array([-50.0 - float(i) for i in range(num_rows)], type=pa.float64())
    arrays = [table.column(i) if i != amount_idx else new_amounts for i in range(table.num_columns)]
    invalid_table = pa.Table.from_arrays(arrays, schema=table.schema)
    pq.write_table(invalid_table, dest_path)


# ==============================================================================
# 1. Full Multi-Batch Lifecycle, Cumulative Evolution & Metrics
# ==============================================================================

def test_phase7_full_lifecycle_clean_and_evolved(tmp_path: Path):
    """
    Validates complete end-to-end pipeline across sequential batches (1, 2, 3):
    - Cumulative schema evolution (baseline -> currency -> sales_channel).
    - Conservation invariant: I = V + Q + U (3000 = 3000 + 0 + 0).
    - Technical audit columns added to stored Parquet files.
    - Metadata catalog lineage and historical query verification.
    - Operational metrics collection and report generation.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    quar_dir = tmp_path / "quarantine"
    rep_dir = tmp_path / "reports"
    meta_db = str(tmp_path / "metadata.duckdb")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")
    shutil.copyfile("data/generated/batch_003_add_sales_channel.parquet", landing / "batch_003_add_sales_channel.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        base_quarantine_dir=str(quar_dir),
        metadata_db_path=meta_db,
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_full_lifecycle_run")

    # Pipeline Level
    assert pipe_res.status == PipelineExecutionStatus.SUCCESS
    assert pipe_res.total_batches == 3
    assert pipe_res.successful_batches == 3
    assert pipe_res.committed_batches == 3
    assert pipe_res.total_records == 3000
    assert pipe_res.total_valid_records == 3000
    assert pipe_res.total_quarantined_records == 0

    # Physical Storage Validation
    orders_dir = proc_dir / "orders"
    for bid in ["batch_001", "batch_002", "batch_003"]:
        target_file = orders_dir / f"{bid}.parquet"
        assert target_file.exists()
        t = pq.read_table(str(target_file))
        assert len(t) == 1000
        # Technical audit columns present
        assert "source_file" in t.column_names
        assert "run_id" in t.column_names
        assert "ingested_at" in t.column_names

    # Reconciliation Validation
    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.RECONCILED
    assert recon_res.input_record_count == 3000
    assert recon_res.valid_record_count == 3000
    assert recon_res.quarantined_record_count == 0
    assert recon_res.unaccounted_record_count == 0
    assert recon_res.persisted_record_count == 3000
    assert recon_res.missing_artifacts_count == 0
    assert recon_res.processed_artifacts_count == 3
    # Invariant: I = V + Q + U
    assert recon_res.input_record_count == (
        recon_res.valid_record_count + recon_res.quarantined_record_count + recon_res.unaccounted_record_count
    )

    # Operational Metrics Validation
    metrics_collector = MetricsCollector()
    metrics = metrics_collector.collect_metrics(pipe_res, recon_res, orchestrator.metadata_catalog)

    assert metrics.run_id == "phase7_full_lifecycle_run"
    assert metrics.pipeline_status == "SUCCESS"
    assert metrics.reconciliation_status == "RECONCILED"
    assert metrics.total_batches == 3
    assert metrics.successful_batches == 3
    assert metrics.input_records == 3000
    assert metrics.validated_records == 3000
    assert metrics.persisted_records == 3000
    assert metrics.reconciliation_rate_pct == 100.0

    metrics_file = metrics_collector.save_metrics_report(metrics, str(rep_dir))
    assert os.path.isfile(metrics_file)

    # Report Generator Validation
    generator = ReportGenerator()
    json_path = generator.generate_json_report(pipe_res, recon_res, str(rep_dir))
    md_path = generator.generate_markdown_report(pipe_res, recon_res, str(rep_dir))

    assert os.path.isfile(json_path)
    assert os.path.isfile(md_path)

    # Catalog Historical Queries
    cat = orchestrator.metadata_catalog
    history = cat.get_pipeline_runs()
    assert len(history) >= 1
    assert history[0]["run_id"] == "phase7_full_lifecycle_run"
    summary = cat.get_execution_summary()
    assert summary["total_runs"] >= 1
    assert summary["successful_runs"] >= 1


# ==============================================================================
# 2. Mixed Valid & Quarantined Records + Invariant Enforcement
# ==============================================================================

def test_phase7_mixed_valid_and_quarantined_records(tmp_path: Path):
    """
    Validates pipeline with mixed clean records and validation failures (batch 6 duplicates):
    - Valid records persisted to storage.
    - Invalid records routed to durable quarantine Parquet + JSON metadata.
    - Conservation invariant: I = V + Q + U (2000 = 1950 + 50 + 0).
    - Reconciliation status: PARTIAL.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    quar_dir = tmp_path / "quarantine"
    meta_db = str(tmp_path / "metadata.duckdb")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_006_duplicates.parquet", landing / "batch_006_duplicates.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        base_quarantine_dir=str(quar_dir),
        metadata_db_path=meta_db,
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_mixed_quarantine_run")

    assert pipe_res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert pipe_res.total_batches == 2
    assert pipe_res.successful_batches == 1
    assert pipe_res.partial_batches == 1
    assert pipe_res.total_records == 2000
    assert pipe_res.total_valid_records == 1950
    assert pipe_res.total_quarantined_records == 50

    # Physical Quarantine Assets Check
    quar_target = quar_dir / "orders" / "batch_006"
    assert (quar_target / "invalid_records.parquet").exists()
    assert (quar_target / "quarantine_metadata.json").exists()

    quar_table = pq.read_table(str(quar_target / "invalid_records.parquet"))
    assert len(quar_table) == 50

    with open(quar_target / "quarantine_metadata.json", "r", encoding="utf-8") as f:
        q_meta = json.load(f)
    assert q_meta["invalid_record_count"] == 50
    assert q_meta["total_batch_records"] == 1000
    assert "fingerprint" in q_meta
    assert len(q_meta["failure_categories"]) >= 1

    # Reconciliation Check
    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert recon_res.input_record_count == 2000
    assert recon_res.valid_record_count == 1950
    assert recon_res.quarantined_record_count == 50
    assert recon_res.unaccounted_record_count == 0
    assert recon_res.persisted_record_count == 1950
    # Invariant: I = V + Q + U
    assert recon_res.input_record_count == (
        recon_res.valid_record_count + recon_res.quarantined_record_count + recon_res.unaccounted_record_count
    )
    assert any(i.issue_code == "RECORDS_QUARANTINED" and i.batch_id == "batch_006" for i in recon_res.issues)


# ==============================================================================
# 3. 100% Invalid Batch Accounting
# ==============================================================================

def test_phase7_100_percent_invalid_batch_accounting(tmp_path: Path):
    """
    Validates batch where 100% of rows violate validation rules:
    - 0 valid records persisted.
    - All rows routed to quarantine.
    - Conservation invariant: I = V + Q + U (10 = 0 + 10 + 0).
    - Pipeline status: PARTIAL_SUCCESS.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    quar_dir = tmp_path / "quarantine"
    meta_db = str(tmp_path / "metadata.duckdb")

    # Generate 100% invalid batch (10 rows, all negative amounts)
    _create_invalid_batch(
        source_path="data/generated/batch_001_original.parquet",
        dest_path=str(landing / "batch_001_all_invalid.parquet"),
        num_rows=10,
    )

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        base_quarantine_dir=str(quar_dir),
        metadata_db_path=meta_db,
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_all_invalid_run")

    assert pipe_res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert pipe_res.total_batches == 1
    assert pipe_res.partial_batches == 1
    assert pipe_res.total_records == 10
    assert pipe_res.total_valid_records == 0
    assert pipe_res.total_quarantined_records == 10
    assert pipe_res.skipped_storage_batches == 1

    # Verify no processed parquet written
    proc_file = proc_dir / "orders" / "batch_001.parquet"
    assert not proc_file.exists()

    # Verify quarantine files written
    quar_target = quar_dir / "orders" / "batch_001"
    assert (quar_target / "invalid_records.parquet").exists()
    assert (quar_target / "quarantine_metadata.json").exists()

    # Reconciliation Check
    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert recon_res.input_record_count == 10
    assert recon_res.valid_record_count == 0
    assert recon_res.quarantined_record_count == 10
    assert recon_res.unaccounted_record_count == 0
    assert recon_res.persisted_record_count == 0
    # Invariant: I = V + Q + U
    assert recon_res.input_record_count == (
        recon_res.valid_record_count + recon_res.quarantined_record_count + recon_res.unaccounted_record_count
    )


# ==============================================================================
# 4. Schema-Breaking Change Isolation & Unaccounted Record Attribution
# ==============================================================================

def test_phase7_schema_breaking_isolation_and_unaccounted_rows(tmp_path: Path):
    """
    Validates batch isolation when schema-breaking change occurs:
    - Batch 1 (clean) succeeds.
    - Batch 4 (breaking type change) is rejected safely.
    - Batch 2 (clean compatible evolution) proceeds and succeeds.
    - Conservation invariant: I = V + Q + U (3000 = 2000 + 0 + 1000).
    - Unaccounted rows are explicitly attributed to SCHEMA_REJECTED batch.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    quar_dir = tmp_path / "quarantine"
    meta_db = str(tmp_path / "metadata.duckdb")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_004_breaking_type.parquet", landing / "batch_004_breaking_type.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        base_quarantine_dir=str(quar_dir),
        metadata_db_path=meta_db,
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_breaking_run")

    assert pipe_res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert pipe_res.total_batches == 3
    assert pipe_res.successful_batches == 2
    assert pipe_res.rejected_batches == 1
    assert pipe_res.total_records == 3000
    assert pipe_res.total_valid_records == 2000
    assert pipe_res.total_quarantined_records == 0

    # Reconciliation Verification
    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert recon_res.input_record_count == 3000
    assert recon_res.valid_record_count == 2000
    assert recon_res.quarantined_record_count == 0
    assert recon_res.unaccounted_record_count == 1000
    assert recon_res.persisted_record_count == 2000
    # Invariant: I = V + Q + U
    assert recon_res.input_record_count == (
        recon_res.valid_record_count + recon_res.quarantined_record_count + recon_res.unaccounted_record_count
    )

    # Attribution check
    rejection_issues = [i for i in recon_res.issues if i.issue_code == "SCHEMA_REJECTED_RECORDS"]
    assert len(rejection_issues) == 1
    assert rejection_issues[0].batch_id == "batch_004"
    assert rejection_issues[0].details.get("row_count") == 1000


# ==============================================================================
# 5. Corrupted Parquet Graceful Containment
# ==============================================================================

def test_phase7_corrupted_parquet_graceful_containment(tmp_path: Path):
    """
    Validates that a corrupted Parquet file in the landing directory does not crash the pipeline:
    - Corrupt file is safely contained with ExecutionError(stage=ErrorStage.INGESTION).
    - Clean batch 1 in the same directory succeeds and commits.
    - Pipeline status is PARTIAL_SUCCESS.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    quar_dir = tmp_path / "quarantine"
    meta_db = str(tmp_path / "metadata.duckdb")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    (landing / "batch_099_corrupted.parquet").write_bytes(b"CORRUPTED_PARQUET_MAGIC_HEADER_INVALID")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        base_quarantine_dir=str(quar_dir),
        metadata_db_path=meta_db,
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_corrupt_run")

    assert pipe_res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert pipe_res.total_batches == 2
    assert pipe_res.successful_batches == 1
    assert pipe_res.failed_batches == 1
    assert len(pipe_res.errors) >= 1
    assert any(e.batch_id == "batch_099" and e.stage == ErrorStage.INGESTION for e in pipe_res.errors)

    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)
    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert recon_res.failed_batches == 1


# ==============================================================================
# 6. Duplicate Input Batches in Discovery
# ==============================================================================

def test_phase7_duplicate_input_batches_in_discovery(tmp_path: Path):
    """
    Validates collision detection during batch discovery:
    - Multiple files in landing mapping to the same batch_id (e.g. batch_001_a, batch_001_b).
    - Discovery catches collision and raises DISCOVERY_ERROR.
    - Orchestrator catches it safely as ExecutionError(stage=ErrorStage.DISCOVERY).
    - Pipeline result is FAILED, logged in metadata catalog, zero artifacts created.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    meta_db = str(tmp_path / "metadata.duckdb")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_variant_a.parquet")
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_variant_b.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=meta_db,
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_dup_discovery_run")

    assert pipe_res.status == PipelineExecutionStatus.FAILED
    assert len(pipe_res.errors) == 1
    assert pipe_res.errors[0].stage == ErrorStage.DISCOVERY
    assert pipe_res.errors[0].error_code == "DISCOVERY_ERROR"
    assert "Duplicate batch ID 'batch_001' detected" in pipe_res.errors[0].message

    # Catalog logged the failure
    failed_runs = orchestrator.metadata_catalog.get_failed_runs()
    assert any(r["run_id"] == "phase7_dup_discovery_run" for r in failed_runs)


# ==============================================================================
# 7. Empty Landing Directory Handling
# ==============================================================================

def test_phase7_empty_landing_directory(tmp_path: Path):
    """
    Validates execution on empty landing directory:
    - Pipeline completes with SUCCESS, total_batches=0, records=0.
    - Reconciliation completes with RECONCILED, I = V + Q + U = 0.
    """
    empty_landing = tmp_path / "empty_landing"
    empty_landing.mkdir()
    proc_dir = tmp_path / "processed"
    meta_db = str(tmp_path / "metadata.duckdb")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=meta_db,
    )
    pipe_res = orchestrator.run_pipeline(str(empty_landing), run_id="phase7_empty_run")

    assert pipe_res.status == PipelineExecutionStatus.SUCCESS
    assert pipe_res.total_batches == 0
    assert pipe_res.total_records == 0

    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.RECONCILED
    assert recon_res.input_record_count == 0
    assert recon_res.valid_record_count == 0
    assert recon_res.quarantined_record_count == 0
    assert recon_res.unaccounted_record_count == 0


# ==============================================================================
# 8. Missing Input File During Batch Processing
# ==============================================================================

def test_phase7_missing_input_file_during_processing(tmp_path: Path):
    """
    Validates handling when an input file is removed after discovery before processing:
    - BatchProcessor returns FAILED (file not found).
    - Orchestrator creates ExecutionError(stage=ErrorStage.INGESTION).
    - Pipeline status is FAILED, logged to metadata catalog.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    meta_db = str(tmp_path / "metadata.duckdb")

    target = landing / "batch_001_temp.parquet"
    shutil.copyfile("data/generated/batch_001_original.parquet", target)

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=meta_db,
    )

    # Mock discover_batches to return reference, then delete the file before run
    batches = orchestrator.discover_batches(str(landing))
    assert len(batches) == 1
    os.remove(target)

    # Monkey-patch discover_batches for this invocation
    orchestrator.discover_batches = lambda *args, **kwargs: batches

    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_missing_file_run")

    assert pipe_res.status == PipelineExecutionStatus.FAILED
    assert pipe_res.failed_batches == 1
    assert any(e.stage == ErrorStage.INGESTION for e in pipe_res.errors)

    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)
    assert recon_res.status == ReconciliationStatus.FAILED


# ==============================================================================
# 9. Storage Persistence Failure & Durability Accounting
# ==============================================================================

def test_phase7_storage_failure_durability_accounting(tmp_path: Path):
    """
    Validates durability accounting when storage fails for batch 1 while batch 2 succeeds:
    - Batch 1 valid records pass data validation (1000 records), but storage fails.
    - Batch 2 valid records pass data validation (1000 records) and persist successfully.
    - Orchestrator status: PARTIAL_SUCCESS.
    - Reconciliation Engine:
      - Valid records validated: 2000.
      - Persisted records: 1000 (only batch 2 was durably written to disk).
      - Issues include STORAGE_FAILURE for batch_001.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    orders_dir = proc_dir / "orders"
    orders_dir.mkdir(parents=True)
    meta_db = str(tmp_path / "metadata.duckdb")

    # Pre-populate corrupt file to cause storage collision failure on batch_001
    (orders_dir / "batch_001.parquet").write_bytes(b"CORRUPT_CONFLICTING_STORAGE_DATA")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=meta_db,
    )
    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_storage_fail_run")

    assert pipe_res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert pipe_res.total_batches == 2
    assert pipe_res.storage_failed_batches == 1
    assert pipe_res.committed_batches == 1

    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.PARTIAL
    # Durability check: 2000 rows were validated, but only 1000 were durably persisted!
    assert recon_res.valid_record_count == 2000
    assert recon_res.persisted_record_count == 1000
    assert any(i.issue_code == "STORAGE_FAILURE" and i.batch_id == "batch_001" for i in recon_res.issues)


# ==============================================================================
# 10. Metadata Catalog Failure Containment
# ==============================================================================

def test_phase7_metadata_catalog_failure_containment(tmp_path: Path):
    """
    Validates containment when metadata catalog logging fails:
    - Storage persists valid records successfully.
    - MetadataCatalog throws an unhandled exception during log_batch_execution.
    - Orchestrator captures CATALOG_FAILED execution error.
    - Processed Parquet remains safe and committed on disk.
    - Pipeline completes with catalog_failed_batches == 1.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    meta_db = str(tmp_path / "metadata.duckdb")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=meta_db,
    )

    # Simulate catalog crash on log_batch_execution
    def _mock_log_batch_fail(*args, **kwargs):
        raise RuntimeError("Simulated DuckDB catalog disk full error")

    orchestrator.metadata_catalog.log_batch_execution = _mock_log_batch_fail

    pipe_res = orchestrator.run_pipeline(str(landing), run_id="phase7_cat_fail_run")

    assert pipe_res.catalog_failed_batches == 1
    assert any(e.stage == ErrorStage.CATALOG and e.error_code == "CATALOG_FAILED" for e in pipe_res.errors)

    # Processed Parquet was committed safely
    proc_file = proc_dir / "orders" / "batch_001.parquet"
    assert proc_file.exists()


# ==============================================================================
# 11. Idempotency: Repeated Pipeline Execution
# ==============================================================================

def test_phase7_idempotency_repeated_pipeline_execution(tmp_path: Path):
    """
    Validates idempotency across repeated pipeline runs:
    - Run 1 processes batch 1 and registers execution in idempotency store.
    - Run 2 processes the same directory without --reprocess:
      - StorageManager returns ALREADY_PROCESSED.
      - Output Parquet is not overwritten.
      - Total committed batches in run 2 is 0, already_processed_batches is 1.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    meta_db = str(tmp_path / "metadata.duckdb")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=meta_db,
    )

    # Run 1
    res1 = orchestrator.run_pipeline(str(landing), run_id="phase7_idemp_run_1")
    assert res1.status == PipelineExecutionStatus.SUCCESS
    assert res1.committed_batches == 1
    assert res1.already_processed_batches == 0

    mtime1 = (proc_dir / "orders" / "batch_001.parquet").stat().st_mtime

    # Run 2 (identical data, no allow_reprocessing)
    res2 = orchestrator.run_pipeline(str(landing), run_id="phase7_idemp_run_2", allow_reprocessing=False)
    assert res2.status == PipelineExecutionStatus.SUCCESS
    assert res2.committed_batches == 0
    assert res2.already_processed_batches == 1

    mtime2 = (proc_dir / "orders" / "batch_001.parquet").stat().st_mtime
    assert mtime1 == mtime2  # File not touched or overwritten


# ==============================================================================
# 12. Idempotency: Legitimate Reprocessing on Changed Data
# ==============================================================================

def test_phase7_reprocess_changed_source_data(tmp_path: Path):
    """
    Validates legitimate reprocessing when input data has changed:
    - Run 1 processes batch 1 (1000 rows).
    - Source file is modified (e.g. truncated to 500 rows).
    - Run 2 executed with allow_reprocessing=True:
      - StorageManager updates output Parquet atomically.
      - Metadata catalog records updated batch.
      - Reconciliation passes with updated record count (500 rows).
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    meta_db = str(tmp_path / "metadata.duckdb")
    batch_file = landing / "batch_001_original.parquet"

    shutil.copyfile("data/generated/batch_001_original.parquet", batch_file)

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=meta_db,
    )

    # Run 1
    res1 = orchestrator.run_pipeline(str(landing), run_id="phase7_reproc_run_1")
    assert res1.total_valid_records == 1000

    # Modify input file
    table = pq.read_table(str(batch_file)).slice(0, 500)
    pq.write_table(table, str(batch_file))

    # Run 2 with allow_reprocessing=True
    res2 = orchestrator.run_pipeline(str(landing), run_id="phase7_reproc_run_2", allow_reprocessing=True)
    assert res2.status == PipelineExecutionStatus.SUCCESS
    assert res2.committed_batches == 1
    assert res2.total_valid_records == 500

    # Verify physical file updated
    proc_table = pq.read_table(str(proc_dir / "orders" / "batch_001.parquet"))
    assert len(proc_table) == 500

    # Reconciliation Check
    recon_engine = ReconciliationEngine()
    recon_res = recon_engine.reconcile(res2)
    assert recon_res.status == ReconciliationStatus.RECONCILED
    assert recon_res.valid_record_count == 500
    assert recon_res.persisted_record_count == 500


# ==============================================================================
# 13. Safe Retries & Bounded Exhaustion
# ==============================================================================

def test_phase7_safe_retries_and_exhaustion():
    """
    Validates bounded retry policy and exhaustion behavior:
    - Transient error retries up to max_retries.
    - RetryExhaustedError raised when retries exceed boundary.
    - Error is classified correctly.
    """
    policy = RetryPolicy(
        max_retries=2,
        initial_delay_sec=0.01,
        backoff_factor=1.5,
        max_delay_sec=0.05,
        jitter=False,
    )

    attempts = 0

    def _failing_operation():
        nonlocal attempts
        attempts += 1
        raise PermissionError("The process cannot access the file because it is being used by another process")

    with pytest.raises(RetryExhaustedError) as exc_info:
        policy.execute("test_retry_op", _failing_operation)

    assert attempts == 3  # Initial attempt + 2 retries
    assert exc_info.value.attempts == 3
    assert exc_info.value.operation_name == "test_retry_op"


# ==============================================================================
# 14. CLI Runner Integration with --metrics Flag
# ==============================================================================

def test_phase7_cli_full_integration_with_metrics(tmp_path: Path):
    """
    Validates CLI execution via scripts/run_pipeline.py with --metrics flag:
    - CLI arguments: --input-dir, --output-dir, --report-dir, --report-format both, --metrics.
    - Exit code 0.
    - Generated reports verified:
      - JSON reconciliation report
      - Markdown reconciliation report
      - JSON operational metrics report
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    exit_code = main([
        "--input-dir", str(landing),
        "--output-dir", str(proc_dir),
        "--report-dir", str(rep_dir),
        "--report-format", "both",
        "--metrics",
        "--run-id", "phase7_cli_metrics_run",
    ])

    assert exit_code == 0

    json_report = rep_dir / "reconciliation_report_phase7_cli_metrics_run.json"
    md_report = rep_dir / "reconciliation_report_phase7_cli_metrics_run.md"
    metrics_report = rep_dir / "operational_metrics_phase7_cli_metrics_run.json"

    assert json_report.exists()
    assert md_report.exists()
    assert metrics_report.exists()

    with open(metrics_report, "r", encoding="utf-8") as f:
        metrics_data = json.load(f)

    assert metrics_data["run_id"] == "phase7_cli_metrics_run"
    assert metrics_data["pipeline_status"] == "SUCCESS"
    assert metrics_data["reconciliation_status"] == "RECONCILED"
    assert metrics_data["record_accounting"]["input_records"] == 1000
    assert metrics_data["record_accounting"]["persisted_records"] == 1000
    assert metrics_data["record_accounting"]["reconciliation_rate_pct"] == 100.0


# ==============================================================================
# 15. CLI Runner Integration with --reprocess Flag
# ==============================================================================

def test_phase7_cli_runner_with_reprocess_flag(tmp_path: Path):
    """
    Validates CLI execution with --reprocess flag after input modification:
    - Initial CLI run commits 1000 records.
    - Source file truncated to 200 records.
    - Second CLI run with --reprocess updates storage and metrics cleanly.
    """
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"
    source_file = landing / "batch_001_original.parquet"

    shutil.copyfile("data/generated/batch_001_original.parquet", source_file)

    # Initial Run
    exit1 = main([
        "--input-dir", str(landing),
        "--output-dir", str(proc_dir),
        "--report-dir", str(rep_dir),
        "--run-id", "phase7_cli_reproc_1",
    ])
    assert exit1 == 0

    # Modify source Parquet
    table = pq.read_table(str(source_file)).slice(0, 200)
    pq.write_table(table, str(source_file))

    # Second Run with --reprocess and --metrics
    exit2 = main([
        "--input-dir", str(landing),
        "--output-dir", str(proc_dir),
        "--report-dir", str(rep_dir),
        "--reprocess",
        "--metrics",
        "--run-id", "phase7_cli_reproc_2",
    ])
    assert exit2 == 0

    # Check updated report
    rep_file = rep_dir / "reconciliation_report_phase7_cli_reproc_2.json"
    assert rep_file.exists()
    with open(rep_file, "r", encoding="utf-8") as f:
        rep_data = json.load(f)
    assert rep_data["reconciliation"]["valid_record_count"] == 200
    assert rep_data["reconciliation"]["persisted_record_count"] == 200
