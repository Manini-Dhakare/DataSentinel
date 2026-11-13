"""
Phase 6.1 Tests: Idempotency, Input Fingerprinting & Legitimate Reprocessing.

Verifies:
1. InputFingerprinter determinism and key generation.
2. IdempotencyManager state tracking (NEW, ALREADY_PROCESSED, CHANGED_INPUT).
3. Repeated identical execution produces ALREADY_PROCESSED and avoids duplicate artifacts.
4. Changed input is detected as CHANGED_INPUT.
5. Legitimate reprocessing succeeds when allow_reprocessing=True.
6. Conflicting reuse fails safely when allow_reprocessing=False.
"""

import os
import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.orchestration.idempotency import (
    IdempotencyManager,
    IdempotencyStatus,
    InputFingerprinter,
)
from src.transformation.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineExecutionStatus,
    BatchExecutionStatus,
)
from src.storage.storage_manager import StorageStatus
from src.storage.metadata_catalog import MetadataCatalog
from src.reconciliation.reconciliation_engine import ReconciliationEngine


def _write_table(path: str, rows: int = 10, offset: int = 0):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    table = pa.Table.from_arrays([
        pa.array(list(range(offset, offset + rows)), type=pa.int64()),
        pa.array([f"item_{i}" for i in range(offset, offset + rows)], type=pa.string()),
    ], names=["order_id", "item_name"])
    pq.write_table(table, path)


def test_input_fingerprinter_deterministic(tmp_path: Path):
    f1 = str(tmp_path / "f1.parquet")
    f2 = str(tmp_path / "f2.parquet")
    _write_table(f1, 5)
    _write_table(f2, 5)

    fp1 = InputFingerprinter.compute_file_fingerprint(f1)
    fp2 = InputFingerprinter.compute_file_fingerprint(f2)
    assert fp1 == fp2
    assert len(fp1) == 64

    # Change content
    _write_table(f2, 5, offset=10)
    fp3 = InputFingerprinter.compute_file_fingerprint(f2)
    assert fp3 != fp1

    key = InputFingerprinter.compute_idempotency_key("orders", "batch_001", fp1)
    assert key == f"orders:batch_001:{fp1}"


def test_idempotency_manager_lifecycle(tmp_path: Path):
    meta_db = str(tmp_path / "meta.duckdb")
    catalog = MetadataCatalog(db_path=meta_db)
    idemp_mgr = IdempotencyManager(catalog)

    input_file = str(tmp_path / "batch_001.parquet")
    _write_table(input_file, 10)

    # 1. New batch
    status, rec = idemp_mgr.check_batch(input_file, "batch_001", "orders")
    assert status == IdempotencyStatus.NEW
    assert rec is None

    # 2. Register execution
    fp = InputFingerprinter.compute_file_fingerprint(input_file)
    dummy_out = str(tmp_path / "out.parquet")
    _write_table(dummy_out, 10)

    idemp_mgr.register_execution(
        table_name="orders",
        batch_id="batch_001",
        source_file="batch_001.parquet",
        source_fingerprint=fp,
        run_id="run_1",
        status="SUCCESS",
        processed_parquet_path=dummy_out,
        record_count=10,
    )

    # 3. Check again with identical input -> ALREADY_PROCESSED
    status2, rec2 = idemp_mgr.check_batch(input_file, "batch_001", "orders")
    assert status2 == IdempotencyStatus.ALREADY_PROCESSED
    assert rec2 is not None
    assert rec2.run_id == "run_1"
    assert rec2.record_count == 10

    # 4. Modify input -> CHANGED_INPUT
    _write_table(input_file, 15, offset=5)
    status3, rec3 = idemp_mgr.check_batch(input_file, "batch_001", "orders")
    assert status3 == IdempotencyStatus.CHANGED_INPUT
    assert rec3 is not None
    assert rec3.source_fingerprint == fp  # points to previous fingerprint


def test_pipeline_repeated_identical_execution(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    meta_db = tmp_path / "meta.duckdb"

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=str(meta_db),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    # First execution -> COMMITTED
    res1 = orchestrator.run_pipeline(str(landing), run_id="run_first")
    assert res1.status == PipelineExecutionStatus.SUCCESS
    assert res1.committed_batches == 1
    assert res1.already_processed_batches == 0
    assert len(res1.artifacts) == 1

    # Second execution on identical input -> ALREADY_PROCESSED
    res2 = orchestrator.run_pipeline(str(landing), run_id="run_second")
    assert res2.status == PipelineExecutionStatus.SUCCESS
    assert res2.already_processed_batches == 1
    assert res2.committed_batches == 0
    assert len(res2.artifacts) == 1

    # Reconcile second run -> fully reconciled, no duplicate artifacts
    recon = ReconciliationEngine().reconcile(res2)
    assert recon.status.value == "RECONCILED"
    assert recon.duplicate_artifacts_count == 0
    assert recon.persisted_record_count == 1000


def test_pipeline_legitimate_reprocessing_with_flag(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_dir = tmp_path / "processed"
    meta_db = tmp_path / "meta.duckdb"

    batch_file = landing / "batch_001_original.parquet"
    shutil.copyfile("data/generated/batch_001_original.parquet", batch_file)

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(proc_dir),
        metadata_db_path=str(meta_db),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    # First run
    res1 = orchestrator.run_pipeline(str(landing), run_id="run_v1")
    assert res1.status == PipelineExecutionStatus.SUCCESS
    assert res1.committed_batches == 1

    # Upstream data updates (e.g. fewer rows or changed records)
    tbl = pq.read_table(str(batch_file))
    modified_tbl = tbl.slice(0, 500)  # cut to 500 rows
    pq.write_table(modified_tbl, str(batch_file))

    # Second run without allow_reprocessing -> FAILS with STORAGE_FAILED (conflict detection)
    res_conflict = orchestrator.run_pipeline(str(landing), run_id="run_conflict", allow_reprocessing=False)
    assert res_conflict.storage_failed_batches == 1
    assert res_conflict.status != PipelineExecutionStatus.SUCCESS

    # Third run WITH allow_reprocessing=True -> SUCCEEDS and updates output atomically
    res_reprocessed = orchestrator.run_pipeline(str(landing), run_id="run_v2", allow_reprocessing=True)
    assert res_reprocessed.status == PipelineExecutionStatus.SUCCESS
    assert res_reprocessed.committed_batches == 1
    assert res_reprocessed.storage_failed_batches == 0

    # Verify updated row count
    stored_path = orchestrator.storage_manager.get_batch_parquet_path("orders", "batch_001")
    updated_pq = pq.read_table(stored_path)
    assert len(updated_pq) == 500
