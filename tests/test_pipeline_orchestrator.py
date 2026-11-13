"""Tests for PipelineOrchestrator in Phase 5.0."""

import os
import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.transformation.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineExecutionResult,
    PipelineExecutionStatus,
    BatchExecutionResult,
    BatchExecutionStatus,
    InputBatchReference,
)
from src.quarantine.quarantine_manager import QuarantineRecord


def test_extract_batch_id():
    assert PipelineOrchestrator.extract_batch_id("batch_001_original.parquet") == "batch_001"
    assert PipelineOrchestrator.extract_batch_id("batch_002_add_currency.parquet") == "batch_002"
    assert PipelineOrchestrator.extract_batch_id("batch_009.parquet") == "batch_009"
    assert PipelineOrchestrator.extract_batch_id("orders_20261113.parquet") == "orders_20261113"


def test_is_quarantine_durable(tmp_path: Path):
    pq_file = tmp_path / "q.parquet"
    meta_file = tmp_path / "q_meta.json"

    # Files don't exist yet -> not durable
    q_rec = QuarantineRecord(
        batch_id="b1",
        table_name="orders",
        record_count=5,
        parquet_path=str(pq_file),
        metadata_path=str(meta_file),
        status="QUARANTINED",
        quarantined_at="2026-11-13T10:00:00Z",
    )
    assert not PipelineOrchestrator.is_quarantine_durable(q_rec)

    # Create dummy files
    pq_file.write_bytes(b"dummy parquet")
    meta_file.write_text("{}", encoding="utf-8")
    assert PipelineOrchestrator.is_quarantine_durable(q_rec)

    # 0 records -> not durable
    q_rec_zero = QuarantineRecord(
        batch_id="b1",
        table_name="orders",
        record_count=0,
        parquet_path=str(pq_file),
        metadata_path=str(meta_file),
        status="QUARANTINED",
        quarantined_at="2026-11-13T10:00:00Z",
    )
    assert not PipelineOrchestrator.is_quarantine_durable(q_rec_zero)


def test_orchestrator_empty_input_directory(tmp_path: Path):
    empty_landing = tmp_path / "landing"
    empty_landing.mkdir()

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(empty_landing))

    assert res.status == PipelineExecutionStatus.SUCCESS
    assert res.total_batches == 0
    assert len(res.batch_results) == 0


def test_orchestrator_duplicate_batch_ids_fails(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()

    # Create two files that parse to the same batch_id "batch_001"
    f1 = landing / "batch_001_v1.parquet"
    f2 = landing / "batch_001_v2.parquet"
    f1.write_bytes(b"dummy")
    f2.write_bytes(b"dummy")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.status == PipelineExecutionStatus.FAILED
    assert "Duplicate batch ID" in str(res.error_message)


def test_orchestrator_clean_single_batch(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()

    # Copy real batch_001_original.parquet into landing
    src_b1 = Path("data/generated/batch_001_original.parquet")
    dst_b1 = landing / "batch_001_original.parquet"
    shutil.copyfile(src_b1, dst_b1)

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert isinstance(res, PipelineExecutionResult)
    assert res.status == PipelineExecutionStatus.SUCCESS
    assert res.total_batches == 1
    assert res.successful_batches == 1
    assert res.total_valid_records > 0
    assert res.total_quarantined_records == 0
    assert len(res.batch_results) == 1
    assert isinstance(res.batch_results[0], BatchExecutionResult)
    assert res.batch_results[0].status == BatchExecutionStatus.SUCCESS
    assert isinstance(res.batch_results[0].input_ref, InputBatchReference)
    assert res.batch_results[0].input_ref.batch_id == "batch_001"

    # Verify processed file exists
    processed_file = tmp_path / "processed" / "orders" / "batch_001.parquet"
    assert processed_file.exists()


def test_orchestrator_clean_empty_batch(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()

    src_b9 = Path("data/generated/batch_009_empty.parquet")
    dst_b9 = landing / "batch_009_empty.parquet"
    shutil.copyfile(src_b9, dst_b9)

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.status == PipelineExecutionStatus.SUCCESS
    assert res.total_batches == 1
    assert res.successful_batches == 1
    assert res.total_valid_records == 0
    assert res.batch_results[0].storage_status.value == "SKIPPED"


def test_orchestrator_breaking_schema_rejection(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()

    src_b4 = Path("data/generated/batch_004_breaking_type.parquet")
    dst_b4 = landing / "batch_004_breaking_type.parquet"
    shutil.copyfile(src_b4, dst_b4)

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    # Since all batches failed and produced no output -> FAILED
    assert res.status == PipelineExecutionStatus.FAILED
    assert res.rejected_batches == 1
    assert res.total_valid_records == 0


def test_orchestrator_mixed_clean_and_quarantined_batches(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_006_duplicates.parquet", landing / "batch_006_duplicates.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert res.total_batches == 2
    assert res.successful_batches == 1
    assert res.partial_batches == 1
    assert res.total_valid_records > 0
    assert res.total_quarantined_records > 0


# --- Defect-01 targeted tests ---

def test_orchestrator_conflicting_existing_batch_contained(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_orders = tmp_path / "processed" / "orders"
    proc_orders.mkdir(parents=True)

    # Pre-create conflicting batch_001.parquet in processed storage
    conflict_table = pa.Table.from_arrays(
        [pa.array([999], type=pa.int64()), pa.array(["Hacker"], type=pa.string())],
        names=["order_id", "customer_name"],
    )
    pq.write_table(conflict_table, str(proc_orders / "batch_001.parquet"))

    # Place two batches in landing: batch_001 and batch_002
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    # Assert loop did not abort: both batches were processed
    assert res.total_batches == 2
    assert len(res.batch_results) == 2

    # Batch 001 failed storage, Batch 002 succeeded
    b1_res = res.batch_results[0]
    b2_res = res.batch_results[1]
    assert b1_res.batch_id == "batch_001"
    assert b1_res.storage_status.value == "STORAGE_FAILED"
    assert b2_res.batch_id == "batch_002"
    assert b2_res.storage_status.value == "COMMITTED"

    # Overall status is PARTIAL_SUCCESS because batch_002 produced durable committed output
    assert res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert res.storage_failed_batches == 1
    assert res.committed_batches == 1
    assert res.error_message is not None
    assert "batch_001" in res.error_message


def test_orchestrator_corrupted_destination_file_contained(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_orders = tmp_path / "processed" / "orders"
    proc_orders.mkdir(parents=True)

    # Write corrupt destination file
    (proc_orders / "batch_001.parquet").write_bytes(b"CORRUPT_BYTES")
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.total_batches == 1
    assert res.batch_results[0].storage_status.value == "STORAGE_FAILED"
    assert "Corrupted Parquet file" in str(res.batch_results[0].error_message)
    assert res.status == PipelineExecutionStatus.FAILED


def test_orchestrator_audit_column_collision_contained(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    # Configure audit column name to collide with an existing business column 'order_id'
    cfg = {
        "storage": {
            "processed_dir": str(tmp_path / "processed"),
            "metadata_db_path": str(tmp_path / "meta.duckdb"),
        },
        "audit_metadata": {
            "add_technical_columns": True,
            "columns": {
                "source_file": "order_id",  # Collision!
                "run_id": "run_id",
                "ingested_at": "ingested_at",
            },
        },
    }

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    orchestrator.storage_manager.audit_columns_cfg = cfg["audit_metadata"]["columns"]

    res = orchestrator.run_pipeline(input_dir=str(landing))
    assert res.total_batches == 1
    assert res.batch_results[0].storage_status.value == "STORAGE_FAILED"
    assert "Collision detected" in str(res.batch_results[0].error_message)
    assert res.status == PipelineExecutionStatus.FAILED


def test_orchestrator_all_storage_operations_fail(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_orders = tmp_path / "processed" / "orders"
    proc_orders.mkdir(parents=True)

    # Pre-populate corrupt destinations for both files
    (proc_orders / "batch_001.parquet").write_bytes(b"CORRUPT_1")
    (proc_orders / "batch_002.parquet").write_bytes(b"CORRUPT_2")

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.total_batches == 2
    assert res.storage_failed_batches == 2
    assert res.status == PipelineExecutionStatus.FAILED
    assert res.total_valid_records > 0  # Valid records were parsed by BatchProcessor but failed storage


def test_orchestrator_all_quarantined_is_partial_success(tmp_path: Path):
    import decimal
    landing = tmp_path / "landing"
    landing.mkdir()

    # Create table where all rows have identical schema but negative amounts (100% invalid)
    t = pq.read_table("data/generated/batch_001_original.parquet")
    neg_amounts = pa.array([decimal.Decimal("-10.00")] * len(t), type=pa.decimal128(12, 2))
    f = pa.field("amount", pa.decimal128(12, 2), nullable=False)
    t_invalid = t.set_column(t.schema.get_field_index("amount"), f, neg_amounts)
    pq.write_table(t_invalid, str(landing / "batch_099_all_invalid.parquet"))

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.total_batches == 1
    assert res.total_valid_records == 0
    assert res.total_quarantined_records == len(t)
    # Status matrix rule: 100% invalid records with successful quarantine is PARTIAL_SUCCESS
    assert res.status == PipelineExecutionStatus.PARTIAL_SUCCESS

