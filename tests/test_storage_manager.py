"""Tests for StorageManager in Phase 5.0."""

import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.storage.storage_manager import (
    StorageManager,
    StorageRecord,
    StorageStatus,
)


def _sample_table() -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array([101, 102, 103], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            pa.array([10.5, 20.0, 35.75], type=pa.float64()),
        ],
        names=["order_id", "customer_name", "amount"],
    )


def test_storage_manager_init(tmp_path: Path):
    config = {
        "storage": {
            "processed_dir": str(tmp_path / "processed"),
            "staging_dir": str(tmp_path / "staging"),
        },
        "audit_metadata": {
            "add_technical_columns": True,
        },
    }
    sm = StorageManager(pipeline_config=config)
    assert sm.base_processed_dir == str((tmp_path / "processed").resolve())
    assert sm.add_technical_columns is True


def test_save_empty_table_returns_skipped(tmp_path: Path):
    sm = StorageManager(base_processed_dir=str(tmp_path / "processed"))
    empty_table = pa.Table.from_arrays(
        [pa.array([], type=pa.int64()), pa.array([], type=pa.string())],
        names=["id", "name"],
    )
    rec = sm.persist_valid_records(
        batch_id="batch_009",
        valid_table=empty_table,
        run_id="run_001",
        source_file="batch_009_empty.parquet",
    )
    assert rec.status == StorageStatus.SKIPPED
    assert rec.record_count == 0
    assert rec.parquet_path is None


def test_save_none_table_returns_skipped(tmp_path: Path):
    sm = StorageManager(base_processed_dir=str(tmp_path / "processed"))
    rec = sm.persist_valid_records(
        batch_id="batch_none",
        valid_table=None,
        run_id="run_001",
    )
    assert rec.status == StorageStatus.SKIPPED
    assert rec.record_count == 0


def test_successful_save_with_audit_columns(tmp_path: Path):
    cfg = {
        "storage": {"processed_dir": str(tmp_path / "processed")},
        "audit_metadata": {"add_technical_columns": True},
    }
    sm = StorageManager(pipeline_config=cfg)
    table = _sample_table()

    rec = sm.persist_valid_records(
        batch_id="batch_001",
        valid_table=table,
        run_id="run_20261113",
        source_file="batch_001.parquet",
    )

    assert rec.status == StorageStatus.COMMITTED
    assert rec.record_count == 3
    assert rec.parquet_path is not None

    parquet_file = Path(rec.parquet_path)
    assert parquet_file.exists()

    # Read back parquet and verify audit columns
    stored_table = pq.read_table(parquet_file)
    assert "source_file" in stored_table.column_names
    assert "run_id" in stored_table.column_names
    assert "ingested_at" in stored_table.column_names
    assert stored_table["source_file"].to_pylist() == ["batch_001.parquet"] * 3
    assert stored_table["run_id"].to_pylist() == ["run_20261113"] * 3
    assert len(stored_table) == 3


def test_idempotent_save_returns_already_processed(tmp_path: Path):
    sm = StorageManager(base_processed_dir=str(tmp_path / "processed"))
    table = _sample_table()

    # First write
    rec1 = sm.persist_valid_records(
        batch_id="batch_002",
        valid_table=table,
        run_id="run_1",
    )
    assert rec1.status == StorageStatus.COMMITTED

    # Identical second write -> ALREADY_PROCESSED
    rec2 = sm.persist_valid_records(
        batch_id="batch_002",
        valid_table=table,
        run_id="run_2",
    )
    assert rec2.status == StorageStatus.ALREADY_PROCESSED
    assert rec2.fingerprint == rec1.fingerprint
    assert rec2.record_count == 3
    assert rec2.parquet_path == rec1.parquet_path


def test_conflicting_save_returns_storage_failed(tmp_path: Path):
    sm = StorageManager(base_processed_dir=str(tmp_path / "processed"))
    table1 = _sample_table()
    rec1 = sm.persist_valid_records(
        batch_id="batch_003",
        valid_table=table1,
    )
    assert rec1.status == StorageStatus.COMMITTED

    # Different table with same batch_id -> returns STORAGE_FAILED
    table2 = pa.Table.from_arrays(
        [pa.array([999], type=pa.int64()), pa.array(["Hacker"], type=pa.string()), pa.array([0.0], type=pa.float64())],
        names=["order_id", "customer_name", "amount"],
    )
    rec2 = sm.persist_valid_records(
        batch_id="batch_003",
        valid_table=table2,
    )
    assert rec2.status == StorageStatus.STORAGE_FAILED
    assert "Conflicting reuse" in str(rec2.error_message)


def test_audit_column_collision_detection(tmp_path: Path):
    cfg = {
        "storage": {"processed_dir": str(tmp_path / "processed")},
        "audit_metadata": {"add_technical_columns": True},
    }
    sm = StorageManager(pipeline_config=cfg)
    table_with_audit = pa.Table.from_arrays(
        [
            pa.array([1]),
            pa.array(["existing_run"]),
        ],
        names=["id", "run_id"],
    )
    rec = sm.persist_valid_records(
        batch_id="batch_collision",
        valid_table=table_with_audit,
    )
    assert rec.status == StorageStatus.STORAGE_FAILED
    assert "collision" in str(rec.error_message).lower()


def test_disabled_technical_columns(tmp_path: Path):
    cfg = {
        "storage": {"processed_dir": str(tmp_path / "processed")},
        "audit_metadata": {"add_technical_columns": False},
    }
    sm = StorageManager(pipeline_config=cfg)
    table = _sample_table()
    rec = sm.persist_valid_records(batch_id="batch_no_tech", valid_table=table)
    assert rec.status == StorageStatus.COMMITTED

    stored_table = pq.read_table(rec.parquet_path)
    assert "source_file" not in stored_table.column_names
    assert "run_id" not in stored_table.column_names
    assert "ingested_at" not in stored_table.column_names


def test_canonicalize_table_for_fingerprint():
    # Table with custom schemaguard metadata
    schema = pa.schema(
        [pa.field("a", pa.int64())],
        metadata={b"schemaguard_test": b"val", b"custom_meta": b"stay"},
    )
    tbl = pa.Table.from_arrays([pa.array([1, 2, 3])], schema=schema)
    canon = StorageManager.canonicalize_table_for_fingerprint(tbl, audit_column_names=set())
    assert b"schemaguard_test" not in canon.schema.metadata
    assert b"custom_meta" in canon.schema.metadata


# --- Issue-02 & Defect-01 targeted tests ---

def test_drift_file_with_audit_verified_with_audit_disabled(tmp_path: Path):
    proc_dir = str(tmp_path / "processed")
    sm_enabled = StorageManager(
        base_processed_dir=proc_dir,
        pipeline_config={"audit_metadata": {"add_technical_columns": True}},
    )
    table = _sample_table()
    rec1 = sm_enabled.persist_valid_records(batch_id="batch_drift_1", valid_table=table)
    assert rec1.status == StorageStatus.COMMITTED

    # Re-verify using a manager where technical audit columns are disabled in config
    sm_disabled = StorageManager(
        base_processed_dir=proc_dir,
        pipeline_config={"audit_metadata": {"add_technical_columns": False}},
    )
    rec2 = sm_disabled.persist_valid_records(batch_id="batch_drift_1", valid_table=table)
    assert rec2.status == StorageStatus.ALREADY_PROCESSED
    assert rec2.fingerprint == rec1.fingerprint


def test_drift_file_without_audit_verified_with_audit_enabled(tmp_path: Path):
    proc_dir = str(tmp_path / "processed")
    sm_disabled = StorageManager(
        base_processed_dir=proc_dir,
        pipeline_config={"audit_metadata": {"add_technical_columns": False}},
    )
    table = _sample_table()
    rec1 = sm_disabled.persist_valid_records(batch_id="batch_drift_2", valid_table=table)
    assert rec1.status == StorageStatus.COMMITTED

    # Re-verify using a manager where technical audit columns are enabled in config
    sm_enabled = StorageManager(
        base_processed_dir=proc_dir,
        pipeline_config={"audit_metadata": {"add_technical_columns": True}},
    )
    rec2 = sm_enabled.persist_valid_records(batch_id="batch_drift_2", valid_table=table)
    assert rec2.status == StorageStatus.ALREADY_PROCESSED
    assert rec2.fingerprint == rec1.fingerprint


def test_drift_genuine_business_data_change_fails(tmp_path: Path):
    proc_dir = str(tmp_path / "processed")
    sm = StorageManager(base_processed_dir=proc_dir)
    table1 = _sample_table()
    rec1 = sm.persist_valid_records(batch_id="batch_drift_3", valid_table=table1)
    assert rec1.status == StorageStatus.COMMITTED

    # Same schema but different business amounts
    table_diff = pa.Table.from_arrays(
        [
            pa.array([101, 102, 103], type=pa.int64()),
            pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
            pa.array([999.0, 888.0, 777.0], type=pa.float64()),
        ],
        names=["order_id", "customer_name", "amount"],
    )
    rec2 = sm.persist_valid_records(batch_id="batch_drift_3", valid_table=table_diff)
    assert rec2.status == StorageStatus.STORAGE_FAILED
    assert "Conflicting reuse" in str(rec2.error_message)


def test_drift_business_metadata_preserved(tmp_path: Path):
    proc_dir = str(tmp_path / "processed")
    sm = StorageManager(base_processed_dir=proc_dir)
    schema = pa.schema(
        [
            pa.field("order_id", pa.int64()),
            pa.field("customer_name", pa.string()),
            pa.field("amount", pa.float64()),
        ],
        metadata={b"domain": b"retail", b"classification": b"internal"},
    )
    table = pa.Table.from_arrays(
        [
            pa.array([101], type=pa.int64()),
            pa.array(["Alice"], type=pa.string()),
            pa.array([10.5], type=pa.float64()),
        ],
        schema=schema,
    )
    rec1 = sm.persist_valid_records(batch_id="batch_meta_1", valid_table=table)
    assert rec1.status == StorageStatus.COMMITTED

    # Re-verify identical table with business metadata
    rec2 = sm.persist_valid_records(batch_id="batch_meta_1", valid_table=table)
    assert rec2.status == StorageStatus.ALREADY_PROCESSED


def test_corrupted_existing_parquet_returns_storage_failed(tmp_path: Path):
    proc_dir = tmp_path / "processed" / "orders"
    proc_dir.mkdir(parents=True)
    bad_file = proc_dir / "batch_corrupt.parquet"
    bad_file.write_bytes(b"CORRUPTED_PARQUET_HEADER_DATA")

    sm = StorageManager(base_processed_dir=str(tmp_path / "processed"))
    table = _sample_table()
    rec = sm.persist_valid_records(batch_id="batch_corrupt", valid_table=table)
    assert rec.status == StorageStatus.STORAGE_FAILED
    assert "Corrupted Parquet file" in str(rec.error_message)


def test_existing_parquet_missing_metadata_returns_storage_failed(tmp_path: Path):
    proc_dir = tmp_path / "processed" / "orders"
    proc_dir.mkdir(parents=True)
    plain_file = proc_dir / "batch_no_meta.parquet"
    table = _sample_table()
    # Write plain parquet without SchemaGuard metadata
    pq.write_table(table, str(plain_file))

    sm = StorageManager(base_processed_dir=str(tmp_path / "processed"))
    rec = sm.persist_valid_records(batch_id="batch_no_meta", valid_table=table)
    assert rec.status == StorageStatus.STORAGE_FAILED
    assert "lacks valid SchemaGuard embedded metadata" in str(rec.error_message)
