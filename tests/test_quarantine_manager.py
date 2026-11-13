"""
Unit and integration tests for Quarantine Manager (Phase 4.2).

Validates:
1. Successful Parquet persistence of invalid_table.
2. Schema, type, and value fidelity (e.g. decimal, timestamp, nulls).
3. JSON metadata sidecar correctness and traceability.
4. Multiple failures per record correctly serialized in metadata.
5. Handling of zero invalid records (SKIPPED_NO_INVALID, no files created).
6. Idempotent retries with identical batch IDs (ALREADY_EXISTS).
7. Conflicting reuse of existing batch IDs (raises ValueError).
8. Atomic write failures & automatic cleanup of staging directories.
9. Rejection of unsafe path components & directory traversal.
10. Metadata serialization of Decimals, datetimes, and nulls without crashing.
11. Reading back persisted quarantine tables and metadata via manager methods.
"""

import os
import json
from decimal import Decimal
from datetime import datetime, timezone
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.validation.models import ValidationFailure
from src.validation.validator import DataValidator, ValidationResult, ValidationMetrics
from src.quarantine.quarantine_manager import QuarantineManager, QuarantineRecord


CUSTOMERS_PATH = "data/generated/customers.parquet"


@pytest.fixture
def validator():
    v = DataValidator()
    v.load_customer_master(CUSTOMERS_PATH)
    return v


@pytest.fixture
def quarantine_mgr(tmp_path):
    q_dir = tmp_path / "quarantine"
    return QuarantineManager(base_quarantine_dir=str(q_dir))


def test_quarantine_batch_006_duplicates(validator, quarantine_mgr):
    """
    1. Tests successful quarantine persistence of Batch 006 (50 duplicate order rows).
    """
    table = pq.read_table("data/generated/batch_006_duplicates.parquet")
    val_res = validator.validate_orders_table(table, source_file="batch_006_duplicates.parquet")

    assert val_res.metrics.invalid_records == 50
    record = quarantine_mgr.quarantine_validation_result(
        val_res,
        batch_id="batch_006_test",
        source_file="batch_006_duplicates.parquet",
    )

    assert record.status == "QUARANTINED"
    assert record.record_count == 50
    assert os.path.exists(record.parquet_path)
    assert os.path.exists(record.metadata_path)

    # 2. Verify schema and data preservation
    q_table = quarantine_mgr.load_quarantine_table("orders", "batch_006_test")
    assert len(q_table) == 50
    assert q_table.schema == table.schema
    assert q_table.column("amount").type == pa.decimal128(12, 2)

    # 3. Verify metadata sidecar
    meta = quarantine_mgr.load_quarantine_metadata("orders", "batch_006_test")
    assert meta["batch_id"] == "batch_006_test"
    assert meta["table_name"] == "orders"
    assert meta["source_file"] == "batch_006_duplicates.parquet"
    assert meta["invalid_record_count"] == 50
    assert meta["total_batch_records"] == 1000
    assert "DUPLICATE_KEY" in meta["failure_categories"]
    assert "order_id" in meta["affected_columns"]
    assert len(meta["failures"]) == 50


def test_quarantine_multiple_failures_per_record(quarantine_mgr):
    """
    4. Tests multiple failures associated with a single record preserved in metadata.
    """
    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=True),
        pa.field("customer_id", pa.int64(), nullable=True),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=True),
        pa.field("amount", pa.decimal128(12, 2), nullable=True),
        pa.field("status", pa.string(), nullable=True),
    ])
    invalid_tbl = pa.Table.from_arrays([
        pa.array([None], type=pa.int64()),
        pa.array([999990], type=pa.int64()),
        pa.array([datetime.now()], type=pa.timestamp("us")),
        pa.array([Decimal("-100.00")], type=pa.decimal128(12, 2)),
        pa.array([None], type=pa.string()),
    ], schema=schema)

    failures = [
        ValidationFailure(source_row_index=0, record_identifier="row_0", category="NULL_VIOLATION", column="order_id", reason="Missing PK", raw_record={"amount": Decimal("-100.00")}),
        ValidationFailure(source_row_index=0, record_identifier="row_0", category="INVALID_FOREIGN_KEY", column="customer_id", reason="Orphan FK", raw_record={"amount": Decimal("-100.00")}),
        ValidationFailure(source_row_index=0, record_identifier="row_0", category="RANGE_VIOLATION", column="amount", reason="Negative amount", raw_record={"amount": Decimal("-100.00")}),
        ValidationFailure(source_row_index=0, record_identifier="row_0", category="NULL_VIOLATION", column="status", reason="Missing status", raw_record={"amount": Decimal("-100.00")}),
    ]
    val_res = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=1, valid_records=0, invalid_records=1, null_violations=2, foreign_key_violations=1, range_violations=1),
        valid_table=invalid_tbl.slice(0, 0),
        invalid_table=invalid_tbl,
        failures=failures,
    )

    rec = quarantine_mgr.quarantine_validation_result(val_res, batch_id="multi_fail_batch")
    assert rec.status == "QUARANTINED"
    assert rec.record_count == 1

    meta = quarantine_mgr.load_quarantine_metadata("orders", "multi_fail_batch")
    assert meta["invalid_record_count"] == 1
    assert len(meta["failures"]) == 4
    assert set(meta["failure_categories"]) == {"NULL_VIOLATION", "INVALID_FOREIGN_KEY", "RANGE_VIOLATION"}
    assert set(meta["affected_columns"]) == {"order_id", "customer_id", "amount", "status"}


def test_quarantine_empty_invalid_records(validator, quarantine_mgr):
    """
    5. Handling batches with zero invalid records (Batch 001) returns SKIPPED_NO_INVALID without creating files.
    """
    table = pq.read_table("data/generated/batch_001_original.parquet")
    val_res = validator.validate_orders_table(table)

    rec = quarantine_mgr.quarantine_validation_result(val_res, batch_id="clean_batch_001")
    assert rec.status == "SKIPPED_NO_INVALID"
    assert rec.record_count == 0
    assert rec.parquet_path is None
    assert rec.metadata_path is None

    # Verify no folder or files were created
    target_dir = quarantine_mgr.get_batch_dir("orders", "clean_batch_001")
    assert not os.path.exists(target_dir)


def test_quarantine_idempotent_retry(validator, quarantine_mgr):
    """
    6. Identical re-submission of the same batch returns ALREADY_EXISTS idempotently.
    """
    table = pq.read_table("data/generated/batch_007_invalid_foreign_keys.parquet")
    val_res = validator.validate_orders_table(table)

    rec1 = quarantine_mgr.quarantine_validation_result(val_res, batch_id="batch_007_idempotent")
    assert rec1.status == "QUARANTINED"

    # Re-submit identical batch
    rec2 = quarantine_mgr.quarantine_validation_result(val_res, batch_id="batch_007_idempotent")
    assert rec2.status == "ALREADY_EXISTS"
    assert rec2.record_count == rec1.record_count
    assert rec2.parquet_path == rec1.parquet_path


def test_quarantine_conflicting_batch_id_rejected(validator, quarantine_mgr):
    """
    7. Conflicting reuse of existing batch ID with different records raises ValueError.
    """
    tbl6 = pq.read_table("data/generated/batch_006_duplicates.parquet")
    val6 = validator.validate_orders_table(tbl6)

    # First write
    quarantine_mgr.quarantine_validation_result(val6, batch_id="conflict_batch_id")

    # Second write with a different table payload under same batch_id
    tbl7 = pq.read_table("data/generated/batch_007_invalid_foreign_keys.parquet")
    val7 = validator.validate_orders_table(tbl7)

    # Modify invalid table slice to create a count conflict
    val7.invalid_table = val7.invalid_table.slice(0, 10)
    with pytest.raises(ValueError, match="Conflicting reuse of batch_id 'conflict_batch_id'"):
        quarantine_mgr.quarantine_validation_result(val7, batch_id="conflict_batch_id")


def test_quarantine_atomic_write_failure_cleanup(quarantine_mgr, monkeypatch):
    """
    8. Simulates write failure during staging; verifies cleanup of temporary artifacts.
    """
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = pa.Table.from_arrays([pa.array([1, 2])], schema=schema)
    val_res = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=2, invalid_records=2),
        invalid_table=tbl,
        failures=[ValidationFailure(source_row_index=0, record_identifier="1", category="ERR", column="id", reason="test", raw_record={})],
    )

    # Monkeypatch json.dump to raise an IOError during write
    def mock_json_dump(*args, **kwargs):
        raise IOError("Disk full simulation")

    monkeypatch.setattr(json, "dump", mock_json_dump)

    with pytest.raises(IOError, match="Failed to atomically persist quarantine batch"):
        quarantine_mgr.quarantine_validation_result(val_res, batch_id="atomic_fail_batch")

    # Ensure final destination does not exist
    dest_dir = quarantine_mgr.get_batch_dir("orders", "atomic_fail_batch")
    assert not os.path.exists(dest_dir)


def test_quarantine_unsafe_path_rejection(quarantine_mgr):
    """
    9. Path traversal or illegal path components are rejected.
    """
    val_res = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=1, invalid_records=1),
        invalid_table=pa.Table.from_arrays([pa.array([1])], names=["id"]),
        failures=[ValidationFailure(source_row_index=0, record_identifier="1", category="ERR", column="id", reason="test", raw_record={})],
    )

    with pytest.raises(ValueError, match="Unsafe path component detected"):
        quarantine_mgr.quarantine_validation_result(val_res, batch_id="../../escape")

    with pytest.raises(ValueError, match="Unsafe path component detected"):
        quarantine_mgr.quarantine_validation_result(val_res, batch_id="batch/slash")

    val_res.table_name = "orders/bad"
    with pytest.raises(ValueError, match="Unsafe path component detected"):
        quarantine_mgr.quarantine_validation_result(val_res, batch_id="valid_batch")


def test_quarantine_json_encoder_types():
    """
    10. Verifies QuarantineJSONEncoder safely handles Decimals and datetimes.
    """
    from src.quarantine.quarantine_manager import QuarantineJSONEncoder

    data = {
        "amount": Decimal("123.45"),
        "timestamp": datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc),
        "none_val": None,
        "integer": 42,
    }
    encoded = json.dumps(data, cls=QuarantineJSONEncoder)
    decoded = json.loads(encoded)
    assert decoded["amount"] == "123.45"
    assert "2026-11-13T12:00:00" in decoded["timestamp"]
    assert decoded["none_val"] is None


def test_quarantine_fingerprint_identical_batches(quarantine_mgr):
    """
    11. Fingerprint test: Identical batches produce identical fingerprint and return ALREADY_EXISTS.
    """
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("amount", pa.decimal128(10, 2)),
        pa.field("note", pa.string()),
    ])
    tbl = pa.Table.from_arrays([
        pa.array([1, 2], type=pa.int64()),
        pa.array([Decimal("19.99"), Decimal("45.50")], type=pa.decimal128(10, 2)),
        pa.array(["err1", "err2"], type=pa.string()),
    ], schema=schema)

    val_res = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=2, invalid_records=2),
        invalid_table=tbl,
        failures=[],
    )

    rec1 = quarantine_mgr.quarantine_validation_result(val_res, batch_id="fp_identical")
    assert rec1.status == "QUARANTINED"

    meta1 = quarantine_mgr.load_quarantine_metadata("orders", "fp_identical")
    assert "fingerprint" in meta1
    assert len(meta1["fingerprint"]) == 64

    # Second submission identical
    rec2 = quarantine_mgr.quarantine_validation_result(val_res, batch_id="fp_identical")
    assert rec2.status == "ALREADY_EXISTS"


def test_quarantine_fingerprint_same_count_different_values(quarantine_mgr):
    """
    12. Fingerprint test: Same row count but different values raises ValueError on reuse of batch_id.
    """
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("amount", pa.decimal128(10, 2)),
    ])
    tbl1 = pa.Table.from_arrays([
        pa.array([1, 2], type=pa.int64()),
        pa.array([Decimal("10.00"), Decimal("20.00")], type=pa.decimal128(10, 2)),
    ], schema=schema)
    tbl2 = pa.Table.from_arrays([
        pa.array([1, 2], type=pa.int64()),
        pa.array([Decimal("10.00"), Decimal("99.99")], type=pa.decimal128(10, 2)),  # different value
    ], schema=schema)

    val_res1 = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=2, invalid_records=2),
        invalid_table=tbl1,
        failures=[],
    )
    val_res2 = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=2, invalid_records=2),
        invalid_table=tbl2,
        failures=[],
    )

    quarantine_mgr.quarantine_validation_result(val_res1, batch_id="fp_same_count_diff_val")

    with pytest.raises(ValueError, match="Conflicting reuse of batch_id 'fp_same_count_diff_val'"):
        quarantine_mgr.quarantine_validation_result(val_res2, batch_id="fp_same_count_diff_val")


def test_quarantine_fingerprint_same_values_different_schema(quarantine_mgr):
    """
    13. Fingerprint test: Same numerical values but different schema (e.g. int32 vs int64, metadata) raises ValueError.
    """
    tbl_int64 = pa.Table.from_arrays([
        pa.array([1, 2], type=pa.int64())
    ], names=["id"])
    tbl_int32 = pa.Table.from_arrays([
        pa.array([1, 2], type=pa.int32())
    ], names=["id"])

    val_res1 = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=2, invalid_records=2),
        invalid_table=tbl_int64,
        failures=[],
    )
    val_res2 = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=2, invalid_records=2),
        invalid_table=tbl_int32,
        failures=[],
    )

    quarantine_mgr.quarantine_validation_result(val_res1, batch_id="fp_diff_schema")

    with pytest.raises(ValueError, match="Conflicting reuse of batch_id 'fp_diff_schema'"):
        quarantine_mgr.quarantine_validation_result(val_res2, batch_id="fp_diff_schema")


def test_quarantine_fingerprint_different_row_ordering(quarantine_mgr):
    """
    14. Fingerprint test: Same records but different row ordering produces a different fingerprint and raises ValueError.
    """
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl_order_a = pa.Table.from_arrays([pa.array([1, 2], type=pa.int64())], schema=schema)
    tbl_order_b = pa.Table.from_arrays([pa.array([2, 1], type=pa.int64())], schema=schema)

    val_res1 = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=2, invalid_records=2),
        invalid_table=tbl_order_a,
        failures=[],
    )
    val_res2 = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=2, invalid_records=2),
        invalid_table=tbl_order_b,
        failures=[],
    )

    quarantine_mgr.quarantine_validation_result(val_res1, batch_id="fp_diff_ordering")

    with pytest.raises(ValueError, match="Conflicting reuse of batch_id 'fp_diff_ordering'"):
        quarantine_mgr.quarantine_validation_result(val_res2, batch_id="fp_diff_ordering")


def test_quarantine_fingerprint_null_and_decimal_precision(quarantine_mgr):
    """
    15. Fingerprint test: Sensitive to null placement and exact decimal scale/precision.
    """
    # Null placement sensitivity
    schema_nulls = pa.schema([pa.field("val", pa.int64(), nullable=True)])
    tbl_null_first = pa.Table.from_arrays([pa.array([None, 1], type=pa.int64())], schema=schema_nulls)
    tbl_null_second = pa.Table.from_arrays([pa.array([1, None], type=pa.int64())], schema=schema_nulls)

    fp1 = QuarantineManager.compute_table_fingerprint(tbl_null_first)
    fp2 = QuarantineManager.compute_table_fingerprint(tbl_null_second)
    assert fp1 != fp2

    # Decimal precision/scale sensitivity
    schema_d1 = pa.schema([pa.field("amount", pa.decimal128(10, 2))])
    schema_d2 = pa.schema([pa.field("amount", pa.decimal128(12, 4))])

    tbl_d1 = pa.Table.from_arrays([pa.array([Decimal("10.50")], type=pa.decimal128(10, 2))], schema=schema_d1)
    tbl_d2 = pa.Table.from_arrays([pa.array([Decimal("10.5000")], type=pa.decimal128(12, 4))], schema=schema_d2)

    fp_d1 = QuarantineManager.compute_table_fingerprint(tbl_d1)
    fp_d2 = QuarantineManager.compute_table_fingerprint(tbl_d2)
    assert fp_d1 != fp_d2


def test_quarantine_existing_metadata_missing_fingerprint_fails_safely(quarantine_mgr):
    """
    16. Fingerprint test: When existing metadata lacks a valid fingerprint, fail safely with ValueError.
    """
    schema = pa.schema([pa.field("id", pa.int64())])
    tbl = pa.Table.from_arrays([pa.array([1], type=pa.int64())], schema=schema)
    val_res = ValidationResult(
        table_name="orders",
        is_valid_batch=False,
        metrics=ValidationMetrics(total_records=1, invalid_records=1),
        invalid_table=tbl,
        failures=[],
    )

    batch_dir = quarantine_mgr.get_batch_dir("orders", "legacy_batch")
    os.makedirs(batch_dir, exist_ok=True)
    meta_path = os.path.join(batch_dir, "quarantine_metadata.json")
    
    # Write legacy metadata without "fingerprint"
    legacy_meta = {
        "batch_id": "legacy_batch",
        "table_name": "orders",
        "invalid_record_count": 1,
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(legacy_meta, f)

    with pytest.raises(ValueError, match="lacks a valid SHA-256 fingerprint"):
        quarantine_mgr.quarantine_validation_result(val_res, batch_id="legacy_batch")

