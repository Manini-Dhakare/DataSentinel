"""
Unit tests for SchemaDetector (Phase 3.1).

Tests focused scenarios against the original baseline schema in config/source_schema.yaml:
- Batch 001: Exact match, no differences.
- Batch 002: currency appears in added_columns.
- Batch 003: currency and sales_channel appear in added_columns.
- Batch 004: amount appears in type_mismatches.
- Batch 005: order_id appears in missing_columns.
- Batch 009: Empty Parquet file matching baseline schema.
- Error handling for missing files and invalid configurations.
"""

import os
import pytest
from src.schema.schema_detector import inspect_schema, SchemaDetector, normalize_type_str


BASE_DIR = "data/generated"


def test_normalize_type_str():
    assert normalize_type_str("decimal128(12, 2)") == "decimal(12,2)"
    assert normalize_type_str("decimal(12, 2)") == "decimal(12,2)"
    assert normalize_type_str("timestamp[us, tz=UTC]") == "timestamp[us]"
    assert normalize_type_str("int64") == "int64"
    assert normalize_type_str("string") == "string"


def test_batch_001_exact_match():
    """
    Batch 001 matches the original baseline schema exactly.
    Expected: is_exact_match is True, no added/missing/mismatch columns.
    """
    file_path = os.path.join(BASE_DIR, "batch_001_original.parquet")
    result = inspect_schema(file_path=file_path, table_name="orders")

    assert result.error is None
    assert result.row_count == 1000
    assert result.is_exact_match is True
    assert len(result.detected_columns) == 5
    assert len(result.added_columns) == 0
    assert len(result.missing_columns) == 0
    assert len(result.type_mismatches) == 0
    assert len(result.nullability_differences) == 0

    det_names = [c.name for c in result.detected_columns]
    assert det_names == ["order_id", "customer_id", "order_timestamp", "amount", "status"]


def test_batch_002_add_currency():
    """
    Batch 002 introduces currency.
    Expected: currency appears in added_columns, no missing columns or type mismatches.
    """
    file_path = os.path.join(BASE_DIR, "batch_002_add_currency.parquet")
    result = inspect_schema(file_path=file_path, table_name="orders")

    assert result.error is None
    assert result.row_count == 1000
    assert result.is_exact_match is False
    assert len(result.missing_columns) == 0
    assert len(result.type_mismatches) == 0
    assert len(result.nullability_differences) == 0

    added_names = [c.name for c in result.added_columns]
    assert added_names == ["currency"]
    assert result.added_columns[0].data_type == "string"
    assert result.added_columns[0].nullable is True


def test_batch_003_add_sales_channel():
    """
    Batch 003 introduces both currency and sales_channel on top of baseline.
    Expected: currency and sales_channel appear in added_columns.
    """
    file_path = os.path.join(BASE_DIR, "batch_003_add_sales_channel.parquet")
    result = inspect_schema(file_path=file_path, table_name="orders")

    assert result.error is None
    assert result.row_count == 1000
    assert result.is_exact_match is False
    assert len(result.missing_columns) == 0
    assert len(result.type_mismatches) == 0
    assert len(result.nullability_differences) == 0

    added_names = [c.name for c in result.added_columns]
    assert set(added_names) == {"currency", "sales_channel"}


def test_batch_004_breaking_type_change():
    """
    Batch 004 has amount stored as string instead of decimal(12,2).
    Expected: amount appears in type_mismatches.
    """
    file_path = os.path.join(BASE_DIR, "batch_004_breaking_type.parquet")
    result = inspect_schema(file_path=file_path, table_name="orders")

    assert result.error is None
    assert result.row_count == 1000
    assert result.is_exact_match is False
    assert len(result.added_columns) == 0
    assert len(result.missing_columns) == 0
    assert len(result.type_mismatches) == 1

    mismatch = result.type_mismatches[0]
    assert mismatch.column == "amount"
    assert mismatch.expected_type == "decimal(12,2)"
    assert mismatch.detected_type == "string"


def test_batch_005_missing_order_id():
    """
    Batch 005 omits order_id.
    Expected: order_id appears in missing_columns.
    """
    file_path = os.path.join(BASE_DIR, "batch_005_missing_order_id.parquet")
    result = inspect_schema(file_path=file_path, table_name="orders")

    assert result.error is None
    assert result.row_count == 1000
    assert result.is_exact_match is False
    assert len(result.added_columns) == 0
    assert len(result.type_mismatches) == 0

    missing_names = [c.name for c in result.missing_columns]
    assert missing_names == ["order_id"]
    assert result.missing_columns[0].data_type == "int64"
    assert result.missing_columns[0].nullable is False


def test_batch_009_empty_parquet():
    """
    Batch 009 is an empty Parquet file with 0 rows matching original baseline schema.
    Expected: is_exact_match is True, 0 rows, 5 detected columns.
    """
    file_path = os.path.join(BASE_DIR, "batch_009_empty.parquet")
    result = inspect_schema(file_path=file_path, table_name="orders")

    assert result.error is None
    assert result.row_count == 0
    assert result.is_exact_match is True
    assert len(result.detected_columns) == 5
    assert len(result.added_columns) == 0
    assert len(result.missing_columns) == 0
    assert len(result.type_mismatches) == 0


def test_error_handling_nonexistent_file():
    """
    Detector safely returns an error result when file does not exist.
    """
    result = inspect_schema("data/generated/non_existent_file.parquet")
    assert result.error is not None
    assert "File not found" in result.error
    assert result.is_exact_match is False


def test_error_handling_invalid_config():
    """
    Detector handles missing configuration gracefully.
    """
    detector = SchemaDetector(config_path="config/non_existent_config.yaml")
    result = detector.inspect_parquet("data/generated/batch_001_original.parquet")
    assert result.error is not None
    assert "Configuration error" in result.error
