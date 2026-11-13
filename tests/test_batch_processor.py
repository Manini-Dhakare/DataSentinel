"""
Unit and integration tests for BatchProcessor (Phase 4.3).

Tests:
1. Batch 001 (Original Baseline): SUCCESS, 0 quarantined, valid_table equals input.
2. Batch 002 (Compatible Evolution - add currency): SUCCESS, evolved schema applied, valid_table has currency.
3. Batch 003 (Compatible Evolution - add sales_channel): SUCCESS, evolved schema applied.
4. Batch 004 (Breaking Evolution - decimal to string): SCHEMA_REJECTED before validation/quarantine.
5. Batch 005 (Breaking Evolution - missing order_id): SCHEMA_REJECTED before validation/quarantine.
6. Batch 006 (Duplicate PKs): PARTIAL_SUCCESS, 950 valid, 50 quarantined.
7. Batch 007 (Invalid Foreign Keys): PARTIAL_SUCCESS, 970 valid, 30 quarantined.
8. Batch 008 (Null Violations): PARTIAL_SUCCESS, 960 valid, 40 quarantined.
9. Batch 009 (Empty Batch): SUCCESS, 0 records, 0 quarantined, status SKIPPED_NO_INVALID.
10. All-invalid batch (e.g. batch with missing required column at validation time or 100% invalid FKs): PARTIAL_SUCCESS, 0 valid, N invalid.
11. Quarantine persistence failure prevents SUCCESS and marks batch as FAILED.
12. Idempotent reprocessing with identical batch ID succeeds and preserves quarantine status ALREADY_EXISTS.
13. Conflicting reuse of batch ID raises error or marks batch FAILED.
14. Non-existent file marks batch FAILED.
"""

import os
import shutil
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion.batch_processor import (
    BatchProcessor,
    BatchProcessingResult,
    BatchProcessingStatus,
)
from src.quarantine.quarantine_manager import QuarantineRecord


@pytest.fixture
def processor(tmp_path):
    q_dir = tmp_path / "quarantine"
    return BatchProcessor(
        schema_config_path="config/source_schema.yaml",
        evolution_rules_path="config/evolution_rules.yaml",
        pipeline_config_path="config/pipeline_config.yaml",
        customer_master_path="data/generated/customers.parquet",
        base_quarantine_dir=str(q_dir),
    )


def test_process_batch_001_original(processor):
    """
    Batch 001: Baseline schema, all 1000 records valid.
    Expect: SUCCESS, 1000 valid, 0 invalid, quarantine SKIPPED_NO_INVALID.
    """
    res = processor.process_batch(
        file_path="data/generated/batch_001_original.parquet",
        batch_id="batch_001_test",
    )

    assert res.status == BatchProcessingStatus.SUCCESS
    assert res.row_count == 1000
    assert res.valid_record_count == 1000
    assert res.invalid_record_count == 0
    assert res.valid_table is not None
    assert len(res.valid_table) == 1000
    assert res.quarantine is not None
    assert res.quarantine.status == "SKIPPED_NO_INVALID"
    assert res.error_message is None


def test_process_batch_002_compatible_currency(processor):
    """
    Batch 002: Adds nullable 'currency' column.
    Expect: SUCCESS, schema evolved, 1000 valid records with currency field.
    """
    res = processor.process_batch(
        file_path="data/generated/batch_002_add_currency.parquet",
        batch_id="batch_002_test",
    )

    assert res.status == BatchProcessingStatus.SUCCESS
    assert res.row_count == 1000
    assert res.valid_record_count == 1000
    assert res.invalid_record_count == 0
    assert res.schema_evolution is not None
    assert res.schema_evolution.is_evolved is True
    assert "currency" in res.valid_table.column_names
    assert res.quarantine.status == "SKIPPED_NO_INVALID"


def test_process_batch_003_compatible_sales_channel(processor):
    """
    Batch 003: Adds nullable 'sales_channel' column.
    Expect: SUCCESS, schema evolved, 1000 valid records.
    """
    res = processor.process_batch(
        file_path="data/generated/batch_003_add_sales_channel.parquet",
        batch_id="batch_003_test",
    )

    assert res.status == BatchProcessingStatus.SUCCESS
    assert res.row_count == 1000
    assert res.valid_record_count == 1000
    assert res.invalid_record_count == 0
    assert res.schema_evolution.is_evolved is True
    assert "sales_channel" in res.valid_table.column_names


def test_process_batch_004_breaking_type_change(processor, monkeypatch):
    """
    Batch 004: Changes amount from decimal to string.
    Expect: SCHEMA_REJECTED before validation or quarantine.
    Uses spies to verify validator and quarantine are never called.
    """
    validate_spy = False
    quarantine_spy = False

    def spy_validate(*args, **kwargs):
        nonlocal validate_spy
        validate_spy = True
        return None

    def spy_quarantine(*args, **kwargs):
        nonlocal quarantine_spy
        quarantine_spy = True
        return None

    monkeypatch.setattr(processor.validator, "validate_orders_table", spy_validate)
    monkeypatch.setattr(processor.quarantine_manager, "quarantine_validation_result", spy_quarantine)

    res = processor.process_batch(
        file_path="data/generated/batch_004_breaking_type.parquet",
        batch_id="batch_004_test",
    )

    assert res.status == BatchProcessingStatus.SCHEMA_REJECTED
    assert res.schema_evolution is not None
    assert res.schema_evolution.status == "REJECTED"
    assert res.validation is None
    assert res.quarantine is None
    assert res.valid_table is None
    assert "BREAKING" in res.error_message
    assert validate_spy is False
    assert quarantine_spy is False


def test_process_batch_005_breaking_missing_column(processor, monkeypatch):
    """
    Batch 005: Missing required 'order_id' column.
    Expect: SCHEMA_REJECTED before validation or quarantine.
    Uses spies to verify validator and quarantine are never called.
    """
    validate_spy = False
    quarantine_spy = False

    def spy_validate(*args, **kwargs):
        nonlocal validate_spy
        validate_spy = True
        return None

    def spy_quarantine(*args, **kwargs):
        nonlocal quarantine_spy
        quarantine_spy = True
        return None

    monkeypatch.setattr(processor.validator, "validate_orders_table", spy_validate)
    monkeypatch.setattr(processor.quarantine_manager, "quarantine_validation_result", spy_quarantine)

    res = processor.process_batch(
        file_path="data/generated/batch_005_missing_order_id.parquet",
        batch_id="batch_005_test",
    )

    assert res.status == BatchProcessingStatus.SCHEMA_REJECTED
    assert res.schema_evolution is not None
    assert res.schema_evolution.status == "REJECTED"
    assert res.validation is None
    assert res.quarantine is None
    assert res.valid_table is None
    assert "BREAKING" in res.error_message
    assert validate_spy is False
    assert quarantine_spy is False


def test_process_batch_006_duplicates_partial_success(processor):
    """
    Batch 006: Contains 50 duplicate order_ids.
    Expect: PARTIAL_SUCCESS, 950 valid records, 50 quarantined records.
    """
    res = processor.process_batch(
        file_path="data/generated/batch_006_duplicates.parquet",
        batch_id="batch_006_test",
    )

    assert res.status == BatchProcessingStatus.PARTIAL_SUCCESS
    assert res.row_count == 1000
    assert res.valid_record_count == 950
    assert res.invalid_record_count == 50
    assert len(res.valid_table) == 950
    assert res.quarantine is not None
    assert res.quarantine.status == "QUARANTINED"
    assert res.quarantine.record_count == 50
    assert os.path.exists(res.quarantine.parquet_path)
    assert os.path.exists(res.quarantine.metadata_path)


def test_process_batch_007_invalid_foreign_keys_partial_success(processor):
    """
    Batch 007: Contains orphan customer_ids.
    Expect: PARTIAL_SUCCESS, 950 valid records, 50 quarantined records.
    """
    res = processor.process_batch(
        file_path="data/generated/batch_007_invalid_foreign_keys.parquet",
        batch_id="batch_007_test",
    )

    assert res.status == BatchProcessingStatus.PARTIAL_SUCCESS
    assert res.row_count == 1000
    assert res.valid_record_count == 950
    assert res.invalid_record_count == 50
    assert res.quarantine.status == "QUARANTINED"
    assert res.quarantine.record_count == 50


def test_process_batch_008_null_violations_schema_rejected(processor, monkeypatch):
    """
    Batch 008: Physical Parquet relaxes non-nullable columns to nullable=True.
    Under evolution_rules.yaml, nullability relaxation of required fields is classified as BREAKING.
    Expect: SCHEMA_REJECTED before validation or quarantine.
    Uses spies to verify validator and quarantine are never called.
    """
    validate_spy = False
    quarantine_spy = False

    def spy_validate(*args, **kwargs):
        nonlocal validate_spy
        validate_spy = True
        return None

    def spy_quarantine(*args, **kwargs):
        nonlocal quarantine_spy
        quarantine_spy = True
        return None

    monkeypatch.setattr(processor.validator, "validate_orders_table", spy_validate)
    monkeypatch.setattr(processor.quarantine_manager, "quarantine_validation_result", spy_quarantine)

    res = processor.process_batch(
        file_path="data/generated/batch_008_null_violations.parquet",
        batch_id="batch_008_test",
    )

    assert res.status == BatchProcessingStatus.SCHEMA_REJECTED
    assert res.schema_evolution is not None
    assert res.schema_evolution.status == "REJECTED"
    assert "BREAKING" in res.error_message
    assert res.validation is None
    assert res.quarantine is None
    assert res.valid_table is None
    assert validate_spy is False
    assert quarantine_spy is False


def test_process_batch_009_empty_batch(processor):
    """
    Batch 009: 0 rows in Parquet.
    Expect: SUCCESS, 0 records, quarantine SKIPPED_NO_INVALID.
    """
    res = processor.process_batch(
        file_path="data/generated/batch_009_empty.parquet",
        batch_id="batch_009_test",
    )

    assert res.status == BatchProcessingStatus.SUCCESS
    assert res.row_count == 0
    assert res.valid_record_count == 0
    assert res.invalid_record_count == 0
    assert len(res.valid_table) == 0
    assert res.quarantine.status == "SKIPPED_NO_INVALID"


def test_process_all_invalid_batch(processor, tmp_path):
    """
    Batch with 100% invalid records (e.g. all orphan customer IDs).
    Expect: PARTIAL_SUCCESS with 0 valid records and all records quarantined.
    """
    # Create a small 5-row batch where all customer_ids are orphans
    schema = pq.read_table("data/generated/batch_001_original.parquet").schema
    sample_table = pq.read_table("data/generated/batch_001_original.parquet").slice(0, 5)
    
    # Overwrite customer_id with non-existent IDs (999991..999995)
    orphan_ids = [999991, 999992, 999993, 999994, 999995]
    cols = []
    for f in schema:
        if f.name == "customer_id":
            cols.append(pa.array(orphan_ids, type=pa.int64()))
        else:
            cols.append(sample_table.column(f.name))
    all_invalid_table = pa.Table.from_arrays(cols, schema=schema)
    
    test_file = str(tmp_path / "all_invalid.parquet")
    pq.write_table(all_invalid_table, test_file)

    res = processor.process_batch(file_path=test_file, batch_id="all_invalid_test")
    assert res.status == BatchProcessingStatus.PARTIAL_SUCCESS
    assert res.row_count == 5
    assert res.valid_record_count == 0
    assert res.invalid_record_count == 5
    assert len(res.valid_table) == 0
    assert res.quarantine.status == "QUARANTINED"
    assert res.quarantine.record_count == 5


def test_process_quarantine_failure_marks_batch_failed(processor, monkeypatch):
    """
    Condition 4: If quarantine persistence fails, NEVER report success; report FAILED.
    """
    # Monkeypatch quarantine_validation_result to simulate an I/O crash
    def mock_quarantine_fail(*args, **kwargs):
        raise IOError("Disk out of space during quarantine write")

    monkeypatch.setattr(processor.quarantine_manager, "quarantine_validation_result", mock_quarantine_fail)

    res = processor.process_batch(
        file_path="data/generated/batch_006_duplicates.parquet",
        batch_id="fail_quarantine_batch",
    )

    assert res.status == BatchProcessingStatus.FAILED
    assert "Quarantine persistence failed" in res.error_message
    assert res.valid_table is None  # Ensures valid table is not committed if quarantine fails


def test_process_idempotent_reprocessing(processor):
    """
    Condition 5: Reprocessing identical batch produces ALREADY_EXISTS from QuarantineManager.
    """
    # First execution
    res1 = processor.process_batch(
        file_path="data/generated/batch_006_duplicates.parquet",
        batch_id="idempotent_batch_id",
    )
    assert res1.status == BatchProcessingStatus.PARTIAL_SUCCESS
    assert res1.quarantine.status == "QUARANTINED"

    # Second execution with exact same batch ID and file
    res2 = processor.process_batch(
        file_path="data/generated/batch_006_duplicates.parquet",
        batch_id="idempotent_batch_id",
    )
    assert res2.status == BatchProcessingStatus.PARTIAL_SUCCESS
    assert res2.quarantine.status == "ALREADY_EXISTS"
    assert res2.valid_record_count == 950
    assert res2.invalid_record_count == 50


def test_process_conflicting_batch_id_fails(processor):
    """
    Condition 5: Reusing existing batch ID with different records triggers ValueError and results in FAILED.
    """
    # First execution with Batch 006 (50 duplicates)
    res1 = processor.process_batch(
        file_path="data/generated/batch_006_duplicates.parquet",
        batch_id="conflict_batch_test",
    )
    assert res1.status == BatchProcessingStatus.PARTIAL_SUCCESS

    # Second execution using SAME batch ID but Batch 007 (30 orphan FKs)
    res2 = processor.process_batch(
        file_path="data/generated/batch_007_invalid_foreign_keys.parquet",
        batch_id="conflict_batch_test",
    )
    assert res2.status == BatchProcessingStatus.FAILED
    assert "Quarantine persistence failed" in res2.error_message
    assert "Conflicting reuse of batch_id" in res2.error_message


def test_process_nonexistent_file_fails(processor):
    """
    Non-existent file reports FAILED early.
    """
    res = processor.process_batch(
        file_path="data/generated/does_not_exist.parquet",
        batch_id="missing_file_batch",
    )
    assert res.status == BatchProcessingStatus.FAILED
    assert "Source file not found" in res.error_message


def test_process_manual_review_rejection_gate(processor, monkeypatch):
    """
    Finding 1 & Finding 3: Exercises MANUAL_REVIEW classification.
    Verifies that SchemaEvolver rejects it, BatchProcessor sets SCHEMA_REJECTED,
    and neither DataValidator nor QuarantineManager is called.
    """
    from src.schema.evolution_classifier import EvolutionClassificationResult, ClassificationStatus

    # Configure classifier to return MANUAL_REVIEW
    def mock_classify(comparison):
        return EvolutionClassificationResult(
            file_path=comparison.file_path,
            table_name=comparison.table_name,
            overall_status=ClassificationStatus.MANUAL_REVIEW,
            reasons=["Unrecognized widening requires human review"],
            requires_manual_review=True,
        )

    validate_called = False
    quarantine_called = False

    def spy_validate(*args, **kwargs):
        nonlocal validate_called
        validate_called = True
        return None

    def spy_quarantine(*args, **kwargs):
        nonlocal quarantine_called
        quarantine_called = True
        return None

    monkeypatch.setattr(processor.classifier, "classify", mock_classify)
    monkeypatch.setattr(processor.validator, "validate_orders_table", spy_validate)
    monkeypatch.setattr(processor.quarantine_manager, "quarantine_validation_result", spy_quarantine)

    res = processor.process_batch(
        file_path="data/generated/batch_001_original.parquet",
        batch_id="manual_review_test_batch",
    )

    # Status assertions
    assert res.status == BatchProcessingStatus.SCHEMA_REJECTED
    assert res.batch_id == "manual_review_test_batch"
    assert res.table_name == "orders"
    assert res.valid_record_count == 0
    assert res.invalid_record_count == 0

    # Gate assertions: downstream methods never invoked
    assert validate_called is False
    assert quarantine_called is False

    # Structure assertions
    assert res.valid_table is None
    assert res.validation is None
    assert res.quarantine is None
    assert res.evolution_classification is not None
    assert res.evolution_classification.overall_status == ClassificationStatus.MANUAL_REVIEW
    assert res.schema_evolution is not None
    assert res.schema_evolution.status == "REJECTED"
    assert "MANUAL_REVIEW" in res.error_message


def test_process_base_schema_lookup_exception_handling(processor, monkeypatch):
    """
    Finding 2: Unexpected exception in get_base_schema_def() returns structured FAILED result.
    Downstream validation and quarantine are not called.
    """
    def mock_lookup_error(table_name):
        raise KeyError(f"Simulated database/config error reading schema for '{table_name}'")

    validate_called = False
    quarantine_called = False

    def spy_validate(*args, **kwargs):
        nonlocal validate_called
        validate_called = True
        return None

    def spy_quarantine(*args, **kwargs):
        nonlocal quarantine_called
        quarantine_called = True
        return None

    monkeypatch.setattr(processor, "get_base_schema_def", mock_lookup_error)
    monkeypatch.setattr(processor.validator, "validate_orders_table", spy_validate)
    monkeypatch.setattr(processor.quarantine_manager, "quarantine_validation_result", spy_quarantine)

    res = processor.process_batch(
        file_path="data/generated/batch_001_original.parquet",
        batch_id="schema_lookup_err_batch",
    )

    assert res.status == BatchProcessingStatus.FAILED
    assert res.batch_id == "schema_lookup_err_batch"
    assert res.table_name == "orders"
    assert res.source_file == "data/generated/batch_001_original.parquet"
    assert res.valid_record_count == 0
    assert res.invalid_record_count == 0
    assert res.valid_table is None
    assert res.validation is None
    assert res.quarantine is None
    assert res.schema_comparison is not None
    assert res.evolution_classification is not None
    assert "Base schema lookup failed" in res.error_message
    assert "Simulated database/config error" in res.error_message
    assert validate_called is False
    assert quarantine_called is False


def test_process_schema_evolution_exception_handling(processor, monkeypatch):
    """
    Finding 2: Unexpected exception in evolver.evolve() returns structured FAILED result.
    Downstream validation and quarantine are not called.
    """
    def mock_evolve_error(base_schema_def, classification):
        raise RuntimeError("Simulated crash inside schema evolution engine")

    validate_called = False
    quarantine_called = False

    def spy_validate(*args, **kwargs):
        nonlocal validate_called
        validate_called = True
        return None

    def spy_quarantine(*args, **kwargs):
        nonlocal quarantine_called
        quarantine_called = True
        return None

    monkeypatch.setattr(processor.evolver, "evolve", mock_evolve_error)
    monkeypatch.setattr(processor.validator, "validate_orders_table", spy_validate)
    monkeypatch.setattr(processor.quarantine_manager, "quarantine_validation_result", spy_quarantine)

    res = processor.process_batch(
        file_path="data/generated/batch_001_original.parquet",
        batch_id="schema_evolve_err_batch",
    )

    assert res.status == BatchProcessingStatus.FAILED
    assert res.batch_id == "schema_evolve_err_batch"
    assert res.table_name == "orders"
    assert res.source_file == "data/generated/batch_001_original.parquet"
    assert res.valid_record_count == 0
    assert res.invalid_record_count == 0
    assert res.valid_table is None
    assert res.validation is None
    assert res.quarantine is None
    assert res.schema_comparison is not None
    assert res.evolution_classification is not None
    assert "Schema evolution failed" in res.error_message
    assert "Simulated crash inside schema evolution engine" in res.error_message
    assert validate_called is False
    assert quarantine_called is False
