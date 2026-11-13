"""
Tests for Phase 5.1 Orchestration Contracts and Models.
"""

import pytest
from src.orchestration.models import (
    PipelineExecutionStatus,
    BatchExecutionStatus,
    ArtifactType,
    ErrorStage,
    ExecutionError,
    InputBatchReference,
    OutputArtifactReference,
    BatchExecutionResult,
    PipelineExecutionResult,
)
from src.transformation.pipeline_orchestrator import (
    PipelineBatchResult,
    PipelineExecutionResult as Phase5PipelineExecutionResult,
    PipelineExecutionStatus as Phase5PipelineExecutionStatus,
)
from src.ingestion.batch_processor import (
    BatchProcessingResult,
    BatchProcessingStatus,
    SchemaEvolutionResult,
    ValidationResult,
    ValidationMetrics,
)
from src.quarantine.quarantine_manager import QuarantineRecord
from src.storage.storage_manager import StorageStatus
from src.storage.metadata_catalog import CatalogStatus


def test_pipeline_execution_status_values():
    # Exactly three ternary values matching Phase 5.0 status matrix
    assert len(PipelineExecutionStatus) == 3
    assert set(PipelineExecutionStatus) == {
        PipelineExecutionStatus.SUCCESS,
        PipelineExecutionStatus.PARTIAL_SUCCESS,
        PipelineExecutionStatus.FAILED,
    }
    assert [s.value for s in PipelineExecutionStatus] == ["SUCCESS", "PARTIAL_SUCCESS", "FAILED"]


def test_batch_execution_status_values():
    # Exactly seven granular batch execution statuses retaining component failures
    assert len(BatchExecutionStatus) == 7
    assert BatchExecutionStatus.SUCCESS.value == "SUCCESS"
    assert BatchExecutionStatus.PARTIAL_SUCCESS.value == "PARTIAL_SUCCESS"
    assert BatchExecutionStatus.SCHEMA_REJECTED.value == "SCHEMA_REJECTED"
    assert BatchExecutionStatus.FAILED.value == "FAILED"
    assert BatchExecutionStatus.STORAGE_FAILED.value == "STORAGE_FAILED"
    assert BatchExecutionStatus.CATALOG_FAILED.value == "CATALOG_FAILED"
    assert BatchExecutionStatus.SKIPPED.value == "SKIPPED"


def test_artifact_type_and_error_stage_enums():
    assert ArtifactType.PROCESSED_PARQUET.value == "PROCESSED_PARQUET"
    assert ArtifactType.QUARANTINE_PARQUET.value == "QUARANTINE_PARQUET"
    assert ArtifactType.QUARANTINE_METADATA.value == "QUARANTINE_METADATA"
    assert ArtifactType.METADATA_CATALOG.value == "METADATA_CATALOG"

    assert ErrorStage.DISCOVERY.value == "DISCOVERY"
    assert ErrorStage.STORAGE.value == "STORAGE"
    assert ErrorStage.CATALOG.value == "CATALOG"
    assert ErrorStage.ORCHESTRATION.value == "ORCHESTRATION"


def test_execution_error_construction_and_validation():
    # Valid construction
    err = ExecutionError(
        stage=ErrorStage.STORAGE,
        message="Disk quota exceeded",
        error_code="STORAGE_QUOTA_EXCEEDED",
        batch_id="batch_001",
        details={"disk_free_mb": 0},
    )
    assert err.stage == ErrorStage.STORAGE
    assert err.message == "Disk quota exceeded"
    assert err.batch_id == "batch_001"
    assert err.details["disk_free_mb"] == 0

    # Serialization
    d = err.to_dict()
    assert d["stage"] == "STORAGE"
    assert d["error_code"] == "STORAGE_QUOTA_EXCEEDED"
    assert d["batch_id"] == "batch_001"

    # Validation: empty message
    with pytest.raises(ValueError, match="non-empty message"):
        ExecutionError(stage=ErrorStage.STORAGE, message="  ")

    # Validation: empty error_code
    with pytest.raises(ValueError, match="non-empty error_code"):
        ExecutionError(stage=ErrorStage.STORAGE, message="Valid msg", error_code="")


def test_input_batch_reference_construction_and_validation():
    ref = InputBatchReference(
        batch_id="batch_001",
        file_path="data/landing/batch_001.parquet",
        table_name="orders",
        file_size_bytes=1024,
    )
    assert ref.batch_id == "batch_001"
    assert ref.file_path == "data/landing/batch_001.parquet"
    assert ref.file_size_bytes == 1024

    d = ref.to_dict()
    assert d["batch_id"] == "batch_001"
    assert d["file_size_bytes"] == 1024

    # Validation: empty batch_id
    with pytest.raises(ValueError, match="non-empty batch_id"):
        InputBatchReference(batch_id="", file_path="data/test.parquet")

    # Validation: empty file_path
    with pytest.raises(ValueError, match="non-empty file_path"):
        InputBatchReference(batch_id="b1", file_path="  ")

    # Validation: negative file_size_bytes
    with pytest.raises(ValueError, match="cannot be negative"):
        InputBatchReference(batch_id="b1", file_path="data/test.parquet", file_size_bytes=-5)


def test_output_artifact_reference_construction_and_validation():
    art = OutputArtifactReference(
        artifact_type=ArtifactType.PROCESSED_PARQUET,
        file_path="data/processed/orders/batch_001.parquet",
        record_count=100,
        fingerprint="abc123sha",
        is_durable=True,
    )
    assert art.artifact_type == ArtifactType.PROCESSED_PARQUET
    assert art.record_count == 100
    assert art.fingerprint == "abc123sha"

    d = art.to_dict()
    assert d["artifact_type"] == "PROCESSED_PARQUET"
    assert d["record_count"] == 100

    # Validation: empty file_path
    with pytest.raises(ValueError, match="non-empty file_path"):
        OutputArtifactReference(artifact_type=ArtifactType.OTHER, file_path="")

    # Validation: negative record_count
    with pytest.raises(ValueError, match="cannot be negative"):
        OutputArtifactReference(
            artifact_type=ArtifactType.PROCESSED_PARQUET,
            file_path="data/p.parquet",
            record_count=-10,
        )


def test_batch_execution_result_construction_and_validation():
    input_ref = InputBatchReference(
        batch_id="batch_001",
        file_path="data/landing/batch_001.parquet",
    )
    art = OutputArtifactReference(
        artifact_type=ArtifactType.PROCESSED_PARQUET,
        file_path="data/processed/orders/batch_001.parquet",
        record_count=50,
    )

    res = BatchExecutionResult(
        batch_id="batch_001",
        status=BatchExecutionStatus.SUCCESS,
        source_file="data/landing/batch_001.parquet",
        input_ref=input_ref,
        artifacts=[art],
    )
    assert res.batch_id == "batch_001"
    assert res.status == BatchExecutionStatus.SUCCESS
    assert len(res.artifacts) == 1

    d = res.to_dict()
    assert d["batch_id"] == "batch_001"
    assert d["status"] == "SUCCESS"
    assert d["input_ref"]["batch_id"] == "batch_001"
    assert len(d["artifacts"]) == 1

    # Validation: empty batch_id
    with pytest.raises(ValueError, match="non-empty batch_id"):
        BatchExecutionResult(batch_id="", status="SUCCESS", source_file="b.parquet")

    # Validation: empty source_file
    with pytest.raises(ValueError, match="non-empty source_file"):
        BatchExecutionResult(batch_id="b1", status="SUCCESS", source_file="   ")

    # Validation: mismatch between input_ref and batch_id
    wrong_ref = InputBatchReference(batch_id="batch_999", file_path="f.parquet")
    with pytest.raises(ValueError, match="Batch ID mismatch"):
        BatchExecutionResult(
            batch_id="batch_001",
            status="SUCCESS",
            source_file="f.parquet",
            input_ref=wrong_ref,
        )

    # Synthesis of ExecutionError from error_message
    res_err = BatchExecutionResult(
        batch_id="batch_002",
        status=BatchExecutionStatus.STORAGE_FAILED,
        source_file="f2.parquet",
        storage_status=StorageStatus.STORAGE_FAILED,
        error_message="Storage write conflict",
    )
    assert len(res_err.errors) == 1
    assert res_err.errors[0].stage == ErrorStage.STORAGE
    assert "Storage write conflict" in res_err.errors[0].message


def test_batch_execution_result_from_pipeline_batch_result():
    bp_res = BatchProcessingResult(
        batch_id="batch_006",
        table_name="orders",
        status=BatchProcessingStatus.PARTIAL_SUCCESS,
        source_file="batch_006.parquet",
        row_count=10,
        valid_record_count=8,
        invalid_record_count=2,
        quarantine=QuarantineRecord(
            batch_id="batch_006",
            table_name="orders",
            record_count=2,
            parquet_path="data/quarantine/orders/batch_006.parquet",
            metadata_path="data/quarantine/orders/batch_006_metadata.json",
            status="QUARANTINED",
            quarantined_at="2026-11-13T10:00:00Z",
        ),
    )
    pbr = PipelineBatchResult(
        batch_id="batch_006",
        source_file="batch_006.parquet",
        batch_processing=bp_res,
        storage_status=StorageStatus.COMMITTED,
        catalog_status=CatalogStatus.LOGGED,
        processed_path="data/processed/orders/batch_006.parquet",
        error_message=None,
    )

    ber = BatchExecutionResult.from_pipeline_batch_result(pbr)
    assert ber.batch_id == "batch_006"
    assert ber.status == BatchExecutionStatus.PARTIAL_SUCCESS
    assert ber.storage_status == StorageStatus.COMMITTED
    assert ber.catalog_status == CatalogStatus.LOGGED
    # Processed parquet + quarantine parquet + quarantine metadata
    assert len(ber.artifacts) == 3
    types = [a.artifact_type for a in ber.artifacts]
    assert ArtifactType.PROCESSED_PARQUET in types
    assert ArtifactType.QUARANTINE_PARQUET in types
    assert ArtifactType.QUARANTINE_METADATA in types


def test_pipeline_execution_result_valid_construction():
    b1 = BatchExecutionResult(
        batch_id="b1",
        status=BatchExecutionStatus.SUCCESS,
        source_file="b1.parquet",
    )
    res = PipelineExecutionResult(
        run_id="run_100",
        status=PipelineExecutionStatus.SUCCESS,
        started_at="2026-11-13T10:00:00Z",
        completed_at="2026-11-13T10:00:05Z",
        total_batches=1,
        successful_batches=1,
        partial_batches=0,
        rejected_batches=0,
        failed_batches=0,
        committed_batches=1,
        already_processed_batches=0,
        skipped_storage_batches=0,
        storage_failed_batches=0,
        logged_batches=1,
        catalog_failed_batches=0,
        total_records=10,
        total_valid_records=10,
        total_quarantined_records=0,
        batch_results=[b1],
    )
    assert res.run_id == "run_100"
    assert res.status == PipelineExecutionStatus.SUCCESS
    assert res.total_batches == 1

    d = res.to_dict()
    assert d["run_id"] == "run_100"
    assert d["status"] == "SUCCESS"
    assert len(d["batch_results"]) == 1


def test_pipeline_execution_result_invariant_negative_counters():
    with pytest.raises(ValueError, match="cannot be negative"):
        PipelineExecutionResult(
            run_id="run_neg",
            status=PipelineExecutionStatus.FAILED,
            started_at="2026-11-13T10:00:00Z",
            completed_at="2026-11-13T10:00:05Z",
            total_batches=1,
            successful_batches=-1,  # Negative!
            partial_batches=0,
            rejected_batches=0,
            failed_batches=2,
            committed_batches=0,
            already_processed_batches=0,
            skipped_storage_batches=0,
            storage_failed_batches=0,
            logged_batches=0,
            catalog_failed_batches=0,
            total_records=0,
            total_valid_records=0,
            total_quarantined_records=0,
        )


def test_pipeline_execution_result_invariant_batch_count_mismatch():
    with pytest.raises(ValueError, match="Batch counter mismatch"):
        PipelineExecutionResult(
            run_id="run_sum_mismatch",
            status=PipelineExecutionStatus.FAILED,
            started_at="2026-11-13T10:00:00Z",
            completed_at="2026-11-13T10:00:05Z",
            total_batches=3,  # Mismatch: 3 != 1 + 0 + 0 + 1 = 2
            successful_batches=1,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=1,
            committed_batches=1,
            already_processed_batches=0,
            skipped_storage_batches=0,
            storage_failed_batches=1,
            logged_batches=2,
            catalog_failed_batches=0,
            total_records=10,
            total_valid_records=5,
            total_quarantined_records=0,
        )


def test_pipeline_execution_result_invariant_record_count_mismatch():
    # valid_records > total_records
    with pytest.raises(ValueError, match="cannot exceed total_records"):
        PipelineExecutionResult(
            run_id="run_rec_mismatch",
            status=PipelineExecutionStatus.SUCCESS,
            started_at="2026-11-13T10:00:00Z",
            completed_at="2026-11-13T10:00:05Z",
            total_batches=1,
            successful_batches=1,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=0,
            committed_batches=1,
            already_processed_batches=0,
            skipped_storage_batches=0,
            storage_failed_batches=0,
            logged_batches=1,
            catalog_failed_batches=0,
            total_records=10,
            total_valid_records=15,  # Exceeds total!
            total_quarantined_records=0,
        )

    # valid + quarantined > total
    with pytest.raises(ValueError, match="exceeds total_records"):
        PipelineExecutionResult(
            run_id="run_rec_sum_mismatch",
            status=PipelineExecutionStatus.PARTIAL_SUCCESS,
            started_at="2026-11-13T10:00:00Z",
            completed_at="2026-11-13T10:00:05Z",
            total_batches=1,
            successful_batches=0,
            partial_batches=1,
            rejected_batches=0,
            failed_batches=0,
            committed_batches=0,
            already_processed_batches=0,
            skipped_storage_batches=0,
            storage_failed_batches=0,
            logged_batches=1,
            catalog_failed_batches=0,
            total_records=10,
            total_valid_records=8,
            total_quarantined_records=5,  # 8 + 5 = 13 > 10!
        )


def test_pipeline_execution_result_invariant_batch_results_length():
    b1 = BatchExecutionResult(batch_id="b1", status="SUCCESS", source_file="b1.parquet")
    with pytest.raises(ValueError, match="does not match total_batches"):
        PipelineExecutionResult(
            run_id="run_len_mismatch",
            status=PipelineExecutionStatus.SUCCESS,
            started_at="2026-11-13T10:00:00Z",
            completed_at="2026-11-13T10:00:05Z",
            total_batches=2,  # Expects 2, but provided only 1!
            successful_batches=2,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=0,
            committed_batches=2,
            already_processed_batches=0,
            skipped_storage_batches=0,
            storage_failed_batches=0,
            logged_batches=2,
            catalog_failed_batches=0,
            total_records=20,
            total_valid_records=20,
            total_quarantined_records=0,
            batch_results=[b1],
        )


def test_pipeline_execution_result_invariant_status_consistency():
    # Inconsistent: status is SUCCESS but failed_batches > 0
    with pytest.raises(ValueError, match="status is SUCCESS but failed_batches=1"):
        PipelineExecutionResult(
            run_id="run_inc_1",
            status=PipelineExecutionStatus.SUCCESS,
            started_at="2026-11-13T10:00:00Z",
            completed_at="2026-11-13T10:00:05Z",
            total_batches=1,
            successful_batches=0,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=1,
            committed_batches=0,
            already_processed_batches=0,
            skipped_storage_batches=0,
            storage_failed_batches=0,
            logged_batches=1,
            catalog_failed_batches=0,
            total_records=10,
            total_valid_records=0,
            total_quarantined_records=0,
        )

    # Inconsistent: status is SUCCESS but storage_failed_batches > 0
    with pytest.raises(ValueError, match="status is SUCCESS but storage_failed_batches=1"):
        PipelineExecutionResult(
            run_id="run_inc_2",
            status=PipelineExecutionStatus.SUCCESS,
            started_at="2026-11-13T10:00:00Z",
            completed_at="2026-11-13T10:00:05Z",
            total_batches=1,
            successful_batches=1,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=0,
            committed_batches=0,
            already_processed_batches=0,
            skipped_storage_batches=0,
            storage_failed_batches=1,
            logged_batches=1,
            catalog_failed_batches=0,
            total_records=10,
            total_valid_records=10,
            total_quarantined_records=0,
        )


def test_pipeline_execution_result_from_phase5_result():
    bp_res = BatchProcessingResult(
        batch_id="b1",
        table_name="orders",
        status=BatchProcessingStatus.SUCCESS,
        source_file="b1.parquet",
        row_count=10,
        valid_record_count=10,
        invalid_record_count=0,
    )
    pbr = PipelineBatchResult(
        batch_id="b1",
        source_file="b1.parquet",
        batch_processing=bp_res,
        storage_status=StorageStatus.COMMITTED,
        catalog_status=CatalogStatus.LOGGED,
        processed_path="data/processed/orders/b1.parquet",
    )
    p5_res = Phase5PipelineExecutionResult(
        run_id="run_p5",
        status=Phase5PipelineExecutionStatus.SUCCESS,
        started_at="2026-11-13T10:00:00Z",
        completed_at="2026-11-13T10:00:05Z",
        total_batches=1,
        successful_batches=1,
        partial_batches=0,
        rejected_batches=0,
        failed_batches=0,
        committed_batches=1,
        already_processed_batches=0,
        skipped_storage_batches=0,
        storage_failed_batches=0,
        logged_batches=1,
        catalog_failed_batches=0,
        total_records=10,
        total_valid_records=10,
        total_quarantined_records=0,
        batch_results=[pbr],
        error_message=None,
    )

    adapted = PipelineExecutionResult.from_phase5_result(p5_res)
    assert adapted.run_id == "run_p5"
    assert adapted.status == "SUCCESS"
    assert len(adapted.batch_results) == 1
    assert isinstance(adapted.batch_results[0], BatchExecutionResult)
    assert adapted.batch_results[0].batch_id == "b1"
    assert len(adapted.artifacts) == 1
    assert adapted.artifacts[0].file_path == "data/processed/orders/b1.parquet"


def test_adapters_are_unidirectional():
    # Verifies that only forward adapters are implemented and no reverse adapters exist
    assert hasattr(BatchExecutionResult, "from_pipeline_batch_result")
    assert hasattr(PipelineExecutionResult, "from_phase5_result")
    assert not hasattr(BatchExecutionResult, "to_pipeline_batch_result")
    assert not hasattr(PipelineExecutionResult, "to_phase5_result")

