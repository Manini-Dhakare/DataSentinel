"""
Comprehensive Integration Tests for Phase 5.2 Orchestrator Contract Integration & Execution Engine.

Verifies end-to-end integration between PipelineOrchestrator and Phase 5.1 orchestration contracts:
- FR-1: Batch discovery and strongly-typed InputBatchReference generation.
- FR-2: Typed BatchExecutionResult contracts and input lineage links.
- FR-3: Durable OutputArtifactReference registration for processed Parquet, quarantine Parquet, and metadata sidecars.
- FR-4: Structured ExecutionError containment and aggregation across Discovery, Ingestion, Storage, and Catalog stages.
- FR-5: Contract invariant enforcement and counter summation consistency.
- FR-6: Multi-batch execution flows (clean runs, mixed quarantine, breaking schema rejection, storage failure containment).
- FR-7: Serialization integrity (to_dict representations for all contract models).
"""

import os
import shutil
from pathlib import Path
from unittest.mock import patch
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.transformation.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineBatchResult,
    PipelineExecutionResult,
    PipelineExecutionStatus,
    BatchExecutionResult,
    BatchExecutionStatus,
    InputBatchReference,
    OutputArtifactReference,
    ExecutionError,
    ArtifactType,
    ErrorStage,
)
from src.storage.storage_manager import StorageStatus
from src.storage.metadata_catalog import CatalogStatus


# ==============================================================================
# FR-1: BATCH DISCOVERY & TYPED INPUT BATCH REFERENCES
# ==============================================================================

def test_discover_batches_typed_references(tmp_path: Path):
    """Verify discover_batches returns List[InputBatchReference] with complete metadata."""
    landing = tmp_path / "landing"
    landing.mkdir()

    # Create dummy parquet files and non-parquet files
    f1 = landing / "batch_001_original.parquet"
    f2 = landing / "batch_002_add_currency.parquet"
    f_txt = landing / "notes.txt"
    f_csv = landing / "data.csv"

    shutil.copyfile("data/generated/batch_001_original.parquet", f1)
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", f2)
    f_txt.write_text("ignore me", encoding="utf-8")
    f_csv.write_text("a,b\n1,2", encoding="utf-8")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    discovered = orchestrator.discover_batches(str(landing), table_name="orders")

    # Assert non-parquet files are excluded
    assert len(discovered) == 2
    assert all(isinstance(ref, InputBatchReference) for ref in discovered)

    # Assert deterministic ordering by path
    assert discovered[0].batch_id == "batch_001"
    assert discovered[0].file_path == str(f1)
    assert discovered[0].table_name == "orders"
    assert discovered[0].file_size_bytes == os.path.getsize(f1)
    assert discovered[0].file_size_bytes > 0
    assert discovered[0].discovered_at is not None

    assert discovered[1].batch_id == "batch_002"
    assert discovered[1].file_path == str(f2)
    assert discovered[1].file_size_bytes == os.path.getsize(f2)


def test_discover_batches_empty_and_nonexistent(tmp_path: Path):
    """Verify empty and nonexistent directories return empty lists without exceptions."""
    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    # Nonexistent
    assert orchestrator.discover_batches(str(tmp_path / "missing")) == []

    # Empty
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    assert orchestrator.discover_batches(str(empty_dir)) == []


def test_discover_batches_duplicate_detection(tmp_path: Path):
    """Verify duplicate batch IDs in the same directory raise ValueError."""
    landing = tmp_path / "landing"
    landing.mkdir()

    f1 = landing / "batch_001_part1.parquet"
    f2 = landing / "batch_001_part2.parquet"
    f1.write_bytes(b"dummy")
    f2.write_bytes(b"dummy")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    with pytest.raises(ValueError, match="Duplicate batch ID 'batch_001' detected"):
        orchestrator.discover_batches(str(landing))


# ==============================================================================
# FR-2: TYPED BATCH EXECUTION CONTRACT & INPUT LINEAGE
# ==============================================================================

def test_pipeline_produces_typed_batch_results(tmp_path: Path):
    """Verify run_pipeline produces PipelineExecutionResult containing BatchExecutionResults with input_ref."""
    landing = tmp_path / "landing"
    landing.mkdir()

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing), table_name="orders")

    assert isinstance(res, PipelineExecutionResult)
    assert res.status == PipelineExecutionStatus.SUCCESS
    assert len(res.batch_results) == 1

    b_res = res.batch_results[0]
    assert isinstance(b_res, BatchExecutionResult)
    assert isinstance(b_res, PipelineBatchResult)
    assert b_res.batch_id == "batch_001"
    assert b_res.status == BatchExecutionStatus.SUCCESS
    assert b_res.storage_status == StorageStatus.COMMITTED
    assert b_res.catalog_status == CatalogStatus.LOGGED

    # Lineage link
    assert b_res.input_ref is not None
    assert isinstance(b_res.input_ref, InputBatchReference)
    assert b_res.input_ref.batch_id == "batch_001"
    assert b_res.input_ref.file_path == str(landing / "batch_001_original.parquet")


# ==============================================================================
# FR-3: DURABLE ARTIFACT LINEAGE TRACKING
# ==============================================================================

def test_clean_batch_artifact_registration(tmp_path: Path):
    """Verify clean batch produces PROCESSED_PARQUET OutputArtifactReference."""
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert len(res.artifacts) == 1
    art = res.artifacts[0]
    assert isinstance(art, OutputArtifactReference)
    assert art.artifact_type == ArtifactType.PROCESSED_PARQUET
    assert art.is_durable is True
    assert art.record_count > 0
    assert art.fingerprint is not None
    assert os.path.isfile(art.file_path)
    assert "batch_001.parquet" in art.file_path
    assert art.metadata["batch_id"] == "batch_001"


def test_quarantined_batch_artifact_registration(tmp_path: Path):
    """Verify quarantined batch produces QUARANTINE_PARQUET and QUARANTINE_METADATA artifacts."""
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_006_duplicates.parquet", landing / "batch_006_duplicates.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    b_res = res.batch_results[0]
    assert b_res.status == BatchExecutionStatus.PARTIAL_SUCCESS

    # Batch 006 has both valid records (persisted) and quarantined records
    art_types = [a.artifact_type for a in b_res.artifacts]
    assert ArtifactType.PROCESSED_PARQUET in art_types
    assert ArtifactType.QUARANTINE_PARQUET in art_types
    assert ArtifactType.QUARANTINE_METADATA in art_types

    # Verify all artifacts physically exist
    for a in b_res.artifacts:
        assert os.path.isfile(a.file_path)
        assert a.is_durable is True

    # Check pipeline-level aggregation
    assert len(res.artifacts) == 3


# ==============================================================================
# FR-4: STRUCTURED EXECUTION ERROR TRACKING
# ==============================================================================

def test_discovery_error_tracking(tmp_path: Path):
    """Verify discovery failure creates ExecutionError with ErrorStage.DISCOVERY."""
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "batch_001_a.parquet").write_bytes(b"dummy")
    (landing / "batch_001_b.parquet").write_bytes(b"dummy")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.status == PipelineExecutionStatus.FAILED
    assert len(res.errors) >= 1
    err = res.errors[0]
    assert isinstance(err, ExecutionError)
    assert err.stage == ErrorStage.DISCOVERY
    assert err.error_code == "DISCOVERY_ERROR"
    assert "Duplicate batch ID" in err.message


def test_schema_rejection_error_tracking(tmp_path: Path):
    """Verify breaking schema rejection creates ExecutionError with ErrorStage.INGESTION."""
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_004_breaking_type.parquet", landing / "batch_004_breaking_type.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.status == PipelineExecutionStatus.FAILED
    assert res.rejected_batches == 1
    b_res = res.batch_results[0]
    assert b_res.status == BatchExecutionStatus.SCHEMA_REJECTED

    assert len(b_res.errors) >= 1
    ingest_err = next((e for e in b_res.errors if e.stage == ErrorStage.INGESTION), None)
    assert ingest_err is not None
    assert ingest_err.error_code == "SCHEMA_REJECTED"
    assert ingest_err.batch_id == "batch_004"

    # Verify error is aggregated to pipeline result
    assert ingest_err in res.errors


def test_storage_failure_error_tracking(tmp_path: Path):
    """Verify storage conflict creates ExecutionError with ErrorStage.STORAGE."""
    landing = tmp_path / "landing"
    landing.mkdir()
    proc_orders = tmp_path / "processed" / "orders"
    proc_orders.mkdir(parents=True)

    # Pre-populate corrupt file
    (proc_orders / "batch_001.parquet").write_bytes(b"CORRUPT")
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.status == PipelineExecutionStatus.FAILED
    assert res.storage_failed_batches == 1
    b_res = res.batch_results[0]
    assert b_res.status == BatchExecutionStatus.STORAGE_FAILED

    stor_err = next((e for e in b_res.errors if e.stage == ErrorStage.STORAGE), None)
    assert stor_err is not None
    assert stor_err.error_code == "STORAGE_FAILED"
    assert stor_err.batch_id == "batch_001"
    assert "Corrupted Parquet file" in stor_err.message
    assert stor_err in res.errors


def test_catalog_failure_error_tracking(tmp_path: Path):
    """Verify catalog logging failure creates ExecutionError with ErrorStage.CATALOG."""
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    # Mock catalog to throw an exception on log_batch_execution
    with patch.object(
        orchestrator.metadata_catalog,
        "log_batch_execution",
        side_effect=RuntimeError("DuckDB lock timeout"),
    ):
        res = orchestrator.run_pipeline(input_dir=str(landing))

    # Batch output was committed, but catalog logging failed -> PARTIAL_SUCCESS
    assert res.status == PipelineExecutionStatus.PARTIAL_SUCCESS
    assert res.catalog_failed_batches == 1
    b_res = res.batch_results[0]
    assert b_res.status == BatchExecutionStatus.CATALOG_FAILED

    cat_err = next((e for e in b_res.errors if e.stage == ErrorStage.CATALOG), None)
    assert cat_err is not None
    assert cat_err.error_code == "CATALOG_FAILED"
    assert "DuckDB lock timeout" in cat_err.message
    assert cat_err in res.errors


# ==============================================================================
# FR-5: CONTRACT INVARIANT ENFORCEMENT
# ==============================================================================

def test_multi_batch_invariant_consistency(tmp_path: Path):
    """Verify all 5 Phase 5.1 invariants hold on multi-batch pipeline execution."""
    landing = tmp_path / "landing"
    landing.mkdir()

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_002_add_currency.parquet", landing / "batch_002_add_currency.parquet")
    shutil.copyfile("data/generated/batch_006_duplicates.parquet", landing / "batch_006_duplicates.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing))

    # Invariant 1: Counters non-negative
    assert res.total_batches == 3
    assert res.successful_batches >= 0
    assert res.partial_batches >= 0
    assert res.rejected_batches >= 0
    assert res.failed_batches >= 0
    assert res.total_records >= 0

    # Invariant 2: Sum of batch outcomes equals total_batches
    outcome_sum = (
        res.successful_batches
        + res.partial_batches
        + res.rejected_batches
        + res.failed_batches
    )
    assert res.total_batches == outcome_sum

    # Invariant 3: batch_results length matches total_batches
    assert len(res.batch_results) == res.total_batches

    # Invariant 4: Record counts bounded
    assert res.total_valid_records <= res.total_records
    assert res.total_valid_records + res.total_quarantined_records <= res.total_records

    # Invariant 5: Status consistency
    assert res.status == PipelineExecutionStatus.PARTIAL_SUCCESS  # Due to batch 006 quarantine


# ==============================================================================
# FR-6: MULTI-BATCH SCENARIOS & FAILURE CONTAINMENT
# ==============================================================================

def test_mixed_clean_and_breaking_containment(tmp_path: Path):
    """Verify clean batch commits while breaking batch is contained, yielding PARTIAL_SUCCESS."""
    landing = tmp_path / "landing"
    landing.mkdir()

    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_004_breaking_type.parquet", landing / "batch_004_breaking_type.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing))

    assert res.total_batches == 2
    assert res.successful_batches == 1
    assert res.rejected_batches == 1
    assert res.committed_batches == 1

    # Overall is PARTIAL_SUCCESS because batch_001 produced durable committed storage
    assert res.status == PipelineExecutionStatus.PARTIAL_SUCCESS

    b1, b4 = res.batch_results[0], res.batch_results[1]
    assert b1.status == BatchExecutionStatus.SUCCESS
    assert b1.storage_status == StorageStatus.COMMITTED

    assert b4.status == BatchExecutionStatus.SCHEMA_REJECTED
    assert b4.storage_status == StorageStatus.SKIPPED


# ==============================================================================
# FR-7: SERIALIZATION INTEGRITY
# ==============================================================================

def test_serialization_integrity(tmp_path: Path):
    """Verify to_dict on PipelineExecutionResult and BatchExecutionResult produces complete typed structures."""
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )

    res = orchestrator.run_pipeline(input_dir=str(landing))
    d = res.to_dict()

    assert isinstance(d, dict)
    assert d["status"] == "SUCCESS"
    assert d["total_batches"] == 1
    assert d["successful_batches"] == 1
    assert len(d["batch_results"]) == 1
    assert len(d["artifacts"]) == 1

    b_dict = d["batch_results"][0]
    assert b_dict["batch_id"] == "batch_001"
    assert b_dict["status"] == "SUCCESS"
    assert b_dict["input_ref"]["batch_id"] == "batch_001"
    assert b_dict["input_ref"]["file_size_bytes"] > 0
    assert len(b_dict["artifacts"]) == 1
    assert b_dict["artifacts"][0]["artifact_type"] == "PROCESSED_PARQUET"
