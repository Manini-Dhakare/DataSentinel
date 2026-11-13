"""
Orchestration Contracts and Strongly-Typed Result Models for SchemaGuard (Phase 5.1).

Defines clean, reusable, strongly-typed orchestration contracts:
- PipelineExecutionStatus (ternary: SUCCESS, PARTIAL_SUCCESS, FAILED - consistent with Phase 5.0)
- BatchExecutionStatus (granular: SUCCESS, PARTIAL_SUCCESS, SCHEMA_REJECTED, FAILED, STORAGE_FAILED, CATALOG_FAILED, SKIPPED)
- InputBatchReference (formalizes landing/input batch paths, identifiers, and metadata)
- OutputArtifactReference (tracks published Parquet, quarantine files, and metadata sidecars)
- ExecutionError (structured operational error reporting across pipeline stages)
- BatchExecutionResult (comprehensive per-batch execution summary reusing existing Phase 4/5 models)
- PipelineExecutionResult (strongly-typed multi-batch execution result with invariant enforcement)

Adapters:
- Provides unidirectional forward adapters from Phase 5.0 models to Phase 5.1 contracts:
  * BatchExecutionResult.from_pipeline_batch_result()
  * PipelineExecutionResult.from_phase5_result()
"""

from enum import Enum
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field

# Reuse existing Phase 4 and Phase 5 contracts without duplication
from src.ingestion.batch_processor import (
    BatchProcessingResult,
    BatchProcessingStatus,
)
from src.storage.storage_manager import (
    StorageRecord,
    StorageStatus,
)
from src.storage.metadata_catalog import (
    CatalogRecord,
    CatalogStatus,
)


class PipelineExecutionStatus(str, Enum):
    """
    Overall status of a multi-batch pipeline execution run.
    Strictly ternary (SUCCESS, PARTIAL_SUCCESS, FAILED) matching Phase 5.0 status matrix.
    """
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"


class BatchExecutionStatus(str, Enum):
    """Granular execution status for an individual batch."""
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    SCHEMA_REJECTED = "SCHEMA_REJECTED"
    FAILED = "FAILED"
    STORAGE_FAILED = "STORAGE_FAILED"
    CATALOG_FAILED = "CATALOG_FAILED"
    SKIPPED = "SKIPPED"


class ArtifactType(str, Enum):
    """Types of durable data artifacts produced during orchestration."""
    PROCESSED_PARQUET = "PROCESSED_PARQUET"
    QUARANTINE_PARQUET = "QUARANTINE_PARQUET"
    QUARANTINE_METADATA = "QUARANTINE_METADATA"
    METADATA_CATALOG = "METADATA_CATALOG"
    OTHER = "OTHER"


class ErrorStage(str, Enum):
    """Pipeline lifecycle stage where an error occurred."""
    DISCOVERY = "DISCOVERY"
    INGESTION = "INGESTION"
    SCHEMA_EVOLUTION = "SCHEMA_EVOLUTION"
    VALIDATION = "VALIDATION"
    QUARANTINE = "QUARANTINE"
    STORAGE = "STORAGE"
    CATALOG = "CATALOG"
    ORCHESTRATION = "ORCHESTRATION"


@dataclass
class ExecutionError:
    """
    Structured execution error capturing stage, error code, context, and timestamp.
    """
    stage: Union[ErrorStage, str]
    message: str
    error_code: str = "EXECUTION_ERROR"
    batch_id: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    details: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if not self.message or not str(self.message).strip():
            raise ValueError("ExecutionError requires a non-empty message.")
        if not self.error_code or not str(self.error_code).strip():
            raise ValueError("ExecutionError requires a non-empty error_code.")
        if isinstance(self.stage, str):
            try:
                self.stage = ErrorStage(self.stage)
            except ValueError:
                pass

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.stage.value if isinstance(self.stage, ErrorStage) else str(self.stage),
            "message": self.message,
            "error_code": self.error_code,
            "batch_id": self.batch_id,
            "timestamp": self.timestamp,
            "details": self.details or {},
        }


@dataclass
class InputBatchReference:
    """
    Formal representation of a discovered input batch ready for processing.
    """
    batch_id: str
    file_path: str
    table_name: str = "orders"
    file_size_bytes: Optional[int] = None
    discovered_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self):
        if not self.batch_id or not str(self.batch_id).strip():
            raise ValueError("InputBatchReference requires a non-empty batch_id.")
        if not self.file_path or not str(self.file_path).strip():
            raise ValueError("InputBatchReference requires a non-empty file_path.")
        if not self.table_name or not str(self.table_name).strip():
            raise ValueError("InputBatchReference requires a non-empty table_name.")
        if self.file_size_bytes is not None and self.file_size_bytes < 0:
            raise ValueError("file_size_bytes cannot be negative.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "file_path": self.file_path,
            "table_name": self.table_name,
            "file_size_bytes": self.file_size_bytes,
            "discovered_at": self.discovered_at,
        }


@dataclass
class OutputArtifactReference:
    """
    Reference to a durable output artifact produced by the pipeline.
    """
    artifact_type: Union[ArtifactType, str]
    file_path: str
    record_count: Optional[int] = None
    fingerprint: Optional[str] = None
    is_durable: bool = True
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if not self.file_path or not str(self.file_path).strip():
            raise ValueError("OutputArtifactReference requires a non-empty file_path.")
        if self.record_count is not None and self.record_count < 0:
            raise ValueError("record_count cannot be negative.")
        if isinstance(self.artifact_type, str):
            try:
                self.artifact_type = ArtifactType(self.artifact_type)
            except ValueError:
                pass

    def to_dict(self) -> Dict[str, Any]:
        return {
            "artifact_type": self.artifact_type.value if isinstance(self.artifact_type, ArtifactType) else str(self.artifact_type),
            "file_path": self.file_path,
            "record_count": self.record_count,
            "fingerprint": self.fingerprint,
            "is_durable": self.is_durable,
            "created_at": self.created_at,
            "metadata": self.metadata or {},
        }


@dataclass
class BatchExecutionResult:
    """
    Strongly-typed contract for an individual batch execution.
    Integrates existing Phase 4 BatchProcessingResult, StorageStatus, and CatalogStatus.
    """
    batch_id: str
    status: Union[BatchExecutionStatus, PipelineExecutionStatus, str]
    source_file: str
    table_name: str = "orders"
    input_ref: Optional[InputBatchReference] = None
    batch_processing: Optional[BatchProcessingResult] = None
    storage_status: Optional[StorageStatus] = None
    catalog_status: Optional[CatalogStatus] = None
    processed_path: Optional[str] = None
    artifacts: List[OutputArtifactReference] = field(default_factory=list)
    errors: List[ExecutionError] = field(default_factory=list)
    error_message: Optional[str] = None

    def __post_init__(self):
        if not self.batch_id or not str(self.batch_id).strip():
            raise ValueError("BatchExecutionResult requires a non-empty batch_id.")
        if not self.source_file or not str(self.source_file).strip():
            raise ValueError("BatchExecutionResult requires a non-empty source_file.")
        if self.input_ref and self.input_ref.batch_id != self.batch_id:
            raise ValueError(
                f"Batch ID mismatch between result ('{self.batch_id}') and input_ref ('{self.input_ref.batch_id}')."
            )

        if isinstance(self.status, str):
            try:
                self.status = BatchExecutionStatus(self.status)
            except ValueError:
                try:
                    self.status = PipelineExecutionStatus(self.status)
                except ValueError:
                    pass

        if self.error_message and not self.errors:
            stage = ErrorStage.ORCHESTRATION
            if self.storage_status == StorageStatus.STORAGE_FAILED:
                stage = ErrorStage.STORAGE
            elif self.catalog_status == CatalogStatus.CATALOG_FAILED:
                stage = ErrorStage.CATALOG
            self.errors.append(
                ExecutionError(
                    stage=stage,
                    message=self.error_message,
                    error_code="BATCH_ERROR",
                    batch_id=self.batch_id,
                )
            )

    @classmethod
    def from_pipeline_batch_result(cls, pbr: Any) -> "BatchExecutionResult":
        """
        Forward Adapter: Constructs a strongly-typed BatchExecutionResult from a Phase 5.0 PipelineBatchResult.
        Unidirectional conversion (Phase 5.0 -> Phase 5.1).
        """
        status = BatchExecutionStatus.SUCCESS
        if getattr(pbr, "storage_status", None) == StorageStatus.STORAGE_FAILED:
            status = BatchExecutionStatus.STORAGE_FAILED
        elif getattr(pbr, "catalog_status", None) == CatalogStatus.CATALOG_FAILED:
            status = BatchExecutionStatus.CATALOG_FAILED
        elif getattr(pbr, "batch_processing", None):
            bp_status = pbr.batch_processing.status
            if bp_status == BatchProcessingStatus.PARTIAL_SUCCESS:
                status = BatchExecutionStatus.PARTIAL_SUCCESS
            elif bp_status == BatchProcessingStatus.SCHEMA_REJECTED:
                status = BatchExecutionStatus.SCHEMA_REJECTED
            elif bp_status == BatchProcessingStatus.FAILED:
                status = BatchExecutionStatus.FAILED

        artifacts: List[OutputArtifactReference] = []
        if getattr(pbr, "processed_path", None):
            artifacts.append(
                OutputArtifactReference(
                    artifact_type=ArtifactType.PROCESSED_PARQUET,
                    file_path=pbr.processed_path,
                    record_count=getattr(pbr.batch_processing, "valid_record_count", None),
                )
            )
        if getattr(pbr, "batch_processing", None) and getattr(pbr.batch_processing, "quarantine", None):
            q = pbr.batch_processing.quarantine
            if getattr(q, "parquet_path", None):
                artifacts.append(
                    OutputArtifactReference(
                        artifact_type=ArtifactType.QUARANTINE_PARQUET,
                        file_path=q.parquet_path,
                        record_count=getattr(q, "record_count", None),
                    )
                )
            if getattr(q, "metadata_path", None):
                artifacts.append(
                    OutputArtifactReference(
                        artifact_type=ArtifactType.QUARANTINE_METADATA,
                        file_path=q.metadata_path,
                    )
                )

        return cls(
            batch_id=pbr.batch_id,
            status=status,
            source_file=pbr.source_file,
            batch_processing=pbr.batch_processing,
            storage_status=pbr.storage_status,
            catalog_status=pbr.catalog_status,
            processed_path=pbr.processed_path,
            artifacts=artifacts,
            error_message=pbr.error_message,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status.value if hasattr(self.status, "value") else str(self.status),
            "source_file": self.source_file,
            "table_name": self.table_name,
            "input_ref": self.input_ref.to_dict() if self.input_ref else None,
            "batch_processing": self.batch_processing.to_dict() if self.batch_processing else None,
            "storage_status": self.storage_status.value if self.storage_status else None,
            "catalog_status": self.catalog_status.value if self.catalog_status else None,
            "processed_path": self.processed_path,
            "artifacts": [a.to_dict() for a in self.artifacts],
            "errors": [e.to_dict() for e in self.errors],
            "error_message": self.error_message,
        }


@dataclass
class PipelineExecutionResult:
    """
    Strongly-typed contract for an overall multi-batch pipeline execution.
    Enforces invariant checks: non-negative counters, batch summation consistency,
    record count bounds, and status validation.
    """
    run_id: str
    status: Union[PipelineExecutionStatus, str]
    started_at: str
    completed_at: str
    total_batches: int

    # A. Batch processing counters
    successful_batches: int
    partial_batches: int
    rejected_batches: int
    failed_batches: int

    # B. Storage counters
    committed_batches: int
    already_processed_batches: int
    skipped_storage_batches: int
    storage_failed_batches: int

    # C. Catalog counters
    logged_batches: int
    catalog_failed_batches: int

    # Data record counters
    total_records: int
    total_valid_records: int
    total_quarantined_records: int

    batch_results: List[Union[BatchExecutionResult, Any]] = field(default_factory=list)
    artifacts: List[OutputArtifactReference] = field(default_factory=list)
    errors: List[ExecutionError] = field(default_factory=list)
    error_message: Optional[str] = None

    def __post_init__(self):
        if not self.run_id or not str(self.run_id).strip():
            raise ValueError("PipelineExecutionResult requires a non-empty run_id.")
        if not self.started_at or not str(self.started_at).strip():
            raise ValueError("PipelineExecutionResult requires a non-empty started_at.")
        if not self.completed_at or not str(self.completed_at).strip():
            raise ValueError("PipelineExecutionResult requires a non-empty completed_at.")

        # Invariant 1: Counters cannot be negative
        for name in [
            "total_batches", "successful_batches", "partial_batches",
            "rejected_batches", "failed_batches", "committed_batches",
            "already_processed_batches", "skipped_storage_batches",
            "storage_failed_batches", "logged_batches", "catalog_failed_batches",
            "total_records", "total_valid_records", "total_quarantined_records",
        ]:
            val = getattr(self, name, 0)
            if val < 0:
                raise ValueError(f"Counter '{name}' cannot be negative (got {val}).")

        # Invariant 2: Batch count summation consistency
        sum_batches = (
            self.successful_batches
            + self.partial_batches
            + self.rejected_batches
            + self.failed_batches
        )
        if self.total_batches != sum_batches:
            raise ValueError(
                f"Batch counter mismatch: total_batches ({self.total_batches}) does not equal "
                f"sum of batch outcomes ({sum_batches} = {self.successful_batches} succ + "
                f"{self.partial_batches} part + {self.rejected_batches} rej + {self.failed_batches} fail)."
            )

        # Invariant 3: batch_results length matches total_batches when results provided
        if self.batch_results and len(self.batch_results) != self.total_batches:
            raise ValueError(
                f"batch_results length ({len(self.batch_results)}) does not match total_batches ({self.total_batches})."
            )

        # Invariant 4: Record count consistency
        if self.total_valid_records > self.total_records:
            raise ValueError(
                f"total_valid_records ({self.total_valid_records}) cannot exceed total_records ({self.total_records})."
            )
        if self.total_valid_records + self.total_quarantined_records > self.total_records:
            raise ValueError(
                f"Sum of valid ({self.total_valid_records}) and quarantined ({self.total_quarantined_records}) records "
                f"exceeds total_records ({self.total_records})."
            )

        # Invariant 5: Status consistency
        status_val = self.status.value if hasattr(self.status, "value") else str(self.status)

        if status_val == "SUCCESS":
            if self.failed_batches > 0:
                raise ValueError(f"Inconsistent result: status is SUCCESS but failed_batches={self.failed_batches}.")
            if self.storage_failed_batches > 0:
                raise ValueError(f"Inconsistent result: status is SUCCESS but storage_failed_batches={self.storage_failed_batches}.")
            if self.catalog_failed_batches > 0:
                raise ValueError(f"Inconsistent result: status is SUCCESS but catalog_failed_batches={self.catalog_failed_batches}.")
            if self.rejected_batches > 0:
                raise ValueError(f"Inconsistent result: status is SUCCESS but rejected_batches={self.rejected_batches}.")
            if self.partial_batches > 0:
                raise ValueError(f"Inconsistent result: status is SUCCESS but partial_batches={self.partial_batches}.")

    @classmethod
    def from_phase5_result(cls, r: Any) -> "PipelineExecutionResult":
        """
        Forward Adapter: Converts a Phase 5.0 PipelineExecutionResult into a Phase 5.1 PipelineExecutionResult.
        Unidirectional conversion (Phase 5.0 -> Phase 5.1).
        """
        batch_results: List[BatchExecutionResult] = [
            BatchExecutionResult.from_pipeline_batch_result(b)
            if not isinstance(b, BatchExecutionResult) else b
            for b in getattr(r, "batch_results", [])
        ]
        all_artifacts: List[OutputArtifactReference] = []
        for b in batch_results:
            all_artifacts.extend(b.artifacts)

        return cls(
            run_id=r.run_id,
            status=r.status.value if hasattr(r.status, "value") else str(r.status),
            started_at=r.started_at,
            completed_at=r.completed_at,
            total_batches=r.total_batches,
            successful_batches=r.successful_batches,
            partial_batches=r.partial_batches,
            rejected_batches=r.rejected_batches,
            failed_batches=r.failed_batches,
            committed_batches=r.committed_batches,
            already_processed_batches=r.already_processed_batches,
            skipped_storage_batches=r.skipped_storage_batches,
            storage_failed_batches=r.storage_failed_batches,
            logged_batches=r.logged_batches,
            catalog_failed_batches=r.catalog_failed_batches,
            total_records=r.total_records,
            total_valid_records=r.total_valid_records,
            total_quarantined_records=r.total_quarantined_records,
            batch_results=batch_results,
            artifacts=all_artifacts,
            error_message=r.error_message,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status.value if hasattr(self.status, "value") else str(self.status),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "total_batches": self.total_batches,
            "successful_batches": self.successful_batches,
            "partial_batches": self.partial_batches,
            "rejected_batches": self.rejected_batches,
            "failed_batches": self.failed_batches,
            "committed_batches": self.committed_batches,
            "already_processed_batches": self.already_processed_batches,
            "skipped_storage_batches": self.skipped_storage_batches,
            "storage_failed_batches": self.storage_failed_batches,
            "logged_batches": self.logged_batches,
            "catalog_failed_batches": self.catalog_failed_batches,
            "total_records": self.total_records,
            "total_valid_records": self.total_valid_records,
            "total_quarantined_records": self.total_quarantined_records,
            "batch_results": [b.to_dict() if hasattr(b, "to_dict") else b for b in self.batch_results],
            "artifacts": [a.to_dict() for a in self.artifacts],
            "errors": [e.to_dict() for e in self.errors],
            "error_message": self.error_message,
        }
