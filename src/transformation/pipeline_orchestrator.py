"""
Pipeline Orchestrator Module for SchemaGuard (Phase 5.2).

Coordinates multi-batch ingestion, processed storage, and operational metadata cataloging
natively using strongly-typed orchestration contracts:
1. Discovers and sequences Parquet batches deterministically as InputBatchReferences.
2. Coordinates single-batch execution through Phase 4.3 BatchProcessor.
3. Routes clean valid records into StorageManager with atomic publication and fingerprinting.
4. Logs execution lineage, schema evolutions, and validation metrics into MetadataCatalog (DuckDB).
5. Captures OutputArtifactReferences (processed Parquet, quarantine Parquet, quarantine metadata).
6. Captures structured ExecutionErrors across ingestion, storage, and catalog stages.
7. Produces authoritative BatchExecutionResult and PipelineExecutionResult with strict invariant validation.
"""

import os
import re
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Union
import yaml

from src.ingestion.batch_processor import (
    BatchProcessor,
    BatchProcessingResult,
    BatchProcessingStatus,
)
from src.storage.storage_manager import (
    StorageManager,
    StorageRecord,
    StorageStatus,
)
from src.storage.metadata_catalog import (
    MetadataCatalog,
    CatalogRecord,
    CatalogStatus,
)
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
from src.orchestration.idempotency import (
    IdempotencyManager,
    InputFingerprinter,
    IdempotencyStatus,
)
from src.orchestration.retry import (
    RetryPolicy,
    ErrorClassifier,
    RetryExhaustedError,
)


class PipelineBatchResult(BatchExecutionResult):
    """
    Backwards-compatible batch execution result subclassing BatchExecutionResult.
    Provides full attribute compatibility for Phase 5.0 and Phase 5.1 callers.
    """

    def __init__(
        self,
        batch_id: str,
        source_file: str,
        batch_processing: Optional[BatchProcessingResult] = None,
        storage_status: Optional[StorageStatus] = None,
        catalog_status: Optional[CatalogStatus] = None,
        processed_path: Optional[str] = None,
        error_message: Optional[str] = None,
        status: Optional[Union[BatchExecutionStatus, PipelineExecutionStatus, str]] = None,
        table_name: str = "orders",
        input_ref: Optional[InputBatchReference] = None,
        artifacts: Optional[List[OutputArtifactReference]] = None,
        errors: Optional[List[ExecutionError]] = None,
    ):
        if status is None:
            if storage_status == StorageStatus.STORAGE_FAILED:
                status = BatchExecutionStatus.STORAGE_FAILED
            elif catalog_status == CatalogStatus.CATALOG_FAILED:
                status = BatchExecutionStatus.CATALOG_FAILED
            elif batch_processing:
                if batch_processing.status == BatchProcessingStatus.PARTIAL_SUCCESS:
                    status = BatchExecutionStatus.PARTIAL_SUCCESS
                elif batch_processing.status == BatchProcessingStatus.SCHEMA_REJECTED:
                    status = BatchExecutionStatus.SCHEMA_REJECTED
                elif batch_processing.status == BatchProcessingStatus.FAILED:
                    status = BatchExecutionStatus.FAILED
                else:
                    status = BatchExecutionStatus.SUCCESS
            else:
                status = BatchExecutionStatus.SUCCESS

        if artifacts is None:
            artifacts = []
            if processed_path:
                rec_cnt = getattr(batch_processing, "valid_record_count", None) if batch_processing else None
                artifacts.append(
                    OutputArtifactReference(
                        artifact_type=ArtifactType.PROCESSED_PARQUET,
                        file_path=processed_path,
                        record_count=rec_cnt,
                    )
                )
            if batch_processing and getattr(batch_processing, "quarantine", None):
                q = batch_processing.quarantine
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

        super().__init__(
            batch_id=batch_id,
            status=status,
            source_file=source_file,
            table_name=table_name,
            input_ref=input_ref,
            batch_processing=batch_processing,
            storage_status=storage_status,
            catalog_status=catalog_status,
            processed_path=processed_path,
            artifacts=artifacts,
            errors=errors if errors is not None else [],
            error_message=error_message,
        )


class PipelineOrchestrator:
    """
    Coordinates multi-batch pipeline execution, data storage, and metadata logging.
    Natively produces strongly typed Phase 5.1 orchestration contracts.
    """

    def __init__(
        self,
        schema_config_path: str = "config/source_schema.yaml",
        evolution_rules_path: str = "config/evolution_rules.yaml",
        pipeline_config_path: str = "config/pipeline_config.yaml",
        customer_master_path: Optional[str] = "data/generated/customers.parquet",
        base_quarantine_dir: Optional[str] = None,
        base_processed_dir: Optional[str] = None,
        metadata_db_path: Optional[str] = None,
    ):
        self.pipeline_config_path = pipeline_config_path
        self.pipeline_config = self._load_yaml(pipeline_config_path) if os.path.exists(pipeline_config_path) else {}

        # Initialize Phase 4.3 BatchProcessor
        self.batch_processor = BatchProcessor(
            schema_config_path=schema_config_path,
            evolution_rules_path=evolution_rules_path,
            pipeline_config_path=pipeline_config_path,
            customer_master_path=customer_master_path,
            base_quarantine_dir=base_quarantine_dir,
        )

        # Initialize Phase 5.0 StorageManager
        self.storage_manager = StorageManager(
            base_processed_dir=base_processed_dir,
            pipeline_config_path=pipeline_config_path,
        )

        # Initialize Phase 5.0 MetadataCatalog
        self.metadata_catalog = MetadataCatalog(
            db_path=metadata_db_path,
            pipeline_config_path=pipeline_config_path,
        )

        # Initialize Phase 6 IdempotencyManager & RetryPolicy
        self.idempotency_manager = IdempotencyManager(self.metadata_catalog)
        retry_cfg = self.pipeline_config.get("retries", {})
        self.retry_policy = RetryPolicy(
            max_retries=retry_cfg.get("max_retries", 3),
            initial_delay_sec=retry_cfg.get("initial_delay_sec", 0.05),
            backoff_factor=retry_cfg.get("backoff_factor", 2.0),
            max_delay_sec=retry_cfg.get("max_delay_sec", 1.0),
            jitter=retry_cfg.get("jitter", True),
        )

    @staticmethod
    def _load_yaml(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, dict) else {}

    @staticmethod
    def extract_batch_id(file_path: str) -> str:
        """
        Extracts a deterministic batch ID from filename:
        - batch_<digits_or_alphanumeric> (e.g. batch_001_original.parquet -> batch_001)
        - UUID pattern
        - Fallback: sanitized base name
        """
        filename = os.path.basename(file_path)
        stem, _ = os.path.splitext(filename)

        # Standard batch prefix pattern
        m = re.match(r"^(batch_\d+[a-zA-Z0-9]*)", stem, re.IGNORECASE)
        if m:
            return m.group(1).lower()

        # Standalone UUID pattern
        m_uuid = re.match(r"^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", stem)
        if m_uuid:
            return m_uuid.group(1).lower()

        # General alphanumeric sanitization
        sanitized = re.sub(r"[^a-zA-Z0-9_\-]", "_", stem)
        if sanitized:
            return sanitized.lower()

        raise ValueError(f"Cannot extract valid batch ID from filename: '{filename}'")

    def discover_batches(
        self,
        input_dir: str,
        table_name: str = "orders",
    ) -> List[InputBatchReference]:
        """
        Discovers Parquet files in input_dir, ignores non-parquet files,
        verifies no duplicate batch IDs exist in the same run,
        and returns deterministic sorted List[InputBatchReference].
        """
        if not os.path.exists(input_dir):
            return []

        discovered: List[InputBatchReference] = []
        seen_batch_ids: Dict[str, str] = {}

        entries = [e for e in os.scandir(input_dir) if e.is_file() and e.name.lower().endswith(".parquet")]
        entries.sort(key=lambda e: e.path)

        for entry in entries:
            bid = self.extract_batch_id(entry.path)
            if bid in seen_batch_ids:
                raise ValueError(
                    f"Duplicate batch ID '{bid}' detected in input directory: "
                    f"'{seen_batch_ids[bid]}' and '{entry.name}'"
                )
            seen_batch_ids[bid] = entry.name

            try:
                fsize = os.path.getsize(entry.path)
            except OSError:
                fsize = None

            ref = InputBatchReference(
                batch_id=bid,
                file_path=entry.path,
                table_name=table_name,
                file_size_bytes=fsize,
                discovered_at=datetime.now(timezone.utc).isoformat(),
            )
            discovered.append(ref)

        return discovered

    @staticmethod
    def is_quarantine_durable(quarantine_record: Optional[Any]) -> bool:
        """
        Critical Section 5 & 6 rule:
        A quarantine result qualifies as durable output only when:
        - record_count > 0
        - status is QUARANTINED or ALREADY_EXISTS
        - parquet_path is populated and physically exists as a file
        - metadata_path is populated and physically exists as a file
        """
        if not quarantine_record:
            return False
        if getattr(quarantine_record, "record_count", 0) <= 0:
            return False
        status = getattr(quarantine_record, "status", None)
        if status not in ("QUARANTINED", "ALREADY_EXISTS"):
            return False
        p_path = getattr(quarantine_record, "parquet_path", None)
        m_path = getattr(quarantine_record, "metadata_path", None)
        if not p_path or not os.path.isfile(p_path):
            return False
        if not m_path or not os.path.isfile(m_path):
            return False
        return True

    def run_pipeline(
        self,
        input_dir: str,
        table_name: str = "orders",
        run_id: Optional[str] = None,
        allow_reprocessing: bool = False,
    ) -> PipelineExecutionResult:
        """
        Executes sequential multi-batch pipeline:
        1. Discovers input files as strongly-typed InputBatchReferences.
        2. Dispatches to BatchProcessor.
        3. Persists valid records to StorageManager with atomic publication and fingerprinting.
        4. Logs to MetadataCatalog.
        5. Collects durable OutputArtifactReferences and structured ExecutionErrors.
        6. Computes overall status and returns PipelineExecutionResult enforcing all Phase 5.1 invariants.
        """
        started_at = datetime.now(timezone.utc).isoformat()
        if not run_id:
            run_id = uuid.uuid4().hex

        # 1. Discover batches
        try:
            discovered_batches = self.discover_batches(input_dir, table_name=table_name)
        except Exception as e:
            # Duplicate batch ID or discovery crash
            completed_at = datetime.now(timezone.utc).isoformat()
            disc_err = ExecutionError(
                stage=ErrorStage.DISCOVERY,
                message=f"Batch discovery failed: {e}",
                error_code="DISCOVERY_ERROR",
            )
            res = PipelineExecutionResult(
                run_id=run_id,
                status=PipelineExecutionStatus.FAILED,
                started_at=started_at,
                completed_at=completed_at,
                total_batches=0,
                successful_batches=0,
                partial_batches=0,
                rejected_batches=0,
                failed_batches=0,
                committed_batches=0,
                already_processed_batches=0,
                skipped_storage_batches=0,
                storage_failed_batches=0,
                logged_batches=0,
                catalog_failed_batches=0,
                total_records=0,
                total_valid_records=0,
                total_quarantined_records=0,
                batch_results=[],
                artifacts=[],
                errors=[disc_err],
                error_message=f"Batch discovery failed: {e}",
            )
            self.metadata_catalog.log_pipeline_run(
                run_id=run_id,
                started_at=started_at,
                completed_at=completed_at,
                status=PipelineExecutionStatus.FAILED.value,
                total_batches=0,
                successful_batches=0,
                partial_batches=0,
                rejected_batches=0,
                failed_batches=0,
                total_records=0,
                total_valid_records=0,
                total_quarantined_records=0,
                error_message=res.error_message,
            )
            return res

        # Case 1: Empty Discovery -> SUCCESS
        if not discovered_batches:
            completed_at = datetime.now(timezone.utc).isoformat()
            res = PipelineExecutionResult(
                run_id=run_id,
                status=PipelineExecutionStatus.SUCCESS,
                started_at=started_at,
                completed_at=completed_at,
                total_batches=0,
                successful_batches=0,
                partial_batches=0,
                rejected_batches=0,
                failed_batches=0,
                committed_batches=0,
                already_processed_batches=0,
                skipped_storage_batches=0,
                storage_failed_batches=0,
                logged_batches=0,
                catalog_failed_batches=0,
                total_records=0,
                total_valid_records=0,
                total_quarantined_records=0,
                batch_results=[],
                artifacts=[],
                errors=[],
                error_message=None,
            )
            self.metadata_catalog.log_pipeline_run(
                run_id=run_id,
                started_at=started_at,
                completed_at=completed_at,
                status=PipelineExecutionStatus.SUCCESS.value,
                total_batches=0,
                successful_batches=0,
                partial_batches=0,
                rejected_batches=0,
                failed_batches=0,
                total_records=0,
                total_valid_records=0,
                total_quarantined_records=0,
            )
            return res

        pipeline_batch_results: List[PipelineBatchResult] = []
        all_pipeline_artifacts: List[OutputArtifactReference] = []
        all_pipeline_errors: List[ExecutionError] = []

        # 2. Sequential execution across discovered batches
        for batch_ref in discovered_batches:
            batch_id = batch_ref.batch_id
            file_path = batch_ref.file_path
            batch_artifacts: List[OutputArtifactReference] = []
            batch_errors: List[ExecutionError] = []

            # A. Process batch through BatchProcessor
            bp_result = self.batch_processor.process_batch(
                file_path=file_path,
                batch_id=batch_id,
                table_name=table_name,
            )

            # Ingestion errors
            if bp_result.status == BatchProcessingStatus.SCHEMA_REJECTED:
                batch_errors.append(
                    ExecutionError(
                        stage=ErrorStage.INGESTION,
                        message=bp_result.error_message or f"Schema rejected for batch '{batch_id}'",
                        error_code="SCHEMA_REJECTED",
                        batch_id=batch_id,
                    )
                )
            elif bp_result.status == BatchProcessingStatus.FAILED:
                batch_errors.append(
                    ExecutionError(
                        stage=ErrorStage.INGESTION,
                        message=bp_result.error_message or f"Processing failed for batch '{batch_id}'",
                        error_code="BATCH_PROCESSING_FAILED",
                        batch_id=batch_id,
                    )
                )

            # Quarantine artifacts
            if bp_result.quarantine:
                q = bp_result.quarantine
                if q.parquet_path and os.path.isfile(q.parquet_path):
                    batch_artifacts.append(
                        OutputArtifactReference(
                            artifact_type=ArtifactType.QUARANTINE_PARQUET,
                            file_path=q.parquet_path,
                            record_count=q.record_count,
                            is_durable=True,
                            metadata={"batch_id": batch_id, "table_name": table_name},
                        )
                    )
                if q.metadata_path and os.path.isfile(q.metadata_path):
                    batch_artifacts.append(
                        OutputArtifactReference(
                            artifact_type=ArtifactType.QUARANTINE_METADATA,
                            file_path=q.metadata_path,
                            record_count=None,
                            is_durable=True,
                            metadata={"batch_id": batch_id, "table_name": table_name},
                        )
                    )

            # B. Storage Manager processing (with bounded retry for transient I/O locks)
            storage_record: Optional[StorageRecord] = None
            if bp_result.valid_table is not None and len(bp_result.valid_table) > 0:
                try:
                    storage_record = self.retry_policy.execute(
                        f"storage_persist_{batch_id}",
                        lambda: self.storage_manager.persist_valid_records(
                            valid_table=bp_result.valid_table,
                            batch_id=batch_id,
                            table_name=table_name,
                            run_id=run_id,
                            source_file=os.path.basename(file_path),
                            allow_reprocess=allow_reprocessing,
                        ),
                    )
                except Exception as e:
                    storage_record = StorageRecord(
                        batch_id=batch_id,
                        table_name=table_name,
                        record_count=len(bp_result.valid_table),
                        parquet_path=None,
                        status=StorageStatus.STORAGE_FAILED,
                        persisted_at=datetime.now(timezone.utc).isoformat(),
                        error_message=f"Storage persistence error: {e}",
                    )
            else:
                # Valid table empty, None, or rejected
                storage_record = StorageRecord(
                    batch_id=batch_id,
                    table_name=table_name,
                    record_count=0,
                    parquet_path=None,
                    status=StorageStatus.SKIPPED,
                    persisted_at=datetime.now(timezone.utc).isoformat(),
                )

            if storage_record.status == StorageStatus.STORAGE_FAILED:
                batch_errors.append(
                    ExecutionError(
                        stage=ErrorStage.STORAGE,
                        message=storage_record.error_message or f"Storage persistence failed for batch '{batch_id}'",
                        error_code="STORAGE_FAILED",
                        batch_id=batch_id,
                    )
                )
            elif storage_record.status in (StorageStatus.COMMITTED, StorageStatus.ALREADY_PROCESSED) and storage_record.parquet_path and os.path.isfile(storage_record.parquet_path):
                batch_artifacts.append(
                    OutputArtifactReference(
                        artifact_type=ArtifactType.PROCESSED_PARQUET,
                        file_path=storage_record.parquet_path,
                        record_count=storage_record.record_count,
                        fingerprint=storage_record.fingerprint,
                        is_durable=True,
                        metadata={"batch_id": batch_id, "table_name": table_name, "run_id": run_id},
                    )
                )

            # C. Metadata Catalog logging (with bounded retry for transient database contention)
            processed_path = storage_record.parquet_path if storage_record else None
            try:
                cat_record = self.retry_policy.execute(
                    f"catalog_log_{batch_id}",
                    lambda: self.metadata_catalog.log_batch_execution(
                        run_id=run_id,
                        batch_result=bp_result,
                        processed_parquet_path=processed_path,
                    ),
                )
            except Exception as e:
                cat_record = CatalogRecord(
                    run_id=run_id,
                    batch_id=batch_id,
                    status=CatalogStatus.CATALOG_FAILED,
                    error_message=f"Catalog logging error: {e}",
                )

            if cat_record.status == CatalogStatus.CATALOG_FAILED:
                batch_errors.append(
                    ExecutionError(
                        stage=ErrorStage.CATALOG,
                        message=cat_record.error_message or f"Catalog logging failed for batch '{batch_id}'",
                        error_code="CATALOG_FAILED",
                        batch_id=batch_id,
                    )
                )

            # D. Determine granular BatchExecutionStatus
            if storage_record.status == StorageStatus.STORAGE_FAILED:
                batch_status = BatchExecutionStatus.STORAGE_FAILED
            elif cat_record.status == CatalogStatus.CATALOG_FAILED:
                batch_status = BatchExecutionStatus.CATALOG_FAILED
            elif bp_result.status == BatchProcessingStatus.SCHEMA_REJECTED:
                batch_status = BatchExecutionStatus.SCHEMA_REJECTED
            elif bp_result.status == BatchProcessingStatus.FAILED:
                batch_status = BatchExecutionStatus.FAILED
            elif bp_result.status == BatchProcessingStatus.PARTIAL_SUCCESS:
                batch_status = BatchExecutionStatus.PARTIAL_SUCCESS
            else:
                batch_status = BatchExecutionStatus.SUCCESS

            error_msg = None
            if storage_record.status == StorageStatus.STORAGE_FAILED:
                error_msg = storage_record.error_message
            elif cat_record.status == CatalogStatus.CATALOG_FAILED:
                error_msg = cat_record.error_message
            elif bp_result.error_message:
                error_msg = bp_result.error_message

            batch_res = PipelineBatchResult(
                batch_id=batch_id,
                source_file=file_path,
                batch_processing=bp_result,
                storage_status=storage_record.status,
                catalog_status=cat_record.status,
                processed_path=processed_path,
                error_message=error_msg,
                status=batch_status,
                table_name=table_name,
                input_ref=batch_ref,
                artifacts=batch_artifacts,
                errors=batch_errors,
            )
            # Register in idempotency registry if output was committed or already processed
            if storage_record and storage_record.status in (StorageStatus.COMMITTED, StorageStatus.ALREADY_PROCESSED):
                try:
                    src_fp = InputFingerprinter.compute_file_fingerprint(file_path)
                    q_path = bp_result.quarantine.parquet_path if bp_result.quarantine else None
                    self.idempotency_manager.register_execution(
                        table_name=table_name,
                        batch_id=batch_id,
                        source_file=os.path.basename(file_path),
                        source_fingerprint=src_fp,
                        run_id=run_id,
                        status=batch_status.value if hasattr(batch_status, "value") else str(batch_status),
                        processed_parquet_path=storage_record.parquet_path,
                        quarantine_parquet_path=q_path,
                        record_count=storage_record.record_count,
                    )
                except Exception:
                    pass

            pipeline_batch_results.append(batch_res)
            all_pipeline_artifacts.extend(batch_artifacts)
            all_pipeline_errors.extend(batch_errors)

        completed_at = datetime.now(timezone.utc).isoformat()

        # 3. Compute counters
        succ_batches = sum(1 for r in pipeline_batch_results if r.batch_processing.status == BatchProcessingStatus.SUCCESS)
        part_batches = sum(1 for r in pipeline_batch_results if r.batch_processing.status == BatchProcessingStatus.PARTIAL_SUCCESS)
        rej_batches  = sum(1 for r in pipeline_batch_results if r.batch_processing.status == BatchProcessingStatus.SCHEMA_REJECTED)
        fail_batches = sum(1 for r in pipeline_batch_results if r.batch_processing.status == BatchProcessingStatus.FAILED)

        comm_batches = sum(1 for r in pipeline_batch_results if r.storage_status == StorageStatus.COMMITTED)
        alr_batches  = sum(1 for r in pipeline_batch_results if r.storage_status == StorageStatus.ALREADY_PROCESSED)
        skip_batches = sum(1 for r in pipeline_batch_results if r.storage_status == StorageStatus.SKIPPED)
        stor_fail_b  = sum(1 for r in pipeline_batch_results if r.storage_status == StorageStatus.STORAGE_FAILED)

        log_batches  = sum(1 for r in pipeline_batch_results if r.catalog_status == CatalogStatus.LOGGED)
        cat_fail_b   = sum(1 for r in pipeline_batch_results if r.catalog_status == CatalogStatus.CATALOG_FAILED)

        tot_rec  = sum(r.batch_processing.row_count for r in pipeline_batch_results)
        tot_val  = sum(r.batch_processing.valid_record_count for r in pipeline_batch_results)
        tot_quar = sum(r.batch_processing.invalid_record_count for r in pipeline_batch_results)

        # 4. Durable Output Evaluation
        processed_output_durable = any(
            r.storage_status in (StorageStatus.COMMITTED, StorageStatus.ALREADY_PROCESSED)
            and r.processed_path is not None
            and os.path.isfile(r.processed_path)
            for r in pipeline_batch_results
        )

        quarantine_output_durable = any(
            self.is_quarantine_durable(r.batch_processing.quarantine)
            for r in pipeline_batch_results
        )

        has_durable_output = processed_output_durable or quarantine_output_durable

        # 5. Determine Overall PipelineExecutionStatus
        all_batches_success = (succ_batches == len(pipeline_batch_results))
        no_storage_failures = (stor_fail_b == 0)
        no_catalog_failures = (cat_fail_b == 0)

        if all_batches_success and no_storage_failures and no_catalog_failures:
            pipeline_status = PipelineExecutionStatus.SUCCESS
        elif has_durable_output:
            pipeline_status = PipelineExecutionStatus.PARTIAL_SUCCESS
        else:
            pipeline_status = PipelineExecutionStatus.FAILED

        batch_errors_str = [
            f"[{r.batch_id}] {r.error_message}"
            for r in pipeline_batch_results
            if r.error_message
        ]
        if pipeline_status == PipelineExecutionStatus.SUCCESS:
            aggregated_err = None
        elif batch_errors_str:
            aggregated_err = "Pipeline completed with issues: " + "; ".join(batch_errors_str)
        else:
            aggregated_err = "Pipeline completed with partial success or failures"

        exec_res = PipelineExecutionResult(
            run_id=run_id,
            status=pipeline_status,
            started_at=started_at,
            completed_at=completed_at,
            total_batches=len(pipeline_batch_results),
            successful_batches=succ_batches,
            partial_batches=part_batches,
            rejected_batches=rej_batches,
            failed_batches=fail_batches,
            committed_batches=comm_batches,
            already_processed_batches=alr_batches,
            skipped_storage_batches=skip_batches,
            storage_failed_batches=stor_fail_b,
            logged_batches=log_batches,
            catalog_failed_batches=cat_fail_b,
            total_records=tot_rec,
            total_valid_records=tot_val,
            total_quarantined_records=tot_quar,
            batch_results=pipeline_batch_results,
            artifacts=all_pipeline_artifacts,
            errors=all_pipeline_errors,
            error_message=aggregated_err,
        )

        # 6. Log final pipeline run summary (with bounded retry protection)
        try:
            self.retry_policy.execute(
                f"pipeline_run_log_{run_id}",
                lambda: self.metadata_catalog.log_pipeline_run(
                    run_id=run_id,
                    started_at=started_at,
                    completed_at=completed_at,
                    status=pipeline_status.value,
                    total_batches=len(pipeline_batch_results),
                    successful_batches=succ_batches,
                    partial_batches=part_batches,
                    rejected_batches=rej_batches,
                    failed_batches=fail_batches,
                    total_records=tot_rec,
                    total_valid_records=tot_val,
                    total_quarantined_records=tot_quar,
                    error_message=exec_res.error_message,
                ),
            )
        except Exception:
            pass

        return exec_res
