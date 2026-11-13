"""
Reconciliation Engine for SchemaGuard (Phase 5.3).

Evaluates end-to-end multi-batch execution results against data boundaries,
artifact lineage, and accounting invariants:
1. Validates record input, valid, and quarantine conservation.
2. Identifies and categorizes unaccounted records (schema rejections, ingestion failures).
3. Verifies physical output artifact integrity, missing artifacts, and duplicate references.
4. Checks batch execution count consistency and component-level failure containment.
5. Emits strongly typed ReconciliationResult with granular ReconciliationIssues.
"""

import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set

from src.orchestration.models import (
    PipelineExecutionResult,
    PipelineExecutionStatus,
    BatchExecutionResult,
    BatchExecutionStatus,
    ArtifactType,
    OutputArtifactReference,
    ExecutionError,
)
from src.storage.storage_manager import StorageStatus
from src.storage.metadata_catalog import CatalogStatus
from src.reconciliation.models import (
    ReconciliationStatus,
    IssueSeverity,
    ReconciliationIssue,
    ReconciliationResult,
)


class ReconciliationEngine:
    """
    Engine for comprehensive end-to-end pipeline execution reconciliation.
    """

    @staticmethod
    def _normalize_path(path: str) -> str:
        """
        Normalize file path for Windows-safe, case-insensitive, slash-agnostic comparison.
        Converts relative paths to absolute and applies normcase.
        """
        if not path or not isinstance(path, str):
            return ""
        try:
            return os.path.normcase(os.path.abspath(os.path.normpath(path.strip())))
        except Exception:
            return path.strip().lower()

    def reconcile(self, pipeline_result: PipelineExecutionResult) -> ReconciliationResult:
        """
        Executes complete reconciliation of a PipelineExecutionResult.
        """
        run_id = pipeline_result.run_id
        issues: List[ReconciliationIssue] = []

        # 1. Record Accounting
        input_records = pipeline_result.total_records
        valid_records = pipeline_result.total_valid_records
        quarantined_records = pipeline_result.total_quarantined_records

        # Boundary check
        if valid_records + quarantined_records > input_records:
            issues.append(
                ReconciliationIssue(
                    issue_code="RECORD_COUNT_OVERFLOW",
                    severity=IssueSeverity.CRITICAL,
                    message=(
                        f"Valid records ({valid_records}) + quarantined records ({quarantined_records}) "
                        f"exceed total input records ({input_records})."
                    ),
                )
            )
            unaccounted_records = 0
        else:
            unaccounted_records = input_records - (valid_records + quarantined_records)

        # 2. Unaccounted Record Attribution
        if unaccounted_records > 0:
            # Check how many records belong to schema-rejected or failed batches
            rejected_batch_records = 0
            failed_batch_records = 0

            for b in pipeline_result.batch_results:
                b_rows = getattr(b.batch_processing, "row_count", 0) if getattr(b, "batch_processing", None) else 0
                b_status = getattr(b, "status", None)
                if b_status == BatchExecutionStatus.SCHEMA_REJECTED or (
                    getattr(b, "batch_processing", None) and b.batch_processing.status.value == "SCHEMA_REJECTED"
                ):
                    rejected_batch_records += b_rows
                    issues.append(
                        ReconciliationIssue(
                            issue_code="SCHEMA_REJECTED_RECORDS",
                            severity=IssueSeverity.WARNING,
                            message=f"Batch '{b.batch_id}' schema was rejected; {b_rows} records were not ingested.",
                            batch_id=b.batch_id,
                            details={"row_count": b_rows},
                        )
                    )
                elif b_status == BatchExecutionStatus.FAILED or (
                    getattr(b, "batch_processing", None) and b.batch_processing.status.value == "FAILED"
                ):
                    failed_batch_records += b_rows
                    issues.append(
                        ReconciliationIssue(
                            issue_code="FAILED_BATCH_RECORDS",
                            severity=IssueSeverity.ERROR,
                            message=f"Batch '{b.batch_id}' processing failed; {b_rows} records uningested.",
                            batch_id=b.batch_id,
                            details={"row_count": b_rows},
                        )
                    )

            unexplained_records = unaccounted_records - (rejected_batch_records + failed_batch_records)
            if unexplained_records > 0:
                issues.append(
                    ReconciliationIssue(
                        issue_code="UNEXPLAINED_UNACCOUNTED_RECORDS",
                        severity=IssueSeverity.ERROR,
                        message=(
                            f"{unexplained_records} input records are completely unaccounted for "
                            f"(total input: {input_records}, valid: {valid_records}, quarantined: {quarantined_records}, "
                            f"rejected: {rejected_batch_records}, failed: {failed_batch_records})."
                        ),
                        details={"unexplained_count": unexplained_records},
                    )
                )

        # 3. Batch Counter Consistency
        batch_sum = (
            pipeline_result.successful_batches
            + pipeline_result.partial_batches
            + pipeline_result.rejected_batches
            + pipeline_result.failed_batches
        )
        if batch_sum != pipeline_result.total_batches:
            issues.append(
                ReconciliationIssue(
                    issue_code="BATCH_COUNT_MISMATCH",
                    severity=IssueSeverity.CRITICAL,
                    message=(
                        f"Batch outcome sum ({batch_sum}) does not match total batches "
                        f"({pipeline_result.total_batches})."
                    ),
                )
            )

        if pipeline_result.batch_results and len(pipeline_result.batch_results) != pipeline_result.total_batches:
            issues.append(
                ReconciliationIssue(
                    issue_code="BATCH_RESULTS_LENGTH_MISMATCH",
                    severity=IssueSeverity.CRITICAL,
                    message=(
                        f"batch_results list length ({len(pipeline_result.batch_results)}) does not match "
                        f"total_batches ({pipeline_result.total_batches})."
                    ),
                )
            )

        # 4. Component-Level Failure Checks (Storage & Catalog)
        for b in pipeline_result.batch_results:
            b_id = getattr(b, "batch_id", "unknown")
            s_status = getattr(b, "storage_status", None)
            c_status = getattr(b, "catalog_status", None)

            if s_status == StorageStatus.STORAGE_FAILED or str(s_status) == "StorageStatus.STORAGE_FAILED" or str(s_status) == "STORAGE_FAILED":
                issues.append(
                    ReconciliationIssue(
                        issue_code="STORAGE_FAILURE",
                        severity=IssueSeverity.ERROR,
                        message=f"Storage persistence failed for batch '{b_id}': {getattr(b, 'error_message', 'Unknown error')}",
                        batch_id=b_id,
                    )
                )
            if c_status == CatalogStatus.CATALOG_FAILED or str(c_status) == "CatalogStatus.CATALOG_FAILED" or str(c_status) == "CATALOG_FAILED":
                issues.append(
                    ReconciliationIssue(
                        issue_code="CATALOG_FAILURE",
                        severity=IssueSeverity.WARNING,
                        message=f"Metadata catalog logging failed for batch '{b_id}': {getattr(b, 'error_message', 'Unknown error')}",
                        batch_id=b_id,
                    )
                )

            # Check if records were quarantined
            b_invalid = getattr(b.batch_processing, "invalid_record_count", 0) if getattr(b, "batch_processing", None) else 0
            if b_invalid > 0:
                issues.append(
                    ReconciliationIssue(
                        issue_code="RECORDS_QUARANTINED",
                        severity=IssueSeverity.INFO,
                        message=f"Batch '{b_id}' had {b_invalid} invalid records routed to quarantine.",
                        batch_id=b_id,
                        details={"invalid_record_count": b_invalid},
                    )
                )

        # 5. Output Artifact Lineage & Physical Existence
        processed_artifacts_count = 0
        quarantine_artifacts_count = 0
        missing_artifacts_count = 0
        duplicate_artifacts_count = 0
        persisted_record_count = 0

        seen_artifact_paths: Set[str] = set()

        for art in pipeline_result.artifacts:
            art_path = getattr(art, "file_path", "")
            art_path_norm = self._normalize_path(art_path)
            art_type = art.artifact_type.value if hasattr(art.artifact_type, "value") else str(art.artifact_type)
            is_processed = (art_type == ArtifactType.PROCESSED_PARQUET.value or art_type == "PROCESSED_PARQUET")
            is_quarantine = (
                art_type in (
                    ArtifactType.QUARANTINE_PARQUET.value,
                    ArtifactType.QUARANTINE_METADATA.value,
                    "QUARANTINE_PARQUET",
                    "QUARANTINE_METADATA",
                )
            )

            # Duplicate check using normalized path
            is_duplicate = False
            if art_path_norm and art_path_norm in seen_artifact_paths:
                is_duplicate = True
                duplicate_artifacts_count += 1
                issues.append(
                    ReconciliationIssue(
                        issue_code="DUPLICATE_ARTIFACT_REFERENCE",
                        severity=IssueSeverity.WARNING,
                        message=f"Duplicate artifact reference encountered for path: '{art_path}'",
                        artifact_path=art_path,
                    )
                )
            else:
                if art_path_norm:
                    seen_artifact_paths.add(art_path_norm)

            # Physical existence check
            exists_on_disk = bool(art_path and os.path.isfile(art_path))
            if not exists_on_disk:
                missing_artifacts_count += 1
                issues.append(
                    ReconciliationIssue(
                        issue_code="MISSING_OUTPUT_ARTIFACT",
                        severity=IssueSeverity.ERROR,
                        message=f"Durable artifact is missing from disk: '{art_path}'",
                        artifact_path=art_path,
                    )
                )

            # Type accounting and durable record counting
            if is_processed:
                processed_artifacts_count += 1
                if exists_on_disk and not is_duplicate:
                    # Explicit contract durability check (missing durability is not assumed durable)
                    is_durable_val = getattr(art, "is_durable", None)
                    if is_durable_val is True:
                        # Record count validation
                        rec_cnt = getattr(art, "record_count", None)
                        if rec_cnt is None:
                            issues.append(
                                ReconciliationIssue(
                                    issue_code="MISSING_RECORD_COUNT",
                                    severity=IssueSeverity.WARNING,
                                    message=f"Artifact '{art_path}' is missing record_count metadata; cannot verify persisted records.",
                                    artifact_path=art_path,
                                )
                            )
                        elif isinstance(rec_cnt, bool):
                            issues.append(
                                ReconciliationIssue(
                                    issue_code="INVALID_RECORD_COUNT",
                                    severity=IssueSeverity.WARNING,
                                    message=f"Artifact '{art_path}' has boolean record_count ({rec_cnt!r}); expected non-negative integer.",
                                    artifact_path=art_path,
                                    details={"record_count": rec_cnt, "type": "bool"},
                                )
                            )
                        elif not isinstance(rec_cnt, int):
                            issues.append(
                                ReconciliationIssue(
                                    issue_code="INVALID_RECORD_COUNT",
                                    severity=IssueSeverity.WARNING,
                                    message=f"Artifact '{art_path}' has invalid record_count '{rec_cnt}' of type '{type(rec_cnt).__name__}'; expected non-negative integer.",
                                    artifact_path=art_path,
                                    details={"record_count": str(rec_cnt), "type": type(rec_cnt).__name__},
                                )
                            )
                        elif rec_cnt < 0:
                            issues.append(
                                ReconciliationIssue(
                                    issue_code="INVALID_RECORD_COUNT",
                                    severity=IssueSeverity.WARNING,
                                    message=f"Artifact '{art_path}' has negative record_count ({rec_cnt}); expected non-negative integer.",
                                    artifact_path=art_path,
                                    details={"record_count": rec_cnt},
                                )
                            )
                        else:
                            persisted_record_count += rec_cnt
                    elif is_durable_val is False:
                        issues.append(
                            ReconciliationIssue(
                                issue_code="NON_DURABLE_ARTIFACT",
                                severity=IssueSeverity.WARNING,
                                message=f"Artifact '{art_path}' is explicitly marked non-durable; excluded from persisted records.",
                                artifact_path=art_path,
                            )
                        )
                    else:
                        issues.append(
                            ReconciliationIssue(
                                issue_code="MISSING_DURABILITY_METADATA",
                                severity=IssueSeverity.WARNING,
                                message=f"Artifact '{art_path}' is missing explicit durability metadata (is_durable={is_durable_val!r}); excluded from persisted records.",
                                artifact_path=art_path,
                            )
                        )
            elif is_quarantine:
                quarantine_artifacts_count += 1

        # 6. Pipeline Status Consistency Check
        pipeline_status_str = (
            pipeline_result.status.value
            if hasattr(pipeline_result.status, "value")
            else str(pipeline_result.status)
        )

        if pipeline_status_str == "SUCCESS":
            if (
                pipeline_result.failed_batches > 0
                or pipeline_result.rejected_batches > 0
                or pipeline_result.partial_batches > 0
                or missing_artifacts_count > 0
                or unaccounted_records > 0
            ):
                issues.append(
                    ReconciliationIssue(
                        issue_code="INCONSISTENT_PIPELINE_STATUS",
                        severity=IssueSeverity.ERROR,
                        message=(
                            f"Pipeline status is SUCCESS but anomalies exist: failed_batches={pipeline_result.failed_batches}, "
                            f"rejected_batches={pipeline_result.rejected_batches}, partial_batches={pipeline_result.partial_batches}, "
                            f"missing_artifacts={missing_artifacts_count}, unaccounted_records={unaccounted_records}."
                        ),
                    )
                )

        # 7. Determine Final ReconciliationStatus
        has_critical = any(i.severity == IssueSeverity.CRITICAL for i in issues)
        has_error = any(i.severity == IssueSeverity.ERROR for i in issues)
        has_warning = any(i.severity == IssueSeverity.WARNING for i in issues)

        if has_critical:
            recon_status = ReconciliationStatus.FAILED
        elif missing_artifacts_count > 0:
            recon_status = ReconciliationStatus.FAILED
        elif any(i.issue_code == "UNEXPLAINED_UNACCOUNTED_RECORDS" for i in issues):
            recon_status = ReconciliationStatus.FAILED
        elif pipeline_status_str == "FAILED" and (valid_records == 0 and quarantined_records == 0):
            # All batches failed or rejected with no durable data produced
            recon_status = ReconciliationStatus.FAILED
        elif has_error:
            # Errors occurred (e.g. storage failure on a batch), but if another batch succeeded with durable output:
            if processed_artifacts_count > 0 or quarantine_artifacts_count > 0:
                recon_status = ReconciliationStatus.PARTIAL
            else:
                recon_status = ReconciliationStatus.FAILED
        elif pipeline_status_str == "PARTIAL_SUCCESS" or has_warning or quarantined_records > 0:
            # Data was quarantined or compatible schema changes were processed
            recon_status = ReconciliationStatus.PARTIAL
        elif pipeline_status_str == "SUCCESS" and unaccounted_records == 0 and missing_artifacts_count == 0:
            recon_status = ReconciliationStatus.RECONCILED
        else:
            recon_status = ReconciliationStatus.PARTIAL

        # Additional details
        details = {
            "accounted_record_count": valid_records + quarantined_records,
            "reconciliation_rate_pct": (
                round(((valid_records + quarantined_records) / input_records) * 100.0, 2)
                if input_records > 0
                else 100.0
            ),
            "valid_record_rate_pct": (
                round((valid_records / input_records) * 100.0, 2)
                if input_records > 0
                else 100.0
            ),
            "persisted_record_rate_pct": (
                round((persisted_record_count / input_records) * 100.0, 2)
                if input_records > 0
                else 100.0
            ),
            "quarantine_record_rate_pct": (
                round((quarantined_records / input_records) * 100.0, 2)
                if input_records > 0
                else 0.0
            ),
        }

        return ReconciliationResult(
            run_id=run_id,
            status=recon_status,
            input_record_count=input_records,
            valid_record_count=valid_records,
            quarantined_record_count=quarantined_records,
            unaccounted_record_count=unaccounted_records,
            total_batches=pipeline_result.total_batches,
            successful_batches=pipeline_result.successful_batches,
            partial_batches=pipeline_result.partial_batches,
            rejected_batches=pipeline_result.rejected_batches,
            failed_batches=pipeline_result.failed_batches,
            total_artifacts=len(pipeline_result.artifacts),
            processed_artifacts_count=processed_artifacts_count,
            quarantine_artifacts_count=quarantine_artifacts_count,
            missing_artifacts_count=missing_artifacts_count,
            duplicate_artifacts_count=duplicate_artifacts_count,
            persisted_record_count=persisted_record_count,
            issues=issues,
            pipeline_status=pipeline_status_str,
            details=details,
        )
