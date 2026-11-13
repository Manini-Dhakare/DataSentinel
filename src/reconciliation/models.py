"""
Reconciliation Contracts and Strongly-Typed Result Models for SchemaGuard (Phase 5.3).

Defines strongly-typed reconciliation models:
- ReconciliationStatus (RECONCILED, PARTIAL, FAILED)
- IssueSeverity (INFO, WARNING, ERROR, CRITICAL)
- ReconciliationIssue (formal operational discrepancy or warning record)
- ReconciliationResult (comprehensive reconciliation verdict and accounting summary)
"""

from enum import Enum
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field


class ReconciliationStatus(str, Enum):
    """Overall verdict of the data reconciliation process."""
    RECONCILED = "RECONCILED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class IssueSeverity(str, Enum):
    """Severity classification for reconciliation issues."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ReconciliationIssue:
    """
    Structured reconciliation issue capturing anomaly code, severity, message,
    contextual batch ID, artifact path, and timestamp.
    """
    issue_code: str
    severity: Union[IssueSeverity, str]
    message: str
    batch_id: Optional[str] = None
    artifact_path: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self):
        if not self.issue_code or not str(self.issue_code).strip():
            raise ValueError("ReconciliationIssue requires a non-empty issue_code.")
        if not self.message or not str(self.message).strip():
            raise ValueError("ReconciliationIssue requires a non-empty message.")
        if isinstance(self.severity, str):
            try:
                self.severity = IssueSeverity(self.severity.upper())
            except ValueError:
                pass

    def to_dict(self) -> Dict[str, Any]:
        return {
            "issue_code": self.issue_code,
            "severity": self.severity.value if isinstance(self.severity, IssueSeverity) else str(self.severity),
            "message": self.message,
            "batch_id": self.batch_id,
            "artifact_path": self.artifact_path,
            "details": self.details or {},
            "timestamp": self.timestamp,
        }


@dataclass
class ReconciliationResult:
    """
    Strongly-typed contract for an end-to-end reconciliation verdict.
    Enforces non-negative counters, record boundary accounting,
    and mathematical consistency.
    """
    run_id: str
    status: Union[ReconciliationStatus, str]
    input_record_count: int
    valid_record_count: int
    quarantined_record_count: int
    unaccounted_record_count: int
    total_batches: int
    successful_batches: int
    partial_batches: int
    rejected_batches: int
    failed_batches: int
    total_artifacts: int
    processed_artifacts_count: int
    quarantine_artifacts_count: int
    missing_artifacts_count: int
    duplicate_artifacts_count: int
    persisted_record_count: int = 0
    issues: List[ReconciliationIssue] = field(default_factory=list)
    reconciled_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    pipeline_status: str = "UNKNOWN"
    details: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if not self.run_id or not str(self.run_id).strip():
            raise ValueError("ReconciliationResult requires a non-empty run_id.")

        # Invariant 1: Counters cannot be negative
        counter_fields = [
            "input_record_count",
            "valid_record_count",
            "persisted_record_count",
            "quarantined_record_count",
            "unaccounted_record_count",
            "total_batches",
            "successful_batches",
            "partial_batches",
            "rejected_batches",
            "failed_batches",
            "total_artifacts",
            "processed_artifacts_count",
            "quarantine_artifacts_count",
            "missing_artifacts_count",
            "duplicate_artifacts_count",
        ]
        for name in counter_fields:
            val = getattr(self, name, 0)
            if val < 0:
                raise ValueError(f"Counter '{name}' cannot be negative (got {val}).")

        # Invariant 2: Record boundary consistency
        if self.valid_record_count + self.quarantined_record_count > self.input_record_count:
            raise ValueError(
                f"Valid records ({self.valid_record_count}) + quarantined records ({self.quarantined_record_count}) "
                f"cannot exceed total input records ({self.input_record_count})."
            )

        # Invariant 3: Accounting balance
        expected_unaccounted = self.input_record_count - (self.valid_record_count + self.quarantined_record_count)
        if self.unaccounted_record_count != expected_unaccounted:
            raise ValueError(
                f"Unaccounted record count mismatch: expected {expected_unaccounted} "
                f"({self.input_record_count} - {self.valid_record_count} - {self.quarantined_record_count}), "
                f"got {self.unaccounted_record_count}."
            )

        # Invariant 4: Status type coercion
        if isinstance(self.status, str):
            try:
                self.status = ReconciliationStatus(self.status.upper())
            except ValueError:
                pass

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status.value if isinstance(self.status, ReconciliationStatus) else str(self.status),
            "pipeline_status": self.pipeline_status,
            "input_record_count": self.input_record_count,
            "valid_record_count": self.valid_record_count,
            "persisted_record_count": self.persisted_record_count,
            "quarantined_record_count": self.quarantined_record_count,
            "unaccounted_record_count": self.unaccounted_record_count,
            "total_batches": self.total_batches,
            "successful_batches": self.successful_batches,
            "partial_batches": self.partial_batches,
            "rejected_batches": self.rejected_batches,
            "failed_batches": self.failed_batches,
            "total_artifacts": self.total_artifacts,
            "processed_artifacts_count": self.processed_artifacts_count,
            "quarantine_artifacts_count": self.quarantine_artifacts_count,
            "missing_artifacts_count": self.missing_artifacts_count,
            "duplicate_artifacts_count": self.duplicate_artifacts_count,
            "issues": [issue.to_dict() for issue in self.issues],
            "reconciled_at": self.reconciled_at,
            "details": self.details or {},
        }
