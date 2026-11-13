"""
Operational Monitoring Contracts and Metrics Models for SchemaGuard (Phase 6.4).

Defines strongly-typed operational metrics contracts:
- OperationalMetrics: End-to-end execution telemetry aggregating pipeline lifecycle,
  reconciliation accounting (reused directly), historical execution counts,
  and fault-tolerance retry/failure metrics.
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class OperationalMetrics:
    """
    Comprehensive operational telemetry contract capturing execution health,
    reconciliation accounting, duration, and fault-tolerance metrics.
    """
    run_id: str
    pipeline_status: str
    reconciliation_status: str
    started_at: str
    completed_at: str
    duration_seconds: float

    # Lifetime execution counts from metadata catalog
    total_pipeline_executions: int
    successful_executions: int
    partial_executions: int
    failed_executions: int

    # Record-level accounting (reused directly from ReconciliationResult)
    input_records: int
    validated_records: int
    persisted_records: int
    quarantined_records: int
    unaccounted_records: int
    reconciliation_rate_pct: float

    # Batch-level & fault-tolerance metrics
    total_batches: int
    successful_batches: int
    partial_batches: int
    rejected_batches: int
    failed_batches: int
    retry_count: int = 0
    failure_count: int = 0

    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    details: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if not self.run_id or not str(self.run_id).strip():
            raise ValueError("OperationalMetrics requires a non-empty run_id.")
        if self.duration_seconds < 0:
            raise ValueError(f"duration_seconds cannot be negative (got {self.duration_seconds}).")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_status": self.pipeline_status,
            "reconciliation_status": self.reconciliation_status,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "execution_history": {
                "total_pipeline_executions": self.total_pipeline_executions,
                "successful_executions": self.successful_executions,
                "partial_executions": self.partial_executions,
                "failed_executions": self.failed_executions,
            },
            "record_accounting": {
                "input_records": self.input_records,
                "validated_records": self.validated_records,
                "persisted_records": self.persisted_records,
                "quarantined_records": self.quarantined_records,
                "unaccounted_records": self.unaccounted_records,
                "reconciliation_rate_pct": self.reconciliation_rate_pct,
            },
            "batch_summary": {
                "total_batches": self.total_batches,
                "successful_batches": self.successful_batches,
                "partial_batches": self.partial_batches,
                "rejected_batches": self.rejected_batches,
                "failed_batches": self.failed_batches,
            },
            "resilience": {
                "retry_count": self.retry_count,
                "failure_count": self.failure_count,
            },
            "collected_at": self.collected_at,
            "details": self.details or {},
        }
