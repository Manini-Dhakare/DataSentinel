"""
Operational Metrics Collector for SchemaGuard (Phase 6.4).

Aggregates operational telemetry from:
- PipelineExecutionResult (runtime, batch outcomes, errors)
- ReconciliationResult (authoritative record conservation, persisted records)
- MetadataCatalog (historical lifetime execution totals)
"""

import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

from src.orchestration.models import PipelineExecutionResult
from src.reconciliation.models import ReconciliationResult
from src.storage.metadata_catalog import MetadataCatalog
from src.monitoring.models import OperationalMetrics


class MetricsCollector:
    """
    Collects, aggregates, and exports operational telemetry for SchemaGuard runs.
    """

    @staticmethod
    def calculate_duration_seconds(started_at: str, completed_at: str) -> float:
        """Calculates elapsed seconds between ISO-8601 timestamps."""
        try:
            start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            comp_dt = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            duration = (comp_dt - start_dt).total_seconds()
            return max(0.0, round(duration, 3))
        except Exception:
            return 0.0

    def collect_metrics(
        self,
        pipeline_result: PipelineExecutionResult,
        recon_result: ReconciliationResult,
        catalog: Optional[MetadataCatalog] = None,
        retry_count: int = 0,
    ) -> OperationalMetrics:
        """
        Constructs an OperationalMetrics record by combining execution results,
        authoritative reconciliation accounting, and catalog history.
        """
        duration = self.calculate_duration_seconds(
            pipeline_result.started_at,
            pipeline_result.completed_at,
        )

        # Retrieve lifetime historical metrics from DuckDB if available
        history_summary: Dict[str, Any] = {}
        if catalog is not None:
            try:
                history_summary = catalog.get_execution_summary()
            except Exception:
                history_summary = {}

        total_runs = history_summary.get("total_runs", 1)
        succ_runs = history_summary.get("successful_runs", 1 if pipeline_result.status.value == "SUCCESS" else 0)
        part_runs = history_summary.get("partial_runs", 1 if pipeline_result.status.value == "PARTIAL_SUCCESS" else 0)
        fail_runs = history_summary.get("failed_runs", 1 if pipeline_result.status.value == "FAILED" else 0)

        # Count failures in this execution
        failure_count = len(pipeline_result.errors) + len([
            i for i in recon_result.issues
            if getattr(i.severity, "value", str(i.severity)) in ("ERROR", "CRITICAL")
        ])

        rates = recon_result.details or {}
        recon_rate = rates.get("reconciliation_rate_pct", 100.0 if recon_result.input_record_count == 0 else 0.0)

        p_status = (
            pipeline_result.status.value
            if hasattr(pipeline_result.status, "value")
            else str(pipeline_result.status)
        )
        r_status = (
            recon_result.status.value
            if hasattr(recon_result.status, "value")
            else str(recon_result.status)
        )

        return OperationalMetrics(
            run_id=pipeline_result.run_id,
            pipeline_status=p_status,
            reconciliation_status=r_status,
            started_at=pipeline_result.started_at,
            completed_at=pipeline_result.completed_at,
            duration_seconds=duration,
            total_pipeline_executions=total_runs,
            successful_executions=succ_runs,
            partial_executions=part_runs,
            failed_executions=fail_runs,
            # Reused directly from ReconciliationResult
            input_records=recon_result.input_record_count,
            validated_records=recon_result.valid_record_count,
            persisted_records=recon_result.persisted_record_count,
            quarantined_records=recon_result.quarantined_record_count,
            unaccounted_records=recon_result.unaccounted_record_count,
            reconciliation_rate_pct=recon_rate,
            total_batches=pipeline_result.total_batches,
            successful_batches=pipeline_result.successful_batches,
            partial_batches=pipeline_result.partial_batches,
            rejected_batches=pipeline_result.rejected_batches,
            failed_batches=pipeline_result.failed_batches,
            retry_count=retry_count,
            failure_count=failure_count,
        )

    def save_metrics_report(
        self,
        metrics: OperationalMetrics,
        output_dir: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Saves operational metrics to a machine-readable JSON file.
        Returns the absolute path to the generated report.
        """
        os.makedirs(output_dir, exist_ok=True)
        if not filename:
            filename = f"operational_metrics_{metrics.run_id}.json"
        target_path = os.path.join(output_dir, filename)

        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(metrics.to_dict(), f, indent=2, ensure_ascii=False)

        return os.path.abspath(target_path)
