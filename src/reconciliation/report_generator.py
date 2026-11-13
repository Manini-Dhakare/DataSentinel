"""
Report Generator for SchemaGuard (Phase 5.3).

Generates machine-readable JSON and human-readable Markdown reports
from PipelineExecutionResult and ReconciliationResult.
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, Optional

from src.orchestration.models import PipelineExecutionResult
from src.reconciliation.models import ReconciliationResult


class ReportGenerator:
    """
    Produces standardized JSON and Markdown reconciliation reports.
    """

    @staticmethod
    def _calculate_duration(started_at: str, completed_at: str) -> Optional[float]:
        try:
            t0 = datetime.fromisoformat(started_at)
            t1 = datetime.fromisoformat(completed_at)
            return round((t1 - t0).total_seconds(), 3)
        except Exception:
            return None

    def build_report_data(
        self,
        pipeline_result: PipelineExecutionResult,
        recon_result: ReconciliationResult,
    ) -> Dict[str, Any]:
        """
        Builds a comprehensive, deterministic dictionary consolidating
        pipeline execution and reconciliation findings.
        """
        duration_sec = self._calculate_duration(pipeline_result.started_at, pipeline_result.completed_at)

        # Batch execution summaries
        batch_summaries = []
        for b in pipeline_result.batch_results:
            b_id = getattr(b, "batch_id", "unknown")
            b_status = getattr(b, "status", "UNKNOWN")
            b_status_val = b_status.value if hasattr(b_status, "value") else str(b_status)
            s_status = getattr(b, "storage_status", None)
            s_status_val = s_status.value if hasattr(s_status, "value") else (str(s_status) if s_status else None)
            c_status = getattr(b, "catalog_status", None)
            c_status_val = c_status.value if hasattr(c_status, "value") else (str(c_status) if c_status else None)

            bp = getattr(b, "batch_processing", None)
            row_cnt = getattr(bp, "row_count", 0) if bp else 0
            val_cnt = getattr(bp, "valid_record_count", 0) if bp else 0
            inv_cnt = getattr(bp, "invalid_record_count", 0) if bp else 0

            b_arts = [
                {
                    "artifact_type": a.artifact_type.value if hasattr(a.artifact_type, "value") else str(a.artifact_type),
                    "file_path": a.file_path,
                    "record_count": a.record_count,
                    "fingerprint": a.fingerprint,
                    "is_durable": getattr(a, "is_durable", False) is True,
                    "exists_on_disk": os.path.isfile(a.file_path),
                }
                for a in getattr(b, "artifacts", [])
            ]

            b_errs = [
                {
                    "stage": e.stage.value if hasattr(e.stage, "value") else str(e.stage),
                    "error_code": e.error_code,
                    "message": e.message,
                    "timestamp": e.timestamp,
                }
                for e in getattr(b, "errors", [])
            ]

            batch_summaries.append(
                {
                    "batch_id": b_id,
                    "status": b_status_val,
                    "source_file": getattr(b, "source_file", ""),
                    "table_name": getattr(b, "table_name", "orders"),
                    "row_count": row_cnt,
                    "valid_record_count": val_cnt,
                    "invalid_record_count": inv_cnt,
                    "storage_status": s_status_val,
                    "catalog_status": c_status_val,
                    "processed_path": getattr(b, "processed_path", None),
                    "artifacts": b_arts,
                    "errors": b_errs,
                    "error_message": getattr(b, "error_message", None),
                }
            )

        # Artifact lineage
        artifact_lineage = [
            {
                "artifact_type": a.artifact_type.value if hasattr(a.artifact_type, "value") else str(a.artifact_type),
                "file_path": a.file_path,
                "record_count": a.record_count,
                "fingerprint": a.fingerprint,
                "is_durable": getattr(a, "is_durable", False) is True,
                "exists_on_disk": os.path.isfile(a.file_path),
                "metadata": getattr(a, "metadata", {}) or {},
            }
            for a in pipeline_result.artifacts
        ]

        # Structured errors
        structured_errors = [
            {
                "stage": e.stage.value if hasattr(e.stage, "value") else str(e.stage),
                "error_code": e.error_code,
                "message": e.message,
                "batch_id": e.batch_id,
                "timestamp": e.timestamp,
                "details": e.details or {},
            }
            for e in pipeline_result.errors
        ]

        # Reconciliation issues
        reconciliation_issues = [
            {
                "issue_code": i.issue_code,
                "severity": i.severity.value if hasattr(i.severity, "value") else str(i.severity),
                "message": i.message,
                "batch_id": i.batch_id,
                "artifact_path": i.artifact_path,
                "details": i.details or {},
                "timestamp": i.timestamp,
            }
            for i in recon_result.issues
        ]

        p_status_val = (
            pipeline_result.status.value
            if hasattr(pipeline_result.status, "value")
            else str(pipeline_result.status)
        )
        r_status_val = (
            recon_result.status.value
            if hasattr(recon_result.status, "value")
            else str(recon_result.status)
        )

        return {
            "pipeline_run": {
                "run_id": pipeline_result.run_id,
                "status": p_status_val,
                "started_at": pipeline_result.started_at,
                "completed_at": pipeline_result.completed_at,
                "duration_seconds": duration_sec,
                "total_batches": pipeline_result.total_batches,
                "successful_batches": pipeline_result.successful_batches,
                "partial_batches": pipeline_result.partial_batches,
                "rejected_batches": pipeline_result.rejected_batches,
                "failed_batches": pipeline_result.failed_batches,
                "committed_batches": pipeline_result.committed_batches,
                "already_processed_batches": pipeline_result.already_processed_batches,
                "skipped_storage_batches": pipeline_result.skipped_storage_batches,
                "storage_failed_batches": pipeline_result.storage_failed_batches,
                "logged_batches": pipeline_result.logged_batches,
                "catalog_failed_batches": pipeline_result.catalog_failed_batches,
                "error_message": pipeline_result.error_message,
            },
            "reconciliation": {
                "status": r_status_val,
                "reconciled_at": recon_result.reconciled_at,
                "input_record_count": recon_result.input_record_count,
                "valid_record_count": recon_result.valid_record_count,
                "persisted_record_count": recon_result.persisted_record_count,
                "quarantined_record_count": recon_result.quarantined_record_count,
                "unaccounted_record_count": recon_result.unaccounted_record_count,
                "rates": recon_result.details or {},
                "total_artifacts": recon_result.total_artifacts,
                "processed_artifacts_count": recon_result.processed_artifacts_count,
                "quarantine_artifacts_count": recon_result.quarantine_artifacts_count,
                "missing_artifacts_count": recon_result.missing_artifacts_count,
                "duplicate_artifacts_count": recon_result.duplicate_artifacts_count,
            },
            "batch_execution_summary": batch_summaries,
            "artifact_lineage": artifact_lineage,
            "structured_errors": structured_errors,
            "reconciliation_issues": reconciliation_issues,
        }

    def generate_json_report(
        self,
        pipeline_result: PipelineExecutionResult,
        recon_result: ReconciliationResult,
        output_dir: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Saves machine-readable JSON reconciliation report to output_dir.
        Returns the absolute path of the generated file.
        """
        os.makedirs(output_dir, exist_ok=True)
        if not filename:
            filename = f"reconciliation_report_{pipeline_result.run_id}.json"
        target_path = os.path.join(output_dir, filename)

        data = self.build_report_data(pipeline_result, recon_result)

        with open(target_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        return target_path

    def generate_markdown_report(
        self,
        pipeline_result: PipelineExecutionResult,
        recon_result: ReconciliationResult,
        output_dir: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Saves human-readable Markdown reconciliation report to output_dir.
        Returns the absolute path of the generated file.
        """
        os.makedirs(output_dir, exist_ok=True)
        if not filename:
            filename = f"reconciliation_report_{pipeline_result.run_id}.md"
        target_path = os.path.join(output_dir, filename)

        data = self.build_report_data(pipeline_result, recon_result)
        pr = data["pipeline_run"]
        rc = data["reconciliation"]
        rates = rc.get("rates", {})

        md_lines = []
        md_lines.append("# SchemaGuard Execution Summary")
        md_lines.append("")

        # 1. Pipeline Run Information
        md_lines.append("## Pipeline Run Information")
        md_lines.append("")
        md_lines.append("| Property | Value |")
        md_lines.append("| :--- | :--- |")
        md_lines.append(f"| **Run ID** | `{pr['run_id']}` |")
        md_lines.append(f"| **Pipeline Status** | **{pr['status']}** |")
        md_lines.append(f"| **Reconciliation Status** | **{rc['status']}** |")
        md_lines.append(f"| **Started At** | {pr['started_at']} |")
        md_lines.append(f"| **Completed At** | {pr['completed_at']} |")
        md_lines.append(f"| **Duration** | {pr['duration_seconds']}s |")
        md_lines.append(f"| **Total Batches** | {pr['total_batches']} |")
        md_lines.append("")

        # 2. Batch Execution Summary
        md_lines.append("## Batch Execution Summary")
        md_lines.append("")
        if data["batch_execution_summary"]:
            md_lines.append("| Batch ID | Status | Total Rows | Valid Rows | Quarantined Rows | Storage Status | Catalog Status |")
            md_lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
            for b in data["batch_execution_summary"]:
                md_lines.append(
                    f"| `{b['batch_id']}` | {b['status']} | {b['row_count']} | {b['valid_record_count']} | "
                    f"{b['invalid_record_count']} | {b['storage_status']} | {b['catalog_status']} |"
                )
        else:
            md_lines.append("*No batches were executed in this run (empty discovery).*")
        md_lines.append("")

        # 3. Record Reconciliation
        md_lines.append("## Record Reconciliation")
        md_lines.append("")
        md_lines.append("| Metric | Count | Percentage |")
        md_lines.append("| :--- | :--- | :--- |")
        md_lines.append(f"| **Total Input Records** | {rc['input_record_count']} | 100.0% |")
        md_lines.append(f"| **Validated Records** | {rc['valid_record_count']} | {rates.get('valid_record_rate_pct', 0.0)}% |")
        md_lines.append(f"| **Durably Persisted Records** | {rc['persisted_record_count']} | {rates.get('persisted_record_rate_pct', 0.0)}% |")
        md_lines.append(f"| **Quarantined Records** | {rc['quarantined_record_count']} | {rates.get('quarantine_record_rate_pct', 0.0)}% |")
        md_lines.append(f"| **Unaccounted Records** | {rc['unaccounted_record_count']} | - |")
        md_lines.append(f"| **Reconciliation Rate** | - | **{rates.get('reconciliation_rate_pct', 0.0)}%** |")
        md_lines.append("")

        # 4. Artifact Lineage
        md_lines.append("## Artifact Lineage")
        md_lines.append("")
        if data["artifact_lineage"]:
            md_lines.append("| Artifact Type | File Path | Record Count | Durable | Exists on Disk |")
            md_lines.append("| :--- | :--- | :--- | :--- | :--- |")
            for a in data["artifact_lineage"]:
                exists_str = "Yes" if a["exists_on_disk"] else "**NO (MISSING)**"
                durable_str = "Yes" if a["is_durable"] else "No"
                md_lines.append(
                    f"| `{a['artifact_type']}` | `{a['file_path']}` | {a['record_count'] if a['record_count'] is not None else '-'} | "
                    f"{durable_str} | {exists_str} |"
                )
        else:
            md_lines.append("*No durable output artifacts registered for this run.*")
        md_lines.append("")

        # 5. Failed and Rejected Batches
        md_lines.append("## Failed and Rejected Batches")
        md_lines.append("")
        failed_batches = [
            b for b in data["batch_execution_summary"]
            if b["status"] in ("SCHEMA_REJECTED", "FAILED", "STORAGE_FAILED", "CATALOG_FAILED")
        ]
        if failed_batches:
            md_lines.append("| Batch ID | Status | Error Reason |")
            md_lines.append("| :--- | :--- | :--- |")
            for fb in failed_batches:
                err_text = fb.get("error_message") or "Unknown error"
                md_lines.append(f"| `{fb['batch_id']}` | **{fb['status']}** | {err_text} |")
        else:
            md_lines.append("None. All discovered batches processed cleanly or with quarantine containment.")
        md_lines.append("")

        # 6. Structured Errors
        md_lines.append("## Structured Errors")
        md_lines.append("")
        if data["structured_errors"]:
            md_lines.append("| Stage | Error Code | Batch ID | Message |")
            md_lines.append("| :--- | :--- | :--- | :--- |")
            for err in data["structured_errors"]:
                b_str = f"`{err['batch_id']}`" if err.get("batch_id") else "-"
                md_lines.append(f"| `{err['stage']}` | `{err['error_code']}` | {b_str} | {err['message']} |")
        else:
            md_lines.append("None. Zero pipeline operational errors recorded.")
        md_lines.append("")

        # 7. Reconciliation Issues
        md_lines.append("## Reconciliation Issues")
        md_lines.append("")
        if data["reconciliation_issues"]:
            md_lines.append("| Severity | Issue Code | Batch ID | Message |")
            md_lines.append("| :--- | :--- | :--- | :--- |")
            for iss in data["reconciliation_issues"]:
                b_str = f"`{iss['batch_id']}`" if iss.get("batch_id") else "-"
                sev = iss['severity']
                sev_badge = f"**[{sev}]**" if sev in ("CRITICAL", "ERROR") else f"[{sev}]"
                md_lines.append(f"| {sev_badge} | `{iss['issue_code']}` | {b_str} | {iss['message']} |")
        else:
            md_lines.append("None. Zero reconciliation issues encountered.")
        md_lines.append("")

        # 8. Final Reconciliation Verdict
        md_lines.append("## Final Reconciliation Verdict")
        md_lines.append("")
        v_status = rc['status']
        if v_status == "RECONCILED":
            md_lines.append(
                "> **VERDICT: RECONCILED**\n"
                ">\n"
                "> All input records, batch states, and physical output artifacts are 100% accounted for with zero missing assets or unhandled exceptions."
            )
        elif v_status == "PARTIAL":
            md_lines.append(
                "> **VERDICT: PARTIAL**\n"
                ">\n"
                "> Pipeline executed with partial success or data quarantine. All valid records were durably published and all anomalies are cleanly isolated in quarantine or error logs."
            )
        else:
            md_lines.append(
                "> **VERDICT: FAILED**\n"
                ">\n"
                "> Pipeline execution or reconciliation encountered critical anomalies, missing artifacts, or unhandled failures requiring investigation."
            )
        md_lines.append("")

        content = "\n".join(md_lines)
        with open(target_path, "w", encoding="utf-8") as f:
            f.write(content)

        return target_path
