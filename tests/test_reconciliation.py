"""
Unit & Reconciliation Engine Tests for SchemaGuard (Phase 5.3).

Verifies:
- Fully reconciled clean batch
- Fully reconciled quarantined batch
- Mixed successful and rejected batches
- Unaccounted records detection
- Missing artifact detection
- Duplicate artifact reference detection
- Invariant enforcement on ReconciliationResult and ReconciliationIssue
- JSON report generation and schema completeness
- Markdown report generation and required section headings
"""

import os
import json
import shutil
from pathlib import Path
import pytest

from src.transformation.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineExecutionResult,
    PipelineExecutionStatus,
    BatchExecutionResult,
    BatchExecutionStatus,
    ArtifactType,
    OutputArtifactReference,
    ExecutionError,
    ErrorStage,
)
from src.reconciliation.models import (
    ReconciliationStatus,
    IssueSeverity,
    ReconciliationIssue,
    ReconciliationResult,
)
from src.reconciliation.reconciliation_engine import (
    ReconciliationEngine,
)
from src.reconciliation.report_generator import (
    ReportGenerator,
)


def test_reconciliation_issue_validation():
    # Valid issue
    issue = ReconciliationIssue(
        issue_code="TEST_ISSUE",
        severity=IssueSeverity.WARNING,
        message="Test message",
        batch_id="b1",
    )
    assert issue.issue_code == "TEST_ISSUE"
    assert issue.severity == IssueSeverity.WARNING
    assert issue.batch_id == "b1"
    d = issue.to_dict()
    assert d["issue_code"] == "TEST_ISSUE"
    assert d["severity"] == "WARNING"

    # Empty code
    with pytest.raises(ValueError, match="non-empty issue_code"):
        ReconciliationIssue(issue_code="", severity="INFO", message="msg")

    # Empty message
    with pytest.raises(ValueError, match="non-empty message"):
        ReconciliationIssue(issue_code="ERR", severity="INFO", message="  ")


def test_reconciliation_result_invariants():
    # Valid construction
    res = ReconciliationResult(
        run_id="run_001",
        status=ReconciliationStatus.RECONCILED,
        input_record_count=10,
        valid_record_count=8,
        quarantined_record_count=2,
        unaccounted_record_count=0,
        total_batches=1,
        successful_batches=0,
        partial_batches=1,
        rejected_batches=0,
        failed_batches=0,
        total_artifacts=2,
        processed_artifacts_count=1,
        quarantine_artifacts_count=1,
        missing_artifacts_count=0,
        duplicate_artifacts_count=0,
    )
    assert res.status == ReconciliationStatus.RECONCILED
    assert res.run_id == "run_001"

    # Negative counter
    with pytest.raises(ValueError, match="cannot be negative"):
        ReconciliationResult(
            run_id="run_002",
            status=ReconciliationStatus.RECONCILED,
            input_record_count=-1,
            valid_record_count=0,
            quarantined_record_count=0,
            unaccounted_record_count=0,
            total_batches=0,
            successful_batches=0,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=0,
            total_artifacts=0,
            processed_artifacts_count=0,
            quarantine_artifacts_count=0,
            missing_artifacts_count=0,
            duplicate_artifacts_count=0,
        )

    # Overflow: valid + quarantined > input
    with pytest.raises(ValueError, match="cannot exceed total input records"):
        ReconciliationResult(
            run_id="run_003",
            status=ReconciliationStatus.FAILED,
            input_record_count=10,
            valid_record_count=10,
            quarantined_record_count=5,
            unaccounted_record_count=0,
            total_batches=1,
            successful_batches=1,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=0,
            total_artifacts=0,
            processed_artifacts_count=0,
            quarantine_artifacts_count=0,
            missing_artifacts_count=0,
            duplicate_artifacts_count=0,
        )

    # Unaccounted mismatch
    with pytest.raises(ValueError, match="Unaccounted record count mismatch"):
        ReconciliationResult(
            run_id="run_004",
            status=ReconciliationStatus.FAILED,
            input_record_count=10,
            valid_record_count=8,
            quarantined_record_count=0,
            unaccounted_record_count=5,  # expected 10 - 8 = 2
            total_batches=1,
            successful_batches=1,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=0,
            total_artifacts=0,
            processed_artifacts_count=0,
            quarantine_artifacts_count=0,
            missing_artifacts_count=0,
            duplicate_artifacts_count=0,
        )


def test_reconciliation_clean_batch(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.RECONCILED
    assert recon_res.input_record_count > 0
    assert recon_res.valid_record_count == recon_res.input_record_count
    assert recon_res.quarantined_record_count == 0
    assert recon_res.unaccounted_record_count == 0
    assert recon_res.missing_artifacts_count == 0
    assert recon_res.duplicate_artifacts_count == 0
    assert recon_res.processed_artifacts_count == 1


def test_reconciliation_quarantined_batch(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_006_duplicates.parquet", landing / "batch_006_duplicates.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert recon_res.quarantined_record_count == 50
    assert recon_res.valid_record_count == 950
    assert recon_res.unaccounted_record_count == 0
    assert recon_res.missing_artifacts_count == 0
    assert any(i.issue_code == "RECORDS_QUARANTINED" for i in recon_res.issues)


def test_reconciliation_mixed_clean_and_rejected(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")
    shutil.copyfile("data/generated/batch_004_breaking_type.parquet", landing / "batch_004_breaking_type.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.PARTIAL
    assert recon_res.rejected_batches == 1
    assert recon_res.successful_batches == 1
    assert recon_res.unaccounted_record_count > 0
    assert any(i.issue_code == "SCHEMA_REJECTED_RECORDS" for i in recon_res.issues)


def test_reconciliation_missing_artifact_detection(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    # Artificially append an artifact referencing a non-existent file
    missing_art = OutputArtifactReference(
        artifact_type=ArtifactType.PROCESSED_PARQUET,
        file_path=str(tmp_path / "missing_file.parquet"),
        record_count=10,
    )
    pipe_res.artifacts.append(missing_art)

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.FAILED
    assert recon_res.missing_artifacts_count == 1
    assert any(i.issue_code == "MISSING_OUTPUT_ARTIFACT" for i in recon_res.issues)


def test_reconciliation_duplicate_artifact_detection(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    # Duplicate the existing valid artifact
    if pipe_res.artifacts:
        pipe_res.artifacts.append(pipe_res.artifacts[0])

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.duplicate_artifacts_count == 1
    assert any(i.issue_code == "DUPLICATE_ARTIFACT_REFERENCE" for i in recon_res.issues)


def test_reconciliation_empty_input_directory(tmp_path: Path):
    empty_landing = tmp_path / "empty_landing"
    empty_landing.mkdir()

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(empty_landing))

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    assert recon_res.status == ReconciliationStatus.RECONCILED
    assert recon_res.total_batches == 0
    assert recon_res.input_record_count == 0
    assert recon_res.valid_record_count == 0
    assert recon_res.quarantined_record_count == 0


def test_report_generation_json_and_markdown(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    orchestrator = PipelineOrchestrator(
        base_processed_dir=str(tmp_path / "processed"),
        metadata_db_path=str(tmp_path / "meta.duckdb"),
        base_quarantine_dir=str(tmp_path / "quarantine"),
    )
    pipe_res = orchestrator.run_pipeline(str(landing))

    engine = ReconciliationEngine()
    recon_res = engine.reconcile(pipe_res)

    report_dir = tmp_path / "reports"
    generator = ReportGenerator()

    # JSON Report
    json_path = generator.generate_json_report(pipe_res, recon_res, str(report_dir))
    assert os.path.isfile(json_path)
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert "pipeline_run" in data
    assert "reconciliation" in data
    assert "batch_execution_summary" in data
    assert "artifact_lineage" in data
    assert "structured_errors" in data
    assert "reconciliation_issues" in data
    assert data["pipeline_run"]["status"] == "SUCCESS"
    assert data["reconciliation"]["status"] == "RECONCILED"

    # Markdown Report
    md_path = generator.generate_markdown_report(pipe_res, recon_res, str(report_dir))
    assert os.path.isfile(md_path)
    with open(md_path, "r", encoding="utf-8") as f:
        md_text = f.read()

    # Check required headings
    assert "# SchemaGuard Execution Summary" in md_text
    assert "## Pipeline Run Information" in md_text
    assert "## Batch Execution Summary" in md_text
    assert "## Record Reconciliation" in md_text
    assert "## Artifact Lineage" in md_text
    assert "## Failed and Rejected Batches" in md_text
    assert "## Structured Errors" in md_text
    assert "## Reconciliation Issues" in md_text
    assert "## Final Reconciliation Verdict" in md_text
    assert "VERDICT: RECONCILED" in md_text
