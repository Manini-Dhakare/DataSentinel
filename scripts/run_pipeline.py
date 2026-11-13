#!/usr/bin/env python3
"""
Pipeline CLI Runner & End-to-End Execution Interface for SchemaGuard (Phase 5.3).

Executes multi-batch pipeline runs, reconciles record and artifact lifecycles,
and generates machine-readable JSON and human-readable Markdown reports.

Usage:
    python scripts/run_pipeline.py --input-dir data/raw
    python scripts/run_pipeline.py --input-dir data/raw --output-dir data/processed --report-format both
"""

import sys
import os

# Ensure repository root is on sys.path for direct CLI execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from typing import Optional, List, Dict, Any
import yaml

from src.transformation.pipeline_orchestrator import (
    PipelineOrchestrator,
    PipelineExecutionStatus,
)
from src.reconciliation.reconciliation_engine import (
    ReconciliationEngine,
)
from src.reconciliation.models import (
    ReconciliationStatus,
)
from src.reconciliation.report_generator import (
    ReportGenerator,
)
from src.monitoring.metrics_collector import (
    MetricsCollector,
)


def load_yaml_config(path: str) -> Dict[str, Any]:
    """Loads configuration YAML safely."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Configuration file not found: '{path}'")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}


def print_console_summary(
    pipeline_result: Any,
    recon_result: Any,
    json_path: Optional[str] = None,
    md_path: Optional[str] = None,
    metrics_path: Optional[str] = None,
) -> None:
    """Prints a structured execution and reconciliation summary to stdout."""
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
    rates = recon_result.details or {}

    print("\n" + "=" * 70)
    print("SchemaGuard Pipeline Execution & Reconciliation Summary")
    print("=" * 70)
    print(f"Run ID:                  {pipeline_result.run_id}")
    print(f"Pipeline Status:         {p_status}")
    print(f"Reconciliation Verdict:  {r_status}")
    print(f"Started:                 {pipeline_result.started_at}")
    print(f"Completed:               {pipeline_result.completed_at}")
    print("-" * 70)
    print(f"Batch Execution ({pipeline_result.total_batches} total):")
    print(f"  - Successful:          {pipeline_result.successful_batches}")
    print(f"  - Partial / Quarantined: {pipeline_result.partial_batches}")
    print(f"  - Schema Rejected:     {pipeline_result.rejected_batches}")
    print(f"  - Ingestion Failed:    {pipeline_result.failed_batches}")
    print("-" * 70)
    print("Record Conservation:")
    print(f"  - Total Input Rows:    {recon_result.input_record_count}")
    print(f"  - Valid Persisted:     {recon_result.valid_record_count}")
    print(f"  - Quarantined:         {recon_result.quarantined_record_count}")
    print(f"  - Unaccounted Rows:    {recon_result.unaccounted_record_count}")
    print(f"  - Reconciliation Rate: {rates.get('reconciliation_rate_pct', 0.0)}%")
    print("-" * 70)
    print(f"Durable Artifacts ({recon_result.total_artifacts} registered):")
    print(f"  - Processed Parquet:   {recon_result.processed_artifacts_count}")
    print(f"  - Quarantine Assets:   {recon_result.quarantine_artifacts_count}")
    print(f"  - Missing on Disk:     {recon_result.missing_artifacts_count}")
    print(f"  - Duplicate Refs:      {recon_result.duplicate_artifacts_count}")
    if json_path or md_path or metrics_path:
        print("-" * 70)
        print("Generated Reports & Telemetry:")
        if json_path:
            print(f"  - JSON Report:         {json_path}")
        if md_path:
            print(f"  - Markdown Report:     {md_path}")
        if metrics_path:
            print(f"  - Operational Metrics: {metrics_path}")
    print("=" * 70 + "\n")


def build_arg_parser() -> argparse.ArgumentParser:
    """Constructs command line argument parser for SchemaGuard pipeline."""
    parser = argparse.ArgumentParser(
        description="SchemaGuard End-to-End Pipeline Execution & Reconciliation Runner"
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Input landing directory containing Parquet batch files",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Destination directory for processed Parquet files (default: from config)",
    )
    parser.add_argument(
        "--config",
        default="config/pipeline_config.yaml",
        help="Path to pipeline configuration YAML (default: config/pipeline_config.yaml)",
    )
    parser.add_argument(
        "--report-dir",
        default=None,
        help="Directory to save reconciliation reports (default: from config or data/reports)",
    )
    parser.add_argument(
        "--report-format",
        choices=["json", "markdown", "both"],
        default="both",
        help="Reconciliation report format (default: both)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Explicit unique run identifier (default: generated UUID)",
    )
    parser.add_argument(
        "--reprocess",
        action="store_true",
        default=False,
        help="Allow legitimate reprocessing when source data has changed",
    )
    parser.add_argument(
        "--metrics",
        action="store_true",
        default=False,
        help="Generate operational monitoring metrics report (operational_metrics_{run_id}.json)",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """
    Main entry point for CLI execution.
    Returns:
        0: Pipeline SUCCESS and Reconciliation RECONCILED.
        1: Partial success, failures, or operational errors.
    """
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    # 1. Validate Input Directory
    if not os.path.exists(args.input_dir):
        print(f"Error: Input directory does not exist: '{args.input_dir}'", file=sys.stderr)
        return 1
    if not os.path.isdir(args.input_dir):
        print(f"Error: Input path is not a directory: '{args.input_dir}'", file=sys.stderr)
        return 1

    # 2. Validate and Load Configuration
    try:
        config = load_yaml_config(args.config)
    except Exception as e:
        print(f"Error loading configuration '{args.config}': {e}", file=sys.stderr)
        return 1

    storage_cfg = config.get("storage", {})

    # 3. Resolve Output Directory
    output_dir = args.output_dir or storage_cfg.get("processed_dir", "data/processed")
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        print(f"Error creating output directory '{output_dir}': {e}", file=sys.stderr)
        return 1

    # 4. Resolve Report Directory
    report_dir = args.report_dir or storage_cfg.get("reports_dir", "data/reports")
    try:
        os.makedirs(report_dir, exist_ok=True)
    except Exception as e:
        print(f"Error creating report directory '{report_dir}': {e}", file=sys.stderr)
        return 1

    # 5. Resolve Other Storage Settings
    quarantine_dir = storage_cfg.get("quarantine_dir", "data/quarantine")
    metadata_db_path = storage_cfg.get("metadata_db_path", "data/metadata/schemaguard_metadata.duckdb")

    # 6. Initialize Orchestrator
    try:
        orchestrator = PipelineOrchestrator(
            pipeline_config_path=args.config,
            base_processed_dir=output_dir,
            base_quarantine_dir=quarantine_dir,
            metadata_db_path=metadata_db_path,
        )
    except Exception as e:
        print(f"Error initializing PipelineOrchestrator: {e}", file=sys.stderr)
        return 1

    # 7. Execute Pipeline
    try:
        pipeline_result = orchestrator.run_pipeline(
            input_dir=args.input_dir,
            run_id=args.run_id,
            allow_reprocessing=getattr(args, "reprocess", False),
        )
    except Exception as e:
        print(f"Critical execution error during pipeline run: {e}", file=sys.stderr)
        return 1

    # 8. Reconcile
    try:
        recon_engine = ReconciliationEngine()
        recon_result = recon_engine.reconcile(pipeline_result)
    except Exception as e:
        print(f"Error during reconciliation: {e}", file=sys.stderr)
        return 1

    # 9. Generate Reports & Operational Metrics
    json_path = None
    md_path = None
    metrics_path = None
    generator = ReportGenerator()

    try:
        if args.report_format in ("json", "both"):
            json_path = generator.generate_json_report(
                pipeline_result=pipeline_result,
                recon_result=recon_result,
                output_dir=report_dir,
            )
        if args.report_format in ("markdown", "both"):
            md_path = generator.generate_markdown_report(
                pipeline_result=pipeline_result,
                recon_result=recon_result,
                output_dir=report_dir,
            )
        if getattr(args, "metrics", False):
            metrics_collector = MetricsCollector()
            metrics = metrics_collector.collect_metrics(
                pipeline_result=pipeline_result,
                recon_result=recon_result,
                catalog=orchestrator.metadata_catalog,
            )
            metrics_path = metrics_collector.save_metrics_report(
                metrics=metrics,
                output_dir=report_dir,
            )
    except Exception as e:
        print(f"Error generating reports: {e}", file=sys.stderr)
        return 1

    # 10. Print Console Summary
    print_console_summary(
        pipeline_result=pipeline_result,
        recon_result=recon_result,
        json_path=json_path,
        md_path=md_path,
        metrics_path=metrics_path,
    )

    # 11. Exit Code Evaluation
    p_success = pipeline_result.status == PipelineExecutionStatus.SUCCESS
    r_reconciled = recon_result.status == ReconciliationStatus.RECONCILED

    if p_success and r_reconciled:
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
