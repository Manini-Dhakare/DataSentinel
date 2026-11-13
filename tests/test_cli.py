"""
CLI Runner Unit & Functional Tests for SchemaGuard (Phase 5.3).

Verifies scripts/run_pipeline.py:
- Command line arguments parsing and validation
- Missing or invalid input directory handling
- Missing configuration file handling
- Report formats: json, markdown, both
- Proper exit codes (0 for SUCCESS/RECONCILED, 1 for partial/failed)
- Custom run_id propagation
- Console output and report generation
"""

import os
import json
import shutil
from pathlib import Path
import pytest

from scripts.run_pipeline import main, build_arg_parser


def test_cli_arg_parser_defaults():
    parser = build_arg_parser()
    args = parser.parse_args(["--input-dir", "data/raw"])
    assert args.input_dir == "data/raw"
    assert args.output_dir is None
    assert args.config == "config/pipeline_config.yaml"
    assert args.report_dir is None
    assert args.report_format == "both"
    assert args.run_id is None


def test_cli_missing_input_dir(tmp_path: Path):
    nonexistent = str(tmp_path / "missing_landing")
    exit_code = main(["--input-dir", nonexistent])
    assert exit_code == 1


def test_cli_input_path_is_file_not_dir(tmp_path: Path):
    f = tmp_path / "file.txt"
    f.write_text("not a dir", encoding="utf-8")
    exit_code = main(["--input-dir", str(f)])
    assert exit_code == 1


def test_cli_missing_config_file(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    exit_code = main([
        "--input-dir", str(landing),
        "--config", str(tmp_path / "missing_config.yaml"),
    ])
    assert exit_code == 1


def test_cli_clean_execution_both_reports(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    out_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    exit_code = main([
        "--input-dir", str(landing),
        "--output-dir", str(out_dir),
        "--report-dir", str(rep_dir),
        "--report-format", "both",
        "--run-id", "cli_run_001",
    ])

    assert exit_code == 0

    # Verify reports exist
    json_rep = rep_dir / "reconciliation_report_cli_run_001.json"
    md_rep = rep_dir / "reconciliation_report_cli_run_001.md"
    assert json_rep.exists()
    assert md_rep.exists()

    with open(json_rep, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["pipeline_run"]["run_id"] == "cli_run_001"
    assert data["pipeline_run"]["status"] == "SUCCESS"
    assert data["reconciliation"]["status"] == "RECONCILED"


def test_cli_json_only_format(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    out_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    exit_code = main([
        "--input-dir", str(landing),
        "--output-dir", str(out_dir),
        "--report-dir", str(rep_dir),
        "--report-format", "json",
        "--run-id", "cli_run_json",
    ])

    assert exit_code == 0
    assert (rep_dir / "reconciliation_report_cli_run_json.json").exists()
    assert not (rep_dir / "reconciliation_report_cli_run_json.md").exists()


def test_cli_markdown_only_format(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_001_original.parquet", landing / "batch_001_original.parquet")

    out_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    exit_code = main([
        "--input-dir", str(landing),
        "--output-dir", str(out_dir),
        "--report-dir", str(rep_dir),
        "--report-format", "markdown",
        "--run-id", "cli_run_md",
    ])

    assert exit_code == 0
    assert not (rep_dir / "reconciliation_report_cli_run_md.json").exists()
    assert (rep_dir / "reconciliation_report_cli_run_md.md").exists()


def test_cli_partial_success_returns_exit_code_1(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    # Duplicate batch triggers quarantine -> PARTIAL_SUCCESS -> reconciliation PARTIAL
    shutil.copyfile("data/generated/batch_006_duplicates.parquet", landing / "batch_006_duplicates.parquet")

    out_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    exit_code = main([
        "--input-dir", str(landing),
        "--output-dir", str(out_dir),
        "--report-dir", str(rep_dir),
        "--run-id", "cli_run_part",
    ])

    assert exit_code == 1
    # Reports should still be generated
    assert (rep_dir / "reconciliation_report_cli_run_part.json").exists()
    assert (rep_dir / "reconciliation_report_cli_run_part.md").exists()


def test_cli_breaking_schema_rejection_returns_exit_code_1(tmp_path: Path):
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copyfile("data/generated/batch_004_breaking_type.parquet", landing / "batch_004_breaking_type.parquet")

    out_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    exit_code = main([
        "--input-dir", str(landing),
        "--output-dir", str(out_dir),
        "--report-dir", str(rep_dir),
        "--run-id", "cli_run_rej",
    ])

    assert exit_code == 1
    assert (rep_dir / "reconciliation_report_cli_run_rej.json").exists()


def test_cli_empty_directory_returns_exit_code_0(tmp_path: Path):
    empty_landing = tmp_path / "empty_landing"
    empty_landing.mkdir()

    out_dir = tmp_path / "processed"
    rep_dir = tmp_path / "reports"

    exit_code = main([
        "--input-dir", str(empty_landing),
        "--output-dir", str(out_dir),
        "--report-dir", str(rep_dir),
        "--run-id", "cli_run_empty",
    ])

    assert exit_code == 0
    assert (rep_dir / "reconciliation_report_cli_run_empty.json").exists()
    assert (rep_dir / "reconciliation_report_cli_run_empty.md").exists()
