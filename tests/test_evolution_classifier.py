"""
Unit tests for EvolutionClassifier (Phase 3.2.1).

Validates schema change classifications against config/evolution_rules.yaml:
- Adding nullable currency -> COMPATIBLE
- Adding nullable sales_channel -> COMPATIBLE
- Adding a non-nullable column without a default -> BREAKING
- Missing required order_id -> BREAKING
- Missing optional column -> COMPATIBLE (with warning)
- Decimal-to-string type change -> BREAKING
- Explicitly allowed safe type widening (e.g., int32 -> int64, float -> double) -> COMPATIBLE
- Unrecognized/incompatible type conversion -> BREAKING
- Invalid evolution configuration -> BREAKING (with error and manual review flag)
"""

import os
import pytest
from src.schema.schema_detector import (
    inspect_schema,
    SchemaComparisonResult,
    ColumnInfo,
    TypeMismatch,
    NullabilityDifference,
)
from src.schema.evolution_classifier import (
    EvolutionClassifier,
    ClassificationStatus,
    classify_schema_comparison,
)


BASE_DATA_DIR = "data/generated"


def test_batch_001_baseline_exact_match():
    """
    Batch 001 is an exact match against the baseline schema -> COMPATIBLE.
    """
    comp = inspect_schema(os.path.join(BASE_DATA_DIR, "batch_001_original.parquet"))
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.COMPATIBLE
    assert len(classification.breaking_changes) == 0
    assert len(classification.compatible_changes) == 0
    assert classification.requires_manual_review is False
    assert "Exact schema match with canonical baseline" in classification.reasons


def test_batch_002_add_nullable_currency():
    """
    Batch 002 adds nullable currency -> COMPATIBLE.
    """
    comp = inspect_schema(os.path.join(BASE_DATA_DIR, "batch_002_add_currency.parquet"))
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.COMPATIBLE
    assert len(classification.breaking_changes) == 0
    assert len(classification.compatible_changes) == 1
    assert classification.compatible_changes[0].category == "added_nullable_column"
    assert classification.compatible_changes[0].column == "currency"
    assert classification.requires_manual_review is False


def test_batch_003_add_nullable_sales_channel():
    """
    Batch 003 adds nullable currency and sales_channel -> COMPATIBLE.
    """
    comp = inspect_schema(os.path.join(BASE_DATA_DIR, "batch_003_add_sales_channel.parquet"))
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.COMPATIBLE
    assert len(classification.breaking_changes) == 0
    assert len(classification.compatible_changes) == 2
    added_names = [c.column for c in classification.compatible_changes]
    assert set(added_names) == {"currency", "sales_channel"}


def test_add_non_nullable_column_without_default():
    """
    Adding a non-nullable column without default is BREAKING.
    """
    comp = SchemaComparisonResult(
        file_path="data/test_add_non_null.parquet",
        table_name="orders",
        row_count=100,
        detected_columns=[
            ColumnInfo("order_id", "int64", False),
            ColumnInfo("mandatory_tag", "string", False),
        ],
        expected_columns=[
            ColumnInfo("order_id", "int64", False),
        ],
        added_columns=[
            ColumnInfo("mandatory_tag", "string", False),
        ],
        missing_columns=[],
        type_mismatches=[],
        nullability_differences=[],
        is_exact_match=False,
    )
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert len(classification.breaking_changes) == 1
    assert classification.breaking_changes[0].category == "added_non_nullable_column"
    assert classification.breaking_changes[0].column == "mandatory_tag"


def test_batch_005_missing_required_order_id():
    """
    Batch 005 misses required order_id -> BREAKING.
    """
    comp = inspect_schema(os.path.join(BASE_DATA_DIR, "batch_005_missing_order_id.parquet"))
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert len(classification.breaking_changes) == 1
    assert classification.breaking_changes[0].category == "missing_required_column"
    assert classification.breaking_changes[0].column == "order_id"


def test_missing_optional_column():
    """
    Missing an optional (nullable: true) column is COMPATIBLE with a warning.
    """
    comp = SchemaComparisonResult(
        file_path="data/test_missing_optional.parquet",
        table_name="orders",
        row_count=100,
        detected_columns=[
            ColumnInfo("order_id", "int64", False),
        ],
        expected_columns=[
            ColumnInfo("order_id", "int64", False),
            ColumnInfo("notes", "string", True),
        ],
        added_columns=[],
        missing_columns=[
            ColumnInfo("notes", "string", True),
        ],
        type_mismatches=[],
        nullability_differences=[],
        is_exact_match=False,
    )
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.COMPATIBLE
    assert len(classification.breaking_changes) == 0
    assert len(classification.compatible_changes) == 1
    assert classification.compatible_changes[0].category == "missing_optional_column"
    assert len(classification.warnings) == 1


def test_batch_004_decimal_to_string_change():
    """
    Batch 004 changes amount from decimal(12,2) to string -> BREAKING.
    """
    comp = inspect_schema(os.path.join(BASE_DATA_DIR, "batch_004_breaking_type.parquet"))
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert len(classification.breaking_changes) == 1
    assert classification.breaking_changes[0].category == "incompatible_type_change"
    assert classification.breaking_changes[0].column == "amount"


def test_safe_type_widening_approved():
    """
    Widening an int32 to int64 or float to double is explicitly approved in rules -> COMPATIBLE.
    """
    comp = SchemaComparisonResult(
        file_path="data/test_safe_widening.parquet",
        table_name="orders",
        row_count=100,
        detected_columns=[
            ColumnInfo("order_id", "int64", False),
            ColumnInfo("item_count", "int64", False),
        ],
        expected_columns=[
            ColumnInfo("order_id", "int64", False),
            ColumnInfo("item_count", "int32", False),
        ],
        added_columns=[],
        missing_columns=[],
        type_mismatches=[
            TypeMismatch("item_count", "int32", "int64"),
        ],
        nullability_differences=[],
        is_exact_match=False,
    )
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.COMPATIBLE
    assert len(classification.breaking_changes) == 0
    assert len(classification.compatible_changes) == 1
    assert classification.compatible_changes[0].category == "safe_type_widening"
    assert classification.compatible_changes[0].column == "item_count"


def test_unrecognized_or_narrowing_type_conversion():
    """
    Narrowing int64 to int32, or string to boolean is unapproved -> BREAKING.
    """
    comp = SchemaComparisonResult(
        file_path="data/test_narrowing.parquet",
        table_name="orders",
        row_count=100,
        detected_columns=[
            ColumnInfo("order_id", "int32", False),
        ],
        expected_columns=[
            ColumnInfo("order_id", "int64", False),
        ],
        added_columns=[],
        missing_columns=[],
        type_mismatches=[
            TypeMismatch("order_id", "int64", "int32"),
        ],
        nullability_differences=[],
        is_exact_match=False,
    )
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert len(classification.breaking_changes) == 1
    assert classification.breaking_changes[0].category == "incompatible_type_change"
    assert classification.breaking_changes[0].column == "order_id"


def test_nullability_relaxation_required_to_nullable():
    """
    Relaxing a required non-nullable contract to nullable in incoming batch is BREAKING.
    """
    comp = SchemaComparisonResult(
        file_path="data/test_nullability.parquet",
        table_name="orders",
        row_count=100,
        detected_columns=[
            ColumnInfo("status", "string", True),
        ],
        expected_columns=[
            ColumnInfo("status", "string", False),
        ],
        added_columns=[],
        missing_columns=[],
        type_mismatches=[],
        nullability_differences=[
            NullabilityDifference("status", False, True),
        ],
        is_exact_match=False,
    )
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert len(classification.breaking_changes) == 1
    assert classification.breaking_changes[0].category == "relax_required_to_nullable"


def test_invalid_evolution_config_handling():
    """
    Missing evolution configuration safely marks as BREAKING with manual review flag.
    """
    comp = SchemaComparisonResult(
        file_path="data/test.parquet",
        table_name="orders",
        row_count=10,
        detected_columns=[],
        expected_columns=[],
        added_columns=[],
        missing_columns=[],
        type_mismatches=[],
        nullability_differences=[],
        is_exact_match=True,
    )
    classifier = EvolutionClassifier(rules_config_path="config/non_existent_rules.yaml")
    classification = classifier.classify(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert classification.error is not None
    assert classification.requires_manual_review is True


def test_null_approved_widenings_config(tmp_path):
    """
    Configuration contains approved_widenings: null.
    Classification must not crash; no type widening should be approved without an explicit whitelist.
    """
    cfg_file = tmp_path / "null_widenings.yaml"
    cfg_file.write_text("""
version: "1.0"
rules:
  compatible_changes:
    safe_type_widening:
      enabled: true
      approved_widenings: null
    add_optional_metadata:
      allowed_prefixes: null
""")
    comp = SchemaComparisonResult(
        file_path="data/test_type.parquet",
        table_name="orders",
        row_count=100,
        detected_columns=[],
        expected_columns=[],
        added_columns=[],
        missing_columns=[],
        type_mismatches=[TypeMismatch("item_count", "int32", "int64")],
        nullability_differences=[],
        is_exact_match=False,
    )
    classifier = EvolutionClassifier(rules_config_path=str(cfg_file))
    classification = classifier.classify(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert len(classification.breaking_changes) == 1
    assert classification.breaking_changes[0].category == "incompatible_type_change"
    assert len(classification.compatible_changes) == 0


def test_multiple_simultaneous_schema_changes():
    """
    Include one compatible nullable column addition,
    one explicitly approved safe type widening,
    one breaking missing required column.
    Verify all changes are recorded in their respective categories.
    Verify overall_status remains BREAKING.
    """
    comp = SchemaComparisonResult(
        file_path="data/test_multiple.parquet",
        table_name="orders",
        row_count=100,
        detected_columns=[
            ColumnInfo("currency", "string", True),
            ColumnInfo("item_count", "int64", False),
        ],
        expected_columns=[
            ColumnInfo("order_id", "int64", False),
            ColumnInfo("item_count", "int32", False),
        ],
        added_columns=[
            ColumnInfo("currency", "string", True),
        ],
        missing_columns=[
            ColumnInfo("order_id", "int64", False),
        ],
        type_mismatches=[
            TypeMismatch("item_count", "int32", "int64"),
        ],
        nullability_differences=[],
        is_exact_match=False,
    )
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert len(classification.compatible_changes) == 2
    assert len(classification.breaking_changes) == 1

    compat_cats = {c.category for c in classification.compatible_changes}
    assert compat_cats == {"added_nullable_column", "safe_type_widening"}
    assert classification.breaking_changes[0].category == "missing_required_column"
    assert classification.breaking_changes[0].column == "order_id"


def test_optional_column_constrained_to_non_nullable():
    """
    Expected column is nullable, but incoming column is non-nullable.
    Verify overall_status is MANUAL_REVIEW and requires_manual_review is True.
    """
    comp = SchemaComparisonResult(
        file_path="data/test_constrained.parquet",
        table_name="orders",
        row_count=100,
        detected_columns=[
            ColumnInfo("currency", "string", False),
        ],
        expected_columns=[
            ColumnInfo("currency", "string", True),
        ],
        added_columns=[],
        missing_columns=[],
        type_mismatches=[],
        nullability_differences=[
            NullabilityDifference("currency", True, False),
        ],
        is_exact_match=False,
    )
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.MANUAL_REVIEW
    assert classification.requires_manual_review is True
    assert len(classification.breaking_changes) == 0
    assert len(classification.warnings) == 1


def test_malformed_evolution_yaml(tmp_path):
    """
    Supply syntactically invalid YAML.
    Verify classification handles the error without crashing.
    Verify an appropriate error is recorded and overall_status is BREAKING.
    """
    cfg_file = tmp_path / "malformed.yaml"
    cfg_file.write_text(": invalid: yaml: {")

    comp = SchemaComparisonResult(
        file_path="data/test.parquet",
        table_name="orders",
        row_count=10,
        detected_columns=[],
        expected_columns=[],
        added_columns=[],
        missing_columns=[],
        type_mismatches=[],
        nullability_differences=[],
        is_exact_match=True,
    )
    classifier = EvolutionClassifier(rules_config_path=str(cfg_file))
    classification = classifier.classify(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert classification.requires_manual_review is True
    assert classification.error is not None
    assert "Evolution configuration error" in classification.reasons[0]


def test_empty_evolution_yaml(tmp_path):
    """
    Supply an empty YAML file.
    Verify safe error handling without crashing.
    Verify no changes are incorrectly classified as compatible.
    """
    cfg_file = tmp_path / "empty.yaml"
    cfg_file.write_text("")

    comp = SchemaComparisonResult(
        file_path="data/test.parquet",
        table_name="orders",
        row_count=10,
        detected_columns=[],
        expected_columns=[],
        added_columns=[ColumnInfo("currency", "string", True)],
        missing_columns=[],
        type_mismatches=[],
        nullability_differences=[],
        is_exact_match=False,
    )
    classifier = EvolutionClassifier(rules_config_path=str(cfg_file))
    classification = classifier.classify(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert classification.requires_manual_review is True
    assert classification.error is not None
    assert len(classification.compatible_changes) == 0


def test_detector_error_propagation():
    """
    Verify that when detector reports an error:
    - Never produces COMPATIBLE (must be BREAKING)
    - Preserves original detector error
    - Sets requires_manual_review=True
    """
    comp = SchemaComparisonResult(
        file_path="data/corrupted.parquet",
        table_name="orders",
        row_count=0,
        detected_columns=[],
        expected_columns=[],
        added_columns=[],
        missing_columns=[],
        type_mismatches=[],
        nullability_differences=[],
        is_exact_match=False,
        error="Unreadable Parquet file: Parquet magic bytes not found",
    )
    classification = classify_schema_comparison(comp)

    assert classification.overall_status == ClassificationStatus.BREAKING
    assert classification.requires_manual_review is True
    assert classification.error == "Unreadable Parquet file: Parquet magic bytes not found"
    assert "Detector error: Unreadable Parquet file: Parquet magic bytes not found" in classification.reasons

