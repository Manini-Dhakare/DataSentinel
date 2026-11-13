"""
Unit tests for SchemaEvolver (Phase 3.2.2).

Validates controlled schema evolution mechanics:
1. Baseline schema remains unchanged.
2. Batch 001 produces an identical schema.
3. Batch 002 adds nullable currency.
4. Batch 003 adds nullable sales_channel cumulatively.
5. Breaking classification is rejected.
6. Manual-review classification is rejected.
7. Approved type widening updates the target schema.
8. Unapproved type widening is rejected.
9. Original schema is never mutated.
10. Duplicate column additions are handled safely.
11. Invalid input is handled safely.
12. Repeated evolution produces deterministic results.
"""

import copy
import yaml
import pytest
import pyarrow as pa

from src.schema.schema_detector import inspect_schema
from src.schema.evolution_classifier import (
    classify_schema_comparison,
    EvolutionClassificationResult,
    ClassificationStatus,
    ChangeDetail,
)
from src.schema.schema_evolver import SchemaEvolver, evolve_schema, parse_type_to_pyarrow


BASE_DATA_DIR = "data/generated"
CONFIG_PATH = "config/source_schema.yaml"


@pytest.fixture
def base_orders_schema():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return cfg["schemas"]["orders"]


def test_parse_type_to_pyarrow():
    assert parse_type_to_pyarrow("int64") == pa.int64()
    assert parse_type_to_pyarrow("int32") == pa.int32()
    assert parse_type_to_pyarrow("string") == pa.string()
    assert parse_type_to_pyarrow("decimal(12,2)") == pa.decimal128(12, 2)
    assert parse_type_to_pyarrow("timestamp[us]") == pa.timestamp("us")
    assert parse_type_to_pyarrow("double") == pa.float64()
    with pytest.raises(ValueError):
        parse_type_to_pyarrow("unsupported_type_xyz")


def test_baseline_schema_conversion(base_orders_schema):
    """
    1. Baseline schema converts to pyarrow.Schema without changes.
    """
    evolver = SchemaEvolver()
    pa_schema = evolver.to_pyarrow_schema(base_orders_schema)
    assert len(pa_schema) == 5
    assert pa_schema.names == ["order_id", "customer_id", "order_timestamp", "amount", "status"]
    assert pa_schema.field("amount").type == pa.decimal128(12, 2)
    assert pa_schema.field("order_id").nullable is False


def test_batch_001_produces_identical_schema(base_orders_schema):
    """
    2. Batch 001 matches baseline; evolution produces an identical schema with is_evolved=False.
    """
    comp = inspect_schema(f"{BASE_DATA_DIR}/batch_001_original.parquet")
    classification = classify_schema_comparison(comp)

    res = evolve_schema(base_orders_schema, classification)
    assert res.status == "SUCCESS"
    assert res.is_evolved is False
    assert len(res.applied_changes) == 0
    assert list(res.evolved_schema_dict["columns"].keys()) == list(base_orders_schema["columns"].keys())
    assert res.pyarrow_schema == SchemaEvolver().to_pyarrow_schema(base_orders_schema)


def test_batch_002_adds_nullable_currency(base_orders_schema):
    """
    3. Batch 002 adds nullable currency to baseline.
    """
    comp = inspect_schema(f"{BASE_DATA_DIR}/batch_002_add_currency.parquet")
    classification = classify_schema_comparison(comp)

    res = evolve_schema(base_orders_schema, classification)
    assert res.status == "SUCCESS"
    assert res.is_evolved is True
    assert len(res.applied_changes) == 1
    assert res.applied_changes[0].column == "currency"

    # Schema dict check
    evolved_cols = res.evolved_schema_dict["columns"]
    assert "currency" in evolved_cols
    assert evolved_cols["currency"]["type"] == "string"
    assert evolved_cols["currency"]["nullable"] is True

    # PyArrow check
    assert res.pyarrow_schema.names == [
        "order_id", "customer_id", "order_timestamp", "amount", "status", "currency"
    ]
    assert res.pyarrow_schema.field("currency").type == pa.string()
    assert res.pyarrow_schema.field("currency").nullable is True


def test_batch_003_adds_sales_channel_cumulatively(base_orders_schema):
    """
    4. Batch 003 adds nullable sales_channel to the already evolved Batch 002 schema.
    """
    # Step 1: Evolve from baseline to v2 (with currency) using Batch 002
    comp_b2 = inspect_schema(f"{BASE_DATA_DIR}/batch_002_add_currency.parquet")
    class_b2 = classify_schema_comparison(comp_b2)
    res_v2 = evolve_schema(base_orders_schema, class_b2)
    assert "currency" in res_v2.evolved_schema_dict["columns"]

    # Step 2: Now classify Batch 003 against v2 schema
    # Batch 003 has: order_id, customer_id, order_timestamp, amount, status, currency, sales_channel
    # Compared to v2 (which has currency), only sales_channel is new
    from src.schema.schema_detector import SchemaDetector
    # Simulate detector against v2 schema by creating comparison
    comp_b3 = inspect_schema(f"{BASE_DATA_DIR}/batch_003_add_sales_channel.parquet")
    # Batch 003 compared to baseline has both currency & sales_channel added
    class_b3 = classify_schema_comparison(comp_b3)

    # Evolve v2 schema with Batch 003 classification (safe duplicate handling handles currency)
    res_v3 = evolve_schema(res_v2.evolved_schema_dict, class_b3)
    assert res_v3.status == "SUCCESS"
    assert "currency" in res_v3.evolved_schema_dict["columns"]
    assert "sales_channel" in res_v3.evolved_schema_dict["columns"]

    pa_names = res_v3.pyarrow_schema.names
    assert pa_names == [
        "order_id", "customer_id", "order_timestamp", "amount", "status", "currency", "sales_channel"
    ]
    assert res_v3.pyarrow_schema.field("sales_channel").type == pa.string()
    assert res_v3.pyarrow_schema.field("sales_channel").nullable is True


def test_breaking_classification_is_rejected(base_orders_schema):
    """
    5. Breaking classification (Batch 004 & Batch 005) is rejected without mutating base schema.
    """
    # Batch 004: amount changed to string
    comp_b4 = inspect_schema(f"{BASE_DATA_DIR}/batch_004_breaking_type.parquet")
    class_b4 = classify_schema_comparison(comp_b4)
    res_b4 = evolve_schema(base_orders_schema, class_b4)
    assert res_b4.status == "REJECTED"
    assert res_b4.is_evolved is False
    assert "BREAKING" in res_b4.rejection_reason

    # Batch 005: missing order_id
    comp_b5 = inspect_schema(f"{BASE_DATA_DIR}/batch_005_missing_order_id.parquet")
    class_b5 = classify_schema_comparison(comp_b5)
    res_b5 = evolve_schema(base_orders_schema, class_b5)
    assert res_b5.status == "REJECTED"
    assert res_b5.is_evolved is False
    assert "BREAKING" in res_b5.rejection_reason


def test_manual_review_classification_is_rejected(base_orders_schema):
    """
    6. Manual-review classification is rejected.
    """
    class_mr = EvolutionClassificationResult(
        file_path="dummy.parquet",
        table_name="orders",
        overall_status=ClassificationStatus.MANUAL_REVIEW,
        reasons=["Column currency made non-nullable (requires manual review)"],
        requires_manual_review=True,
    )
    res = evolve_schema(base_orders_schema, class_mr)
    assert res.status == "REJECTED"
    assert res.is_evolved is False
    assert "MANUAL_REVIEW" in res.rejection_reason


def test_approved_type_widening_updates_schema(base_orders_schema):
    """
    7. Approved type widening (e.g. amount or integer column) updates the target schema.
    """
    # Create base schema with item_count: int32
    schema_with_int32 = copy.deepcopy(base_orders_schema)
    schema_with_int32["columns"]["item_count"] = {"type": "int32", "nullable": False}

    # Classification with approved safe widening int32 -> int64
    class_widen = EvolutionClassificationResult(
        file_path="dummy.parquet",
        table_name="orders",
        overall_status=ClassificationStatus.COMPATIBLE,
        compatible_changes=[
            ChangeDetail(
                category="safe_type_widening",
                column="item_count",
                description="Safe type widening approved for item_count: int32 -> int64",
                details={"expected_type": "int32", "detected_type": "int64"},
            )
        ],
    )

    res = evolve_schema(schema_with_int32, class_widen)
    assert res.status == "SUCCESS"
    assert res.is_evolved is True
    assert res.evolved_schema_dict["columns"]["item_count"]["type"] == "int64"
    assert res.pyarrow_schema.field("item_count").type == pa.int64()


def test_unapproved_type_widening_is_rejected(base_orders_schema):
    """
    8. Unapproved type change (which classifier marks as BREAKING) is rejected by evolver.
    """
    class_bad_type = EvolutionClassificationResult(
        file_path="dummy.parquet",
        table_name="orders",
        overall_status=ClassificationStatus.BREAKING,
        breaking_changes=[
            ChangeDetail(
                category="incompatible_type_change",
                column="amount",
                description="Incompatible type change: decimal(12,2) -> string",
                details={"expected_type": "decimal(12,2)", "detected_type": "string"},
            )
        ],
        reasons=["Incompatible type change: decimal(12,2) -> string"],
    )

    res = evolve_schema(base_orders_schema, class_bad_type)
    assert res.status == "REJECTED"
    assert res.is_evolved is False


def test_original_schema_is_never_mutated(base_orders_schema):
    """
    9. Original schema object is deeply preserved and never mutated in place.
    """
    orig_copy = copy.deepcopy(base_orders_schema)
    comp_b2 = inspect_schema(f"{BASE_DATA_DIR}/batch_002_add_currency.parquet")
    class_b2 = classify_schema_comparison(comp_b2)

    res = evolve_schema(base_orders_schema, class_b2)
    assert base_orders_schema == orig_copy
    assert "currency" not in base_orders_schema["columns"]
    assert "currency" in res.evolved_schema_dict["columns"]


def test_duplicate_column_addition_handled_safely(base_orders_schema):
    """
    10. Duplicate column additions in classification do not duplicate or corrupt columns.
    """
    class_dupes = EvolutionClassificationResult(
        file_path="dummy.parquet",
        table_name="orders",
        overall_status=ClassificationStatus.COMPATIBLE,
        compatible_changes=[
            ChangeDetail(
                category="added_nullable_column",
                column="currency",
                description="Added nullable currency",
                details={"data_type": "string", "nullable": True},
            ),
            ChangeDetail(
                category="added_nullable_column",
                column="currency",
                description="Duplicate added currency",
                details={"data_type": "string", "nullable": True},
            ),
        ],
    )

    res = evolve_schema(base_orders_schema, class_dupes)
    assert res.status == "SUCCESS"
    assert res.pyarrow_schema.names.count("currency") == 1
    assert len(res.applied_changes) == 1


def test_invalid_input_handled_safely():
    """
    11. Invalid schema input (e.g. non-dict or missing 'columns') is handled safely.
    """
    class_valid = EvolutionClassificationResult(
        file_path="dummy.parquet",
        table_name="orders",
        overall_status=ClassificationStatus.COMPATIBLE,
    )

    res1 = evolve_schema({}, class_valid)
    assert res1.status == "REJECTED"
    assert "Invalid base schema" in res1.rejection_reason

    res2 = evolve_schema("not-a-dict", class_valid)
    assert res2.status == "REJECTED"
    assert "Invalid base schema" in res2.rejection_reason


def test_repeated_evolution_deterministic(base_orders_schema):
    """
    12. Repeated evolution calls on identical inputs produce identical deterministic schemas.
    """
    comp = inspect_schema(f"{BASE_DATA_DIR}/batch_002_add_currency.parquet")
    classification = classify_schema_comparison(comp)

    res1 = evolve_schema(base_orders_schema, classification)
    res2 = evolve_schema(base_orders_schema, classification)

    assert res1.evolved_schema_dict == res2.evolved_schema_dict
    assert res1.pyarrow_schema == res2.pyarrow_schema
    assert res1.to_dict() == res2.to_dict()


def test_conflicting_type_on_duplicate_column_addition(base_orders_schema):
    """
    Task 1: Conflicting type on duplicate column addition.
    When a structured classification attempts to add a column that already exists
    in the base schema, but with a conflicting data type:
    - Existing amount type: decimal(12,2).
    - Incoming amount type: string.
    - Expect status: REJECTED.
    - Verify is_evolved is False.
    - Verify applied_changes is empty.
    - Verify rejection_reason identifies the conflict (column name, existing type, incoming type).
    - Verify original schema remains unchanged.
    - Verify pyarrow_schema is the original baseline schema.
    """
    orig_copy = copy.deepcopy(base_orders_schema)
    class_conflict = EvolutionClassificationResult(
        file_path="dummy.parquet",
        table_name="orders",
        overall_status=ClassificationStatus.COMPATIBLE,
        compatible_changes=[
            ChangeDetail(
                category="added_nullable_column",
                column="amount",
                description="Conflicting amount column as string",
                details={"data_type": "string", "nullable": True},
            )
        ],
    )

    res = evolve_schema(base_orders_schema, class_conflict)
    assert res.status == "REJECTED"
    assert res.is_evolved is False
    assert len(res.applied_changes) == 0
    assert "conflicting duplicate column 'amount'" in res.rejection_reason
    assert "decimal(12,2)" in res.rejection_reason
    assert "string" in res.rejection_reason
    assert res.evolved_schema_dict["columns"]["amount"]["type"] == "decimal(12,2)"
    assert res.evolved_schema_dict["columns"]["amount"]["nullable"] is False
    assert res.pyarrow_schema.field("amount").type == pa.decimal128(12, 2)
    assert base_orders_schema == orig_copy


def test_unsupported_pyarrow_type_in_base_schema():
    """
    Task 2: Unsupported PyArrow type in base schema.
    When base_schema_def contains an unsupported or malformed data type,
    parse_type_to_pyarrow() raises a ValueError during schema parsing.
    Verify:
    - An exception is raised rather than returning a corrupted / partially evolved schema.
    - The original input dictionary is not mutated.
    """
    bad_base_schema = {
        "columns": {
            "order_id": {"type": "int64", "nullable": False},
            "invalid_col": {"type": "unsupported_xyz_type", "nullable": True},
        }
    }
    orig_copy = copy.deepcopy(bad_base_schema)
    class_valid = EvolutionClassificationResult(
        file_path="dummy.parquet",
        table_name="orders",
        overall_status=ClassificationStatus.COMPATIBLE,
    )

    with pytest.raises(ValueError, match="Unsupported data type for PyArrow conversion: unsupported_xyz_type"):
        evolve_schema(bad_base_schema, class_valid)

    # Confirm original schema is never mutated
    assert bad_base_schema == orig_copy

