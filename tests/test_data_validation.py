"""
Unit and integration tests for Data Validation (Phase 4.1).

Covers all 12 required verification scenarios:
1. Multiple violations on one row (all failures collected, row counted once).
2. One invalid row with multiple failures appears only once in invalid_table.
3. First duplicate occurrence remains valid; subsequent duplicates are invalid.
4. Uninitialized customer master raises clear configuration/validation error.
5. Initialized but empty customer master rejects all foreign keys.
6. Newly required field from applicable schema information enforced dynamically.
7. Optional currency and sales_channel null values accepted cleanly.
8. Missing required column flagged at schema level.
9. Empty input table returns typed empty valid_table and invalid_table.
10. Malformed amount handled safely without crashing.
11. Original input table remains unchanged.
12. Metrics accounting with overlapping violations.
"""

from decimal import Decimal
from datetime import datetime, timezone
import copy
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.validation.models import OrderRecord, CustomerRecord, ValidationFailure
from src.validation.validator import DataValidator, validate_batch


CUSTOMERS_PATH = "data/generated/customers.parquet"


@pytest.fixture
def validator():
    v = DataValidator()
    v.load_customer_master(CUSTOMERS_PATH)
    return v


def test_customer_pydantic_model():
    """Validates CustomerRecord model."""
    valid_c = CustomerRecord(
        customer_id=101,
        customer_name="Alice Smith",
        email="alice@example.com",
        created_at=datetime.now(timezone.utc),
    )
    assert valid_c.customer_id == 101

    with pytest.raises(Exception):
        CustomerRecord(
            customer_id=101,
            customer_name="   ",  # blank
            email="invalid_email",
            created_at=datetime.now(timezone.utc),
        )


def test_order_pydantic_model():
    """Validates OrderRecord model and negative amount constraint."""
    valid_o = OrderRecord(
        order_id=501,
        customer_id=1001,
        order_timestamp=datetime.now(timezone.utc),
        amount=Decimal("150.50"),
        status="COMPLETED",
        currency="USD",
        sales_channel="web",
    )
    assert valid_o.amount == Decimal("150.50")

    # Boundary check: amount == 0.00 is valid
    zero_o = OrderRecord(
        order_id=502,
        customer_id=1001,
        order_timestamp=datetime.now(timezone.utc),
        amount=Decimal("0.00"),
        status="PENDING",
    )
    assert zero_o.amount == Decimal("0.00")

    # Negative amount is invalid
    with pytest.raises(Exception):
        OrderRecord(
            order_id=503,
            customer_id=1001,
            order_timestamp=datetime.now(timezone.utc),
            amount=Decimal("-0.01"),
            status="PENDING",
        )


def test_batch_001_clean_records(validator):
    """Batch 001 must have 1000 valid records and 0 failures."""
    table = pq.read_table("data/generated/batch_001_original.parquet")
    res = validator.validate_orders_table(table)

    assert res.is_valid_batch is True
    assert res.metrics.total_records == 1000
    assert res.metrics.valid_records == 1000
    assert res.metrics.invalid_records == 0
    assert len(res.failures) == 0
    assert len(res.valid_table) == 1000
    assert len(res.invalid_table) == 0


def test_batch_002_clean_records_with_currency(validator):
    """Batch 002 with currency must have 1000 valid records (currency optional nulls allowed)."""
    table = pq.read_table("data/generated/batch_002_add_currency.parquet")
    res = validator.validate_orders_table(table)

    assert res.is_valid_batch is True
    assert res.metrics.valid_records == 1000
    assert res.metrics.invalid_records == 0
    assert len(res.valid_table) == 1000
    assert len(res.invalid_table) == 0


def test_batch_003_clean_records_with_sales_channel(validator):
    """Batch 003 with sales_channel must have 1000 valid records."""
    table = pq.read_table("data/generated/batch_003_add_sales_channel.parquet")
    res = validator.validate_orders_table(table)

    assert res.is_valid_batch is True
    assert res.metrics.valid_records == 1000
    assert res.metrics.invalid_records == 0
    assert len(res.valid_table) == 1000
    assert len(res.invalid_table) == 0


def test_batch_006_duplicate_primary_keys(validator):
    """Batch 006 has 50 duplicate order_id records."""
    table = pq.read_table("data/generated/batch_006_duplicates.parquet")
    res = validator.validate_orders_table(table)

    assert res.is_valid_batch is False
    assert res.metrics.total_records == 1000
    assert res.metrics.valid_records == 950
    assert res.metrics.invalid_records == 50
    assert res.metrics.duplicate_violations == 50
    assert all(f.category == "DUPLICATE_KEY" for f in res.failures)
    assert len(res.valid_table) == 950
    assert len(res.invalid_table) == 50


def test_batch_007_invalid_foreign_keys(validator):
    """Batch 007 has 50 orphaned customer_id records."""
    table = pq.read_table("data/generated/batch_007_invalid_foreign_keys.parquet")
    res = validator.validate_orders_table(table)

    assert res.is_valid_batch is False
    assert res.metrics.total_records == 1000
    assert res.metrics.valid_records == 950
    assert res.metrics.invalid_records == 50
    assert res.metrics.foreign_key_violations == 50
    assert all(f.category == "INVALID_FOREIGN_KEY" for f in res.failures)
    assert len(res.valid_table) == 950
    assert len(res.invalid_table) == 50


def test_batch_008_null_violations(validator):
    """Batch 008 has 50 null violations across required columns."""
    table = pq.read_table("data/generated/batch_008_null_violations.parquet")
    res = validator.validate_orders_table(table)

    assert res.is_valid_batch is False
    assert res.metrics.total_records == 1000
    assert res.metrics.valid_records == 950
    assert res.metrics.invalid_records == 50
    assert res.metrics.null_violations == 50
    assert all(f.category == "NULL_VIOLATION" for f in res.failures)
    assert len(res.valid_table) == 950
    assert len(res.invalid_table) == 50


def test_batch_009_empty_batch(validator):
    """Batch 009 is an empty batch (0 rows) and must return typed empty tables."""
    table = pq.read_table("data/generated/batch_009_empty.parquet")
    res = validator.validate_orders_table(table)

    assert res.is_valid_batch is True
    assert res.metrics.total_records == 0
    assert res.metrics.valid_records == 0
    assert res.metrics.invalid_records == 0
    assert len(res.failures) == 0
    assert len(res.valid_table) == 0
    assert len(res.invalid_table) == 0
    assert res.valid_table.schema == table.schema
    assert res.invalid_table.schema == table.schema


def test_amount_negative_range_violation(validator):
    """Custom table with negative amounts must be flagged as RANGE_VIOLATION."""
    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])
    valid_cid = list(validator.valid_customer_ids)[0]
    table = pa.Table.from_arrays([
        pa.array([90001, 90002], type=pa.int64()),
        pa.array([valid_cid, valid_cid], type=pa.int64()),
        pa.array([datetime.now(), datetime.now()], type=pa.timestamp("us")),
        pa.array([Decimal("50.00"), Decimal("-10.00")], type=pa.decimal128(12, 2)),
        pa.array(["COMPLETED", "COMPLETED"], type=pa.string()),
    ], schema=schema)

    res = validator.validate_orders_table(table)
    assert res.is_valid_batch is False
    assert res.metrics.valid_records == 1
    assert res.metrics.invalid_records == 1
    assert res.metrics.range_violations == 1
    assert res.failures[0].category == "RANGE_VIOLATION"
    assert res.failures[0].column == "amount"
    assert len(res.valid_table) == 1
    assert len(res.invalid_table) == 1


# -------------------------------------------------------------
# Targeted New Tests for Phase 4.1 Corrections
# -------------------------------------------------------------

def test_multiple_violations_on_single_row():
    """
    1. Multiple violations on one row:
       order_id = null
       customer_id = 999990 (not in master)
       amount = -100.00
       status = null
       Verifies:
       - Every applicable error is reported (2 NULLs, 1 FK, 1 RANGE).
       - Record is counted as exactly 1 invalid record.
       - Appears exactly once in invalid_table.
    """
    v = DataValidator(valid_customer_ids={1001, 1002})
    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=True),
        pa.field("customer_id", pa.int64(), nullable=True),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=True),
        pa.field("amount", pa.decimal128(12, 2), nullable=True),
        pa.field("status", pa.string(), nullable=True),
    ])
    table = pa.Table.from_arrays([
        pa.array([None], type=pa.int64()),
        pa.array([999990], type=pa.int64()),
        pa.array([datetime.now()], type=pa.timestamp("us")),
        pa.array([Decimal("-100.00")], type=pa.decimal128(12, 2)),
        pa.array([None], type=pa.string()),
    ], schema=schema)

    res = v.validate_orders_table(table)
    assert res.is_valid_batch is False
    assert res.metrics.total_records == 1
    assert res.metrics.valid_records == 0
    assert res.metrics.invalid_records == 1

    # Violation counters count every failure
    assert res.metrics.null_violations == 2  # order_id and status
    assert res.metrics.foreign_key_violations == 1  # 999990
    assert res.metrics.range_violations == 1  # -100.00

    # Total failures collected is 4
    assert len(res.failures) == 4
    categories = [f.category for f in res.failures]
    assert categories.count("NULL_VIOLATION") == 2
    assert "INVALID_FOREIGN_KEY" in categories
    assert "RANGE_VIOLATION" in categories

    # Row appears exactly once in invalid_table
    assert len(res.invalid_table) == 1
    assert len(res.valid_table) == 0


def test_first_duplicate_valid_subsequent_invalid(validator):
    """
    3. First duplicate occurrence remains valid; subsequent duplicates are invalid.
    """
    valid_cid = list(validator.valid_customer_ids)[0]
    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])
    table = pa.Table.from_arrays([
        pa.array([77001, 77001, 77001], type=pa.int64()),
        pa.array([valid_cid, valid_cid, valid_cid], type=pa.int64()),
        pa.array([datetime.now(), datetime.now(), datetime.now()], type=pa.timestamp("us")),
        pa.array([Decimal("10.00"), Decimal("20.00"), Decimal("30.00")], type=pa.decimal128(12, 2)),
        pa.array(["COMPLETED", "COMPLETED", "COMPLETED"], type=pa.string()),
    ], schema=schema)

    res = validator.validate_orders_table(table)
    assert res.metrics.total_records == 3
    assert res.metrics.valid_records == 1  # First occurrence accepted
    assert res.metrics.invalid_records == 2  # 2 duplicate occurrences rejected
    assert res.metrics.duplicate_violations == 2
    assert len(res.valid_table) == 1
    assert len(res.invalid_table) == 2


def test_uninitialized_customer_master():
    """
    4. Uninitialized customer master (valid_customer_ids is None) raises a clear error.
    """
    v = DataValidator(valid_customer_ids=None)
    table = pq.read_table("data/generated/batch_001_original.parquet")

    with pytest.raises(ValueError, match="Customer master is uninitialized"):
        v.validate_orders_table(table)


def test_initialized_empty_customer_master():
    """
    5. Initialized but empty customer master (valid_customer_ids = set())
       fails all non-null customer_id foreign key checks.
    """
    v = DataValidator(valid_customer_ids=set())
    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])
    table = pa.Table.from_arrays([
        pa.array([80001, 80002], type=pa.int64()),
        pa.array([1001, 1002], type=pa.int64()),
        pa.array([datetime.now(), datetime.now()], type=pa.timestamp("us")),
        pa.array([Decimal("10.00"), Decimal("20.00")], type=pa.decimal128(12, 2)),
        pa.array(["COMPLETED", "COMPLETED"], type=pa.string()),
    ], schema=schema)

    res = v.validate_orders_table(table)
    assert res.is_valid_batch is False
    assert res.metrics.foreign_key_violations == 2
    assert res.metrics.invalid_records == 2
    assert res.metrics.valid_records == 0
    assert len(res.invalid_table) == 2


def test_newly_required_field_from_applicable_schema(validator):
    """
    6. Newly required field from applicable schema information enforced dynamically
       without changing hardcoded lists.
    """
    # Create an evolved schema definition where 'tax_id' is non-nullable (required)
    evolved_schema = {
        "columns": {
            "order_id": {"type": "int64", "nullable": False},
            "customer_id": {"type": "int64", "nullable": False},
            "order_timestamp": {"type": "timestamp[us]", "nullable": False},
            "amount": {"type": "decimal(12,2)", "nullable": False},
            "status": {"type": "string", "nullable": False},
            "tax_id": {"type": "string", "nullable": False},  # NEW REQUIRED FIELD
        }
    }
    valid_cid = list(validator.valid_customer_ids)[0]
    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("tax_id", pa.string(), nullable=True),
    ])
    # One row with valid tax_id, one row with null tax_id
    table = pa.Table.from_arrays([
        pa.array([81001, 81002], type=pa.int64()),
        pa.array([valid_cid, valid_cid], type=pa.int64()),
        pa.array([datetime.now(), datetime.now()], type=pa.timestamp("us")),
        pa.array([Decimal("10.00"), Decimal("20.00")], type=pa.decimal128(12, 2)),
        pa.array(["COMPLETED", "COMPLETED"], type=pa.string()),
        pa.array(["TAX-12345", None], type=pa.string()),
    ], schema=schema)

    res = validator.validate_orders_table(table, applicable_schema=evolved_schema)
    assert res.metrics.valid_records == 1
    assert res.metrics.invalid_records == 1
    assert res.metrics.null_violations == 1
    assert res.failures[0].column == "tax_id"


def test_missing_required_column_schema_error(validator):
    """
    8. Missing required column (e.g. order_id column missing) detected at schema level.
    """
    schema = pa.schema([
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])
    table = pa.Table.from_arrays([
        pa.array([1001], type=pa.int64()),
        pa.array([datetime.now()], type=pa.timestamp("us")),
        pa.array([Decimal("10.00")], type=pa.decimal128(12, 2)),
        pa.array(["COMPLETED"], type=pa.string()),
    ], schema=schema)

    res = validator.validate_orders_table(table)
    assert res.is_valid_batch is False
    assert len(res.schema_errors) == 1
    assert "missing required columns" in res.schema_errors[0]
    assert res.metrics.invalid_records == 1
    assert res.metrics.valid_records == 0
    assert len(res.invalid_table) == 1


def test_malformed_amount_safe_handling(validator):
    """
    10. Malformed amount value does not crash entire batch; records MALFORMED_RECORD.
    """
    valid_cid = list(validator.valid_customer_ids)[0]
    # Using a string column for amount to pass an unparseable amount string
    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=False),
        pa.field("amount", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])
    table = pa.Table.from_arrays([
        pa.array([82001, 82002], type=pa.int64()),
        pa.array([valid_cid, valid_cid], type=pa.int64()),
        pa.array([datetime.now(), datetime.now()], type=pa.timestamp("us")),
        pa.array(["50.00", "INVALID_NOT_A_DECIMAL"], type=pa.string()),
        pa.array(["COMPLETED", "COMPLETED"], type=pa.string()),
    ], schema=schema)

    res = validator.validate_orders_table(table)
    assert res.metrics.valid_records == 1
    assert res.metrics.invalid_records == 1
    assert res.metrics.malformed_violations == 1
    assert res.failures[0].category == "MALFORMED_RECORD"
    assert res.failures[0].column == "amount"
    assert res.failures[0].raw_record["amount"] == "INVALID_NOT_A_DECIMAL"


def test_original_input_table_immutability(validator):
    """
    11. Original input table remains completely unchanged after validation.
    """
    table = pq.read_table("data/generated/batch_001_original.parquet")
    orig_rows = table.to_pylist()
    orig_schema = copy.deepcopy(table.schema)

    res = validator.validate_orders_table(table)
    assert table.to_pylist() == orig_rows
    assert table.schema == orig_schema
    assert res.valid_table.to_pylist() == orig_rows


def test_metrics_accounting_with_overlapping_violations():
    """
    12. Metrics accounting:
        - valid_records + invalid_records == total_records
        - Violation counters count distinct failure instances
        - invalid_records counts unique invalid rows
    """
    v = DataValidator(valid_customer_ids={1001})
    schema = pa.schema([
        pa.field("order_id", pa.int64(), nullable=True),
        pa.field("customer_id", pa.int64(), nullable=True),
        pa.field("order_timestamp", pa.timestamp("us"), nullable=True),
        pa.field("amount", pa.decimal128(12, 2), nullable=True),
        pa.field("status", pa.string(), nullable=True),
    ])
    table = pa.Table.from_arrays([
        # Row 0: perfectly clean
        # Row 1: null order_id + negative amount (2 violations, 1 invalid row)
        # Row 2: invalid foreign key (1 violation, 1 invalid row)
        pa.array([90001, None, 90003], type=pa.int64()),
        pa.array([1001, 1001, 99999], type=pa.int64()),
        pa.array([datetime.now(), datetime.now(), datetime.now()], type=pa.timestamp("us")),
        pa.array([Decimal("10.00"), Decimal("-5.00"), Decimal("20.00")], type=pa.decimal128(12, 2)),
        pa.array(["COMPLETED", "COMPLETED", "COMPLETED"], type=pa.string()),
    ], schema=schema)

    res = v.validate_orders_table(table)
    m = res.metrics
    assert m.total_records == 3
    assert m.valid_records == 1
    assert m.invalid_records == 2
    assert m.valid_records + m.invalid_records == m.total_records

    # Violations: 1 null, 1 range, 1 foreign_key
    assert m.null_violations == 1
    assert m.range_violations == 1
    assert m.foreign_key_violations == 1
    assert len(res.failures) == 3
    assert len(res.valid_table) == 1
    assert len(res.invalid_table) == 2
