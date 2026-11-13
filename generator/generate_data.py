"""
Synthetic Parquet Data Generator for SchemaGuard.

Generates reproducible synthetic datasets for:
- Customers (canonical customer master)
- Orders (Batches 001 through 009 testing various schema evolution & quality scenarios)
"""

import os
import json
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Any, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq


FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Sam", "Chris", "Pat", "Riley",
    "Casey", "Jamie", "Avery", "Logan", "Parker", "Dakota", "Reese", "Quinn"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson"
]

DOMAINS = ["example.com", "testmail.org", "samplecorp.net", "mockdata.io"]
ORDER_STATUSES = ["PENDING", "COMPLETED", "CANCELLED", "REFUNDED", "PROCESSING"]
CURRENCIES = ["USD", "EUR", "GBP", "CAD", "AUD", "JPY"]
SALES_CHANNELS = ["web", "mobile_app", "in_store", "call_center", "partner_api"]


def get_customers_pa_schema() -> pa.Schema:
    return pa.schema([
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("customer_name", pa.string(), nullable=False),
        pa.field("email", pa.string(), nullable=False),
        pa.field("created_at", pa.timestamp("us", tz=None), nullable=False),
    ])


def get_batch_001_pa_schema() -> pa.Schema:
    """Original orders schema."""
    return pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us", tz=None), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])


def get_batch_002_pa_schema() -> pa.Schema:
    """Batch 002: Add nullable currency."""
    return pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us", tz=None), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("currency", pa.string(), nullable=True),
    ])


def get_batch_003_pa_schema() -> pa.Schema:
    """Batch 003: Add nullable sales_channel."""
    return pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us", tz=None), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("currency", pa.string(), nullable=True),
        pa.field("sales_channel", pa.string(), nullable=True),
    ])


def get_batch_004_pa_schema() -> pa.Schema:
    """Batch 004: Breaking type change - amount is string."""
    return pa.schema([
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us", tz=None), nullable=False),
        pa.field("amount", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])


def get_batch_005_pa_schema() -> pa.Schema:
    """Batch 005: Breaking column removal - order_id is removed."""
    return pa.schema([
        pa.field("customer_id", pa.int64(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us", tz=None), nullable=False),
        pa.field("amount", pa.decimal128(12, 2), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ])


def get_batch_008_pa_schema() -> pa.Schema:
    """Batch 008: Schema allows nullable fields so null records can physically be written."""
    return pa.schema([
        pa.field("order_id", pa.int64(), nullable=True),
        pa.field("customer_id", pa.int64(), nullable=True),
        pa.field("order_timestamp", pa.timestamp("us", tz=None), nullable=True),
        pa.field("amount", pa.decimal128(12, 2), nullable=True),
        pa.field("status", pa.string(), nullable=True),
    ])


class SyntheticDataGenerator:
    def __init__(self, seed: int = 42, base_output_dir: str = "data/generated", report_dir: str = "data/reports"):
        self.seed = seed
        self.base_output_dir = base_output_dir
        self.report_dir = report_dir
        self.rng = random.Random(seed)
        self.base_datetime = datetime(2026, 1, 1, 0, 0, 0)

    def _random_timestamp(self, days_range: int = 90) -> datetime:
        offset_seconds = self.rng.randint(0, days_range * 86400)
        offset_micros = self.rng.randint(0, 999999)
        return self.base_datetime + timedelta(seconds=offset_seconds, microseconds=offset_micros)

    def generate_customers(self, count: int) -> pa.Table:
        """Generates reproducible customers dataset with unique customer_id and email."""
        customer_ids = list(range(1001, 1001 + count))
        customer_names = []
        emails = []
        created_ats = []

        for cid in customer_ids:
            fn = FIRST_NAMES[cid % len(FIRST_NAMES)]
            ln = LAST_NAMES[(cid // len(FIRST_NAMES)) % len(LAST_NAMES)]
            name = f"{fn} {ln}"
            domain = DOMAINS[cid % len(DOMAINS)]
            email = f"user_{cid}_{fn.lower()}.{ln.lower()}@{domain}"
            created_at = self._random_timestamp(days_range=60)

            customer_names.append(name)
            emails.append(email)
            created_ats.append(created_at)

        schema = get_customers_pa_schema()
        table = pa.Table.from_arrays(
            [
                pa.array(customer_ids, type=pa.int64()),
                pa.array(customer_names, type=pa.string()),
                pa.array(emails, type=pa.string()),
                pa.array(created_ats, type=pa.timestamp("us")),
            ],
            schema=schema
        )
        return table

    def _generate_base_orders(self, count: int, customer_count: int, start_order_id: int) -> Dict[str, list]:
        """Generates raw orders data dictionary with normal distributions, boundary amounts, out-of-order timestamps."""
        order_ids = []
        customer_ids = []
        order_timestamps = []
        amounts = []
        statuses = []

        for i in range(count):
            oid = start_order_id + i
            # Pick a valid customer id from generated customer set
            cid = 1001 + (self.rng.randint(0, customer_count - 1))
            
            # Timestamp generation with out-of-order and late-arriving variations
            # Some orders placed recently, some late-arriving (past date)
            skew_days = self.rng.choice([10, 20, 45, 60, 90, 120])
            ts = self.base_datetime + timedelta(days=skew_days, seconds=self.rng.randint(0, 86400), microseconds=self.rng.randint(0, 999999))

            # Realistic amounts including boundary conditions (0.00, small values, high values)
            r_val = self.rng.random()
            if r_val < 0.05:
                # Boundary value: exactly 0.00 (e.g. promotional free order)
                amt = Decimal("0.00")
            elif r_val < 0.10:
                # Small boundary amount
                amt = Decimal("0.01")
            elif r_val > 0.95:
                # Large order amount
                amt = Decimal(f"{self.rng.randint(5000, 25000)}.{self.rng.randint(0, 99):02d}")
            else:
                amt = Decimal(f"{self.rng.randint(10, 1500)}.{self.rng.randint(0, 99):02d}")

            status = self.rng.choice(ORDER_STATUSES)

            order_ids.append(oid)
            customer_ids.append(cid)
            order_timestamps.append(ts)
            amounts.append(amt)
            statuses.append(status)

        # Shuffle some timestamp order to simulate late-arriving and out-of-order arrivals
        return {
            "order_id": order_ids,
            "customer_id": customer_ids,
            "order_timestamp": order_timestamps,
            "amount": amounts,
            "status": statuses,
        }

    def generate_all(self, rows_per_batch: int = 1000) -> Dict[str, Any]:
        """Generates customers.parquet and all 9 batches according to specification."""
        os.makedirs(self.base_output_dir, exist_ok=True)
        os.makedirs(self.report_dir, exist_ok=True)

        customer_count = max(50, rows_per_batch // 2)
        customers_table = self.generate_customers(customer_count)
        customers_path = os.path.join(self.base_output_dir, "customers.parquet")
        pq.write_table(customers_table, customers_path)

        batches_summary = {}

        # -------------------------------------------------------------
        # Batch 001 - Original schema
        # -------------------------------------------------------------
        b1_raw = self._generate_base_orders(rows_per_batch, customer_count, start_order_id=100001)
        b1_schema = get_batch_001_pa_schema()
        b1_table = pa.Table.from_arrays(
            [
                pa.array(b1_raw["order_id"], type=pa.int64()),
                pa.array(b1_raw["customer_id"], type=pa.int64()),
                pa.array(b1_raw["order_timestamp"], type=pa.timestamp("us")),
                pa.array(b1_raw["amount"], type=pa.decimal128(12, 2)),
                pa.array(b1_raw["status"], type=pa.string()),
            ],
            schema=b1_schema
        )
        b1_path = os.path.join(self.base_output_dir, "batch_001_original.parquet")
        pq.write_table(b1_table, b1_path)
        batches_summary["batch_001_original.parquet"] = {
            "row_count": len(b1_table),
            "intentional_invalid_records": 0,
            "schema_change": "None (Canonical original baseline)",
            "columns": b1_schema.names,
            "types": {f.name: str(f.type) for f in b1_schema}
        }

        # -------------------------------------------------------------
        # Batch 002 - Compatible addition (currency: string, nullable)
        # -------------------------------------------------------------
        b2_raw = self._generate_base_orders(rows_per_batch, customer_count, start_order_id=200001)
        currencies = [
            self.rng.choice(CURRENCIES) if self.rng.random() > 0.15 else None
            for _ in range(rows_per_batch)
        ]
        b2_schema = get_batch_002_pa_schema()
        b2_table = pa.Table.from_arrays(
            [
                pa.array(b2_raw["order_id"], type=pa.int64()),
                pa.array(b2_raw["customer_id"], type=pa.int64()),
                pa.array(b2_raw["order_timestamp"], type=pa.timestamp("us")),
                pa.array(b2_raw["amount"], type=pa.decimal128(12, 2)),
                pa.array(b2_raw["status"], type=pa.string()),
                pa.array(currencies, type=pa.string()),
            ],
            schema=b2_schema
        )
        b2_path = os.path.join(self.base_output_dir, "batch_002_add_currency.parquet")
        pq.write_table(b2_table, b2_path)
        batches_summary["batch_002_add_currency.parquet"] = {
            "row_count": len(b2_table),
            "intentional_invalid_records": 0,
            "schema_change": "Compatible addition of nullable currency: string",
            "columns": b2_schema.names,
            "types": {f.name: str(f.type) for f in b2_schema}
        }

        # -------------------------------------------------------------
        # Batch 003 - Another compatible addition (sales_channel: string, nullable)
        # -------------------------------------------------------------
        b3_raw = self._generate_base_orders(rows_per_batch, customer_count, start_order_id=300001)
        b3_currencies = [self.rng.choice(CURRENCIES) for _ in range(rows_per_batch)]
        sales_channels = [
            self.rng.choice(SALES_CHANNELS) if self.rng.random() > 0.10 else None
            for _ in range(rows_per_batch)
        ]
        b3_schema = get_batch_003_pa_schema()
        b3_table = pa.Table.from_arrays(
            [
                pa.array(b3_raw["order_id"], type=pa.int64()),
                pa.array(b3_raw["customer_id"], type=pa.int64()),
                pa.array(b3_raw["order_timestamp"], type=pa.timestamp("us")),
                pa.array(b3_raw["amount"], type=pa.decimal128(12, 2)),
                pa.array(b3_raw["status"], type=pa.string()),
                pa.array(b3_currencies, type=pa.string()),
                pa.array(sales_channels, type=pa.string()),
            ],
            schema=b3_schema
        )
        b3_path = os.path.join(self.base_output_dir, "batch_003_add_sales_channel.parquet")
        pq.write_table(b3_table, b3_path)
        batches_summary["batch_003_add_sales_channel.parquet"] = {
            "row_count": len(b3_table),
            "intentional_invalid_records": 0,
            "schema_change": "Compatible addition of nullable sales_channel: string",
            "columns": b3_schema.names,
            "types": {f.name: str(f.type) for f in b3_schema}
        }

        # -------------------------------------------------------------
        # Batch 004 - Breaking type change (amount: string)
        # -------------------------------------------------------------
        b4_raw = self._generate_base_orders(rows_per_batch, customer_count, start_order_id=400001)
        amount_strings = [str(amt) for amt in b4_raw["amount"]]
        b4_schema = get_batch_004_pa_schema()
        b4_table = pa.Table.from_arrays(
            [
                pa.array(b4_raw["order_id"], type=pa.int64()),
                pa.array(b4_raw["customer_id"], type=pa.int64()),
                pa.array(b4_raw["order_timestamp"], type=pa.timestamp("us")),
                pa.array(amount_strings, type=pa.string()),
                pa.array(b4_raw["status"], type=pa.string()),
            ],
            schema=b4_schema
        )
        b4_path = os.path.join(self.base_output_dir, "batch_004_breaking_type.parquet")
        pq.write_table(b4_table, b4_path)
        batches_summary["batch_004_breaking_type.parquet"] = {
            "row_count": len(b4_table),
            "intentional_invalid_records": len(b4_table),  # entire batch has breaking schema
            "schema_change": "Breaking type change: amount changed from decimal(12,2) to string",
            "columns": b4_schema.names,
            "types": {f.name: str(f.type) for f in b4_schema}
        }

        # -------------------------------------------------------------
        # Batch 005 - Breaking column removal (order_id removed)
        # -------------------------------------------------------------
        b5_raw = self._generate_base_orders(rows_per_batch, customer_count, start_order_id=500001)
        b5_schema = get_batch_005_pa_schema()
        b5_table = pa.Table.from_arrays(
            [
                pa.array(b5_raw["customer_id"], type=pa.int64()),
                pa.array(b5_raw["order_timestamp"], type=pa.timestamp("us")),
                pa.array(b5_raw["amount"], type=pa.decimal128(12, 2)),
                pa.array(b5_raw["status"], type=pa.string()),
            ],
            schema=b5_schema
        )
        b5_path = os.path.join(self.base_output_dir, "batch_005_missing_order_id.parquet")
        pq.write_table(b5_table, b5_path)
        batches_summary["batch_005_missing_order_id.parquet"] = {
            "row_count": len(b5_table),
            "intentional_invalid_records": len(b5_table),  # entire batch has breaking missing PK
            "schema_change": "Breaking column removal: required order_id dropped",
            "columns": b5_schema.names,
            "types": {f.name: str(f.type) for f in b5_schema}
        }

        # -------------------------------------------------------------
        # Batch 006 - Duplicate records (duplicate order_id)
        # -------------------------------------------------------------
        b6_raw = self._generate_base_orders(rows_per_batch, customer_count, start_order_id=600001)
        # Intentionally duplicate 5% of order IDs (minimum 5 records)
        num_dupes = max(5, int(rows_per_batch * 0.05))
        for d in range(num_dupes):
            # Duplicate earlier order_ids into later slots
            b6_raw["order_id"][rows_per_batch - 1 - d] = b6_raw["order_id"][d]

        b6_schema = get_batch_001_pa_schema()
        b6_table = pa.Table.from_arrays(
            [
                pa.array(b6_raw["order_id"], type=pa.int64()),
                pa.array(b6_raw["customer_id"], type=pa.int64()),
                pa.array(b6_raw["order_timestamp"], type=pa.timestamp("us")),
                pa.array(b6_raw["amount"], type=pa.decimal128(12, 2)),
                pa.array(b6_raw["status"], type=pa.string()),
            ],
            schema=b6_schema
        )
        b6_path = os.path.join(self.base_output_dir, "batch_006_duplicates.parquet")
        pq.write_table(b6_table, b6_path)
        batches_summary["batch_006_duplicates.parquet"] = {
            "row_count": len(b6_table),
            "intentional_invalid_records": num_dupes,
            "schema_change": "None (Data quality violation: duplicate primary keys)",
            "columns": b6_schema.names,
            "types": {f.name: str(f.type) for f in b6_schema}
        }

        # -------------------------------------------------------------
        # Batch 007 - Invalid foreign keys
        # -------------------------------------------------------------
        b7_raw = self._generate_base_orders(rows_per_batch, customer_count, start_order_id=700001)
        # Intentionally inject non-existent customer IDs (e.g. 999991+)
        num_invalid_fk = max(5, int(rows_per_batch * 0.05))
        for f in range(num_invalid_fk):
            b7_raw["customer_id"][rows_per_batch - 1 - f] = 999990 + f

        b7_schema = get_batch_001_pa_schema()
        b7_table = pa.Table.from_arrays(
            [
                pa.array(b7_raw["order_id"], type=pa.int64()),
                pa.array(b7_raw["customer_id"], type=pa.int64()),
                pa.array(b7_raw["order_timestamp"], type=pa.timestamp("us")),
                pa.array(b7_raw["amount"], type=pa.decimal128(12, 2)),
                pa.array(b7_raw["status"], type=pa.string()),
            ],
            schema=b7_schema
        )
        b7_path = os.path.join(self.base_output_dir, "batch_007_invalid_foreign_keys.parquet")
        pq.write_table(b7_table, b7_path)
        batches_summary["batch_007_invalid_foreign_keys.parquet"] = {
            "row_count": len(b7_table),
            "intentional_invalid_records": num_invalid_fk,
            "schema_change": "None (Data quality violation: orphaned foreign keys)",
            "columns": b7_schema.names,
            "types": {f.name: str(f.type) for f in b7_schema}
        }

        # -------------------------------------------------------------
        # Batch 008 - Null violations in required fields
        # -------------------------------------------------------------
        b8_raw = self._generate_base_orders(rows_per_batch, customer_count, start_order_id=800001)
        # Inject nulls into required fields: order_id, customer_id, order_timestamp, amount, status
        num_null_violations = max(5, int(rows_per_batch * 0.05))
        
        # Partition nulls across fields
        for n in range(num_null_violations):
            target_idx = rows_per_batch - 1 - n
            field_mod = n % 5
            if field_mod == 0:
                b8_raw["order_id"][target_idx] = None
            elif field_mod == 1:
                b8_raw["customer_id"][target_idx] = None
            elif field_mod == 2:
                b8_raw["order_timestamp"][target_idx] = None
            elif field_mod == 3:
                b8_raw["amount"][target_idx] = None
            elif field_mod == 4:
                b8_raw["status"][target_idx] = None

        b8_schema = get_batch_008_pa_schema()
        b8_table = pa.Table.from_arrays(
            [
                pa.array(b8_raw["order_id"], type=pa.int64()),
                pa.array(b8_raw["customer_id"], type=pa.int64()),
                pa.array(b8_raw["order_timestamp"], type=pa.timestamp("us")),
                pa.array(b8_raw["amount"], type=pa.decimal128(12, 2)),
                pa.array(b8_raw["status"], type=pa.string()),
            ],
            schema=b8_schema
        )
        b8_path = os.path.join(self.base_output_dir, "batch_008_null_violations.parquet")
        pq.write_table(b8_table, b8_path)
        batches_summary["batch_008_null_violations.parquet"] = {
            "row_count": len(b8_table),
            "intentional_invalid_records": num_null_violations,
            "schema_change": "None structurally, contains null values in required canonical columns",
            "columns": b8_schema.names,
            "types": {f.name: str(f.type) for f in b8_schema}
        }

        # -------------------------------------------------------------
        # Batch 009 - Valid Empty Batch
        # -------------------------------------------------------------
        b9_schema = get_batch_001_pa_schema()
        b9_table = pa.Table.from_arrays(
            [
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.int64()),
                pa.array([], type=pa.timestamp("us")),
                pa.array([], type=pa.decimal128(12, 2)),
                pa.array([], type=pa.string()),
            ],
            schema=b9_schema
        )
        b9_path = os.path.join(self.base_output_dir, "batch_009_empty.parquet")
        pq.write_table(b9_table, b9_path)
        batches_summary["batch_009_empty.parquet"] = {
            "row_count": 0,
            "intentional_invalid_records": 0,
            "schema_change": "None (Valid empty Parquet file with original schema)",
            "columns": b9_schema.names,
            "types": {f.name: str(f.type) for f in b9_schema}
        }

        # -------------------------------------------------------------
        # Generation Report
        # -------------------------------------------------------------
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "random_seed": self.seed,
            "requested_rows_per_batch": rows_per_batch,
            "customers": {
                "file_name": "customers.parquet",
                "row_count": len(customers_table),
                "columns": customers_table.schema.names,
                "types": {f.name: str(f.type) for f in customers_table.schema}
            },
            "batches": batches_summary
        }

        report_path = os.path.join(self.report_dir, "generation_report.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        return report
