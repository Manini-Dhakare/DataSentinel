"""
Data Validator Engine for SchemaGuard (Phase 4.1).

Performs batch and row-level data validation:
- Schema-aware required-field enforcement (derived from source schema or evolved schema)
- Missing required column detection at schema level
- Comprehensive evaluation of all validation rules per record (collects all failures without short-circuiting)
- Domain & range checks (amount >= 0.00)
- In-batch duplicate primary key detection (first occurrence valid, subsequent duplicates invalid)
- Referential integrity validation with strict customer master safety (uninitialized vs initialized-empty)
- Non-crashing malformed record handling
- Deterministic invalid_table and valid_table outputs with identical schemas
- Accurate metrics distinguishing unique invalid rows from violation counters
"""

import os
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, List, Set, Optional, Tuple, Union
import yaml
import pyarrow as pa
import pyarrow.parquet as pq

from src.validation.models import ValidationFailure, OrderRecord


@dataclass
class ValidationMetrics:
    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    null_violations: int = 0
    duplicate_violations: int = 0
    foreign_key_violations: int = 0
    range_violations: int = 0
    malformed_violations: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "null_violations": self.null_violations,
            "duplicate_violations": self.duplicate_violations,
            "foreign_key_violations": self.foreign_key_violations,
            "range_violations": self.range_violations,
            "malformed_violations": self.malformed_violations,
        }


@dataclass
class ValidationResult:
    table_name: str
    is_valid_batch: bool
    metrics: ValidationMetrics
    valid_table: Optional[pa.Table] = None
    invalid_table: Optional[pa.Table] = None
    failures: List[ValidationFailure] = field(default_factory=list)
    schema_errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "is_valid_batch": self.is_valid_batch,
            "metrics": self.metrics.to_dict(),
            "failures_count": len(self.failures),
            "schema_errors": self.schema_errors,
            "failures_sample": [f.model_dump() for f in self.failures[:10]],
        }


class DataValidator:
    """
    Validates Parquet tables or batch dictionaries against business constraints,
    primary key uniqueness, and foreign key referential integrity.
    """

    def __init__(
        self,
        valid_customer_ids: Optional[Set[int]] = None,
        schema_config: Optional[Union[Dict[str, Any], str]] = "config/source_schema.yaml",
    ):
        """
        Args:
            valid_customer_ids: Pre-loaded set of valid customer IDs.
                                None indicates UNINITIALIZED.
                                An empty set indicates INITIALIZED BUT EMPTY.
            schema_config: Optional dictionary containing table schema definitions,
                           or path to YAML schema configuration file.
        """
        self.valid_customer_ids: Optional[Set[int]] = valid_customer_ids
        self._schema_cache: Optional[Dict[str, Any]] = None

        if isinstance(schema_config, dict):
            self._schema_cache = schema_config
        elif isinstance(schema_config, str):
            self.schema_config_path = schema_config
        else:
            self.schema_config_path = "config/source_schema.yaml"

    def load_customer_master(self, customers_parquet_path: str) -> None:
        """
        Loads valid customer IDs from customers.parquet into the validator set.
        """
        if not os.path.exists(customers_parquet_path):
            raise FileNotFoundError(f"Customer master file not found: {customers_parquet_path}")

        table = pq.read_table(customers_parquet_path, columns=["customer_id"])
        c_ids = table.column("customer_id").to_pylist()
        self.valid_customer_ids = set(c_ids)

    def get_table_schema_def(self, table_name: str = "orders") -> Dict[str, Any]:
        """
        Retrieves the canonical or applicable schema definition for a table.
        Fails clearly if schema definition is missing.
        """
        if self._schema_cache is not None:
            schemas = self._schema_cache.get("schemas", self._schema_cache)
            if table_name in schemas:
                return schemas[table_name]
            if "columns" in self._schema_cache:
                return self._schema_cache

        if not hasattr(self, "schema_config_path") or not os.path.exists(self.schema_config_path):
            raise ValueError(f"Schema configuration not found or not specified for table '{table_name}'")

        try:
            with open(self.schema_config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
                schemas = cfg.get("schemas", {})
                if table_name not in schemas:
                    raise ValueError(f"Table '{table_name}' not defined in schema configuration")
                return schemas[table_name]
        except Exception as e:
            raise ValueError(f"Failed to load schema configuration for '{table_name}': {e}")

    def resolve_required_fields(
        self,
        table_name: str = "orders",
        applicable_schema: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """
        Resolves non-nullable required fields dynamically from an applicable schema definition
        (such as an evolved schema dictionary or canonical source_schema.yaml).
        """
        schema_def = applicable_schema if applicable_schema is not None else self.get_table_schema_def(table_name)
        columns = schema_def.get("columns")
        if not isinstance(columns, dict):
            raise ValueError(f"Invalid schema definition for '{table_name}': missing or malformed 'columns'")

        required_fields = [
            col_name for col_name, meta in columns.items()
            if not bool(meta.get("nullable", False))
        ]
        return required_fields

    def validate_orders_table(
        self,
        table: pa.Table,
        applicable_schema: Optional[Dict[str, Any]] = None,
        source_file: str = "unknown.parquet",
    ) -> ValidationResult:
        """
        Validates an orders PyArrow table:
        - Resolves required non-nullable fields dynamically from applicable schema.
        - Identifies missing required columns at schema level.
        - Evaluates EVERY applicable validation rule per record (does not short-circuit).
        - Enforces customer master safety: raises ValueError if uninitialized; fails FK check if initialized-empty.
        - Returns both valid_table and invalid_table as typed PyArrow tables.
        """
        # Customer master safety check
        if self.valid_customer_ids is None:
            raise ValueError(
                "Customer master is uninitialized. You must either call load_customer_master() "
                "or supply valid_customer_ids (even as set() if empty) before running validation."
            )

        # Schema-aware required fields resolution
        required_fields = self.resolve_required_fields(
            table_name="orders",
            applicable_schema=applicable_schema
        )

        num_rows = len(table)
        metrics = ValidationMetrics(total_records=num_rows)

        # Check for missing required columns at the schema level
        table_columns = set(table.column_names)
        missing_required = [f for f in required_fields if f not in table_columns]
        if missing_required:
            schema_err = f"Schema validation error: missing required columns in batch: {missing_required}"
            # All rows in batch are invalid due to missing schema contract
            empty_table = table.slice(0, 0)
            return ValidationResult(
                table_name="orders",
                is_valid_batch=False,
                metrics=ValidationMetrics(
                    total_records=num_rows,
                    valid_records=0,
                    invalid_records=num_rows,
                    null_violations=0,
                ),
                valid_table=empty_table,
                invalid_table=table,
                failures=[
                    ValidationFailure(
                        source_row_index=-1,
                        record_identifier="BATCH_HEADER",
                        category="SCHEMA_VIOLATION",
                        column=", ".join(missing_required),
                        reason=schema_err,
                        raw_record={"columns": list(table_columns)},
                    )
                ],
                schema_errors=[schema_err],
            )

        # Handle empty batches cleanly
        if num_rows == 0:
            empty_table = table.slice(0, 0)
            return ValidationResult(
                table_name="orders",
                is_valid_batch=True,
                metrics=metrics,
                valid_table=empty_table,
                invalid_table=empty_table,
                failures=[],
                schema_errors=[],
            )

        failures: List[ValidationFailure] = []
        valid_indices: List[int] = []
        invalid_indices: List[int] = []
        seen_order_ids: Set[int] = set()

        rows = table.to_pylist()

        for idx, row in enumerate(rows):
            row_failures: List[ValidationFailure] = []
            oid = row.get("order_id")
            rec_id = str(oid) if oid is not None else f"row_{idx}"

            # 1. Evaluate required non-nullable fields
            for col in required_fields:
                val = row.get(col)
                if val is None or (isinstance(val, str) and not val.strip()):
                    row_failures.append(
                        ValidationFailure(
                            source_row_index=idx,
                            record_identifier=rec_id,
                            category="NULL_VIOLATION",
                            column=col,
                            reason=f"Required field '{col}' is null or empty",
                            raw_record=row,
                        )
                    )
                    metrics.null_violations += 1

            # 2. Evaluate duplicate primary keys
            if oid is not None:
                if oid in seen_order_ids:
                    row_failures.append(
                        ValidationFailure(
                            source_row_index=idx,
                            record_identifier=str(oid),
                            category="DUPLICATE_KEY",
                            column="order_id",
                            reason=f"Duplicate primary key 'order_id={oid}' detected in batch",
                            raw_record=row,
                        )
                    )
                    metrics.duplicate_violations += 1
                else:
                    seen_order_ids.add(oid)

            # 3. Evaluate numeric range and malformed amount
            if "amount" in row and row["amount"] is not None:
                amt = row["amount"]
                try:
                    amt_dec = Decimal(str(amt)) if not isinstance(amt, Decimal) else amt
                    if amt_dec < Decimal("0.00"):
                        row_failures.append(
                            ValidationFailure(
                                source_row_index=idx,
                                record_identifier=rec_id,
                                category="RANGE_VIOLATION",
                                column="amount",
                                reason=f"Amount {amt_dec} violates minimum constraint >= 0.00",
                                raw_record=row,
                            )
                        )
                        metrics.range_violations += 1
                except Exception as e:
                    row_failures.append(
                        ValidationFailure(
                            source_row_index=idx,
                            record_identifier=rec_id,
                            category="MALFORMED_RECORD",
                            column="amount",
                            reason=f"Cannot parse amount '{amt}' as decimal: {e}",
                            raw_record=row,
                        )
                    )
                    metrics.malformed_violations += 1

            # 4. Evaluate foreign key referential integrity
            # Uninitialized was checked above; here valid_customer_ids is a Set (possibly empty)
            cid = row.get("customer_id")
            if cid is not None:
                if cid not in self.valid_customer_ids:
                    row_failures.append(
                        ValidationFailure(
                            source_row_index=idx,
                            record_identifier=rec_id,
                            category="INVALID_FOREIGN_KEY",
                            column="customer_id",
                            reason=f"Foreign key customer_id={cid} does not exist in customer master",
                            raw_record=row,
                        )
                    )
                    metrics.foreign_key_violations += 1

            # Partition row into valid or invalid
            if row_failures:
                failures.extend(row_failures)
                invalid_indices.append(idx)
            else:
                valid_indices.append(idx)

        metrics.valid_records = len(valid_indices)
        metrics.invalid_records = len(invalid_indices)
        is_valid_batch = (len(invalid_indices) == 0)

        # Slices preserve input schema, types, and column order
        valid_table = table.take(valid_indices) if valid_indices else table.slice(0, 0)
        invalid_table = table.take(invalid_indices) if invalid_indices else table.slice(0, 0)

        return ValidationResult(
            table_name="orders",
            is_valid_batch=is_valid_batch,
            metrics=metrics,
            valid_table=valid_table,
            invalid_table=invalid_table,
            failures=failures,
            schema_errors=[],
        )


def validate_batch(
    table: pa.Table,
    customers_master_path: Optional[str] = "data/generated/customers.parquet",
    applicable_schema: Optional[Dict[str, Any]] = None,
) -> ValidationResult:
    """
    Public convenience function to validate an orders Parquet table.
    """
    validator = DataValidator(schema_config="config/source_schema.yaml")
    if customers_master_path:
        validator.load_customer_master(customers_master_path)
    return validator.validate_orders_table(table, applicable_schema=applicable_schema)
