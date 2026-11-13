"""
Schema Detector Module for SchemaGuard.

Inspects Parquet files, extracts their physical schemas, and compares them
against configured canonical baseline schemas.
"""

import os
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import yaml
import pyarrow.parquet as pq
import pyarrow as pa


def normalize_type_str(type_str: str) -> str:
    """
    Normalizes Parquet/PyArrow and YAML type strings for consistent comparison.
    Example:
        'decimal128(12, 2)' -> 'decimal(12,2)'
        'decimal(12, 2)'    -> 'decimal(12,2)'
        'timestamp[us, tz=...]' -> 'timestamp[us]'
    """
    t = type_str.strip().lower()
    t = t.replace(" ", "")
    if t.startswith("decimal128"):
        t = t.replace("decimal128", "decimal")
    if t.startswith("decimal256"):
        t = t.replace("decimal256", "decimal")
    if t.startswith("timestamp[us"):
        t = "timestamp[us]"
    elif t.startswith("timestamp[ms"):
        t = "timestamp[ms]"
    elif t.startswith("timestamp[s"):
        t = "timestamp[s]"
    elif t.startswith("timestamp[ns"):
        t = "timestamp[ns]"
    return t


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool


@dataclass
class TypeMismatch:
    column: str
    expected_type: str
    detected_type: str


@dataclass
class NullabilityDifference:
    column: str
    expected_nullable: bool
    detected_nullable: bool


@dataclass
class SchemaComparisonResult:
    file_path: str
    table_name: str
    row_count: int
    detected_columns: List[ColumnInfo]
    expected_columns: List[ColumnInfo]
    added_columns: List[ColumnInfo]
    missing_columns: List[ColumnInfo]
    type_mismatches: List[TypeMismatch]
    nullability_differences: List[NullabilityDifference]
    is_exact_match: bool
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "table_name": self.table_name,
            "row_count": self.row_count,
            "is_exact_match": self.is_exact_match,
            "error": self.error,
            "detected_columns": [
                {"name": c.name, "data_type": c.data_type, "nullable": c.nullable}
                for c in self.detected_columns
            ],
            "added_columns": [
                {"name": c.name, "data_type": c.data_type, "nullable": c.nullable}
                for c in self.added_columns
            ],
            "missing_columns": [
                {"name": c.name, "data_type": c.data_type, "nullable": c.nullable}
                for c in self.missing_columns
            ],
            "type_mismatches": [
                {
                    "column": m.column,
                    "expected_type": m.expected_type,
                    "detected_type": m.detected_type,
                }
                for m in self.type_mismatches
            ],
            "nullability_differences": [
                {
                    "column": n.column,
                    "expected_nullable": n.expected_nullable,
                    "detected_nullable": n.detected_nullable,
                }
                for n in self.nullability_differences
            ],
        }


class SchemaDetector:
    def __init__(self, config_path: str = "config/source_schema.yaml"):
        self.config_path = config_path
        self._config_cache: Optional[Dict[str, Any]] = None

    def load_config(self) -> Dict[str, Any]:
        """Loads canonical source schema configuration safely."""
        if self._config_cache is not None:
            return self._config_cache

        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                if not isinstance(config, dict) or "schemas" not in config:
                    raise ValueError(
                        f"Invalid configuration format in {self.config_path}: missing 'schemas' root key"
                    )
                self._config_cache = config
                return config
        except Exception as e:
            raise ValueError(f"Failed to load configuration from {self.config_path}: {e}")

    def inspect_parquet(self, file_path: str, table_name: str = "orders") -> SchemaComparisonResult:
        """
        Inspects the physical schema of a Parquet file and compares it with the expected table schema.
        Handles missing files, unreadable files, and missing configurations safely.
        """
        # Validate file existence
        if not os.path.exists(file_path):
            return SchemaComparisonResult(
                file_path=file_path,
                table_name=table_name,
                row_count=0,
                detected_columns=[],
                expected_columns=[],
                added_columns=[],
                missing_columns=[],
                type_mismatches=[],
                nullability_differences=[],
                is_exact_match=False,
                error=f"File not found: {file_path}",
            )

        # Validate configuration
        try:
            config = self.load_config()
            table_schemas = config.get("schemas", {})
            if table_name not in table_schemas:
                return SchemaComparisonResult(
                    file_path=file_path,
                    table_name=table_name,
                    row_count=0,
                    detected_columns=[],
                    expected_columns=[],
                    added_columns=[],
                    missing_columns=[],
                    type_mismatches=[],
                    nullability_differences=[],
                    is_exact_match=False,
                    error=f"Table '{table_name}' definition not found in configuration",
                )
            expected_table_def = table_schemas[table_name]
        except Exception as e:
            return SchemaComparisonResult(
                file_path=file_path,
                table_name=table_name,
                row_count=0,
                detected_columns=[],
                expected_columns=[],
                added_columns=[],
                missing_columns=[],
                type_mismatches=[],
                nullability_differences=[],
                is_exact_match=False,
                error=f"Configuration error: {e}",
            )

        # Read Parquet schema and metadata safely
        try:
            parquet_file = pq.ParquetFile(file_path)
            arrow_schema = parquet_file.schema_arrow
            row_count = parquet_file.metadata.num_rows
        except Exception as e:
            return SchemaComparisonResult(
                file_path=file_path,
                table_name=table_name,
                row_count=0,
                detected_columns=[],
                expected_columns=[],
                added_columns=[],
                missing_columns=[],
                type_mismatches=[],
                nullability_differences=[],
                is_exact_match=False,
                error=f"Unreadable Parquet file: {e}",
            )

        # Parse detected columns
        detected_columns: List[ColumnInfo] = []
        detected_col_map: Dict[str, ColumnInfo] = {}
        for field_obj in arrow_schema:
            col_info = ColumnInfo(
                name=field_obj.name,
                data_type=str(field_obj.type),
                nullable=field_obj.nullable,
            )
            detected_columns.append(col_info)
            detected_col_map[field_obj.name] = col_info

        # Parse expected columns from configuration
        expected_columns: List[ColumnInfo] = []
        expected_col_map: Dict[str, ColumnInfo] = {}
        cfg_columns = expected_table_def.get("columns", {})
        for col_name, col_meta in cfg_columns.items():
            col_info = ColumnInfo(
                name=col_name,
                data_type=str(col_meta.get("type")),
                nullable=bool(col_meta.get("nullable", False)),
            )
            expected_columns.append(col_info)
            expected_col_map[col_name] = col_info

        # Compute differences
        added_columns: List[ColumnInfo] = []
        for col_name, det_col in detected_col_map.items():
            if col_name not in expected_col_map:
                added_columns.append(det_col)

        missing_columns: List[ColumnInfo] = []
        type_mismatches: List[TypeMismatch] = []
        nullability_differences: List[NullabilityDifference] = []

        for col_name, exp_col in expected_col_map.items():
            if col_name not in detected_col_map:
                missing_columns.append(exp_col)
            else:
                det_col = detected_col_map[col_name]
                # Check data type mismatch
                norm_exp = normalize_type_str(exp_col.data_type)
                norm_det = normalize_type_str(det_col.data_type)
                if norm_exp != norm_det:
                    type_mismatches.append(
                        TypeMismatch(
                            column=col_name,
                            expected_type=exp_col.data_type,
                            detected_type=det_col.data_type,
                        )
                    )
                # Check nullability differences
                if exp_col.nullable != det_col.nullable:
                    nullability_differences.append(
                        NullabilityDifference(
                            column=col_name,
                            expected_nullable=exp_col.nullable,
                            detected_nullable=det_col.nullable,
                        )
                    )

        is_exact_match = (
            len(added_columns) == 0
            and len(missing_columns) == 0
            and len(type_mismatches) == 0
            and len(nullability_differences) == 0
        )

        return SchemaComparisonResult(
            file_path=file_path,
            table_name=table_name,
            row_count=row_count,
            detected_columns=detected_columns,
            expected_columns=expected_columns,
            added_columns=added_columns,
            missing_columns=missing_columns,
            type_mismatches=type_mismatches,
            nullability_differences=nullability_differences,
            is_exact_match=is_exact_match,
            error=None,
        )


def inspect_schema(
    file_path: str,
    table_name: str = "orders",
    config_path: str = "config/source_schema.yaml",
) -> SchemaComparisonResult:
    """
    Public convenience function to inspect a Parquet file's schema.
    """
    detector = SchemaDetector(config_path=config_path)
    return detector.inspect_parquet(file_path=file_path, table_name=table_name)
