"""
Schema Evolution Engine for SchemaGuard (Phase 3.2.2).

Produces a new canonical schema and PyArrow schema using explicitly approved
evolution classifications.
"""

import copy
import re
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import pyarrow as pa

from src.schema.evolution_classifier import (
    EvolutionClassificationResult,
    ClassificationStatus,
    ChangeDetail,
)
from src.schema.schema_detector import normalize_type_str


def parse_type_to_pyarrow(type_str: str) -> pa.DataType:
    """
    Safely converts normalized or configuration/Parquet type strings into PyArrow DataType objects.
    Supports standard primitives, parameterized decimals, and timestamps.
    """
    s = type_str.strip().lower().replace(" ", "")
    if s == "int64":
        return pa.int64()
    if s == "int32":
        return pa.int32()
    if s == "int16":
        return pa.int16()
    if s == "int8":
        return pa.int8()
    if s in ("string", "str", "utf8"):
        return pa.string()
    if s in ("float", "float32"):
        return pa.float32()
    if s in ("double", "float64"):
        return pa.float64()
    if s == "date32":
        return pa.date32()
    if s == "date64":
        return pa.date64()
    if s in ("bool", "boolean"):
        return pa.bool_()
    if s.startswith("timestamp"):
        m = re.search(r"\[(.*?)\]", s)
        unit = m.group(1) if m else "us"
        return pa.timestamp(unit)
    if s.startswith("decimal"):
        m = re.search(r"\((.*?)\)", s)
        if m:
            parts = m.group(1).split(",")
            prec = int(parts[0])
            scale = int(parts[1]) if len(parts) > 1 else 0
            return pa.decimal128(prec, scale)
        return pa.decimal128(12, 2)
    raise ValueError(f"Unsupported data type for PyArrow conversion: {type_str}")


@dataclass
class SchemaEvolutionResult:
    table_name: str
    is_evolved: bool
    status: str
    evolved_schema_dict: Dict[str, Any]
    pyarrow_schema: Optional[pa.Schema]
    applied_changes: List[ChangeDetail] = field(default_factory=list)
    rejection_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "is_evolved": self.is_evolved,
            "status": self.status,
            "applied_changes": [
                {
                    "category": c.category,
                    "column": c.column,
                    "description": c.description,
                    "details": c.details,
                }
                for c in self.applied_changes
            ],
            "rejection_reason": self.rejection_reason,
            "pyarrow_schema": str(self.pyarrow_schema) if self.pyarrow_schema else None,
            "evolved_schema_dict": self.evolved_schema_dict,
        }


class SchemaEvolver:
    """
    Controls schema evolution by applying approved changes to a base canonical schema.
    """

    def to_pyarrow_schema(self, schema_def: Dict[str, Any]) -> pa.Schema:
        """
        Constructs a valid PyArrow Schema from a canonical table schema dictionary.
        Preserves column order and nullability.
        """
        columns = schema_def.get("columns", {})
        fields = []
        for col_name, col_meta in columns.items():
            type_str = col_meta.get("type", "string")
            nullable = bool(col_meta.get("nullable", True))
            pa_type = parse_type_to_pyarrow(type_str)
            fields.append(pa.field(col_name, pa_type, nullable=nullable))
        return pa.schema(fields)

    def evolve(
        self,
        base_schema_def: Dict[str, Any],
        classification: EvolutionClassificationResult,
    ) -> SchemaEvolutionResult:
        """
        Evolves a base schema definition using an approved evolution classification.
        - Accepts only COMPATIBLE classifications.
        - Rejects BREAKING and MANUAL_REVIEW classifications.
        - Deep-copies the base schema ensuring immutability.
        - Preserves existing column order and adds new columns at the end.
        - Returns a SchemaEvolutionResult with the updated dictionary and pyarrow.Schema.
        """
        # Validate inputs
        if not isinstance(base_schema_def, dict) or "columns" not in base_schema_def:
            return SchemaEvolutionResult(
                table_name=getattr(classification, "table_name", "unknown"),
                is_evolved=False,
                status="REJECTED",
                evolved_schema_dict={},
                pyarrow_schema=None,
                rejection_reason="Invalid base schema dictionary: missing 'columns' key",
            )

        table_name = classification.table_name or "unknown"

        # 1. Enforce strict status gates
        if classification.overall_status == ClassificationStatus.BREAKING:
            reason = (
                f"Evolution rejected: classification status is BREAKING. "
                f"Reasons: {'; '.join(classification.reasons)}"
            )
            return SchemaEvolutionResult(
                table_name=table_name,
                is_evolved=False,
                status="REJECTED",
                evolved_schema_dict=copy.deepcopy(base_schema_def),
                pyarrow_schema=self.to_pyarrow_schema(base_schema_def),
                rejection_reason=reason,
            )

        if classification.overall_status == ClassificationStatus.MANUAL_REVIEW:
            reason = (
                f"Evolution rejected: classification status requires MANUAL_REVIEW. "
                f"Reasons: {'; '.join(classification.reasons)}"
            )
            return SchemaEvolutionResult(
                table_name=table_name,
                is_evolved=False,
                status="REJECTED",
                evolved_schema_dict=copy.deepcopy(base_schema_def),
                pyarrow_schema=self.to_pyarrow_schema(base_schema_def),
                rejection_reason=reason,
            )

        if classification.overall_status != ClassificationStatus.COMPATIBLE:
            return SchemaEvolutionResult(
                table_name=table_name,
                is_evolved=False,
                status="REJECTED",
                evolved_schema_dict=copy.deepcopy(base_schema_def),
                pyarrow_schema=self.to_pyarrow_schema(base_schema_def),
                rejection_reason=f"Evolution rejected: unsupported classification status '{classification.overall_status}'",
            )

        # 2. Immutability: Deep copy the base schema
        evolved_dict = copy.deepcopy(base_schema_def)
        evolved_columns: Dict[str, Any] = evolved_dict.setdefault("columns", {})
        applied_changes: List[ChangeDetail] = []

        # 3. Apply approved compatible changes
        for change in classification.compatible_changes:
            category = change.category
            col_name = change.column

            if category in ("added_nullable_column", "add_optional_metadata"):
                # Handle potential duplicate column addition safely
                if col_name in evolved_columns:
                    existing_type = normalize_type_str(evolved_columns[col_name].get("type", ""))
                    incoming_type = normalize_type_str(change.details.get("data_type", "string"))
                    if existing_type == incoming_type:
                        # Identical column definition already present, skip duplicate addition safely
                        continue
                    else:
                        # Conflicting type for duplicate column: reject evolution
                        reason = (
                            f"Evolution rejected: conflicting duplicate column '{col_name}'. "
                            f"Existing type: '{existing_type}', conflicting incoming type: '{incoming_type}'"
                        )
                        return SchemaEvolutionResult(
                            table_name=table_name,
                            is_evolved=False,
                            status="REJECTED",
                            evolved_schema_dict=copy.deepcopy(base_schema_def),
                            pyarrow_schema=self.to_pyarrow_schema(base_schema_def),
                            applied_changes=[],
                            rejection_reason=reason,
                        )

                col_type = change.details.get("data_type", "string")
                # Normalize type string (e.g. decimal128(12, 2) -> decimal(12,2))
                norm_type = normalize_type_str(col_type)
                evolved_columns[col_name] = {
                    "type": norm_type,
                    "nullable": True,
                    "default": None,
                    "description": change.description,
                }
                applied_changes.append(change)

            elif category == "safe_type_widening":
                if col_name in evolved_columns:
                    new_type = change.details.get("detected_type", "")
                    if new_type:
                        evolved_columns[col_name]["type"] = normalize_type_str(new_type)
                        applied_changes.append(change)

            elif category == "missing_optional_column":
                # Omitting an optional column requires no schema mutation
                continue

        # 4. Generate pyarrow.Schema
        pa_schema = self.to_pyarrow_schema(evolved_dict)
        is_evolved = len(applied_changes) > 0

        return SchemaEvolutionResult(
            table_name=table_name,
            is_evolved=is_evolved,
            status="SUCCESS",
            evolved_schema_dict=evolved_dict,
            pyarrow_schema=pa_schema,
            applied_changes=applied_changes,
            rejection_reason=None,
        )


def evolve_schema(
    base_schema_def: Dict[str, Any],
    classification: EvolutionClassificationResult,
) -> SchemaEvolutionResult:
    """
    Public convenience function to evolve a canonical base schema using a classification result.
    """
    evolver = SchemaEvolver()
    return evolver.evolve(base_schema_def=base_schema_def, classification=classification)
