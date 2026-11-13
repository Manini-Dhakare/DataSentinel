"""
Schema Evolution Classifier for SchemaGuard.

Classifies schema comparison results against evolution_rules.yaml into:
- COMPATIBLE
- BREAKING
- MANUAL_REVIEW
"""

import os
from enum import Enum
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import yaml

from src.schema.schema_detector import (
    SchemaComparisonResult,
    ColumnInfo,
    TypeMismatch,
    NullabilityDifference,
    normalize_type_str,
)


class ClassificationStatus(str, Enum):
    COMPATIBLE = "COMPATIBLE"
    BREAKING = "BREAKING"
    MANUAL_REVIEW = "MANUAL_REVIEW"


@dataclass
class ChangeDetail:
    category: str
    column: str
    description: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EvolutionClassificationResult:
    file_path: str
    table_name: str
    overall_status: ClassificationStatus
    compatible_changes: List[ChangeDetail] = field(default_factory=list)
    breaking_changes: List[ChangeDetail] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)
    requires_manual_review: bool = False
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "table_name": self.table_name,
            "overall_status": self.overall_status.value,
            "requires_manual_review": self.requires_manual_review,
            "error": self.error,
            "reasons": self.reasons,
            "warnings": self.warnings,
            "compatible_changes": [
                {
                    "category": c.category,
                    "column": c.column,
                    "description": c.description,
                    "details": c.details,
                }
                for c in self.compatible_changes
            ],
            "breaking_changes": [
                {
                    "category": b.category,
                    "column": b.column,
                    "description": b.description,
                    "details": b.details,
                }
                for b in self.breaking_changes
            ],
        }


class EvolutionClassifier:
    def __init__(self, rules_config_path: str = "config/evolution_rules.yaml"):
        self.rules_config_path = rules_config_path
        self._rules_cache: Optional[Dict[str, Any]] = None

    def load_rules(self) -> Dict[str, Any]:
        """Loads and validates evolution rules configuration safely."""
        if self._rules_cache is not None:
            return self._rules_cache

        if not os.path.exists(self.rules_config_path):
            raise FileNotFoundError(f"Evolution rules configuration not found: {self.rules_config_path}")

        try:
            with open(self.rules_config_path, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
                if not isinstance(cfg, dict) or "rules" not in cfg:
                    raise ValueError(
                        f"Invalid evolution rules format in {self.rules_config_path}: missing 'rules' section"
                    )
                self._rules_cache = cfg
                return cfg
        except Exception as e:
            raise ValueError(f"Failed to load evolution rules from {self.rules_config_path}: {e}")

    def classify(self, comparison: SchemaComparisonResult) -> EvolutionClassificationResult:
        """
        Classifies the schema comparison result using evolution rules.
        """
        # If the detector already encountered an error, propagate as breaking/error
        if comparison.error:
            return EvolutionClassificationResult(
                file_path=comparison.file_path,
                table_name=comparison.table_name,
                overall_status=ClassificationStatus.BREAKING,
                reasons=[f"Detector error: {comparison.error}"],
                error=comparison.error,
                requires_manual_review=True,
            )

        # Load rules safely
        try:
            rules_cfg = self.load_rules()
        except Exception as e:
            return EvolutionClassificationResult(
                file_path=comparison.file_path,
                table_name=comparison.table_name,
                overall_status=ClassificationStatus.BREAKING,
                reasons=[f"Evolution configuration error: {e}"],
                error=str(e),
                requires_manual_review=True,
            )

        rules = rules_cfg.get("rules") or {}
        compat_rules = rules.get("compatible_changes") or {}
        breaking_rules = rules.get("breaking_changes") or {}

        # Extract approved widenings
        safe_widening_cfg = compat_rules.get("safe_type_widening") or {}
        safe_widening_enabled = safe_widening_cfg.get("enabled", False)
        raw_approved = safe_widening_cfg.get("approved_widenings")
        approved_widenings: Dict[str, List[str]] = (
            raw_approved if (safe_widening_enabled and isinstance(raw_approved, dict)) else {}
        )

        # Add nullable column rule
        add_nullable_cfg = compat_rules.get("add_nullable_column") or {}
        add_nullable_enabled = add_nullable_cfg.get("enabled", False)
        require_default_null = add_nullable_cfg.get("require_default_null", True)

        # Optional metadata rule
        metadata_cfg = compat_rules.get("add_optional_metadata") or {}
        raw_prefixes = metadata_cfg.get("allowed_prefixes")
        allowed_metadata_prefixes: List[str] = (
            raw_prefixes if isinstance(raw_prefixes, list) else []
        )

        compatible_changes: List[ChangeDetail] = []
        breaking_changes: List[ChangeDetail] = []
        warnings: List[str] = []
        reasons: List[str] = []
        requires_manual_review = False

        # -------------------------------------------------------------
        # 1. Evaluate Added Columns
        # -------------------------------------------------------------
        for col in comparison.added_columns:
            # Check if optional metadata
            is_metadata = any(col.name.startswith(p) for p in allowed_metadata_prefixes)
            if col.nullable and (add_nullable_enabled or is_metadata):
                desc = (
                    f"Added optional metadata column '{col.name}' ({col.data_type})"
                    if is_metadata
                    else f"Added nullable column '{col.name}' ({col.data_type}) with implicit null default"
                )
                compatible_changes.append(
                    ChangeDetail(
                        category="added_nullable_column",
                        column=col.name,
                        description=desc,
                        details={"data_type": col.data_type, "nullable": col.nullable},
                    )
                )
            else:
                # Adding a non-nullable column without default is breaking
                reason = (
                    f"Added column '{col.name}' is non-nullable (nullable={col.nullable}). "
                    "Non-nullable column additions without default values violate existing contracts."
                )
                breaking_changes.append(
                    ChangeDetail(
                        category="added_non_nullable_column",
                        column=col.name,
                        description=reason,
                        details={"data_type": col.data_type, "nullable": col.nullable},
                    )
                )
                reasons.append(reason)

        # -------------------------------------------------------------
        # 2. Evaluate Missing Columns
        # -------------------------------------------------------------
        for col in comparison.missing_columns:
            if not col.nullable:
                reason = f"Required column '{col.name}' is missing from incoming schema"
                breaking_changes.append(
                    ChangeDetail(
                        category="missing_required_column",
                        column=col.name,
                        description=reason,
                        details={"data_type": col.data_type, "nullable": col.nullable},
                    )
                )
                reasons.append(reason)
            else:
                # Missing optional column
                desc = f"Optional column '{col.name}' is missing from incoming batch (treated as null)"
                warnings.append(desc)
                compatible_changes.append(
                    ChangeDetail(
                        category="missing_optional_column",
                        column=col.name,
                        description=desc,
                        details={"data_type": col.data_type, "nullable": col.nullable},
                    )
                )

        # -------------------------------------------------------------
        # 3. Evaluate Data Type Mismatches
        # -------------------------------------------------------------
        for mismatch in comparison.type_mismatches:
            exp_norm = normalize_type_str(mismatch.expected_type)
            det_norm = normalize_type_str(mismatch.detected_type)

            # Check if this is an explicitly approved safe widening
            # Note: in incoming batch evolution, detected_type widens expected_type
            allowed_targets = approved_widenings.get(exp_norm, [])
            norm_allowed = [normalize_type_str(t) for t in allowed_targets]

            if det_norm in norm_allowed:
                desc = (
                    f"Safe type widening approved for column '{mismatch.column}': "
                    f"'{mismatch.expected_type}' -> '{mismatch.detected_type}'"
                )
                compatible_changes.append(
                    ChangeDetail(
                        category="safe_type_widening",
                        column=mismatch.column,
                        description=desc,
                        details={
                            "expected_type": mismatch.expected_type,
                            "detected_type": mismatch.detected_type,
                        },
                    )
                )
            else:
                # Check for narrowing or completely incompatible types
                reason = (
                    f"Incompatible or unapproved type change for column '{mismatch.column}': "
                    f"'{mismatch.expected_type}' -> '{mismatch.detected_type}'"
                )
                breaking_changes.append(
                    ChangeDetail(
                        category="incompatible_type_change",
                        column=mismatch.column,
                        description=reason,
                        details={
                            "expected_type": mismatch.expected_type,
                            "detected_type": mismatch.detected_type,
                        },
                    )
                )
                reasons.append(reason)

        # -------------------------------------------------------------
        # 4. Evaluate Nullability Differences
        # -------------------------------------------------------------
        for null_diff in comparison.nullability_differences:
            if not null_diff.expected_nullable and null_diff.detected_nullable:
                # Required in canonical contract, but nullable in incoming batch
                reason = (
                    f"Column '{null_diff.column}' contract relaxed from non-nullable "
                    f"(expected nullable={null_diff.expected_nullable}) to nullable "
                    f"(detected nullable={null_diff.detected_nullable})"
                )
                breaking_changes.append(
                    ChangeDetail(
                        category="relax_required_to_nullable",
                        column=null_diff.column,
                        description=reason,
                        details={
                            "expected_nullable": null_diff.expected_nullable,
                            "detected_nullable": null_diff.detected_nullable,
                        },
                    )
                )
                reasons.append(reason)
            elif null_diff.expected_nullable and not null_diff.detected_nullable:
                # Optional in contract, but made strictly non-nullable in batch
                desc = (
                    f"Column '{null_diff.column}' made non-nullable in incoming batch "
                    f"(expected nullable={null_diff.expected_nullable}, detected nullable={null_diff.detected_nullable})"
                )
                warnings.append(desc)
                requires_manual_review = True
                reasons.append(f"{desc} (requires manual review to verify producer contract)")

        # -------------------------------------------------------------
        # 5. Determine Overall Status
        # -------------------------------------------------------------
        if len(breaking_changes) > 0:
            overall_status = ClassificationStatus.BREAKING
        elif requires_manual_review:
            overall_status = ClassificationStatus.MANUAL_REVIEW
        else:
            overall_status = ClassificationStatus.COMPATIBLE
            if len(compatible_changes) == 0:
                reasons.append("Exact schema match with canonical baseline")

        return EvolutionClassificationResult(
            file_path=comparison.file_path,
            table_name=comparison.table_name,
            overall_status=overall_status,
            compatible_changes=compatible_changes,
            breaking_changes=breaking_changes,
            warnings=warnings,
            reasons=reasons,
            requires_manual_review=requires_manual_review,
            error=None,
        )


def classify_schema_comparison(
    comparison: SchemaComparisonResult,
    rules_config_path: str = "config/evolution_rules.yaml",
) -> EvolutionClassificationResult:
    """
    Public convenience function to classify schema comparison results.
    """
    classifier = EvolutionClassifier(rules_config_path=rules_config_path)
    return classifier.classify(comparison)
