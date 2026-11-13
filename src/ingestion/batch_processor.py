"""
Batch Processor Module for SchemaGuard (Phase 4.3).

Coordinates end-to-end processing of incoming Parquet batches:
1. Schema detection & delta inspection (Phase 3.1)
2. Schema evolution classification (Phase 3.2.1)
3. Schema evolution execution & validation gate (Phase 3.2.2)
4. Data validation with customer master referential integrity (Phase 4.1)
5. Quarantine persistence & routing of invalid records with fingerprint idempotency (Phase 4.2)
"""

import os
import uuid
from enum import Enum
from typing import Dict, Any, Optional, Set, Union
from dataclasses import dataclass
import yaml
import pyarrow as pa
import pyarrow.parquet as pq

from src.schema.schema_detector import SchemaDetector, SchemaComparisonResult
from src.schema.evolution_classifier import (
    EvolutionClassifier,
    EvolutionClassificationResult,
    ClassificationStatus,
)
from src.schema.schema_evolver import SchemaEvolver, SchemaEvolutionResult
from src.validation.validator import DataValidator, ValidationResult, ValidationMetrics
from src.quarantine.quarantine_manager import QuarantineManager, QuarantineRecord


class BatchProcessingStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    SCHEMA_REJECTED = "SCHEMA_REJECTED"
    FAILED = "FAILED"


@dataclass
class BatchProcessingResult:
    batch_id: str
    table_name: str
    status: BatchProcessingStatus
    source_file: str
    row_count: int
    valid_record_count: int
    invalid_record_count: int
    schema_comparison: Optional[SchemaComparisonResult] = None
    evolution_classification: Optional[EvolutionClassificationResult] = None
    schema_evolution: Optional[SchemaEvolutionResult] = None
    validation: Optional[ValidationResult] = None
    quarantine: Optional[QuarantineRecord] = None
    valid_table: Optional[pa.Table] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "table_name": self.table_name,
            "status": self.status.value,
            "source_file": self.source_file,
            "row_count": self.row_count,
            "valid_record_count": self.valid_record_count,
            "invalid_record_count": self.invalid_record_count,
            "error_message": self.error_message,
            "schema_comparison": self.schema_comparison.to_dict() if self.schema_comparison else None,
            "evolution_classification": self.evolution_classification.to_dict() if self.evolution_classification else None,
            "schema_evolution": self.schema_evolution.to_dict() if self.schema_evolution else None,
            "validation": self.validation.to_dict() if self.validation else None,
            "quarantine": self.quarantine.to_dict() if self.quarantine else None,
        }


class BatchProcessor:
    """
    Cohesive coordinator for SchemaGuard ingestion pipeline.
    Combines Schema Detection, Evolution, Validation, and Quarantine routing.
    """

    def __init__(
        self,
        schema_config_path: str = "config/source_schema.yaml",
        evolution_rules_path: str = "config/evolution_rules.yaml",
        pipeline_config_path: str = "config/pipeline_config.yaml",
        customer_master_path: Optional[str] = "data/generated/customers.parquet",
        valid_customer_ids: Optional[Set[int]] = None,
        base_quarantine_dir: Optional[str] = None,
    ):
        self.schema_config_path = schema_config_path
        self.evolution_rules_path = evolution_rules_path
        self.pipeline_config_path = pipeline_config_path

        # Load pipeline config if available to derive defaults
        self.pipeline_config = self._load_yaml(pipeline_config_path) if os.path.exists(pipeline_config_path) else {}
        
        # Determine quarantine dir
        if base_quarantine_dir is None:
            storage_cfg = self.pipeline_config.get("storage", {})
            base_quarantine_dir = storage_cfg.get("quarantine_dir", "data/quarantine")
        self.base_quarantine_dir = base_quarantine_dir

        # Initialize existing domain components
        self.detector = SchemaDetector(config_path=self.schema_config_path)
        self.classifier = EvolutionClassifier(rules_config_path=self.evolution_rules_path)
        self.evolver = SchemaEvolver()
        self.quarantine_manager = QuarantineManager(base_quarantine_dir=self.base_quarantine_dir)

        # Initialize validator
        self.validator = DataValidator(
            valid_customer_ids=valid_customer_ids,
            schema_config=self.schema_config_path,
        )
        if valid_customer_ids is None and customer_master_path and os.path.exists(customer_master_path):
            self.validator.load_customer_master(customer_master_path)

    @staticmethod
    def _load_yaml(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, dict) else {}

    def get_base_schema_def(self, table_name: str) -> Dict[str, Any]:
        """Loads canonical baseline definition for a given table from schema configuration."""
        cfg = self.detector.load_config()
        schemas = cfg.get("schemas", {})
        if table_name not in schemas:
            raise ValueError(f"Table '{table_name}' definition not found in {self.schema_config_path}")
        return schemas[table_name]

    def process_batch(
        self,
        file_path: str,
        batch_id: Optional[str] = None,
        table_name: str = "orders",
    ) -> BatchProcessingResult:
        """
        Executes end-to-end processing sequence on an incoming Parquet file:
        1. Generates/validates batch_id
        2. Detects schema & compares with baseline
        3. Classifies schema evolution
        4. Evaluates status gates (BREAKING / MANUAL_REVIEW -> SCHEMA_REJECTED)
        5. Validates data records against applicable schema and business rules
        6. Persists invalid records into quarantine (with fingerprint-based idempotency)
        7. Returns unified BatchProcessingResult
        """
        # 1. Resolve batch_id
        if not batch_id or not isinstance(batch_id, str) or not batch_id.strip():
            batch_id = uuid.uuid4().hex
        else:
            batch_id = batch_id.strip()

        # Guard against file absence or unreadable path early
        if not os.path.exists(file_path):
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.FAILED,
                source_file=file_path,
                row_count=0,
                valid_record_count=0,
                invalid_record_count=0,
                error_message=f"Source file not found: {file_path}",
            )

        # 2. Schema Detection
        comparison = self.detector.inspect_parquet(file_path=file_path, table_name=table_name)
        if comparison.error:
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.FAILED,
                source_file=file_path,
                row_count=comparison.row_count,
                valid_record_count=0,
                invalid_record_count=0,
                schema_comparison=comparison,
                error_message=f"Schema detection failed: {comparison.error}",
            )

        # 3. Schema Evolution Classification
        classification = self.classifier.classify(comparison)
        if classification.error:
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.FAILED,
                source_file=file_path,
                row_count=comparison.row_count,
                valid_record_count=0,
                invalid_record_count=0,
                schema_comparison=comparison,
                evolution_classification=classification,
                error_message=f"Evolution classification failed: {classification.error}",
            )

        # 4. Status Gate: Check for BREAKING or MANUAL_REVIEW
        try:
            base_schema_def = self.get_base_schema_def(table_name)
        except Exception as e:
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.FAILED,
                source_file=file_path,
                row_count=comparison.row_count,
                valid_record_count=0,
                invalid_record_count=0,
                schema_comparison=comparison,
                evolution_classification=classification,
                error_message=f"Base schema lookup failed: {e}",
            )

        try:
            evolution_result = self.evolver.evolve(base_schema_def, classification)
        except Exception as e:
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.FAILED,
                source_file=file_path,
                row_count=comparison.row_count,
                valid_record_count=0,
                invalid_record_count=0,
                schema_comparison=comparison,
                evolution_classification=classification,
                error_message=f"Schema evolution failed: {e}",
            )

        if evolution_result.status == "REJECTED":
            # Rejection stops processing before data validation or quarantine persistence
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.SCHEMA_REJECTED,
                source_file=file_path,
                row_count=comparison.row_count,
                valid_record_count=0,
                invalid_record_count=0,
                schema_comparison=comparison,
                evolution_classification=classification,
                schema_evolution=evolution_result,
                error_message=evolution_result.rejection_reason,
            )

        # Determine the applicable schema definition for data validation
        applicable_schema = evolution_result.evolved_schema_dict if evolution_result.is_evolved else base_schema_def

        # 5. Read Parquet table and execute Data Validation
        try:
            input_table = pq.read_table(file_path)
        except Exception as e:
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.FAILED,
                source_file=file_path,
                row_count=comparison.row_count,
                valid_record_count=0,
                invalid_record_count=0,
                schema_comparison=comparison,
                evolution_classification=classification,
                schema_evolution=evolution_result,
                error_message=f"Failed to read Parquet data table: {e}",
            )

        source_filename = os.path.basename(file_path)
        try:
            validation_res = self.validator.validate_orders_table(
                table=input_table,
                applicable_schema=applicable_schema,
                source_file=source_filename,
            )
        except Exception as e:
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.FAILED,
                source_file=file_path,
                row_count=len(input_table),
                valid_record_count=0,
                invalid_record_count=0,
                schema_comparison=comparison,
                evolution_classification=classification,
                schema_evolution=evolution_result,
                error_message=f"Validation execution failed: {e}",
            )

        # 6. Quarantine Persistence & Routing
        quarantine_record = None
        try:
            quarantine_record = self.quarantine_manager.quarantine_validation_result(
                validation_result=validation_res,
                batch_id=batch_id,
                source_file=source_filename,
            )
        except Exception as e:
            # Condition 4: NEVER report success if quarantine persistence fails
            return BatchProcessingResult(
                batch_id=batch_id,
                table_name=table_name,
                status=BatchProcessingStatus.FAILED,
                source_file=file_path,
                row_count=validation_res.metrics.total_records,
                valid_record_count=validation_res.metrics.valid_records,
                invalid_record_count=validation_res.metrics.invalid_records,
                schema_comparison=comparison,
                evolution_classification=classification,
                schema_evolution=evolution_result,
                validation=validation_res,
                valid_table=None,  # Do not release valid records if quarantine failed
                error_message=f"Quarantine persistence failed: {e}",
            )

        # 7. Determine Final Pipeline Status
        total_records = validation_res.metrics.total_records
        valid_records = validation_res.metrics.valid_records
        invalid_records = validation_res.metrics.invalid_records

        if invalid_records == 0:
            # All records are valid (or batch is empty)
            final_status = BatchProcessingStatus.SUCCESS
        elif valid_records == 0:
            # All records are invalid
            final_status = BatchProcessingStatus.PARTIAL_SUCCESS
        else:
            # Mixed batch: some valid, some quarantined
            final_status = BatchProcessingStatus.PARTIAL_SUCCESS

        return BatchProcessingResult(
            batch_id=batch_id,
            table_name=table_name,
            status=final_status,
            source_file=file_path,
            row_count=total_records,
            valid_record_count=valid_records,
            invalid_record_count=invalid_records,
            schema_comparison=comparison,
            evolution_classification=classification,
            schema_evolution=evolution_result,
            validation=validation_res,
            quarantine=quarantine_record,
            valid_table=validation_res.valid_table,
            error_message=None,
        )
