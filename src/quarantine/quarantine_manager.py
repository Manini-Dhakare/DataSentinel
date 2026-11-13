"""
Quarantine Manager Module for SchemaGuard (Phase 4.2).

Persists invalid records and failure metadata:
- Extracts invalid_table from Phase 4.1 ValidationResult
- Persists invalid records as Parquet preserving original PyArrow schema, types, and values
- Generates JSON metadata sidecars with comprehensive failure traceability
- Enforces atomic writes via temporary staging and safe rename
- Supports deterministic batch identification and idempotent retries
- Detects conflicting reuse of batch IDs
- Rejects path traversal and unsafe path components
- Safely handles batches with zero invalid records without producing orphan artifacts
- Safely serializes Decimals, datetimes, and nulls
"""

import os
import re
import json
import shutil
import tempfile
import hashlib
import io
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from src.validation.validator import ValidationResult


class QuarantineJSONEncoder(json.JSONEncoder):
    """
    JSON encoder that handles Decimals, datetimes, and PyArrow/Pydantic types safely.
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        return super().default(obj)


@dataclass
class QuarantineRecord:
    batch_id: str
    table_name: str
    record_count: int
    parquet_path: Optional[str]
    metadata_path: Optional[str]
    status: str  # "QUARANTINED", "SKIPPED_NO_INVALID", "ALREADY_EXISTS"
    quarantined_at: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "table_name": self.table_name,
            "record_count": self.record_count,
            "parquet_path": self.parquet_path,
            "metadata_path": self.metadata_path,
            "status": self.status,
            "quarantined_at": self.quarantined_at,
        }


class QuarantineManager:
    """
    Manages atomic persistence and routing of invalid records into data/quarantine.
    """

    UNSAFE_PATH_PATTERN = re.compile(r"[/\\:\*\?\"<>\|\.\.]")

    def __init__(self, base_quarantine_dir: str = "data/quarantine"):
        self.base_quarantine_dir = os.path.abspath(base_quarantine_dir)

    @staticmethod
    def compute_table_fingerprint(table: pa.Table) -> str:
        """
        Computes a deterministic SHA-256 fingerprint of a PyArrow Table.

        Accounts for:
        - PyArrow schema (column names, types, nullability, and schema-level metadata)
        - Actual values, nulls, row ordering, and Decimal precision
        - Chunking uniformity (via combine_chunks())

        Uses PyArrow IPC streaming serialization to generate an exact canonical binary
        representation without converting Decimals or numbers to floating-point types.
        """
        if table is None:
            return hashlib.sha256(b"__NULL_TABLE__").hexdigest()

        # Ensure chunks are consolidated to make the binary stream invariant to memory layout
        unified_table = table.combine_chunks()
        sink = io.BytesIO()
        with ipc.new_stream(sink, unified_table.schema) as writer:
            writer.write_table(unified_table)
        
        return hashlib.sha256(sink.getvalue()).hexdigest()

    def _validate_path_component(self, name: str, param_name: str) -> None:
        """
        Guards against directory traversal and illegal path characters.
        """
        if not name or not isinstance(name, str) or not name.strip():
            raise ValueError(f"Invalid {param_name}: value must be a non-empty string")
        cleaned = name.strip()
        if ".." in cleaned or "/" in cleaned or "\\" in cleaned or self.UNSAFE_PATH_PATTERN.search(cleaned):
            raise ValueError(
                f"Unsafe path component detected in {param_name}: '{name}'. "
                "Path traversal sequences and directory separators are strictly prohibited."
            )

    def get_batch_dir(self, table_name: str, batch_id: str) -> str:
        """
        Returns the structured directory path for a quarantined table and batch:
        <base_quarantine_dir>/<table_name>/<batch_id>
        """
        self._validate_path_component(table_name, "table_name")
        self._validate_path_component(batch_id, "batch_id")
        return os.path.join(self.base_quarantine_dir, table_name, batch_id)

    def quarantine_validation_result(
        self,
        validation_result: ValidationResult,
        batch_id: str,
        source_file: str = "unknown.parquet",
    ) -> QuarantineRecord:
        """
        Extracts invalid_table and failures from ValidationResult and persists:
        1. <batch_dir>/invalid_records.parquet
        2. <batch_dir>/quarantine_metadata.json

        Behavior:
        - If validation_result has 0 invalid records, returns SKIPPED_NO_INVALID without creating files.
        - If batch_id already exists in quarantine:
          - If identical (same table and record count), behaves idempotently and returns ALREADY_EXISTS.
          - If conflicting (different record count or table), raises ValueError to prevent corrupt overwrites.
        - Writes atomically using temporary staging directory before moving to final destination.
        """
        table_name = validation_result.table_name or "orders"
        self._validate_path_component(table_name, "table_name")
        self._validate_path_component(batch_id, "batch_id")

        now_utc = datetime.now(timezone.utc).isoformat()
        invalid_table = validation_result.invalid_table
        invalid_count = len(invalid_table) if invalid_table is not None else 0

        # Case 1: Empty invalid records (batch was 100% valid or empty)
        if invalid_count == 0 or (invalid_table is None and not validation_result.failures):
            return QuarantineRecord(
                batch_id=batch_id,
                table_name=table_name,
                record_count=0,
                parquet_path=None,
                metadata_path=None,
                status="SKIPPED_NO_INVALID",
                quarantined_at=now_utc,
            )

        target_batch_dir = self.get_batch_dir(table_name, batch_id)
        target_parquet_file = os.path.join(target_batch_dir, "invalid_records.parquet")
        target_metadata_file = os.path.join(target_batch_dir, "quarantine_metadata.json")

        current_fingerprint = self.compute_table_fingerprint(invalid_table)

        # Case 2: Idempotency & Conflict detection for existing batch ID
        if os.path.exists(target_batch_dir):
            if os.path.exists(target_metadata_file):
                with open(target_metadata_file, "r", encoding="utf-8") as f:
                    existing_meta = json.load(f)

                existing_count = existing_meta.get("invalid_record_count")
                existing_table = existing_meta.get("table_name")
                existing_fingerprint = existing_meta.get("fingerprint")

                # If existing metadata lacks a valid fingerprint, fail safely instead of assuming equality
                if not existing_fingerprint or not isinstance(existing_fingerprint, str):
                    raise ValueError(
                        f"Existing quarantine metadata for batch_id '{batch_id}' lacks a valid SHA-256 fingerprint. "
                        "Cannot safely verify batch identity for idempotency."
                    )

                if (
                    existing_table == table_name
                    and existing_count == invalid_count
                    and existing_fingerprint == current_fingerprint
                ):
                    # Idempotent re-submission: identical table, count, and data fingerprint
                    return QuarantineRecord(
                        batch_id=batch_id,
                        table_name=table_name,
                        record_count=existing_count,
                        parquet_path=target_parquet_file,
                        metadata_path=target_metadata_file,
                        status="ALREADY_EXISTS",
                        quarantined_at=existing_meta.get("quarantined_at", now_utc),
                    )
                else:
                    raise ValueError(
                        f"Conflicting reuse of batch_id '{batch_id}' for table '{table_name}'. "
                        f"Existing quarantine record: table='{existing_table}', count={existing_count}, fingerprint='{existing_fingerprint}'. "
                        f"Incoming batch: table='{table_name}', count={invalid_count}, fingerprint='{current_fingerprint}'."
                    )
            else:
                raise ValueError(
                    f"Conflicting unmanaged directory exists at quarantine destination: {target_batch_dir}"
                )

        # Prepare metadata payload
        failure_details = []
        categories_set = set()
        columns_set = set()
        affected_rows_set = set()

        for failure in validation_result.failures:
            categories_set.add(failure.category)
            if failure.column:
                columns_set.add(failure.column)
            if failure.source_row_index >= 0:
                affected_rows_set.add(failure.source_row_index)

            failure_details.append({
                "source_row_index": failure.source_row_index,
                "record_identifier": failure.record_identifier,
                "category": failure.category,
                "column": failure.column,
                "reason": failure.reason,
                "raw_record": failure.raw_record,
            })

        metadata_payload = {
            "batch_id": batch_id,
            "table_name": table_name,
            "fingerprint": current_fingerprint,
            "source_file": source_file,
            "quarantined_at": now_utc,
            "total_batch_records": validation_result.metrics.total_records,
            "valid_record_count": validation_result.metrics.valid_records,
            "invalid_record_count": invalid_count,
            "metrics": validation_result.metrics.to_dict(),
            "failure_categories": sorted(list(categories_set)),
            "affected_columns": sorted(list(columns_set)),
            "affected_source_row_indexes": sorted(list(affected_rows_set)),
            "schema_errors": validation_result.schema_errors,
            "failures": failure_details,
        }

        # Case 3: Atomic write via staging directory
        staging_parent = os.path.dirname(target_batch_dir)
        os.makedirs(staging_parent, exist_ok=True)
        temp_staging_dir = tempfile.mkdtemp(prefix=f".tmp_quarantine_{batch_id}_", dir=staging_parent)

        try:
            temp_parquet_file = os.path.join(temp_staging_dir, "invalid_records.parquet")
            temp_metadata_file = os.path.join(temp_staging_dir, "quarantine_metadata.json")

            # Write invalid_table Parquet file preserving PyArrow types
            pq.write_table(invalid_table, temp_parquet_file)

            # Write JSON metadata sidecar
            with open(temp_metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata_payload, f, indent=2, cls=QuarantineJSONEncoder)

            # Atomic publication: rename staging directory to final target_batch_dir
            os.replace(temp_staging_dir, target_batch_dir)

        except Exception as e:
            # Clean up temporary staging artifacts on failure
            if os.path.exists(temp_staging_dir):
                shutil.rmtree(temp_staging_dir, ignore_errors=True)
            raise IOError(f"Failed to atomically persist quarantine batch '{batch_id}': {e}") from e

        return QuarantineRecord(
            batch_id=batch_id,
            table_name=table_name,
            record_count=invalid_count,
            parquet_path=target_parquet_file,
            metadata_path=target_metadata_file,
            status="QUARANTINED",
            quarantined_at=now_utc,
        )

    def load_quarantine_metadata(self, table_name: str, batch_id: str) -> Dict[str, Any]:
        """
        Reads and returns the JSON metadata sidecar for a quarantined batch.
        """
        batch_dir = self.get_batch_dir(table_name, batch_id)
        meta_file = os.path.join(batch_dir, "quarantine_metadata.json")
        if not os.path.exists(meta_file):
            raise FileNotFoundError(f"Quarantine metadata sidecar not found: {meta_file}")

        with open(meta_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_quarantine_table(self, table_name: str, batch_id: str) -> pa.Table:
        """
        Reads and returns the quarantined invalid_records.parquet table.
        """
        batch_dir = self.get_batch_dir(table_name, batch_id)
        parquet_file = os.path.join(batch_dir, "invalid_records.parquet")
        if not os.path.exists(parquet_file):
            raise FileNotFoundError(f"Quarantined Parquet file not found: {parquet_file}")

        return pq.read_table(parquet_file)
