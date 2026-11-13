"""
Storage Manager Module for SchemaGuard (Phase 5.0).

Handles persistence of validated business records (valid_table) into the
processed data storage area (data/processed):
- Appends technical audit columns (when enabled in config).
- Ensures atomic publication using staging and atomic no-overwrite rename.
- Prevents accidental overwrites of existing batch output.
- Employs SHA-256 Arrow IPC fingerprinting on canonical business data (independent of audit columns)
  to ensure idempotent handling of previously committed batches.
- Cleans up staging artifacts on error and guarantees no partial writes leak.
"""

import os
import json
import tempfile
from enum import Enum
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Set, Union
from dataclasses import dataclass
import yaml
import pyarrow as pa
import pyarrow.parquet as pq

from src.quarantine.quarantine_manager import QuarantineManager


class StorageStatus(str, Enum):
    COMMITTED = "COMMITTED"
    ALREADY_PROCESSED = "ALREADY_PROCESSED"
    SKIPPED = "SKIPPED"
    STORAGE_FAILED = "STORAGE_FAILED"


@dataclass
class StorageRecord:
    batch_id: str
    table_name: str
    record_count: int
    parquet_path: Optional[str]
    status: StorageStatus
    persisted_at: str
    fingerprint: Optional[str] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "table_name": self.table_name,
            "record_count": self.record_count,
            "parquet_path": self.parquet_path,
            "status": self.status.value,
            "persisted_at": self.persisted_at,
            "fingerprint": self.fingerprint,
            "error_message": self.error_message,
        }


SCHEMAGUARD_META_PREFIX = b"schemaguard_"
PARQUET_INTERNAL_KEYS = {b"ARROW:schema", b"pandas"}


class StorageManager:
    """
    Manages atomic persistence of valid PyArrow records into processed storage.
    """

    def __init__(
        self,
        base_processed_dir: Optional[str] = None,
        pipeline_config_path: str = "config/pipeline_config.yaml",
        pipeline_config: Optional[Dict[str, Any]] = None,
    ):
        self.pipeline_config_path = pipeline_config_path
        if pipeline_config is not None:
            self.pipeline_config = pipeline_config
        elif os.path.exists(pipeline_config_path):
            self.pipeline_config = self._load_yaml(pipeline_config_path)
        else:
            self.pipeline_config = {}

        if base_processed_dir is None:
            storage_cfg = self.pipeline_config.get("storage", {})
            base_processed_dir = storage_cfg.get("processed_dir", "data/processed")
        self.base_processed_dir = os.path.abspath(base_processed_dir)

        # Audit metadata settings
        audit_cfg = self.pipeline_config.get("audit_metadata", {})
        self.add_technical_columns = audit_cfg.get("add_technical_columns", True)
        self.audit_columns_cfg = audit_cfg.get("columns", {
            "source_file": "source_file",
            "run_id": "run_id",
            "ingested_at": "ingested_at",
        })

        # Validate duplicate audit column names in configuration
        configured_col_names = list(self.audit_columns_cfg.values())
        if len(configured_col_names) != len(set(configured_col_names)):
            raise ValueError(f"Duplicate audit column names found in configuration: {configured_col_names}")

    @staticmethod
    def _load_yaml(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, dict) else {}

    def get_processed_table_dir(self, table_name: str) -> str:
        return os.path.join(self.base_processed_dir, table_name)

    def get_batch_parquet_path(self, table_name: str, batch_id: str) -> str:
        return os.path.join(self.get_processed_table_dir(table_name), f"{batch_id}.parquet")

    @staticmethod
    def canonicalize_table_for_fingerprint(table: pa.Table, audit_column_names: Set[str]) -> pa.Table:
        """
        Produces a canonical business table for SHA-256 fingerprinting:
        1. Selects only business data columns (excludes technical audit columns).
        2. Strips SchemaGuard metadata (schemaguard_*) and Parquet internal serialization keys.
        3. Preserves legitimate business schema metadata.
        """
        business_cols = [f.name for f in table.schema if f.name not in audit_column_names]
        cleaned_table = table.select(business_cols)

        current_meta = cleaned_table.schema.metadata
        if current_meta:
            cleaned_meta = {
                k: v for k, v in current_meta.items()
                if not k.startswith(SCHEMAGUARD_META_PREFIX) and k not in PARQUET_INTERNAL_KEYS
            }
            cleaned_table = cleaned_table.replace_schema_metadata(cleaned_meta if cleaned_meta else None)
        else:
            cleaned_table = cleaned_table.replace_schema_metadata(None)

        return cleaned_table

    def _verify_existing_file(
        self,
        target_path: str,
        incoming_batch_id: str,
        incoming_fingerprint: str,
        incoming_row_count: int,
    ) -> bool:
        """
        Verifies existing Parquet file integrity, metadata, and data fingerprint:
        - Reads existing table.
        - Checks embedded metadata (schemaguard_content_fingerprint, batch_id, row_count).
        - Reconstructs canonical business table and recomputes fingerprint.
        - Compares recomputed fingerprint with embedded fingerprint and incoming fingerprint.
        Returns True if identical (ALREADY_PROCESSED), raises ValueError if conflicting or corrupted.
        """
        try:
            existing_table = pq.read_table(target_path)
        except Exception as e:
            raise IOError(f"Corrupted Parquet file at destination '{target_path}': {e}") from e

        meta = existing_table.schema.metadata or {}
        embedded_fp = meta.get(b"schemaguard_content_fingerprint", b"").decode("utf-8")
        embedded_batch_id = meta.get(b"schemaguard_batch_id", b"").decode("utf-8")
        raw_row_count = meta.get(b"schemaguard_row_count", b"-1").decode("utf-8")

        try:
            embedded_row_count = int(raw_row_count)
        except ValueError:
            embedded_row_count = -1

        if not embedded_fp or not embedded_batch_id:
            raise ValueError(
                f"Existing Parquet file '{target_path}' lacks valid SchemaGuard embedded metadata."
            )

        if embedded_batch_id != incoming_batch_id:
            raise ValueError(
                f"Batch ID mismatch in existing Parquet file '{target_path}': "
                f"embedded='{embedded_batch_id}', incoming='{incoming_batch_id}'."
            )

        if embedded_row_count != len(existing_table):
            raise ValueError(
                f"Row count mismatch in existing Parquet file '{target_path}': "
                f"embedded={embedded_row_count}, actual={len(existing_table)}."
            )

        # Reconstruct canonical business data
        # Inspect embedded audit columns or detect them from existing table schema for drift compatibility
        raw_audit_cols = meta.get(b"schemaguard_audit_columns", None)
        if raw_audit_cols:
            try:
                audit_cols = set(json.loads(raw_audit_cols.decode("utf-8")))
            except Exception:
                audit_cols = set()
        else:
            # Backward compatibility: inspect which known configured audit columns exist in existing table
            configured_candidates = set(self.audit_columns_cfg.values()) | {"source_file", "run_id", "ingested_at"}
            audit_cols = {c for c in configured_candidates if c in existing_table.column_names}

        canonical_existing = self.canonicalize_table_for_fingerprint(existing_table, audit_cols)
        recomputed_existing_fp = QuarantineManager.compute_table_fingerprint(canonical_existing)

        if recomputed_existing_fp != embedded_fp:
            raise ValueError(
                f"Embedded fingerprint '{embedded_fp}' does not match recomputed data fingerprint '{recomputed_existing_fp}' "
                f"in file '{target_path}'."
            )

        # Compare with incoming batch
        if (
            recomputed_existing_fp == incoming_fingerprint
            and len(existing_table) == incoming_row_count
        ):
            return True
        else:
            raise ValueError(
                f"Conflicting reuse of batch_id '{incoming_batch_id}' in processed storage. "
                f"Existing: count={len(existing_table)}, fingerprint='{recomputed_existing_fp}'. "
                f"Incoming: count={incoming_row_count}, fingerprint='{incoming_fingerprint}'."
            )

    def persist_valid_records(
        self,
        valid_table: Optional[pa.Table],
        batch_id: str,
        table_name: str = "orders",
        run_id: str = "unknown_run",
        source_file: str = "unknown.parquet",
        allow_reprocess: bool = False,
    ) -> StorageRecord:
        """
        Persists valid_table to processed storage atomically without overwriting:
        1. If valid_table is None or has 0 rows, returns SKIPPED without creating files.
        2. Computes content fingerprint on business columns.
        3. Checks if destination Parquet exists. If so, validates existing file and returns ALREADY_PROCESSED.
        4. If add_technical_columns is True, appends audit columns.
        5. Stages write in .tmp_<batch_id>_<uuid>.parquet, flushes & fsyncs.
        6. Publishes atomically using atomic_publish_no_overwrite (prevents overwrites).
        7. Cleans up staging file on any failure.
        """
        now_utc = datetime.now(timezone.utc).isoformat()
        row_count = len(valid_table) if valid_table is not None else 0

        # Rule 3: Zero valid rows returns SKIPPED (not a committed Parquet output)
        if valid_table is None or row_count == 0:
            return StorageRecord(
                batch_id=batch_id,
                table_name=table_name,
                record_count=0,
                parquet_path=None,
                status=StorageStatus.SKIPPED,
                persisted_at=now_utc,
                fingerprint=None,
            )

        try:
            target_dir = self.get_processed_table_dir(table_name)
            os.makedirs(target_dir, exist_ok=True)
            target_parquet_file = self.get_batch_parquet_path(table_name, batch_id)

            # Determine configured audit column names
            audit_cols = set(self.audit_columns_cfg.values()) if self.add_technical_columns else set()

            # Rule 3 in Issue 4: If business table already contains configured audit column name, fail storage
            for col_name in audit_cols:
                if col_name in valid_table.column_names:
                    return StorageRecord(
                        batch_id=batch_id,
                        table_name=table_name,
                        record_count=row_count,
                        parquet_path=None,
                        status=StorageStatus.STORAGE_FAILED,
                        persisted_at=now_utc,
                        fingerprint=None,
                        error_message=(
                            f"Collision detected: Business table already contains audit column '{col_name}'. "
                            "Silently overwriting business data is strictly prohibited."
                        ),
                    )

            # Compute fingerprint on business data before adding audit columns
            canonical_raw = self.canonicalize_table_for_fingerprint(valid_table, audit_cols)
            content_fingerprint = QuarantineManager.compute_table_fingerprint(canonical_raw)

            # Check existing destination
            if os.path.exists(target_parquet_file):
                try:
                    self._verify_existing_file(
                        target_parquet_file,
                        incoming_batch_id=batch_id,
                        incoming_fingerprint=content_fingerprint,
                        incoming_row_count=row_count,
                    )
                    return StorageRecord(
                        batch_id=batch_id,
                        table_name=table_name,
                        record_count=row_count,
                        parquet_path=target_parquet_file,
                        status=StorageStatus.ALREADY_PROCESSED,
                        persisted_at=now_utc,
                        fingerprint=content_fingerprint,
                    )
                except Exception as e:
                    if not allow_reprocess:
                        return StorageRecord(
                            batch_id=batch_id,
                            table_name=table_name,
                            record_count=row_count,
                            parquet_path=target_parquet_file,
                            status=StorageStatus.STORAGE_FAILED,
                            persisted_at=now_utc,
                            fingerprint=content_fingerprint,
                            error_message=str(e),
                        )

            # Prepare table with metadata and audit columns
            table_to_write = valid_table

            # Embed SchemaGuard metadata including audit column list for drift compatibility
            meta_to_embed = {
                b"schemaguard_content_fingerprint": content_fingerprint.encode("utf-8"),
                b"schemaguard_batch_id": batch_id.encode("utf-8"),
                b"schemaguard_row_count": str(row_count).encode("utf-8"),
                b"schemaguard_audit_columns": json.dumps(sorted(list(audit_cols))).encode("utf-8"),
            }
            existing_meta = table_to_write.schema.metadata or {}
            combined_meta = {**existing_meta, **meta_to_embed}
            table_to_write = table_to_write.replace_schema_metadata(combined_meta)

            # Append technical audit columns if enabled
            if self.add_technical_columns:
                source_file_col = self.audit_columns_cfg.get("source_file", "source_file")
                run_id_col = self.audit_columns_cfg.get("run_id", "run_id")
                ingested_at_col = self.audit_columns_cfg.get("ingested_at", "ingested_at")

                now_dt = datetime.now(timezone.utc)
                source_file_arr = pa.array([source_file] * row_count, type=pa.string())
                run_id_arr = pa.array([run_id] * row_count, type=pa.string())
                ingested_at_arr = pa.array([now_dt] * row_count, type=pa.timestamp("us", tz="UTC"))

                table_to_write = table_to_write.append_column(source_file_col, source_file_arr)
                table_to_write = table_to_write.append_column(run_id_col, run_id_arr)
                table_to_write = table_to_write.append_column(ingested_at_col, ingested_at_arr)

            # Staging file on same filesystem
            fd, staging_file = tempfile.mkstemp(prefix=f".tmp_{batch_id}_", suffix=".parquet", dir=target_dir)
            os.close(fd)

            try:
                # Write parquet table
                pq.write_table(table_to_write, staging_file, compression="SNAPPY")

                # Flush and sync staging file descriptor
                with open(staging_file, "a+b") as f:
                    f.flush()
                    os.fsync(f.fileno())

                # Verification before publication
                verify_file = pq.ParquetFile(staging_file)
                staged_rows = verify_file.metadata.num_rows
                verify_file.close()
                if staged_rows != row_count:
                    raise IOError(f"Staged file row count mismatch: expected {row_count}, got {staged_rows}")

                # Atomic publication (or safe replacement on reprocess)
                if allow_reprocess:
                    os.replace(staging_file, target_parquet_file)
                else:
                    self._atomic_publish_no_overwrite(staging_file, target_parquet_file)

            except FileExistsError:
                # Destination was created concurrently
                if os.path.exists(staging_file):
                    os.unlink(staging_file)
                # Re-verify against the now-existing file
                try:
                    self._verify_existing_file(
                        target_parquet_file,
                        incoming_batch_id=batch_id,
                        incoming_fingerprint=content_fingerprint,
                        incoming_row_count=row_count,
                    )
                    return StorageRecord(
                        batch_id=batch_id,
                        table_name=table_name,
                        record_count=row_count,
                        parquet_path=target_parquet_file,
                        status=StorageStatus.ALREADY_PROCESSED,
                        persisted_at=now_utc,
                        fingerprint=content_fingerprint,
                    )
                except Exception as e:
                    if not allow_reprocess:
                        return StorageRecord(
                            batch_id=batch_id,
                            table_name=table_name,
                            record_count=row_count,
                            parquet_path=target_parquet_file,
                            status=StorageStatus.STORAGE_FAILED,
                            persisted_at=now_utc,
                            fingerprint=content_fingerprint,
                            error_message=str(e),
                        )
            except Exception as e:
                # Cleanup staging file on failure
                if os.path.exists(staging_file):
                    try:
                        os.unlink(staging_file)
                    except Exception:
                        pass
                return StorageRecord(
                    batch_id=batch_id,
                    table_name=table_name,
                    record_count=row_count,
                    parquet_path=None,
                    status=StorageStatus.STORAGE_FAILED,
                    persisted_at=now_utc,
                    fingerprint=content_fingerprint,
                    error_message=f"Storage persistence failed: {e}",
                )

            return StorageRecord(
                batch_id=batch_id,
                table_name=table_name,
                record_count=row_count,
                parquet_path=target_parquet_file,
                status=StorageStatus.COMMITTED,
                persisted_at=now_utc,
                fingerprint=content_fingerprint,
            )
        except Exception as e:
            return StorageRecord(
                batch_id=batch_id,
                table_name=table_name,
                record_count=row_count,
                parquet_path=None,
                status=StorageStatus.STORAGE_FAILED,
                persisted_at=now_utc,
                fingerprint=content_fingerprint if "content_fingerprint" in locals() else None,
                error_message=f"Storage error: {e}",
            )

    @staticmethod
    def _atomic_publish_no_overwrite(staging_file: str, destination_file: str) -> None:
        """
        Cross-platform atomic rename that fails if destination already exists.
        POSIX: uses os.link + os.unlink
        Windows: uses os.rename (which fails with FileExistsError if dst exists)
        """
        if os.name == "posix":
            os.link(staging_file, destination_file)
            os.unlink(staging_file)
        else:
            # Windows: os.rename fails with FileExistsError if destination already exists
            os.rename(staging_file, destination_file)
