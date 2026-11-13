"""
Idempotency and Input Fingerprinting Module for SchemaGuard (Phase 6.1).

Provides reliable idempotency detection, input fingerprinting, and legitimate
reprocessing support across multi-batch pipeline executions:
1. Computes deterministic cryptographic signatures on raw input files.
2. Identifies reliable idempotency keys: (table_name, batch_id, source_fingerprint).
3. Detects previously processed inputs to avoid redundant computation.
4. Distinguishes accidental duplicate processing from legitimate reprocessing when input data changes.
5. Prevents duplicate output artifacts during pipeline re-runs.
"""

import os
import hashlib
from enum import Enum
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
import duckdb

from src.storage.metadata_catalog import MetadataCatalog


class IdempotencyStatus(str, Enum):
    """Classification of an input batch against historical executions."""
    NEW = "NEW"                                      # Never seen before
    ALREADY_PROCESSED = "ALREADY_PROCESSED"          # Identical batch and fingerprint already durably committed
    CHANGED_INPUT = "CHANGED_INPUT"                  # Same batch_id exists, but source content changed (reprocess)


@dataclass
class IdempotencyRecord:
    """Historical idempotency state for an executed batch."""
    idempotency_key: str
    table_name: str
    batch_id: str
    source_file: str
    source_fingerprint: str
    run_id: str
    status: str
    processed_parquet_path: Optional[str]
    quarantine_parquet_path: Optional[str]
    record_count: int
    registered_at: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "idempotency_key": self.idempotency_key,
            "table_name": self.table_name,
            "batch_id": self.batch_id,
            "source_file": self.source_file,
            "source_fingerprint": self.source_fingerprint,
            "run_id": self.run_id,
            "status": self.status,
            "processed_parquet_path": self.processed_parquet_path,
            "quarantine_parquet_path": self.quarantine_parquet_path,
            "record_count": self.record_count,
            "registered_at": self.registered_at,
        }


class InputFingerprinter:
    """
    Computes deterministic SHA-256 signatures for input files before ingestion.
    """

    @staticmethod
    def compute_file_fingerprint(file_path: str, chunk_size: int = 65536) -> str:
        """
        Computes SHA-256 hash of raw file bytes in buffered chunks.
        Guarantees deterministic, content-addressable signature independent of OS.
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"Input file for fingerprinting not found: '{file_path}'")

        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def compute_idempotency_key(table_name: str, batch_id: str, source_fingerprint: str) -> str:
        """
        Constructs composite idempotency key: <table_name>:<batch_id>:<fingerprint>.
        """
        return f"{table_name.strip().lower()}:{batch_id.strip().lower()}:{source_fingerprint.strip().lower()}"


class IdempotencyManager:
    """
    Manages batch idempotency registry, detection of previously committed batches,
    and classification of legitimate reprocessing requests using DuckDB.
    """

    def __init__(self, metadata_catalog: MetadataCatalog):
        self.metadata_catalog = metadata_catalog
        self._init_registry()

    def _init_registry(self) -> None:
        """Initializes the idempotency_records table in the metadata catalog if not present."""
        conn = self.metadata_catalog._get_connection()
        try:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS idempotency_records (
                idempotency_key VARCHAR PRIMARY KEY,
                table_name VARCHAR,
                batch_id VARCHAR,
                source_file VARCHAR,
                source_fingerprint VARCHAR,
                run_id VARCHAR,
                status VARCHAR,
                processed_parquet_path VARCHAR,
                quarantine_parquet_path VARCHAR,
                record_count INTEGER,
                registered_at TIMESTAMP
            );
            """)
        finally:
            conn.close()

    def check_batch(
        self,
        file_path: str,
        batch_id: str,
        table_name: str = "orders",
    ) -> Tuple[IdempotencyStatus, Optional[IdempotencyRecord]]:
        """
        Evaluates an incoming batch file against historical idempotency records:
        1. Computes source file SHA-256 fingerprint.
        2. Queries registry for matching (table_name, batch_id, source_fingerprint).
        3. If matching record found AND output artifact exists on disk -> ALREADY_PROCESSED.
        4. If same batch_id exists with different source_fingerprint -> CHANGED_INPUT.
        5. If batch_id has never been seen -> NEW.
        """
        fingerprint = InputFingerprinter.compute_file_fingerprint(file_path)
        idemp_key = InputFingerprinter.compute_idempotency_key(table_name, batch_id, fingerprint)

        conn = self.metadata_catalog._get_connection()
        try:
            # Check exact match
            res = conn.execute(
                "SELECT * FROM idempotency_records WHERE idempotency_key = ?",
                [idemp_key],
            ).fetchone()

            if res:
                cols = [desc[0] for desc in conn.description]
                record_dict = dict(zip(cols, res))
                rec = IdempotencyRecord(
                    idempotency_key=record_dict["idempotency_key"],
                    table_name=record_dict["table_name"],
                    batch_id=record_dict["batch_id"],
                    source_file=record_dict["source_file"],
                    source_fingerprint=record_dict["source_fingerprint"],
                    run_id=record_dict["run_id"],
                    status=record_dict["status"],
                    processed_parquet_path=record_dict["processed_parquet_path"],
                    quarantine_parquet_path=record_dict["quarantine_parquet_path"],
                    record_count=record_dict["record_count"] or 0,
                    registered_at=str(record_dict["registered_at"]),
                )

                # Verify that referenced processed or quarantine output still physically exists
                proc_path = rec.processed_parquet_path
                quar_path = rec.quarantine_parquet_path
                has_valid_output = (proc_path and os.path.isfile(proc_path)) or (quar_path and os.path.isfile(quar_path))

                if has_valid_output:
                    return IdempotencyStatus.ALREADY_PROCESSED, rec
                else:
                    # Output was deleted or moved; allow reprocessing
                    return IdempotencyStatus.NEW, rec

            # Check if same batch_id was executed previously with different content
            res_batch = conn.execute(
                "SELECT * FROM idempotency_records WHERE table_name = ? AND batch_id = ? ORDER BY registered_at DESC",
                [table_name.strip().lower(), batch_id.strip().lower()],
            ).fetchone()

            if res_batch:
                cols = [desc[0] for desc in conn.description]
                record_dict = dict(zip(cols, res_batch))
                rec = IdempotencyRecord(
                    idempotency_key=record_dict["idempotency_key"],
                    table_name=record_dict["table_name"],
                    batch_id=record_dict["batch_id"],
                    source_file=record_dict["source_file"],
                    source_fingerprint=record_dict["source_fingerprint"],
                    run_id=record_dict["run_id"],
                    status=record_dict["status"],
                    processed_parquet_path=record_dict["processed_parquet_path"],
                    quarantine_parquet_path=record_dict["quarantine_parquet_path"],
                    record_count=record_dict["record_count"] or 0,
                    registered_at=str(record_dict["registered_at"]),
                )
                return IdempotencyStatus.CHANGED_INPUT, rec

            return IdempotencyStatus.NEW, None
        finally:
            conn.close()

    def register_execution(
        self,
        table_name: str,
        batch_id: str,
        source_file: str,
        source_fingerprint: str,
        run_id: str,
        status: str,
        processed_parquet_path: Optional[str] = None,
        quarantine_parquet_path: Optional[str] = None,
        record_count: int = 0,
    ) -> IdempotencyRecord:
        """
        Persists or updates an idempotency record in the catalog upon successful execution.
        Uses delete-before-insert semantics to ensure transaction idempotency.
        """
        now_utc = datetime.now(timezone.utc).isoformat()
        key = InputFingerprinter.compute_idempotency_key(table_name, batch_id, source_fingerprint)

        conn = self.metadata_catalog._get_connection()
        try:
            conn.begin()
            conn.execute(
                "DELETE FROM idempotency_records WHERE idempotency_key = ?",
                [key],
            )
            conn.execute(
                """
                INSERT INTO idempotency_records (
                    idempotency_key, table_name, batch_id, source_file,
                    source_fingerprint, run_id, status, processed_parquet_path,
                    quarantine_parquet_path, record_count, registered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    key,
                    table_name.strip().lower(),
                    batch_id.strip().lower(),
                    source_file,
                    source_fingerprint,
                    run_id,
                    status,
                    processed_parquet_path,
                    quarantine_parquet_path,
                    record_count,
                    now_utc,
                ],
            )
            conn.commit()

            return IdempotencyRecord(
                idempotency_key=key,
                table_name=table_name,
                batch_id=batch_id,
                source_file=source_file,
                source_fingerprint=source_fingerprint,
                run_id=run_id,
                status=status,
                processed_parquet_path=processed_parquet_path,
                quarantine_parquet_path=quarantine_parquet_path,
                record_count=record_count,
                registered_at=now_utc,
            )
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            conn.close()
