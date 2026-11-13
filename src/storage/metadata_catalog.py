"""
Metadata Catalog Module for SchemaGuard (Phase 5.0).

Maintains pipeline execution metadata, batch lineage, schema evolutions,
and data quality validation metrics using DuckDB.
"""

import os
import json
from enum import Enum
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import duckdb
import yaml

from src.ingestion.batch_processor import BatchProcessingResult


class CatalogStatus(str, Enum):
    LOGGED = "LOGGED"
    CATALOG_FAILED = "CATALOG_FAILED"


@dataclass
class CatalogRecord:
    run_id: str
    batch_id: Optional[str]
    status: CatalogStatus
    error_message: Optional[str] = None


class MetadataCatalog:
    """
    Manages operational lineage, batch execution history, schema evolution auditing,
    and validation metrics recording in DuckDB.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        pipeline_config_path: str = "config/pipeline_config.yaml",
    ):
        self.pipeline_config_path = pipeline_config_path
        self.pipeline_config = self._load_yaml(pipeline_config_path) if os.path.exists(pipeline_config_path) else {}

        if db_path is None:
            storage_cfg = self.pipeline_config.get("storage", {})
            db_path = storage_cfg.get("metadata_db_path", "data/metadata/schemaguard_metadata.duckdb")
        self.db_path = db_path
        self._is_memory = db_path == ":memory:"
        self._memory_conn = None

        if not self._is_memory:
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        else:
            self._memory_conn = duckdb.connect(":memory:")

        self._init_db()

    @staticmethod
    def _load_yaml(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, dict) else {}

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        if self._is_memory and self._memory_conn:
            return self._memory_conn.cursor()
        return duckdb.connect(self.db_path)

    def close(self) -> None:
        """Closes the underlying database connection if held open."""
        if self._memory_conn:
            try:
                self._memory_conn.close()
            except Exception:
                pass
            self._memory_conn = None

    def _init_db(self) -> None:
        """
        Initializes the four approved metadata tables if they do not exist:
        1. pipeline_runs
        2. batch_executions
        3. schema_evolutions
        4. validation_metrics
        """
        conn = self._get_connection()
        try:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                total_batches INTEGER,
                successful_batches INTEGER,
                partial_batches INTEGER,
                rejected_batches INTEGER,
                failed_batches INTEGER,
                total_records BIGINT,
                valid_records BIGINT,
                invalid_records BIGINT,
                error_message VARCHAR
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS batch_executions (
                run_id VARCHAR,
                batch_id VARCHAR,
                table_name VARCHAR,
                source_file VARCHAR,
                status VARCHAR,
                row_count INTEGER,
                valid_record_count INTEGER,
                invalid_record_count INTEGER,
                processed_parquet_path VARCHAR,
                quarantine_parquet_path VARCHAR,
                quarantine_status VARCHAR,
                error_message VARCHAR,
                PRIMARY KEY (run_id, batch_id)
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_evolutions (
                run_id VARCHAR,
                batch_id VARCHAR,
                table_name VARCHAR,
                is_evolved BOOLEAN,
                evolution_status VARCHAR,
                applied_changes_count INTEGER,
                applied_changes_json VARCHAR,
                rejection_reason VARCHAR,
                PRIMARY KEY (run_id, batch_id)
            );
            """)

            conn.execute("""
            CREATE TABLE IF NOT EXISTS validation_metrics (
                run_id VARCHAR,
                batch_id VARCHAR,
                table_name VARCHAR,
                total_records INTEGER,
                valid_records INTEGER,
                invalid_records INTEGER,
                null_violations INTEGER,
                duplicate_violations INTEGER,
                foreign_key_violations INTEGER,
                range_violations INTEGER,
                malformed_violations INTEGER,
                PRIMARY KEY (run_id, batch_id)
            );
            """)
        finally:
            conn.close()

    def log_batch_execution(
        self,
        run_id: str,
        batch_result: BatchProcessingResult,
        processed_parquet_path: Optional[str] = None,
    ) -> CatalogRecord:
        """
        Records individual batch execution outcome, schema evolution, and validation metrics.
        Uses explicit transaction block with idempotent delete-before-insert semantics.
        All three table modifications commit atomically or roll back on failure.
        """
        conn = None
        in_tx = False
        try:
            conn = self._get_connection()
            conn.begin()
            in_tx = True

            batch_id = batch_result.batch_id
            table_name = batch_result.table_name
            source_file = batch_result.source_file
            status_val = batch_result.status.value
            row_count = batch_result.row_count
            valid_count = batch_result.valid_record_count
            invalid_count = batch_result.invalid_record_count
            error_msg = batch_result.error_message

            # Extract quarantine fields
            quarantine_path = None
            quarantine_status = None
            if batch_result.quarantine:
                quarantine_path = batch_result.quarantine.parquet_path
                quarantine_status = batch_result.quarantine.status

            # 1. Record batch_executions (Delete then insert for idempotency)
            conn.execute(
                "DELETE FROM batch_executions WHERE run_id = ? AND batch_id = ?",
                [run_id, batch_id],
            )
            conn.execute(
                """
                INSERT INTO batch_executions (
                    run_id, batch_id, table_name, source_file, status,
                    row_count, valid_record_count, invalid_record_count,
                    processed_parquet_path, quarantine_parquet_path,
                    quarantine_status, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id, batch_id, table_name, source_file, status_val,
                    row_count, valid_count, invalid_count,
                    processed_parquet_path, quarantine_path,
                    quarantine_status, error_msg,
                ],
            )

            # 2. Record schema_evolutions
            is_evolved = False
            evolution_status = "NONE"
            applied_changes_count = 0
            applied_changes_json = None
            rejection_reason = None

            if batch_result.schema_evolution:
                se = batch_result.schema_evolution
                is_evolved = se.is_evolved
                evolution_status = se.status
                applied_changes_count = len(se.applied_changes)
                applied_changes_json = json.dumps([
                    {"category": c.category, "column": c.column, "description": c.description}
                    for c in se.applied_changes
                ])
                rejection_reason = se.rejection_reason
            elif batch_result.evolution_classification:
                evolution_status = batch_result.evolution_classification.overall_status.value
                rejection_reason = "; ".join(batch_result.evolution_classification.reasons)

            conn.execute(
                "DELETE FROM schema_evolutions WHERE run_id = ? AND batch_id = ?",
                [run_id, batch_id],
            )
            conn.execute(
                """
                INSERT INTO schema_evolutions (
                    run_id, batch_id, table_name, is_evolved, evolution_status,
                    applied_changes_count, applied_changes_json, rejection_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id, batch_id, table_name, is_evolved, evolution_status,
                    applied_changes_count, applied_changes_json, rejection_reason,
                ],
            )

            # 3. Record validation_metrics
            total_rec = row_count
            valid_rec = valid_count
            invalid_rec = invalid_count
            null_viol = 0
            dup_viol = 0
            fk_viol = 0
            range_viol = 0
            malformed_viol = 0

            if batch_result.validation and batch_result.validation.metrics:
                m = batch_result.validation.metrics
                total_rec = m.total_records
                valid_rec = m.valid_records
                invalid_rec = m.invalid_records
                null_viol = m.null_violations
                dup_viol = m.duplicate_violations
                fk_viol = m.foreign_key_violations
                range_viol = m.range_violations
                malformed_viol = m.malformed_violations

            conn.execute(
                "DELETE FROM validation_metrics WHERE run_id = ? AND batch_id = ?",
                [run_id, batch_id],
            )
            conn.execute(
                """
                INSERT INTO validation_metrics (
                    run_id, batch_id, table_name, total_records, valid_records,
                    invalid_records, null_violations, duplicate_violations,
                    foreign_key_violations, range_violations, malformed_violations
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id, batch_id, table_name, total_rec, valid_rec,
                    invalid_rec, null_viol, dup_viol,
                    fk_viol, range_viol, malformed_viol,
                ],
            )

            conn.commit()
            in_tx = False

            return CatalogRecord(
                run_id=run_id,
                batch_id=batch_id,
                status=CatalogStatus.LOGGED,
            )
        except Exception as e:
            if conn and in_tx:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return CatalogRecord(
                run_id=run_id,
                batch_id=batch_result.batch_id if batch_result else None,
                status=CatalogStatus.CATALOG_FAILED,
                error_message=f"Failed to log batch execution: {e}",
            )
        finally:
            if conn:
                conn.close()

    def log_pipeline_run(
        self,
        run_id: str,
        started_at: str,
        completed_at: str,
        status: str,
        total_batches: int,
        successful_batches: int,
        partial_batches: int,
        rejected_batches: int,
        failed_batches: int,
        total_records: int,
        total_valid_records: int,
        total_quarantined_records: int,
        error_message: Optional[str] = None,
    ) -> CatalogRecord:
        """
        Records the aggregated pipeline run outcome in pipeline_runs table using an explicit transaction.
        """
        conn = None
        in_tx = False
        try:
            conn = self._get_connection()
            conn.begin()
            in_tx = True

            conn.execute(
                "DELETE FROM pipeline_runs WHERE run_id = ?",
                [run_id],
            )
            conn.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, started_at, completed_at, status, total_batches,
                    successful_batches, partial_batches, rejected_batches, failed_batches,
                    total_records, valid_records, invalid_records, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id, started_at, completed_at, status, total_batches,
                    successful_batches, partial_batches, rejected_batches, failed_batches,
                    total_records, total_valid_records, total_quarantined_records, error_message,
                ],
            )

            conn.commit()
            in_tx = False

            return CatalogRecord(
                run_id=run_id,
                batch_id=None,
                status=CatalogStatus.LOGGED,
            )
        except Exception as e:
            if conn and in_tx:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return CatalogRecord(
                run_id=run_id,
                batch_id=None,
                status=CatalogStatus.CATALOG_FAILED,
                error_message=f"Failed to log pipeline run: {e}",
            )
        finally:
            if conn:
                conn.close()

    def get_pipeline_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Queries and returns a pipeline run summary by run_id."""
        conn = self._get_connection()
        try:
            res = conn.execute("SELECT * FROM pipeline_runs WHERE run_id = ?", [run_id]).fetchone()
            if not res:
                return None
            cols = [desc[0] for desc in conn.description]
            return dict(zip(cols, res))
        finally:
            conn.close()

    def get_batch_executions(self, run_id: str) -> List[Dict[str, Any]]:
        """Queries all batch executions logged under a run_id."""
        conn = self._get_connection()
        try:
            res = conn.execute("SELECT * FROM batch_executions WHERE run_id = ?", [run_id]).fetchall()
            cols = [desc[0] for desc in conn.description]
            return [dict(zip(cols, row)) for row in res]
        finally:
            conn.close()

    def get_schema_evolutions(self, batch_id: Optional[str] = None, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Queries schema evolution records by batch_id or run_id."""
        conn = self._get_connection()
        try:
            query = "SELECT * FROM schema_evolutions"
            params = []
            conditions = []
            if run_id:
                conditions.append("run_id = ?")
                params.append(run_id)
            if batch_id:
                conditions.append("batch_id = ?")
                params.append(batch_id)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            res = conn.execute(query, params).fetchall()
            cols = [desc[0] for desc in conn.description]
            return [dict(zip(cols, row)) for row in res]
        finally:
            conn.close()

    def get_validation_metrics(self, batch_id: Optional[str] = None, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Queries validation metrics by batch_id or run_id."""
        conn = self._get_connection()
        try:
            query = "SELECT * FROM validation_metrics"
            params = []
            conditions = []
            if run_id:
                conditions.append("run_id = ?")
                params.append(run_id)
            if batch_id:
                conditions.append("batch_id = ?")
                params.append(batch_id)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            res = conn.execute(query, params).fetchall()
            cols = [desc[0] for desc in conn.description]
            return [dict(zip(cols, row)) for row in res]
        finally:
            conn.close()

    def get_pipeline_runs(self, limit: int = 50, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Queries historical pipeline runs ordered by started_at DESC."""
        conn = self._get_connection()
        try:
            query = "SELECT * FROM pipeline_runs"
            params: List[Any] = []
            if status:
                query += " WHERE status = ?"
                params.append(status)
            query += " ORDER BY started_at DESC LIMIT ?"
            params.append(limit)
            res = conn.execute(query, params).fetchall()
            cols = [desc[0] for desc in conn.description]
            return [dict(zip(cols, row)) for row in res]
        finally:
            conn.close()

    def get_latest_pipeline_run(self) -> Optional[Dict[str, Any]]:
        """Queries the most recent pipeline run."""
        runs = self.get_pipeline_runs(limit=1)
        return runs[0] if runs else None

    def get_batch_history(self, batch_id: str, table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Queries execution history for a given batch_id across all pipeline runs."""
        conn = self._get_connection()
        try:
            query = "SELECT * FROM batch_executions WHERE batch_id = ?"
            params: List[Any] = [batch_id]
            if table_name:
                query += " AND table_name = ?"
                params.append(table_name)
            query += " ORDER BY run_id DESC"
            res = conn.execute(query, params).fetchall()
            cols = [desc[0] for desc in conn.description]
            return [dict(zip(cols, row)) for row in res]
        finally:
            conn.close()

    def get_execution_summary(self) -> Dict[str, Any]:
        """
        Aggregates lifetime execution history across all tracked pipeline runs.
        """
        conn = self._get_connection()
        try:
            res = conn.execute("""
                SELECT
                    COUNT(*) AS total_runs,
                    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS successful_runs,
                    COUNT(CASE WHEN status = 'PARTIAL_SUCCESS' THEN 1 END) AS partial_runs,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_runs,
                    COALESCE(SUM(total_batches), 0) AS total_batches_processed,
                    COALESCE(SUM(total_records), 0) AS total_input_records,
                    COALESCE(SUM(valid_records), 0) AS total_valid_records,
                    COALESCE(SUM(invalid_records), 0) AS total_quarantined_records
                FROM pipeline_runs
            """).fetchone()
            if not res:
                return {
                    "total_runs": 0, "successful_runs": 0, "partial_runs": 0, "failed_runs": 0,
                    "total_batches_processed": 0, "total_input_records": 0,
                    "total_valid_records": 0, "total_quarantined_records": 0,
                }
            cols = [desc[0] for desc in conn.description]
            return dict(zip(cols, res))
        finally:
            conn.close()

    def get_failed_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Queries all failed or partial pipeline runs with their error messages."""
        conn = self._get_connection()
        try:
            res = conn.execute("""
                SELECT * FROM pipeline_runs
                WHERE status IN ('FAILED', 'PARTIAL_SUCCESS')
                ORDER BY started_at DESC
                LIMIT ?
            """, [limit]).fetchall()
            cols = [desc[0] for desc in conn.description]
            return [dict(zip(cols, row)) for row in res]
        finally:
            conn.close()

    def list_tables(self) -> List[str]:
        """Lists user-defined tables in DuckDB."""
        conn = self._get_connection()
        try:
            res = conn.execute("SHOW TABLES").fetchall()
            return [row[0] for row in res]
        finally:
            conn.close()
