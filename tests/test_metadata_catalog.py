"""Tests for MetadataCatalog in Phase 5.0."""

from pathlib import Path
import pyarrow as pa
import pytest

from src.storage.metadata_catalog import MetadataCatalog, CatalogStatus
from src.ingestion.batch_processor import (
    BatchProcessingResult,
    BatchProcessingStatus,
    SchemaEvolutionResult,
    ValidationResult,
    ValidationMetrics,
)
from src.quarantine.quarantine_manager import QuarantineRecord


def test_metadata_catalog_initialization(tmp_path: Path):
    db_file = tmp_path / "metadata.duckdb"
    catalog = MetadataCatalog(db_path=str(db_file))
    try:
        assert db_file.exists()
        tables = catalog.list_tables()
        assert "pipeline_runs" in tables
        assert "batch_executions" in tables
        assert "schema_evolutions" in tables
        assert "validation_metrics" in tables
    finally:
        catalog.close()


def test_metadata_catalog_in_memory():
    catalog = MetadataCatalog(db_path=":memory:")
    try:
        tables = catalog.list_tables()
        assert len(tables) == 4
    finally:
        catalog.close()


def test_log_pipeline_run_lifecycle(tmp_path: Path):
    catalog = MetadataCatalog(db_path=str(tmp_path / "meta.duckdb"))
    try:
        rec = catalog.log_pipeline_run(
            run_id="run_001",
            started_at="2026-11-13T10:00:00Z",
            completed_at="2026-11-13T10:00:05Z",
            status="SUCCESS",
            total_batches=3,
            successful_batches=3,
            partial_batches=0,
            rejected_batches=0,
            failed_batches=0,
            total_records=100,
            total_valid_records=100,
            total_quarantined_records=0,
        )
        assert rec.status == CatalogStatus.LOGGED
        run_record = catalog.get_pipeline_run("run_001")
        assert run_record is not None
        assert run_record["status"] == "SUCCESS"
        assert run_record["total_batches"] == 3
        assert run_record["total_records"] == 100
    finally:
        catalog.close()


def test_log_batch_execution_and_query(tmp_path: Path):
    catalog = MetadataCatalog(db_path=str(tmp_path / "meta.duckdb"))
    try:
        batch_res = BatchProcessingResult(
            batch_id="batch_001",
            table_name="orders",
            status=BatchProcessingStatus.SUCCESS,
            source_file="batch_001.parquet",
            row_count=10,
            valid_record_count=10,
            invalid_record_count=0,
            schema_evolution=SchemaEvolutionResult(
                table_name="orders",
                is_evolved=False,
                status="NO_CHANGE",
                evolved_schema_dict={},
                pyarrow_schema=None,
                applied_changes=[],
            ),
            validation=ValidationResult(
                table_name="orders",
                is_valid_batch=True,
                metrics=ValidationMetrics(
                    total_records=10,
                    valid_records=10,
                    invalid_records=0,
                    null_violations=0,
                    duplicate_violations=0,
                    foreign_key_violations=0,
                    range_violations=0,
                    malformed_violations=0,
                ),
            ),
            quarantine=None,
        )

        rec = catalog.log_batch_execution(
            run_id="run_001",
            batch_result=batch_res,
            processed_parquet_path="data/processed/orders/batch_001.parquet",
        )
        assert rec.status == CatalogStatus.LOGGED

        batches = catalog.get_batch_executions("run_001")
        assert len(batches) == 1
        assert batches[0]["batch_id"] == "batch_001"
        assert batches[0]["processed_parquet_path"] == "data/processed/orders/batch_001.parquet"

        # Check schema evolutions and validation metrics
        evolutions = catalog.get_schema_evolutions(batch_id="batch_001")
        assert len(evolutions) == 1
        assert evolutions[0]["is_evolved"] is False

        metrics = catalog.get_validation_metrics(batch_id="batch_001")
        assert len(metrics) == 1
        assert metrics[0]["valid_records"] == 10

        # Idempotent re-log (delete-before-insert)
        rec2 = catalog.log_batch_execution(
            run_id="run_001",
            batch_result=batch_res,
            processed_parquet_path="data/processed/orders/batch_001.parquet",
        )
        assert rec2.status == CatalogStatus.LOGGED
        batches_after = catalog.get_batch_executions("run_001")
        assert len(batches_after) == 1
    finally:
        catalog.close()


def test_log_quarantined_batch_execution(tmp_path: Path):
    catalog = MetadataCatalog(db_path=str(tmp_path / "meta.duckdb"))
    try:
        batch_res = BatchProcessingResult(
            batch_id="batch_006",
            table_name="orders",
            status=BatchProcessingStatus.PARTIAL_SUCCESS,
            source_file="batch_006.parquet",
            row_count=10,
            valid_record_count=8,
            invalid_record_count=2,
            schema_evolution=None,
            validation=ValidationResult(
                table_name="orders",
                is_valid_batch=False,
                metrics=ValidationMetrics(
                    total_records=10,
                    valid_records=8,
                    invalid_records=2,
                    null_violations=0,
                    duplicate_violations=2,
                    foreign_key_violations=0,
                    range_violations=0,
                    malformed_violations=0,
                ),
            ),
            quarantine=QuarantineRecord(
                batch_id="batch_006",
                table_name="orders",
                record_count=2,
                parquet_path="data/quarantine/orders/batch_006.parquet",
                metadata_path="data/quarantine/orders/batch_006_metadata.json",
                status="QUARANTINED",
                quarantined_at="2026-11-13T10:00:00Z",
            ),
        )

        rec = catalog.log_batch_execution(
            run_id="run_002",
            batch_result=batch_res,
            processed_parquet_path="data/processed/orders/batch_006.parquet",
        )
        assert rec.status == CatalogStatus.LOGGED

        batches = catalog.get_batch_executions("run_002")
        assert len(batches) == 1
        assert batches[0]["quarantine_status"] == "QUARANTINED"
        assert batches[0]["quarantine_parquet_path"] == "data/quarantine/orders/batch_006.parquet"
    finally:
        catalog.close()


def _sample_batch_result(batch_id: str = "batch_001") -> BatchProcessingResult:
    return BatchProcessingResult(
        batch_id=batch_id,
        table_name="orders",
        status=BatchProcessingStatus.SUCCESS,
        source_file=f"{batch_id}.parquet",
        row_count=10,
        valid_record_count=10,
        invalid_record_count=0,
        schema_evolution=SchemaEvolutionResult(
            table_name="orders",
            is_evolved=False,
            status="NO_CHANGE",
            evolved_schema_dict={},
            pyarrow_schema=None,
            applied_changes=[],
        ),
        validation=ValidationResult(
            table_name="orders",
            is_valid_batch=True,
            metrics=ValidationMetrics(
                total_records=10,
                valid_records=10,
                invalid_records=0,
                null_violations=0,
                duplicate_violations=0,
                foreign_key_violations=0,
                range_violations=0,
                malformed_violations=0,
            ),
        ),
        quarantine=None,
    )


# --- Issue-01 targeted tests ---

def test_transaction_rollback_preserves_previous_records_file(tmp_path: Path):
    catalog = MetadataCatalog(db_path=str(tmp_path / "meta_tx.duckdb"))
    try:
        # 1. Log initial state
        batch_res = _sample_batch_result("batch_tx_1")
        rec1 = catalog.log_batch_execution("run_tx", batch_res, "orig_path.parquet")
        assert rec1.status == CatalogStatus.LOGGED

        batches_init = catalog.get_batch_executions("run_tx")
        assert len(batches_init) == 1
        assert batches_init[0]["processed_parquet_path"] == "orig_path.parquet"

        # 2. Simulate failure during transaction after batch_executions table write
        real_get_conn = catalog._get_connection
        class FaultyConnWrapper:
            def __init__(self, real):
                self._real = real
            def begin(self):
                return self._real.begin()
            def commit(self):
                return self._real.commit()
            def rollback(self):
                return self._real.rollback()
            def close(self):
                return self._real.close()
            def execute(self, sql, params=None):
                if "INSERT INTO schema_evolutions" in sql:
                    raise RuntimeError("Simulated schema_evolutions insertion crash")
                return self._real.execute(sql, params) if params is not None else self._real.execute(sql)

        catalog._get_connection = lambda: FaultyConnWrapper(real_get_conn())

        rec2 = catalog.log_batch_execution("run_tx", batch_res, "corrupt_path.parquet")
        assert rec2.status == CatalogStatus.CATALOG_FAILED
        assert "Simulated schema_evolutions insertion crash" in str(rec2.error_message)

        # Restore connection
        catalog._get_connection = real_get_conn

        # 3. Assert rollback: original record is completely preserved
        batches_after = catalog.get_batch_executions("run_tx")
        assert len(batches_after) == 1
        assert batches_after[0]["processed_parquet_path"] == "orig_path.parquet"
    finally:
        catalog.close()


def test_transaction_rollback_preserves_previous_records_memory():
    catalog = MetadataCatalog(db_path=":memory:")
    try:
        batch_res = _sample_batch_result("batch_mem_1")
        rec1 = catalog.log_batch_execution("run_mem", batch_res, "orig_mem.parquet")
        assert rec1.status == CatalogStatus.LOGGED

        real_get_conn = catalog._get_connection
        class FaultyCursorWrapper:
            def __init__(self, real):
                self._real = real
            def begin(self):
                return self._real.begin()
            def commit(self):
                return self._real.commit()
            def rollback(self):
                return self._real.rollback()
            def close(self):
                return self._real.close()
            def execute(self, sql, params=None):
                if "INSERT INTO validation_metrics" in sql:
                    raise RuntimeError("Simulated validation_metrics insertion crash")
                return self._real.execute(sql, params) if params is not None else self._real.execute(sql)

        catalog._get_connection = lambda: FaultyCursorWrapper(real_get_conn())
        rec2 = catalog.log_batch_execution("run_mem", batch_res, "new_mem.parquet")
        assert rec2.status == CatalogStatus.CATALOG_FAILED

        catalog._get_connection = real_get_conn
        batches_after = catalog.get_batch_executions("run_mem")
        assert len(batches_after) == 1
        assert batches_after[0]["processed_parquet_path"] == "orig_mem.parquet"
    finally:
        catalog.close()


def test_idempotent_relogging_in_transaction(tmp_path: Path):
    catalog = MetadataCatalog(db_path=str(tmp_path / "meta_idem.duckdb"))
    try:
        batch_res = _sample_batch_result("batch_idem")
        rec1 = catalog.log_batch_execution("run_idem", batch_res, "v1.parquet")
        assert rec1.status == CatalogStatus.LOGGED

        # Re-log same batch with updated path
        rec2 = catalog.log_batch_execution("run_idem", batch_res, "v2.parquet")
        assert rec2.status == CatalogStatus.LOGGED

        batches = catalog.get_batch_executions("run_idem")
        assert len(batches) == 1
        assert batches[0]["processed_parquet_path"] == "v2.parquet"
    finally:
        catalog.close()
