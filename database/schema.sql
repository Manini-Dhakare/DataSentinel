-- SchemaGuard DuckDB Metadata Catalog DDL
-- Operational Database Schema for Lineage, Quality Auditing, and Idempotency

-- 1. Pipeline Runs
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR NOT NULL,
    total_batches INTEGER NOT NULL DEFAULT 0,
    successful_batches INTEGER NOT NULL DEFAULT 0,
    partial_batches INTEGER NOT NULL DEFAULT 0,
    rejected_batches INTEGER NOT NULL DEFAULT 0,
    failed_batches INTEGER NOT NULL DEFAULT 0,
    total_records INTEGER NOT NULL DEFAULT 0,
    valid_records INTEGER NOT NULL DEFAULT 0,
    invalid_records INTEGER NOT NULL DEFAULT 0,
    error_message VARCHAR
);

-- 2. Batch Executions
CREATE TABLE IF NOT EXISTS batch_executions (
    run_id VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    valid_record_count INTEGER NOT NULL DEFAULT 0,
    invalid_record_count INTEGER NOT NULL DEFAULT 0,
    processed_parquet_path VARCHAR,
    quarantine_parquet_path VARCHAR,
    quarantine_status VARCHAR,
    error_message VARCHAR,
    PRIMARY KEY (run_id, batch_id, table_name)
);

-- 3. Schema Evolutions
CREATE TABLE IF NOT EXISTS schema_evolutions (
    run_id VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    is_evolved BOOLEAN NOT NULL DEFAULT FALSE,
    evolution_status VARCHAR NOT NULL,
    applied_changes_count INTEGER NOT NULL DEFAULT 0,
    applied_changes_json VARCHAR,
    rejection_reason VARCHAR,
    PRIMARY KEY (run_id, batch_id, table_name)
);

-- 4. Validation Metrics
CREATE TABLE IF NOT EXISTS validation_metrics (
    run_id VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    total_records INTEGER NOT NULL DEFAULT 0,
    valid_records INTEGER NOT NULL DEFAULT 0,
    invalid_records INTEGER NOT NULL DEFAULT 0,
    null_violations INTEGER NOT NULL DEFAULT 0,
    duplicate_violations INTEGER NOT NULL DEFAULT 0,
    foreign_key_violations INTEGER NOT NULL DEFAULT 0,
    range_violations INTEGER NOT NULL DEFAULT 0,
    malformed_violations INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, batch_id, table_name)
);

-- 5. Idempotency Registry
CREATE TABLE IF NOT EXISTS idempotency_records (
    idempotency_key VARCHAR PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    batch_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_fingerprint VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    processed_parquet_path VARCHAR,
    quarantine_parquet_path VARCHAR,
    record_count INTEGER NOT NULL DEFAULT 0,
    registered_at TIMESTAMP NOT NULL
);
