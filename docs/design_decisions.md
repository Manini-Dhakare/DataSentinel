# Architectural & Design Decisions: SchemaGuard

This document records the foundational architecture, design principles, and decisions governing the SchemaGuard schema evolution and reconciliation pipeline.

---

## 1. Context & Objectives

Data engineering pipelines frequently encounter upstream schema drift—columns added, renamed, deleted, or data types shifted. Without defensive controls, drift leads to silent pipeline failures, downstream analytics corruption, or missed SLAs. 

SchemaGuard provides an automated, deterministic framework to inspect incoming Parquet files, evaluate drift against explicit configuration, quarantine breaking deviations, enforce domain rules, and load clean data into target tables idempotently.

---

## 2. Key Architectural Decisions

### ADR-001: Explicit YAML-Driven Configuration
- **Decision**: All canonical schemas, evolution classification rules, validation rules, and runtime paths are defined in human-readable YAML configurations under `config/`.
- **Rationale**: Keeps configuration strictly decoupled from Python logic. Engineers can adjust thresholds, approve safe widening, or add new nullable fields without altering core pipeline source code.

### ADR-002: Multi-Zone Data Storage Architecture
- **Decision**: Partition data storage into distinct operational zones under `data/`:
  1. `data/raw/`: Raw incoming Parquet batches received from upstream systems. Immutable.
  2. `data/generated/`: Synthesized test, scenario, and benchmark datasets.
  3. `data/processed/`: Target analytical dataset containing validated, reconciled, and enriched records.
  4. `data/quarantine/`: Isolation zone for files with breaking schema changes or individual records failing business constraint checks.
  5. `data/reports/`: Pipeline summary logs, data quality metrics, and validation reports.
  6. `data/metadata/`: Operational state, DuckDB metadata databases, and schema registries.
- **Rationale**: Clear physical separation of stages prevents partial pipeline states from polluting clean analytical targets and guarantees full observability.

### ADR-003: Schema Drift Classification & Type Widening Policy
- **Decision**: Classify all schema differences into two strict categories:
  - **Compatible**:
    - Adding a nullable column with an explicit/implicit null default in target storage.
    - Approved safe type widening conversions explicitly enumerated in `config/evolution_rules.yaml` (e.g., `int8` -> `int16` -> `int32` -> `int64`, IEEE 754 `float` -> `double`, `date32` -> `timestamp[us]`).
    - Adding optional technical metadata columns (prefixed with `meta_` or `_extra_`).
  - **Breaking**:
    - Dropping any required column.
    - Narrowing conversions (e.g., `int64` -> `int32`, `double` -> `float`, decimal scale reduction) due to overflow or precision loss risks.
    - Incompatible or unlisted type conversions (e.g., `string` to numeric, `timestamp` to `string`).
    - Converting a required field to nullable in the incoming schema contract.
    - Modifying or dropping primary keys or foreign keys.
- **Rationale**: Not every type cast is safe in analytics. Restricting type evolution to an explicit whitelist eliminates subtle numeric overflow and silent timestamp truncations.

### ADR-004: Decoupling Schema Nullability from Record-Level Null Validation
- **Decision**: Treat structural schema nullability and row-level null value enforcement as independent checks.
- **Rationale**: An incoming Parquet file might structurally match the schema, yet contain null or blank values in required columns. Schema validation passes the file structure, but record-level validation isolates the invalid rows to `data/quarantine/` while allowing valid rows to proceed.

### ADR-005: Audit Metadata Enrichment
- **Decision**: Every ingested record in `data/processed/` is enriched with technical audit columns:
  - `source_file`: Origin file name from raw batch.
  - `run_id`: Unique deterministic run identifier.
  - `ingested_at`: UTC ISO-8601 timestamp of pipeline processing.
- **Rationale**: Facilitates end-to-end data lineage, debugging, and targeted rollback or selective reprocessing.

### ADR-006: Idempotency & Safe Retries
- **Decision**: Execution runs are identified by deterministic run keys (`run_id`). Pipeline operations use DuckDB transactional writes or deterministic file partitioning. Rerunning a pipeline on identical batch inputs overwrites or skips identical records without creating duplicate records or corrupted states.
- **Rationale**: Network failures, compute preemption, or manual re-executions must never alter target dataset consistency.

### ADR-007: Modular Source Structure
- **Decision**: Modularize logic under `src/`:
  - `ingestion/`: Reading raw batches.
  - `schema/`: Schema comparison, evolution policy evaluation.
  - `validation/`: Domain constraints, null check, foreign key checks.
  - `transformation/`: Safe type casting, audit metadata enrichment.
  - `quarantine/`: Isolating corrupt files and bad rows.
  - `storage/`: Writing to DuckDB / processed Parquet idempotently.
  - `monitoring/`: Metrics collection and Streamlit/Plotly dashboarding.
- **Rationale**: High cohesion and loose coupling allow independent unit testing of each stage.
