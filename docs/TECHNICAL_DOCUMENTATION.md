# SchemaGuard Technical Reference Documentation

## 1. Directory & Package Structure

```text
My_Schema/
├── backend/                       # Backend architectural facade re-exporting core modules
│   └── __init__.py
├── config/                        # Declarative configuration files
│   ├── evolution_rules.yaml       # Allowed schema widening rules and type promotion paths
│   ├── pipeline_config.yaml       # Storage directories, retries, and runtime policies
│   └── source_schema.yaml         # Canonical baseline schemas for business tables
├── database/                      # DuckDB catalog management & DDL
│   ├── __init__.py                # Database connection helpers
│   └── schema.sql                 # DDL definitions for all metadata catalog tables
├── data/                          # Data store partitions
│   ├── generated/                 # Test suites and mock customer masters
│   ├── landing/                   # Raw incoming Parquet batches
│   ├── metadata/                  # schemaguard_metadata.duckdb
│   ├── processed/                 # Validated analytical Parquet store
│   ├── quarantine/                # Quarantined Parquet batches and metadata sidecars
│   └── reports/                   # Output JSON/Markdown reconciliation & metrics reports
├── docs/                          # System documentation
│   ├── ARCHITECTURE.md            # High-level design and component responsibilities
│   ├── PIPELINE_WORKFLOW.md       # Step-by-step execution lifecycle & reconciliation math
│   ├── TECHNICAL_DOCUMENTATION.md # Modules, database schema, CLI flags, configuration
│   ├── data_dictionary.md         # Canonical field definitions and data types
│   └── design_decisions.md        # Architecture decision records (ADRs)
├── frontend/                      # Streamlit interactive operational dashboard
│   ├── __init__.py
│   ├── app.py                     # Main dashboard entry point
│   ├── data_loader.py             # Read-only DuckDB metadata query abstraction
│   ├── components/                # Reusable Plotly charts and layout widgets
│   │   └── charts.py              # Donut, waterfall, stacked bars, trends
│   └── views/                     # Multi-page dashboard view modules
│       ├── overview.py            # Executive KPIs and high-level health
│       ├── monitoring.py          # Execution duration, failure logs, batch drill-down
│       ├── reconciliation.py      # I = V + Q + U proof and waterfall audit
│       ├── schema_evolution.py    # Evolution frequency, applied changes JSON viewer
│       ├── quarantine.py          # Violation categories, quarantine file explorer
│       └── reliability.py         # Idempotency registry, fingerprints, retry matrix
├── scripts/                       # Operational executable launchers
│   ├── run_pipeline.py            # CLI pipeline runner
│   └── run_dashboard.py           # Streamlit dashboard launcher
├── src/                           # Authoritative pipeline engine modules
│   ├── ingestion/                 # BatchProcessor single-batch coordinator
│   ├── monitoring/                # OperationalMetrics and MetricsCollector
│   ├── orchestration/             # Models, IdempotencyManager, and RetryPolicy
│   ├── quarantine/                # QuarantineManager and sidecar generator
│   ├── reconciliation/            # ReconciliationEngine and ReportGenerator
│   ├── schema/                    # SchemaDetector, EvolutionClassifier, SchemaEvolver
│   ├── storage/                   # StorageManager and MetadataCatalog
│   ├── transformation/            # PipelineOrchestrator multi-batch engine
│   └── validation/                # DataValidator and Pydantic schema constraints
├── tests/                         # Comprehensive Pytest test suite (220 tests)
├── requirements.txt               # Pinned Python package dependencies
└── README.md                      # Comprehensive project guide & setup
```

---

## 2. Database Tables & Schema Specifications

The DuckDB operational catalog (`data/metadata/schemaguard_metadata.duckdb`) maintains five relational tables:

### 2.1 `pipeline_runs`
Tracks multi-batch pipeline execution lifecycle.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `run_id` | VARCHAR | PRIMARY KEY | Unique execution identifier (UUID or CLI specified). |
| `started_at` | TIMESTAMP | NOT NULL | UTC execution start timestamp. |
| `completed_at` | TIMESTAMP | | UTC completion timestamp. |
| `status` | VARCHAR | NOT NULL | `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`. |
| `total_batches` | INTEGER | NOT NULL | Total discovered input batches. |
| `successful_batches` | INTEGER | NOT NULL | Batches with 100% valid records committed. |
| `partial_batches` | INTEGER | NOT NULL | Batches where valid rows were persisted and invalid rows quarantined. |
| `rejected_batches` | INTEGER | NOT NULL | Batches rejected by schema gates. |
| `failed_batches` | INTEGER | NOT NULL | Batches failing due to unhandled ingestion errors. |
| `total_records` | INTEGER | NOT NULL | Sum of all records across input batches ($I$). |
| `valid_records` | INTEGER | NOT NULL | Records that passed data validation ($V$). |
| `invalid_records` | INTEGER | NOT NULL | Records routed to quarantine ($Q$). |
| `error_message` | VARCHAR | | High-level execution failure description. |

### 2.2 `batch_executions`
Logs lineage for each single batch processed within a run.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `run_id` | VARCHAR | NOT NULL | Foreign key referencing `pipeline_runs.run_id`. |
| `batch_id` | VARCHAR | NOT NULL | Extracted batch identifier (e.g., `batch_001`). |
| `table_name` | VARCHAR | NOT NULL | Target analytical table (e.g., `orders`). |
| `source_file` | VARCHAR | NOT NULL | Original input file name. |
| `status` | VARCHAR | NOT NULL | `SUCCESS`, `PARTIAL_SUCCESS`, `SCHEMA_REJECTED`, `STORAGE_FAILED`, `FAILED`. |
| `row_count` | INTEGER | NOT NULL | Total rows in the batch file. |
| `valid_record_count` | INTEGER | NOT NULL | Rows meeting all validation rules. |
| `invalid_record_count` | INTEGER | NOT NULL | Rows failing validation rules. |
| `processed_parquet_path`| VARCHAR | | Destination path in processed storage. |
| `quarantine_parquet_path`| VARCHAR | | Destination path in quarantine zone. |
| `quarantine_status` | VARCHAR | | `QUARANTINED`, `ALREADY_EXISTS`, `NONE`. |
| `error_message` | VARCHAR | | Error message if batch failed. |

### 2.3 `schema_evolutions`
Audits schema modifications and classification gates.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `run_id` | VARCHAR | NOT NULL | Reference to `pipeline_runs.run_id`. |
| `batch_id` | VARCHAR | NOT NULL | Extracted batch identifier. |
| `table_name` | VARCHAR | NOT NULL | Target table name. |
| `is_evolved` | BOOLEAN | NOT NULL | `TRUE` if schema evolved; `FALSE` if identical. |
| `evolution_status` | VARCHAR | NOT NULL | `COMPATIBLE`, `APPROVED_TYPE_WIDENING`, `SCHEMA_REJECTED`. |
| `applied_changes_count` | INTEGER | NOT NULL | Count of modifications applied. |
| `applied_changes_json` | VARCHAR | | Serialized JSON list of applied changes. |
| `rejection_reason` | VARCHAR | | Diagnostic reason if rejected. |

### 2.4 `validation_metrics`
Aggregates row-level data quality violation categories.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `run_id` | VARCHAR | NOT NULL | Reference to `pipeline_runs.run_id`. |
| `batch_id` | VARCHAR | NOT NULL | Batch identifier. |
| `table_name` | VARCHAR | NOT NULL | Target table name. |
| `total_records` | INTEGER | NOT NULL | Total batch rows evaluated. |
| `valid_records` | INTEGER | NOT NULL | Clean valid rows. |
| `invalid_records` | INTEGER | NOT NULL | Total violating rows. |
| `null_violations` | INTEGER | NOT NULL | Violations of nullability constraints. |
| `duplicate_violations` | INTEGER | NOT NULL | Duplicate primary key occurrences. |
| `foreign_key_violations`| INTEGER | NOT NULL | Missing customer master foreign key. |
| `range_violations` | INTEGER | NOT NULL | Non-positive amount values (`amount <= 0`). |
| `malformed_violations` | INTEGER | NOT NULL | Unparseable types or invalid dates. |

### 2.5 `idempotency_records`
Content-addressable cryptographic registry for idempotency and reprocessing.

| Column | Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `idempotency_key` | VARCHAR | PRIMARY KEY | Composite key: `<table_name>:<batch_id>:<fingerprint>`. |
| `table_name` | VARCHAR | NOT NULL | Target table name. |
| `batch_id` | VARCHAR | NOT NULL | Batch identifier. |
| `source_file` | VARCHAR | NOT NULL | Input file name. |
| `source_fingerprint` | VARCHAR | NOT NULL | SHA-256 hexadecimal file content hash. |
| `run_id` | VARCHAR | NOT NULL | Execution run identifier. |
| `status` | VARCHAR | NOT NULL | `SUCCESS`, `PARTIAL_SUCCESS`. |
| `processed_parquet_path`| VARCHAR | | Durable output Parquet path. |
| `quarantine_parquet_path`| VARCHAR | | Quarantine Parquet path. |
| `record_count` | INTEGER | NOT NULL | Count of committed records. |
| `registered_at` | TIMESTAMP | NOT NULL | UTC timestamp of registration. |

---

## 3. Configuration Reference

### 3.1 `config/pipeline_config.yaml`
```yaml
version: "1.0"
environment: "development"

pipeline:
  name: "schemaguard_pipeline"
  batch_id_format: "uuid4"
  timezone: "UTC"
  timestamp_format: "%Y-%m-%dT%H:%M:%S.%fZ"

storage:
  base_dir: "data"
  landing_dir: "data/landing"
  processed_dir: "data/processed"
  quarantine_dir: "data/quarantine"
  reports_dir: "data/reports"
  metadata_dir: "data/metadata"
  metadata_db_path: "data/metadata/schemaguard_metadata.duckdb"

audit_metadata:
  add_technical_columns: true
  columns:
    source_file: "source_file"
    run_id: "run_id"
    ingested_at: "ingested_at"

validation:
  fail_fast: false
  track_duplicates: true
  track_invalid_records: true
  quarantine_invalid_rows: true

retries:
  max_retries: 3
  initial_delay_sec: 0.05
  backoff_factor: 2.0
  max_delay_sec: 1.0
  jitter: true
```

---

## 4. CLI Commands & Execution Syntax

### 4.1 Pipeline Runner (`scripts/run_pipeline.py`)

```bash
python scripts/run_pipeline.py [OPTIONS]
```

#### Command-Line Arguments
- `--input-dir` *(required)*: Directory containing incoming Parquet batch files.
- `--output-dir` *(optional)*: Destination directory for processed Parquet files (default: from config).
- `--report-dir` *(optional)*: Destination directory for reconciliation reports (default: from config).
- `--report-format` *(optional)*: Report serialization format: `json`, `markdown`, or `both` (default: `both`).
- `--run-id` *(optional)*: Explicit custom identifier for the pipeline execution.
- `--reprocess` *(optional)*: Flag to allow legitimate reprocessing when input file contents have changed.
- `--metrics` *(optional)*: Flag to generate operational monitoring telemetry JSON (`operational_metrics_<id>.json`).
- `--config` *(optional)*: Path to custom YAML pipeline configuration (default: `config/pipeline_config.yaml`).

#### Exit Codes
- `0`: Pipeline completed with `PipelineExecutionStatus.SUCCESS` and `ReconciliationStatus.RECONCILED`.
- `1`: Pipeline encountered partial success, schema rejections, quarantine routing, or failures.

### 4.2 Dashboard Launcher (`scripts/run_dashboard.py`)

```bash
# Standard local execution on default port 8501
python scripts/run_dashboard.py

# Custom port or headless server flags
python scripts/run_dashboard.py --server.port 8502 --server.address 0.0.0.0
```
