# 🛡️ DataSentinel
### Intelligent Parquet Schema Evolution, Data Validation, & Automated Reconciliation Platform

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![PyArrow](https://img.shields.io/badge/PyArrow-17.0+-orange.svg)](https://arrow.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.0+-yellow.svg)](https://duckdb.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.36+-red.svg)](https://streamlit.io/)
[![Tests](https://img.shields.io/badge/tests-220%20passed-brightgreen.svg)]()
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

---

## 1. Project Overview

**DataSentinel** is an enterprise-grade, Python-based data engineering and quality assurance platform designed for modern lakehouse architectures. Built on **Python 3.12**, **PyArrow**, **DuckDB**, **Pydantic**, **Pandas**, and **Streamlit**, DataSentinel solves the critical challenges of silent data corruption, schema drift, unvalidated ingestion, and reconciliation opacity in analytical data pipelines.

DataSentinel acts as an active gatekeeper between raw landing zones and curated analytical storage. It automatically detects structural schema drift in incoming Parquet batches, evaluates backward compatibility against declarative evolution contracts, validates row-level integrity against relational customer master models, quarantines invalid records into cryptographic sidecars, atomically publishes valid records with technical audit metadata, and mathematically proves data conservation across every pipeline run using the fundamental conservation law:

$$\text{Total Input } (I) = \text{Valid Persisted } (V) + \text{Quarantined } (Q) + \text{Unaccounted } (U)$$

---

## 2. Problem Statement

Modern analytical data platforms ingest high-velocity micro-batches from diverse upstream microservices, transactional databases, and third-party APIs. In real-world data lakehouse operations, pipelines routinely suffer from four catastrophic points of failure:

1. **Uncontrolled Schema Drift:** Upstream teams alter table schemas (renaming fields, changing data types, or adding non-nullable columns) without coordination, leading to runtime pipeline crashes or silent downstream analytical inaccuracies.
2. **Data Quality Decay & Referential Inconsistency:** Incoming records frequently violate business constraints—containing duplicate primary keys, null values in mandatory fields, negative numerical values, or orphan foreign keys referencing non-existent parent entities.
3. **Reconciliation Opacity:** Data engineers often lack mathematical certainty that every record delivered to a landing directory was either successfully written to analytical storage or safely routed to quarantine.
4. **Idempotency & Concurrency Hazards:** Reprocessing batches or retrying failed pipeline runs risks corrupting analytical tables with duplicate entries, while failing to detect genuine upstream source updates.

---

## 3. Project Objectives

DataSentinel was engineered to meet strict production reliability and auditability objectives:

- **Contract-Driven Evolution:** Enable seamless, automated backward-compatible schema changes (e.g., adding nullable fields, safe numeric widening) while strictly rejecting breaking alterations.
- **Fault-Tolerant Isolation:** Ensure that bad or malformed batches are isolated without halting or failing healthy concurrent micro-batches.
- **Row-Level Precision Validation:** Enforce strict Pydantic model validation and cross-entity foreign key referential integrity at the row level rather than discarding entire batches when only isolated records fail.
- **Durable Cryptographic Quarantine:** Segregate corrupt records into dedicated Parquet sidecars alongside rich JSON diagnostic metadata and SHA-256 fingerprints for full auditability.
- **Deterministic Conservation Proofs:** Provide verifiable mathematical reconciliation proofs ($I = V + Q + U$, where $U = 0$ in healthy executions) with automated JSON and Markdown audit report generation.
- **Zero-Loss Idempotency:** Provide content-addressable deduplication using SHA-256 payload hashing, preventing duplicate ingestion while supporting explicit reprocessing flags.
- **Operational Observability:** Provide a unified executive and technical Streamlit dashboard backed by DuckDB for telemetry, trend analysis, and interactive schema inspection.

---

## 4. Key Features Actually Implemented

### Automated Schema Evolution & Classification
- **Structural Inspection:** Compares incoming Parquet schemas against canonical YAML baseline schemas.
- **Declarative Evolution Classifier:** Evaluates schema differences into categorized outcomes:
  - `COMPATIBLE_IDENTICAL`: Identical schema structure.
  - `COMPATIBLE_ADD_NULLABLE_COLUMN`: Safe additions of nullable fields.
  - `COMPATIBLE_TYPE_WIDENING`: Safe type widening (e.g., `int32` $\rightarrow$ `int64`, `float` $\rightarrow$ `double`).
  - `BREAKING_TYPE_CHANGE`: Incompatible conversions rejected at gate.
  - `BREAKING_REMOVE_COLUMN`: Mandatory column deletion rejected.
  - `BREAKING_ADD_NON_NULLABLE_COLUMN`: Non-nullable column addition rejected.
  - `MANUAL_REVIEW_REQUIRED`: Ambiguous modifications gated for administrative review.
- **Immutable Schema Evolver:** Builds updated PyArrow schemas cumulatively without mutating existing baseline definitions.

### Row-Level Data Validation & Referential Integrity
- **Pydantic Model Enforcement:** Validates business rules, data types, and value domains (`OrderModel`, `CustomerModel`).
- **Referential Integrity Verification:** Validates order records against an in-memory customer master catalog to trap dangling foreign keys (`customer_id`).
- **Duplicate Detection:** Tracks and identifies primary key collisions (`order_id`) within and across batches.
- **Multi-Violation Tracking:** Captures all rule violations per row, detailing failure categories, constraint names, and offending columns.

### Durable Cryptographic Quarantine Subsystem
- **Segregated Storage:** Automatically splits partially valid batches, directing bad records to `data/quarantine/{entity}/{batch_id}/`.
- **Dual-Artifact Storage:** Generates an `invalid_records.parquet` file containing corrupted rows alongside a structured `quarantine_metadata.json` sidecar.
- **Deterministic Quarantine Fingerprinting:** Hashes invalid record sets using SHA-256 for integrity verification and duplicate quarantine detection.

### Atomic Storage Engine & Audit Injection
- **Safe Staging Protocol:** Writes analytical Parquet files to transient files (`.tmp`) before atomic filesystem renaming (`os.replace`), preventing partial writes.
- **Technical Audit Injection:** Injects technical audit columns into every persisted record:
  - `source_file`: Path to the originating source file.
  - `run_id`: Unique execution identifier.
  - `ingested_at`: UTC timestamp of ingestion.
- **Content-Addressable Storage Fingerprinting:** Computes deterministic SHA-256 content hashes of persisted tables.

### ACID Metadata Catalog & Operational Lineage
- **Embedded DuckDB Engine:** Tracks state with full transactional integrity in `data/metadata/schemaguard_metadata.duckdb`.
- **Relational Tables:**
  - `pipeline_runs`: High-level run metrics, execution timestamps, and final statuses.
  - `batch_executions`: Batch-level counts, execution states, and schema resolutions.
  - `schema_evolutions`: Detailed structural changes and evolution classifications.
  - `validation_metrics`: Aggregated counts of valid, invalid, duplicate, and quarantined records.
  - `quarantine_records`: File paths and fingerprints of quarantined assets.
  - `idempotency_records`: File hashes and run IDs for deduplication.

### Mathematical Reconciliation ($I = V + Q + U$)
- **Formal Invariant Verification:** Verifies that Total Input ($I$) equals Valid Persisted ($V$) plus Quarantined ($Q$) plus Unaccounted ($U$).
- **Artifact Reconciliation:** Cross-references filesystem artifacts against metadata catalog entries to verify physical asset durability.
- **Automated Reporting:** Generates audit reports in JSON and GitHub-flavored Markdown formats with execution verdicts (`RECONCILED` vs `UNRECONCILED`).

### Resilient Orchestration & Idempotency
- **Content-Addressable Idempotency:** Detects previously ingested files via SHA-256 input fingerprints; marks duplicates as `ALREADY_PROCESSED` unless `--reprocess` is explicitly invoked.
- **Bounded Exponential Retries:** Features full jitter retries to recover from transient I/O locks and concurrency collisions.
- **Partial Batch Isolation:** Allows clean batches to commit even when adjacent batches fail schema or validation checks.

### Interactive Streamlit Operational Dashboard
- **Executive Overview:** High-level KPIs, status distribution donut chart, execution duration trends, and recent runs table.
- **Pipeline Monitoring:** Interactive execution history, filterable by date and status, with expandable batch drill-down inspectors.
- **Data Reconciliation View:** Live invariant audit banner ($I = V + Q + U$) with Plotly waterfall diagrams tracking record conservation.
- **Schema Evolution Inspector:** Historical timeline of schema changes with side-by-side JSON diff viewer.
- **Quarantine Analytics:** Horizontal violation breakdown bar charts and interactive quarantine file explorer.
- **Reliability & Idempotency:** Registry of file hashes, duplicate prevention metrics, and retry policy diagnostic matrices.

---

## 5. Technology Stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Runtime & Language** | Python 3.12 (64-bit) | Core platform runtime environment |
| **Columnar Data Engine** | Apache Arrow / PyArrow (17.0+) | Columnar table representations, Arrow IPC, and Parquet read/write |
| **Analytical Catalog** | DuckDB (1.0+) | Embedded ACID analytical relational database for pipeline lineage |
| **Data Validation** | Pydantic (2.8+) | Declarative data modeling and row-level constraint validation |
| **Data Manipulation** | Pandas (2.2+) | Tabular data processing and metadata transformation |
| **Dashboard Framework** | Streamlit (1.36+) | Interactive multi-page operational web interface |
| **Data Visualization** | Plotly (5.22+) | Interactive charts (waterfall, donut, time-series, bar) |
| **Configuration** | PyYAML (6.0.1+) | Declarative YAML-based schema and pipeline configuration |
| **Testing & QA** | Pytest (8.2+) | Comprehensive automated test suite with full assertion coverage |

---

## 6. System Architecture

```text
                                  Incoming Parquet Batches
                                             │
                                             ▼
                       ┌───────────────────────────────────────────┐
                       │       Deterministic Batch Discovery       │
                       └─────────────────────┬─────────────────────┘
                                             │
                                             ▼
                       ┌───────────────────────────────────────────┐
                       │  Input Fingerprinting & Idempotency Check │
                       └─────────────────────┬─────────────────────┘
                                             │
                                             ▼
                       ┌───────────────────────────────────────────┐
                       │ Schema Detection & Evolution Classifier   │
                       └─────────────┬─────────────────────────────┘
                                     │
                 ┌───────────────────┴───────────────────┐
                 ▼ (Compatible / Widened)                ▼ (Breaking / Unapproved)
   ┌───────────────────────────┐               ┌───────────────────────────┐
   │   Schema Evolution Engine │               │   Schema Rejection Gate   │
   └─────────────┬─────────────┘               └─────────────┬─────────────┘
                 │                                           │
                 ▼                                           │
   ┌───────────────────────────┐                             │
   │  Row-Level Data Validator │                             │
   └───────┬───────────┬───────┘                             │
           │           │                                     │
(Valid)    │           │ (Invalid Rows)                      │
           ▼           ▼                                     │
   ┌──────────────┐  ┌─────────────────────────┐             │
   │Atomic Storage│  │ Durable Quarantine Zone │             │
   │  (.tmp ➔ .pq)│  │ (invalid.pq + json meta)│             │
   └───────┬──────┘  └─────────────┬───────────┘             │
           │                       │                         │
           └───────────┬───────────┴─────────────────────────┘
                       │
                       ▼
   ┌───────────────────────────────────────────────────────────────┐
   │         DuckDB Metadata Catalog & Operational Lineage         │
   │  (Runs, Batches, Evolutions, Validation Metrics, Quarantine)  │
   └───────────────────────────────┬───────────────────────────────┘
                                   │
                                   ▼
   ┌───────────────────────────────────────────────────────────────┐
   │     End-to-End Mathematical Reconciliation Engine ($I=V+Q+U$) │
   │           Dual Audit Reports: Markdown & Structured JSON      │
   └───────────────────────────────┬───────────────────────────────┘
                                   │
                                   ▼
   ┌───────────────────────────────────────────────────────────────┐
   │           Streamlit Observability & Analytics Portal          │
   └───────────────────────────────────────────────────────────────┘
```

---

## 7. Complete Folder Structure

```text
My_Schema/
├── .env.example                       # Safe environment configuration template
├── .gitignore                         # Comprehensive Git exclusion rules
├── README.md                          # Production documentation & project manual
├── requirements.txt                   # Production Python package dependencies
├── backend/                           # Architectural facade re-exporting core modules
│   └── __init__.py                    # Facade exports
├── config/                            # Declarative system configuration files
│   ├── evolution_rules.yaml           # Schema widening and allowed type promotions
│   ├── pipeline_config.yaml           # Pipeline runtime parameters, paths, and retries
│   └── source_schema.yaml             # Baseline canonical entity schemas
├── data/                              # Data directories (tracked via .gitkeep)
│   ├── generated/                     # Canonical customer master & synthetic test batches
│   │   └── .gitkeep
│   ├── landing/                       # Ingestion landing zone for raw Parquet files
│   │   └── .gitkeep
│   ├── metadata/                      # DuckDB catalog directory
│   │   └── .gitkeep
│   ├── processed/                     # Curated analytical Parquet output store
│   │   └── .gitkeep
│   ├── quarantine/                    # Segregated quarantine Parquet files & JSON metadata
│   │   └── .gitkeep
│   ├── raw/                           # Raw staging store
│   │   └── .gitkeep
│   └── reports/                       # Generated reconciliation & metrics reports
│       └── .gitkeep
├── database/                          # DuckDB relational schema definitions
│   ├── __init__.py                    # Database connection utilities
│   └── schema.sql                     # DDL definitions for analytical metadata catalog
├── docs/                              # Comprehensive architecture & design specifications
│   ├── ARCHITECTURE.md                # System design principles & architectural layers
│   ├── data_dictionary.md             # Canonical schemas, entities, and data types
│   ├── design_decisions.md            # Architectural Decision Records (ADRs)
│   ├── PIPELINE_WORKFLOW.md           # Step-by-step pipeline execution lifecycle
│   └── TECHNICAL_DOCUMENTATION.md     # Deep-dive module specifications & CLI reference
├── frontend/                          # Streamlit interactive operational dashboard
│   ├── app.py                         # Main Streamlit web application
│   ├── data_loader.py                 # Thread-safe read-only DuckDB metadata loader
│   ├── __init__.py
│   ├── components/                    # Reusable UI widgets and Plotly charts
│   │   ├── __init__.py
│   │   └── charts.py                  # Donut, waterfall, timeline, and bar charts
│   └── views/                         # Modular dashboard views
│       ├── __init__.py
│       ├── monitoring.py              # Pipeline execution history & batch inspector
│       ├── overview.py                # Executive summary & top-level KPIs
│       ├── quarantine.py              # Quarantine analytics & failure explorer
│       ├── reconciliation.py          # Mathematical reconciliation & waterfall audit
│       ├── reliability.py             # Idempotency registry & retry diagnostics
│       └── schema_evolution.py        # Schema drift history & visual JSON diff
├── generator/                         # Synthetic test data generation
│   ├── __init__.py
│   └── generate_data.py               # Configurable data generator for test cases
├── scripts/                           # Operational execution entry points
│   ├── __init__.py
│   ├── generate_sample_data.py        # Generates test Parquet batches with controlled anomalies
│   ├── run_dashboard.py               # Operational launcher for Streamlit dashboard
│   └── run_pipeline.py                # CLI runner for batch orchestration & reconciliation
├── src/                               # Authoritative core engine implementation
│   ├── __init__.py
│   ├── ingestion/                     # Batch ingestion engine
│   │   ├── __init__.py
│   │   └── batch_processor.py         # Single-batch lifecycle processing
│   ├── monitoring/                    # Observability and metrics
│   │   ├── __init__.py
│   │   ├── metrics_collector.py       # Performance, latency, and throughput recorder
│   │   └── models.py                  # Metrics data contracts
│   ├── orchestration/                 # Pipeline orchestration and fault tolerance
│   │   ├── __init__.py
│   │   ├── idempotency.py             # SHA-256 fingerprinting & duplicate handling
│   │   ├── models.py                  # Pydantic orchestration models & invariants
│   │   └── retry.py                   # Exponential backoff with full jitter
│   ├── quarantine/                    # Quarantine management
│   │   ├── __init__.py
│   │   └── quarantine_manager.py      # Segregated invalid record storage & metadata
│   ├── reconciliation/                # Mathematical reconciliation engine
│   │   ├── __init__.py
│   │   ├── models.py                  # Reconciliation contracts and invariant models
│   │   ├── reconciliation_engine.py   # Conservation law verification ($I=V+Q+U$)
│   │   └── report_generator.py        # Markdown and JSON report generator
│   ├── schema/                        # Schema management and evolution
│   │   ├── __init__.py
│   │   ├── evolution_classifier.py    # Schema change taxonomy and policy classifier
│   │   ├── schema_detector.py         # Parquet schema extraction & drift detector
│   │   └── schema_evolver.py          # Backward-compatible schema evolution engine
│   ├── storage/                       # Storage management and metadata catalog
│   │   ├── __init__.py
│   │   ├── metadata_catalog.py        # ACID DuckDB operational catalog
│   │   └── storage_manager.py         # Atomic Parquet publication with audit injection
│   ├── transformation/                # Multi-batch pipeline orchestrator
│   │   ├── __init__.py
│   │   └── pipeline_orchestrator.py   # Batch scheduling, dependency & error containment
│   └── validation/                    # Row-level validation engine
│       ├── __init__.py
│       ├── models.py                  # Pydantic entity models (Order, Customer)
│       └── validator.py               # Business rules, PK uniqueness, FK referential integrity
└── tests/                             # Automated test suite (220 tests, 100% pass)
    ├── __init__.py
    ├── test_batch_processor.py        # Single-batch lifecycle & edge cases
    ├── test_cli.py                    # CLI arguments, exit codes, and flag handling
    ├── test_dashboard.py              # Streamlit dashboard views & data loader tests
    ├── test_data_validation.py        # Row-level Pydantic & FK referential tests
    ├── test_end_to_end.py             # Multi-batch end-to-end integration workflows
    ├── test_evolution_classifier.py   # Schema change classification rules
    ├── test_metadata_catalog.py       # DuckDB catalog ACID operations & queries
    ├── test_orchestration_integration.py # Multi-batch pipeline integration & containment
    ├── test_orchestration_models.py   # Invariant validation on orchestration models
    ├── test_phase6_history_monitoring.py # Catalog history & metrics collector tests
    ├── test_phase6_idempotency.py     # SHA-256 fingerprinting & deduplication tests
    ├── test_phase6_retry.py           # Retry policy, backoff, and jitter tests
    ├── test_phase7_e2e_reconciliation.py # Full lifecycle reconciliation & CLI flags
    ├── test_pipeline_orchestrator.py  # Orchestrator error handling & isolation
    ├── test_quarantine_manager.py     # Quarantine Parquet creation & sidecars
    ├── test_reconciliation.py         # Conservation math & report generation
    ├── test_schema_detector.py        # Parquet schema detection against baselines
    ├── test_schema_evolver.py         # Cumulative evolution & type widening
    └── test_storage_manager.py        # Atomic Parquet file writes & audit columns
```

---

## 8. Installation Requirements

### System Requirements
- **Operating System:** Windows 10/11, macOS 12+, or Linux (Ubuntu 20.04+, RHEL 8+, Debian 11+)
- **Python Runtime:** Python 3.12 (64-bit recommended)
- **Memory:** Minimum 4 GB RAM (8 GB recommended for analytical micro-batches)
- **Disk Space:** 500 MB free disk space for test datasets and catalog storage

---

## 9. Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/Manini-Dhakare/DataSentinel.git
cd My_Schema
```

### 2. Create and Activate a Virtual Environment
```bash
# Using Python 3.12
python -m venv .venv

# On Windows (PowerShell):
.venv\Scripts\Activate.ps1

# On Windows (Command Prompt):
.venv\Scripts\activate.bat

# On Linux / macOS:
source .venv/bin/activate
```

### 3. Install Production Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 10. Environment Variable Configuration

DataSentinel uses a `.env` file for runtime customization. A template is provided in `.env.example`.

### Setup `.env`
```bash
cp .env.example .env
```

### Available Configuration Parameters
| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `ENVIRONMENT` | `development` | Deployment environment (`development`, `staging`, `production`) |
| `PIPELINE_NAME` | `DataSentinel` | Unique identifier for the pipeline runtime |
| `LOG_LEVEL` | `INFO` | Python logging severity level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `STORAGE_BASE_DIR` | `data` | Base directory for data asset storage |
| `DATA_LANDING_DIR` | `data/landing` | Ingestion landing zone for raw Parquet micro-batches |
| `DATA_PROCESSED_DIR`| `data/processed` | Destination directory for curated analytical Parquet files |
| `DATA_QUARANTINE_DIR`| `data/quarantine` | Segregated storage directory for corrupted records |
| `DATA_REPORTS_DIR` | `data/reports` | Output directory for reconciliation audits & JSON metrics |
| `METADATA_DB_PATH` | `data/metadata/schemaguard_metadata.duckdb` | Persistent DuckDB catalog path |
| `STREAMLIT_SERVER_PORT` | `8501` | Port for the Streamlit dashboard |
| `STREAMLIT_SERVER_ADDRESS` | `localhost` | Host address for the dashboard server |

---

## 11. Commands to Run the Application

### 1. Generate Synthetic Test Datasets
```bash
# Populates customer master and diverse test batches (clean, evolved, invalid, breaking)
python scripts/generate_sample_data.py
```

### 2. Execute the Pipeline via CLI
Execute the complete ingestion, evolution, validation, and reconciliation pipeline:

```bash
# Clean execution with dual format reports and operational metrics
python scripts/run_pipeline.py \
    --input-dir data/landing \
    --output-dir data/processed \
    --report-dir data/reports \
    --report-format both \
    --metrics
```

#### Supported CLI Flags
- `--input-dir <PATH>`: Directory containing incoming Parquet batches (default: `data/landing`).
- `--output-dir <PATH>`: Destination directory for curated Parquet tables (default: `data/processed`).
- `--report-dir <PATH>`: Target directory for reconciliation reports (default: `data/reports`).
- `--report-format {json,markdown,both}`: Output format for audit reports (default: `both`).
- `--metrics`: Flag to collect and export operational runtime metrics to JSON.
- `--reprocess`: Flag to reprocess previously ingested batches when source data has changed.
- `--config <PATH>`: Path to custom `pipeline_config.yaml`.

### 3. Launch the Streamlit Observability Dashboard
```bash
# Using the launcher script:
python scripts/run_dashboard.py

# Or directly through Streamlit:
streamlit run frontend/app.py
```
Open your browser and navigate to `http://localhost:8501`.

---

## 12. Instructions to Execute Tests

DataSentinel maintains a comprehensive, production-grade test suite covering unit, integration, invariant, and regression tests.

```bash
# Run the entire test suite with detailed test progression:
python -m pytest -v

# Run with concise test summary:
python -m pytest -q

# Run specific test modules:
python -m pytest tests/test_schema_detector.py -v
python -m pytest tests/test_reconciliation.py -v
python -m pytest tests/test_end_to_end.py -v
python -m pytest tests/test_phase7_e2e_reconciliation.py -v
```

---

## 13. Actual Test Execution Results

The automated test suite was executed against the project codebase. All tests passed with zero failures and zero regressions.

```text
============================= test session starts =============================
platform win32 -- Python 3.12.4, pytest-9.1.1, pluggy-1.6.0 -- C:\Program Files\Python312\python.exe
cachedir: .pytest_cache
rootdir: C:\CandidateWorkspace\My_Schema
plugins: anyio-4.13.0, langsmith-0.9.0, asyncio-1.4.0
collected 220 items

tests/test_batch_processor.py .................                             [  7%]
tests/test_cli.py ..........                                                [ 12%]
tests/test_dashboard.py ...........                                         [ 17%]
tests/test_data_validation.py ...................                           [ 25%]
tests/test_end_to_end.py ...........                                        [ 30%]
tests/test_evolution_classifier.py ..................                       [ 39%]
tests/test_metadata_catalog.py .............                                [ 45%]
tests/test_orchestration_integration.py ...............                     [ 51%]
tests/test_orchestration_models.py .............                            [ 57%]
tests/test_phase6_history_monitoring.py ...                                 [ 59%]
tests/test_phase6_idempotency.py .....                                      [ 61%]
tests/test_phase6_retry.py ....                                             [ 63%]
tests/test_phase7_e2e_reconciliation.py ....................                [ 72%]
tests/test_pipeline_orchestrator.py ............                            [ 77%]
tests/test_quarantine_manager.py ...........                                [ 82%]
tests/test_reconciliation.py ............                                   [ 88%]
tests/test_schema_detector.py .........                                     [ 92%]
tests/test_schema_evolver.py ...............                                [ 99%]
tests/test_storage_manager.py ...                                           [100%]

======================= 220 passed in 69.24s (0:01:09) ========================
```

### Test Suite Metrics
- **Total Test Cases:** 220
- **Passed:** 220 (100.0%)
- **Failed:** 0
- **Skipped / Errors:** 0
- **Execution Duration:** 69.24 seconds

---

## 14. Example Usage and Outputs

### Terminal Execution Output
When running `scripts/run_pipeline.py`, DataSentinel prints an executive terminal summary:

```text
======================================================================
DataSentinel Pipeline Execution & Reconciliation Summary
======================================================================
Run ID:                  phase7_cli_metrics_run
Pipeline Status:         SUCCESS
Reconciliation Verdict:  RECONCILED
Started:                 2026-11-13T13:48:25.726916+00:00
Completed:               2026-11-13T13:48:26.022219+00:00
----------------------------------------------------------------------
Batch Execution (1 total):
  - Successful:          1
  - Partial / Quarantined: 0
  - Schema Rejected:     0
  - Ingestion Failed:    0
----------------------------------------------------------------------
Record Conservation:
  - Total Input Rows:    1000
  - Valid Persisted:     1000
  - Quarantined:         0
  - Unaccounted Rows:    0
  - Reconciliation Rate: 100.0%
----------------------------------------------------------------------
Durable Artifacts (1 registered):
  - Processed Parquet:   1
  - Quarantine Assets:   0
  - Missing on Disk:     0
  - Duplicate Refs:      0
----------------------------------------------------------------------
Generated Reports & Telemetry:
  - JSON Report:         data/reports/reconciliation_report_phase7_cli_metrics_run.json
  - Markdown Report:     data/reports/reconciliation_report_phase7_cli_metrics_run.md
  - Operational Metrics: data/reports/operational_metrics_phase7_cli_metrics_run.json
======================================================================
```

### Generated Markdown Reconciliation Report Example
```markdown
# DataSentinel Pipeline Reconciliation Report

- **Run ID:** `run_20261113_140928`
- **Execution Timestamp:** `2026-11-13T14:09:28.104210Z`
- **Overall Pipeline Status:** `SUCCESS`
- **Reconciliation Verdict:** `RECONCILED`

## Record Conservation Summary
| Metric | Count | Percentage |
| :--- | :--- | :--- |
| **Total Input Rows ($I$)** | 1,000 | 100.0% |
| **Valid Persisted ($V$)** | 980 | 98.0% |
| **Quarantined ($Q$)** | 20 | 2.0% |
| **Unaccounted Rows ($U$)** | 0 | 0.0% |

> **Conservation Identity:** $1000 = 980 + 20 + 0$ (Status: **VERIFIED**)
```

---

## 15. Limitations

While DataSentinel is built for production robustness, several intentional design scope boundaries exist:

1. **Single-Node Execution Model:** DataSentinel utilizes in-memory PyArrow tables and local DuckDB instances, designed for single-node vertical scalability. Very large distributed datasets requiring multi-node cluster computation (e.g., Petabyte-scale Spark/Ray clusters) are outside current design scope.
2. **Local & Shared Filesystem Focus:** Storage operations are optimized for local filesystems, mounted NAS, and POSIX-compliant volumes using atomic `os.replace`. Direct native object store connectors (S3 multipart atomic committers, ADLS Gen2, GCS) currently require a mounted filesystem layer.
3. **Synchronous Micro-Batch Ingestion:** Ingestion operates on discrete Parquet batches rather than continuous, unbuffered streaming topics (e.g., Apache Kafka or AWS Kinesis event streams).
4. **Relational Referential Boundaries:** Foreign key referential validation requires customer master tables to fit comfortably in available worker memory.

---

## 16. Future Improvements

The following architectural enhancements are planned on the DataSentinel roadmap:

- **Cloud Object Store Adapters:** Native integration with AWS S3, Google Cloud Storage, and Azure Data Lake Storage Gen2 with cloud-native atomic two-phase commit protocols.
- **Distributed Compute Engine Integration:** Offload large-scale batch validation and reconciliation to Apache Spark, Polars, or Ray for horizontal scaling.
- **Automated Quarantine Remediation:** Introduce a rule-based re-ingestion workflow to patch quarantined records (e.g., backfilling missing defaults, schema remapping) and replay them into the pipeline.
- **Real-Time Webhook Alerting:** Instant Slack, PagerDuty, and Microsoft Teams notifications when breaking schema changes are rejected or quarantine rates exceed predefined thresholds.
- **Schema Registry Integration:** Direct synchronization with Apache Kafka / Confluent Schema Registry and AWS Glue Data Catalog for enterprise-wide schema governance.

---

## 17. License & Authors

- **Project:** DataSentinel
- **Author:** Manini Dhakare
- **Contact:** [maninidhakare28@gmail.com](mailto:maninidhakare28@gmail.com)
- **License:** MIT License
