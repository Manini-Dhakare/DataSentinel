# SchemaGuard Pipeline Execution Workflow

## 1. Complete End-to-End Lifecycle

The SchemaGuard pipeline coordinates multi-batch Parquet processing through a deterministic, fault-tolerant sequence:

```text
[Input Landing Zone]
        │
        ▼ (1. Batch Discovery & Deduplication)
[InputBatchReference List]
        │
        ▼ (2. Schema Detection & Delta Analysis)
[Schema Comparison Result]
        │
        ▼ (3. Evolution Classification Gate)
   ┌────┴────────────────────────┐
   ▼                             ▼
[COMPATIBLE / WIDENED]       [BREAKING / MANUAL REVIEW]
   │                             │
   ▼ (4. Schema Evolution)       ▼ (Isolated Rejection)
[Updated Schema]             [SCHEMA_REJECTED Status]
   │                             │
   ▼ (5. Data Validation)        │
   ├──────────────┐              │
   ▼ (Valid)      ▼ (Invalid)    │
[Valid Rows]   [Invalid Rows]    │
   │              │              │
   │              ▼ (6. Quarantine Routing)
   │           [Durable Parquet + Sidecar JSON]
   │                             │
   ▼ (7. Atomic Publication)     │
[Processed Parquet + Audit Meta] │
   │                             │
   └──────────────┬──────────────┘
                  ▼ (8. Metadata Catalog Logging)
          [DuckDB Transaction Commit]
                  │
                  ▼ (9. End-to-End Reconciliation)
          [Reconciliation Result (I = V + Q + U)]
                  │
                  ▼ (10. Operational Metrics & Reports)
          [JSON / Markdown / Telemetry Export]
```

---

## 2. Detailed Execution Stages

### Stage 1: Batch Discovery & Deduplication
- **Scanner:** Discovers all `.parquet` files in the specified landing directory (`--input-dir`).
- **Batch ID Extraction:** Deterministically extracts identifiers via regular expressions:
  - Prefix pattern: `^(batch_\d+[a-zA-Z0-9]*)` (e.g. `batch_001_original.parquet` $\rightarrow$ `batch_001`).
  - Fallback: UUID pattern or sanitized alphanumeric base name.
- **Collision Protection:** If multiple files map to the same batch ID in the same run, discovery raises `DISCOVERY_ERROR`, halting execution before data modification occurs.

### Stage 2: Schema Detection & Evolution Gates
- The incoming file schema is compared against canonical table definitions in `config/source_schema.yaml`.
- The `EvolutionClassifier` maps differences to one of four statuses:
  1. `NO_CHANGE`: Schemas are identical; execution continues immediately.
  2. `COMPATIBLE`: Nullable columns added; schema evolver adds column with `null` default.
  3. `APPROVED_TYPE_WIDENING`: Type widening explicitly approved in `config/evolution_rules.yaml` (e.g., `int32` to `int64`, `float` to `double`); schema is upgraded.
  4. `MANUAL_REVIEW` / `BREAKING`: Dropped columns, non-nullable additions, unapproved type shifts. Batch is marked `SCHEMA_REJECTED`. Valid rows are set to 0; all batch records are attributed as unaccounted.

### Stage 3: Data Validation & Referential Integrity
- Clean and evolved batches are validated against business rules:
  - **Pydantic Model Constraints:** Field formats, enum values, positive amount constraints (`amount > 0`).
  - **Referential Integrity:** Verifies that `customer_id` exists in the customer master table (`data/generated/customers.parquet`).
  - **Duplicate Primary Keys:** Tracks duplicate `order_id`s within the batch. The first occurrence is preserved as valid; subsequent occurrences are marked invalid.

### Stage 4: Durable Quarantine Persistence
- If invalid records are detected, they are routed to `QuarantineManager`:
  - Path: `data/quarantine/<table_name>/<batch_id>/invalid_records.parquet`
  - Sidecar: `data/quarantine/<table_name>/<batch_id>/quarantine_metadata.json`
- Sidecar metadata captures:
  - SHA-256 table fingerprint
  - Total batch records & invalid record count
  - Failure categories (`DUPLICATE_KEY`, `INVALID_FOREIGN_KEY`, `RANGE_VIOLATION`, `NULL_VIOLATION`)
  - Affected column names and failure diagnostics

### Stage 5: Atomic Storage Publication
- Valid records are passed to `StorageManager`:
  - Injects technical audit columns:
    - `source_file`: Original input file name.
    - `run_id`: Unique execution identifier.
    - `ingested_at`: UTC timestamp string.
  - Generates target path: `data/processed/<table_name>/<batch_id>.parquet`.
  - Stages data in `<target_path>.tmp.<uuid>`.
  - Atomically renames temporary file to target path via `os.replace`.

### Stage 6: Operational Metadata Cataloging
- Commits execution telemetry into DuckDB (`data/metadata/schemaguard_metadata.duckdb`) in an ACID transaction:
  - `pipeline_runs`: Aggregates run-level counters and durations.
  - `batch_executions`: Records batch outcomes and output artifact paths.
  - `schema_evolutions`: Captures applied schema diffs and rejection gates.
  - `validation_metrics`: Logs granular violation counters.
  - `idempotency_records`: Records content fingerprint and registration state.

---

## 3. Reconciliation & Conservation Invariant

Reconciliation evaluates the authoritative `PipelineExecutionResult` and verifies physical storage state against the mathematical identity:

$$\mathbf{I = V + Q + U}$$

- $\mathbf{I}$ (**Total Input Records**): Sum of all rows discovered across all input batches.
- $\mathbf{V}$ (**Valid Persisted Records**): Records validated and verified as durably committed to storage.
- $\mathbf{Q}$ (**Quarantined Records**): Invalid records segregated in quarantine Parquet sidecars.
- $\mathbf{U}$ (**Unaccounted Records**): Records in schema-rejected batches or failed ingestion tasks.

### Durability Verification Rule
A record is counted as **persisted** ($V$) only if:
1. It passed row-level data validation.
2. The storage operation returned `StorageStatus.COMMITTED` or `StorageStatus.ALREADY_PROCESSED`.
3. The resulting `PROCESSED_PARQUET` file physically exists on disk and is explicitly flagged with `is_durable=True`.
If storage persistence fails for any reason, validated rows are NOT counted as persisted, and a `STORAGE_FAILURE` issue is registered.

---

## 4. Idempotency & Reprocessing Mechanics

The `IdempotencyManager` prevents redundant computation while allowing intentional re-runs:

| Scenario | Input Condition | Flag | Action | Status |
| :--- | :--- | :---: | :--- | :--- |
| **First Run** | Batch never seen before | Any | Full ingestion, storage, and registration | `NEW` $\rightarrow$ `SUCCESS` |
| **Accidental Duplicate** | Identical file bytes & batch ID | `--reprocess` omitted | Skips processing; prevents duplicate writes; preserves existing files | `ALREADY_PROCESSED` |
| **Legitimate Reprocessing** | Source data changed (new hash) | `--reprocess` enabled | Reprocesses batch; atomically updates storage via `os.replace`; updates catalog | `CHANGED_INPUT` $\rightarrow$ `SUCCESS` |
| **Conflicting Modification** | Source data changed (new hash) | `--reprocess` omitted | Fails safely with `STORAGE_FAILED` to prevent accidental data drift | `STORAGE_FAILED` |

---

## 5. Safe Retries & Error Classification

The `RetryPolicy` manages transient infrastructure faults:

### Classification Matrix
- **Retryable Transient Exceptions:**
  - `duckdb.OperationalError`: Database locked by another writer.
  - `duckdb.TransactionException`: Transaction conflict.
  - `duckdb.IOException`: Temporary storage I/O delay.
  - `PermissionError`: Windows file sharing contention (WinError 32 / 33).
  - `TimeoutError` / `ConnectionError`: Transient network/socket locks.
- **Non-Retryable Deterministic Exceptions:**
  - `pyarrow.ArrowInvalid` / `pyarrow.ArrowTypeError`: Incompatible binary schemas.
  - `ValueError` / `TypeError` / `KeyError`: Data validation or config errors.
  - `FileNotFoundError`: Missing dependencies or input files.

### Bounded Execution Algorithm
$$t_{\text{wait}} = \min(d_{\max}, d_0 \times b^k) \times (1 + \text{random} \times \text{jitter})$$
- Default: `max_retries = 3`, `initial_delay = 0.05s`, `backoff_factor = 2.0`, `max_delay = 1.0s`.
- If attempts exceed `max_retries`, a `RetryExhaustedError` is raised and converted into a contained `ExecutionError`.
