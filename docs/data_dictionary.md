# Data Dictionary: SchemaGuard

This document outlines the canonical data contracts, column definitions, constraints, and metadata attributes for the datasets managed by SchemaGuard.

---

## 1. Customers Dataset

- **Table / Entity Name**: `customers`
- **Description**: Master customer registry recording unique identity and profile creation timestamps.

| Column Name | Data Type | Nullable | Constraint / Role | Default Value | Description |
|:---|:---|:---|:---|:---|:---|
| `customer_id` | `int64` | `false` | Primary Key | N/A | Unique deterministic identifier for each registered customer. |
| `customer_name` | `string` | `false` | Required | N/A | Full name of the customer. |
| `email` | `string` | `false` | Unique, Required | N/A | Validated email address associated with the account. |
| `created_at` | `timestamp[us]` | `false` | Required | N/A | Account creation timestamp in UTC (ISO-8601). |

### Technical Audit Columns (Appended during pipeline processing)
| Column Name | Data Type | Nullable | Description |
|:---|:---|:---|:---|
| `source_file` | `string` | `false` | Origin Parquet filename from the raw batch. |
| `run_id` | `string` | `false` | Deterministic run identifier executing the ingestion. |
| `ingested_at` | `timestamp[us]` | `false` | Processing timestamp recorded upon successful target write in UTC. |

---

## 2. Orders Dataset

- **Table / Entity Name**: `orders`
- **Description**: Transactional sales records tracking customer purchases, monetary amounts, and fulfillment states.

| Column Name | Data Type | Nullable | Constraint / Role | Default Value | Description |
|:---|:---|:---|:---|:---|:---|
| `order_id` | `int64` | `false` | Primary Key | N/A | Unique deterministic identifier for the sales order. |
| `customer_id` | `int64` | `false` | Foreign Key (`customers.customer_id`) | N/A | Identifier of the customer placing the order. |
| `order_timestamp` | `timestamp[us]` | `false` | Required | N/A | Transaction timestamp in UTC (ISO-8601). |
| `amount` | `decimal(12,2)` | `false` | Required, Value >= 0 | N/A | Monetary order total. Must be non-negative. |
| `status` | `string` | `false` | Required | N/A | Order fulfillment state (e.g., `PENDING`, `COMPLETED`, `CANCELLED`). |
| `currency` | `string` | `true` | Optional | `null` | ISO 4217 currency code (e.g., `USD`, `EUR`, `GBP`). |
| `sales_channel` | `string` | `true` | Optional | `null` | Channel through which the order was placed (e.g., `web`, `mobile`). |

### Technical Audit Columns (Appended during pipeline processing)
| Column Name | Data Type | Nullable | Description |
|:---|:---|:---|:---|
| `source_file` | `string` | `false` | Origin Parquet filename from the raw batch. |
| `run_id` | `string` | `false` | Deterministic run identifier executing the ingestion. |
| `ingested_at` | `timestamp[us]` | `false` | Processing timestamp recorded upon successful target write in UTC. |

---

## 3. Data Integrity & Validation Rules

1. **Primary Key Uniqueness**:
   - `customers.customer_id` and `orders.order_id` must be unique within accepted datasets. Duplicate IDs in a batch are flagged, logged, and isolated.
2. **Referential Integrity**:
   - Every `orders.customer_id` must resolve to an existing `customers.customer_id`.
3. **Domain & Value Constraints**:
   - `amount >= 0.00`: Negative transaction amounts are invalid and rejected to quarantine.
   - `email`: Must conform to standard email regex syntax (`^[\w\.-]+@[\w\.-]+\.\w+$`).
4. **Schema Nullability vs. Actual Null Values**:
   - Schema nullability is verified during contract matching.
   - Actual data values are verified separately: if a field has `nullable: false`, any record containing null/empty values fails validation and is sent to quarantine even if the schema structure matches.
5. **Timestamp Standardization**:
   - All timestamp columns represent microsecond-precision timestamps in UTC ISO-8601 format (`YYYY-MM-DDTHH:MM:SS.ffffffZ`).
