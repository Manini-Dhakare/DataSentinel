"""
Backend Package for SchemaGuard.

Provides a unified architectural facade over the core data engineering modules:
- backend.schema: Schema detection and evolution classification
- backend.ingestion: Batch processing and data validation
- backend.storage: Processed Parquet storage and DuckDB metadata catalog
- backend.transformation: Multi-batch pipeline orchestrator
- backend.reconciliation: Multi-dimensional record and artifact reconciliation
- backend.monitoring: Operational metrics collector and contracts
- backend.orchestration: Strongly typed execution contracts, idempotency, and retries
"""

import sys
import os

# Ensure project root is available on path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import src.schema as schema
import src.ingestion as ingestion
import src.storage as storage
import src.transformation as transformation
import src.reconciliation as reconciliation
import src.monitoring as monitoring
import src.orchestration as orchestration

__all__ = [
    "schema",
    "ingestion",
    "storage",
    "transformation",
    "reconciliation",
    "monitoring",
    "orchestration",
]
