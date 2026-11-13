"""
Database Package for SchemaGuard.

Provides DuckDB metadata connection management and schema migration helpers.
"""

import os
from typing import Optional
import duckdb

DEFAULT_METADATA_PATH = "data/metadata/schemaguard_metadata.duckdb"
SCHEMA_SQL_PATH = os.path.join(os.path.dirname(__file__), "schema.sql")


def get_db_connection(db_path: Optional[str] = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Returns a DuckDB connection with optional read_only mode."""
    target_path = db_path or DEFAULT_METADATA_PATH
    if not read_only and target_path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(target_path)), exist_ok=True)
    return duckdb.connect(target_path, read_only=read_only)


def initialize_schema(db_path: Optional[str] = None) -> None:
    """Applies schema DDL to the target DuckDB instance."""
    conn = get_db_connection(db_path, read_only=False)
    try:
        with open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
            ddl = f.read()
        conn.execute(ddl)
    finally:
        conn.close()


__all__ = ["get_db_connection", "initialize_schema", "DEFAULT_METADATA_PATH", "SCHEMA_SQL_PATH"]
