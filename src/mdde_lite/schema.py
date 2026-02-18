"""
MDDE Lite - Minimal Metadata Schema

This module defines the core metadata tables for the educational edition.
The full MDDE framework has 60+ tables; this lite version focuses on 5 essential ones.
"""

import duckdb
from pathlib import Path


def create_schema(db_path: str = "mdde_lite.duckdb") -> duckdb.DuckDBPyConnection:
    """
    Create the minimal MDDE metadata schema.

    Tables:
    - entity: Core business entities (tables/views)
    - attribute: Columns within entities
    - relationship: Links between entities
    - attribute_mapping: Column-level lineage
    - optimizer_diagnostics: SQL quality findings
    """
    conn = duckdb.connect(db_path)

    # 1. Entity - represents tables, views, CTEs
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity (
            entity_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            description VARCHAR,
            entity_type VARCHAR DEFAULT 'table',  -- table, view, cte
            layer VARCHAR,                        -- source, staging, integration, business
            stereotype VARCHAR,                   -- dv_hub, dim_fact, stg_raw, etc.
            source_file VARCHAR,                  -- original SQL file path
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 2. Attribute - columns within entities
    conn.execute("""
        CREATE TABLE IF NOT EXISTS attribute (
            attribute_id VARCHAR PRIMARY KEY,
            entity_id VARCHAR NOT NULL REFERENCES entity(entity_id),
            name VARCHAR NOT NULL,
            data_type VARCHAR,
            ordinal_position INTEGER,
            is_nullable BOOLEAN DEFAULT TRUE,
            is_primary_key BOOLEAN DEFAULT FALSE,
            is_derived BOOLEAN DEFAULT FALSE,     -- calculated column
            expression VARCHAR,                   -- derivation expression if derived
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 3. Relationship - foreign key relationships
    conn.execute("""
        CREATE TABLE IF NOT EXISTS relationship (
            relationship_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            source_entity_id VARCHAR NOT NULL REFERENCES entity(entity_id),
            target_entity_id VARCHAR NOT NULL REFERENCES entity(entity_id),
            cardinality VARCHAR DEFAULT 'many_to_one',  -- one_to_one, one_to_many, many_to_one, many_to_many
            source_attribute_id VARCHAR,
            target_attribute_id VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 4. Attribute Mapping - column-level lineage
    conn.execute("""
        CREATE TABLE IF NOT EXISTS attribute_mapping (
            mapping_id VARCHAR PRIMARY KEY,
            target_entity_id VARCHAR NOT NULL REFERENCES entity(entity_id),
            target_attribute_id VARCHAR NOT NULL,
            source_entity_id VARCHAR REFERENCES entity(entity_id),
            source_attribute_id VARCHAR,
            mapping_type VARCHAR DEFAULT 'direct',  -- direct, rename, derived, constant, aggregation
            transformation VARCHAR,                 -- expression if transformed
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 5. Optimizer Diagnostics - SQL quality findings
    conn.execute("""
        CREATE TABLE IF NOT EXISTS optimizer_diagnostics (
            diagnostic_id VARCHAR PRIMARY KEY,
            entity_id VARCHAR REFERENCES entity(entity_id),
            source_file VARCHAR,
            diagnostic_type VARCHAR NOT NULL,       -- SELECT_STAR, MISSING_ALIAS, etc.
            severity VARCHAR DEFAULT 'warning',     -- info, warning, error
            message VARCHAR NOT NULL,
            line_number INTEGER,
            column_number INTEGER,
            suggestion VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    return conn


def get_schema_info(conn: duckdb.DuckDBPyConnection) -> dict:
    """Return information about the schema tables."""
    tables = conn.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    result = {}
    for (table_name,) in tables:
        columns = conn.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()
        result[table_name] = columns

    return result


if __name__ == "__main__":
    # Demo: create schema and show structure
    conn = create_schema(":memory:")

    print("MDDE Lite - Metadata Schema")
    print("=" * 50)

    for table, columns in get_schema_info(conn).items():
        print(f"\n{table}:")
        for col_name, data_type, nullable in columns:
            null_str = "" if nullable == "YES" else " NOT NULL"
            print(f"  - {col_name}: {data_type}{null_str}")

    conn.close()
