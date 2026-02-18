"""
MDDE Lite - dbt Model Generator

Generates dbt project structure from MDDE metadata:
- schema.yml from entity/attribute metadata
- SQL models from attribute_mapping (lineage)
- sources.yml from source entities

Related articles:
- "Automating Dimensional Models with Metadata & dbt"
- "Business-Friendly Mapping Meets dbt"
- "From Generic LDM to Executable dbt Models"
- "Supercharging MDDE with dbt-osmosis"

This is a simplified version. The full MDDE framework includes:
- Full dbt project scaffolding
- Advanced YAML schema generation
- dbt-osmosis integration for schema propagation
- Incremental model detection
- Test generation from constraints
"""

import os
import yaml
from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
import duckdb


@dataclass
class DbtModel:
    """Represents a dbt model."""
    name: str
    description: str
    sql: str
    schema_yml: Dict[str, Any]
    materialization: str = "view"
    tags: List[str] = field(default_factory=list)


@dataclass
class DbtSource:
    """Represents a dbt source."""
    name: str
    database: Optional[str]
    schema: str
    tables: List[Dict[str, Any]]


def generate_dbt_project(
    conn: duckdb.DuckDBPyConnection,
    output_dir: str = "generated/dbt",
    project_name: str = "mdde_demo"
) -> Dict[str, Any]:
    """
    Generate a complete dbt project from MDDE metadata.

    Args:
        conn: DuckDB connection with MDDE metadata
        output_dir: Output directory for dbt project
        project_name: Name of the dbt project

    Returns:
        Dictionary with generation statistics
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stats = {
        "models_generated": 0,
        "sources_generated": 0,
        "schema_files": 0,
        "files": []
    }

    # Generate dbt_project.yml
    project_yml = _generate_project_yml(project_name)
    project_file = output_path / "dbt_project.yml"
    with open(project_file, "w") as f:
        yaml.dump(project_yml, f, default_flow_style=False, sort_keys=False)
    stats["files"].append(str(project_file))

    # Create directory structure
    models_dir = output_path / "models"
    models_dir.mkdir(exist_ok=True)

    # Generate sources.yml for source entities
    sources = _generate_sources(conn)
    if sources:
        sources_file = models_dir / "sources.yml"
        with open(sources_file, "w") as f:
            yaml.dump({"version": 2, "sources": sources}, f,
                     default_flow_style=False, sort_keys=False)
        stats["sources_generated"] = len(sources)
        stats["files"].append(str(sources_file))

    # Generate models by layer
    layers = ["staging", "integration", "business"]
    for layer in layers:
        layer_dir = models_dir / layer
        entities = _get_entities_by_layer(conn, layer)

        if entities:
            layer_dir.mkdir(exist_ok=True)

            # Generate SQL models
            for entity in entities:
                model = _generate_model(conn, entity)
                if model:
                    # Write SQL file
                    sql_file = layer_dir / f"{model.name}.sql"
                    with open(sql_file, "w") as f:
                        f.write(model.sql)
                    stats["models_generated"] += 1
                    stats["files"].append(str(sql_file))

            # Generate schema.yml for layer
            schema = _generate_schema_yml(conn, layer)
            if schema.get("models"):
                schema_file = layer_dir / "schema.yml"
                with open(schema_file, "w") as f:
                    yaml.dump(schema, f, default_flow_style=False, sort_keys=False)
                stats["schema_files"] += 1
                stats["files"].append(str(schema_file))

    return stats


def _generate_project_yml(project_name: str) -> Dict[str, Any]:
    """Generate dbt_project.yml content."""
    return {
        "name": project_name,
        "version": "1.0.0",
        "config-version": 2,
        "profile": project_name,
        "model-paths": ["models"],
        "analysis-paths": ["analyses"],
        "test-paths": ["tests"],
        "seed-paths": ["seeds"],
        "macro-paths": ["macros"],
        "snapshot-paths": ["snapshots"],
        "clean-targets": ["target", "dbt_packages"],
        "models": {
            project_name: {
                "staging": {
                    "+materialized": "view",
                    "+tags": ["staging"]
                },
                "integration": {
                    "+materialized": "table",
                    "+tags": ["integration"]
                },
                "business": {
                    "+materialized": "table",
                    "+tags": ["business"]
                }
            }
        }
    }


def _generate_sources(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """Generate sources.yml from source entities."""
    sources = []

    # Get source entities (layer = 'source' or stereotype starts with 'src_')
    source_entities = conn.execute("""
        SELECT entity_id, name, description
        FROM entity
        WHERE layer = 'source' OR stereotype LIKE 'src_%'
        ORDER BY name
    """).fetchall()

    if not source_entities:
        return sources

    # Group by source system (simple grouping by prefix)
    source_tables = []
    for entity_id, name, description in source_entities:
        # Get attributes for this entity
        columns = conn.execute("""
            SELECT name, data_type, description, is_primary_key
            FROM attribute
            WHERE entity_id = ?
            ORDER BY ordinal_position
        """, [entity_id]).fetchall()

        table_entry = {
            "name": name,
            "description": description or f"Source table {name}"
        }

        if columns:
            table_entry["columns"] = [
                {
                    "name": col_name,
                    "description": col_desc or f"Column {col_name}",
                    **({"tests": ["unique", "not_null"]} if is_pk else {})
                }
                for col_name, col_type, col_desc, is_pk in columns
            ]

        source_tables.append(table_entry)

    if source_tables:
        sources.append({
            "name": "raw",
            "description": "Raw source data",
            "tables": source_tables
        })

    return sources


def _get_entities_by_layer(
    conn: duckdb.DuckDBPyConnection,
    layer: str
) -> List[tuple]:
    """Get entities for a specific layer."""
    return conn.execute("""
        SELECT entity_id, name, description, stereotype
        FROM entity
        WHERE layer = ?
        ORDER BY name
    """, [layer]).fetchall()


def _generate_model(
    conn: duckdb.DuckDBPyConnection,
    entity: tuple
) -> Optional[DbtModel]:
    """Generate a dbt model from entity metadata."""
    entity_id, name, description, stereotype = entity

    # Get attribute mappings (lineage) for this entity
    mappings = conn.execute("""
        SELECT
            am.target_attribute_id,
            am.source_entity_id,
            am.source_attribute_id,
            am.mapping_type,
            am.transformation,
            se.name as source_entity_name
        FROM attribute_mapping am
        LEFT JOIN entity se ON am.source_entity_id = se.entity_id
        WHERE am.target_entity_id = ?
        ORDER BY am.target_attribute_id
    """, [entity_id]).fetchall()

    # Get all attributes for this entity
    attributes = conn.execute("""
        SELECT attribute_id, name, data_type, is_derived, expression
        FROM attribute
        WHERE entity_id = ?
        ORDER BY ordinal_position
    """, [entity_id]).fetchall()

    if not attributes:
        return None

    # Build SELECT clause
    select_parts = []
    source_refs = set()

    for attr_id, attr_name, data_type, is_derived, expression in attributes:
        # Find mapping for this attribute
        mapping = next(
            (m for m in mappings if m[0] == attr_id),
            None
        )

        if mapping:
            target_attr, source_ent_id, source_attr, map_type, transform, source_name = mapping

            if map_type == "direct" and source_attr:
                select_parts.append(f"    {source_attr} AS {attr_name}")
                if source_name:
                    source_refs.add(source_name)
            elif map_type == "rename" and source_attr:
                select_parts.append(f"    {source_attr} AS {attr_name}")
                if source_name:
                    source_refs.add(source_name)
            elif map_type == "derived" and transform:
                select_parts.append(f"    {transform} AS {attr_name}")
                if source_name:
                    source_refs.add(source_name)
            elif map_type == "aggregation" and transform:
                select_parts.append(f"    {transform} AS {attr_name}")
                if source_name:
                    source_refs.add(source_name)
            elif map_type == "constant" and transform:
                select_parts.append(f"    {transform} AS {attr_name}")
            else:
                select_parts.append(f"    {attr_name}")
        elif is_derived and expression:
            select_parts.append(f"    {expression} AS {attr_name}")
        else:
            select_parts.append(f"    {attr_name}")

    # Build FROM clause with refs
    if source_refs:
        primary_source = list(source_refs)[0]
        from_clause = f"FROM {{{{ ref('{primary_source}') }}}}"
    else:
        from_clause = f"-- TODO: Add source reference\nFROM source_table"

    # Generate SQL
    sql_parts = [
        f"-- Model: {name}",
        f"-- Description: {description or 'Generated from MDDE metadata'}",
        f"-- Stereotype: {stereotype or 'none'}",
        "",
        "SELECT",
        ",\n".join(select_parts),
        from_clause
    ]

    sql = "\n".join(sql_parts)

    # Build schema.yml entry
    schema_yml = {
        "name": name,
        "description": description or f"Model {name}",
        "columns": [
            {"name": attr_name, "description": f"Column {attr_name}"}
            for _, attr_name, _, _, _ in attributes
        ]
    }

    return DbtModel(
        name=name,
        description=description or "",
        sql=sql,
        schema_yml=schema_yml,
        materialization="table" if stereotype else "view"
    )


def _generate_schema_yml(
    conn: duckdb.DuckDBPyConnection,
    layer: str
) -> Dict[str, Any]:
    """Generate schema.yml for a layer."""
    schema = {
        "version": 2,
        "models": []
    }

    entities = _get_entities_by_layer(conn, layer)

    for entity_id, name, description, stereotype in entities:
        attributes = conn.execute("""
            SELECT name, description, is_primary_key, is_nullable
            FROM attribute
            WHERE entity_id = ?
            ORDER BY ordinal_position
        """, [entity_id]).fetchall()

        model_entry = {
            "name": name,
            "description": description or f"Model {name}"
        }

        if attributes:
            columns = []
            for attr_name, attr_desc, is_pk, is_nullable in attributes:
                col = {
                    "name": attr_name,
                    "description": attr_desc or f"Column {attr_name}"
                }

                # Add tests based on constraints
                tests = []
                if is_pk:
                    tests.extend(["unique", "not_null"])
                elif not is_nullable:
                    tests.append("not_null")

                if tests:
                    col["tests"] = tests

                columns.append(col)

            model_entry["columns"] = columns

        schema["models"].append(model_entry)

    return schema


def generate_model_sql(
    entity_name: str,
    columns: List[str],
    source_ref: Optional[str] = None,
    source_type: str = "ref",  # "ref" or "source"
    transformations: Optional[Dict[str, str]] = None
) -> str:
    """
    Generate a simple dbt model SQL file.

    Args:
        entity_name: Name of the model
        columns: List of column names
        source_ref: Reference to source model/table
        source_type: "ref" for {{ ref() }} or "source" for {{ source() }}
        transformations: Optional dict of column transformations

    Returns:
        Generated SQL string
    """
    transformations = transformations or {}

    # Build SELECT clause
    select_parts = []
    for col in columns:
        if col in transformations:
            select_parts.append(f"    {transformations[col]} AS {col}")
        else:
            select_parts.append(f"    {col}")

    select_clause = ",\n".join(select_parts)

    # Build FROM clause
    if source_ref:
        if source_type == "source":
            from_clause = f"FROM {{{{ source('raw', '{source_ref}') }}}}"
        else:
            from_clause = f"FROM {{{{ ref('{source_ref}') }}}}"
    else:
        from_clause = "FROM source_table"

    return f"""-- Model: {entity_name}

SELECT
{select_clause}
{from_clause}
"""


def generate_schema_yml_entry(
    model_name: str,
    description: str,
    columns: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Generate a schema.yml entry for a model.

    Args:
        model_name: Name of the model
        description: Model description
        columns: List of column definitions

    Returns:
        Dictionary for schema.yml
    """
    return {
        "name": model_name,
        "description": description,
        "columns": columns
    }


if __name__ == "__main__":
    from src.mdde_lite.schema import create_schema

    print("MDDE Lite - dbt Generator Demo")
    print("=" * 50)

    # Create in-memory database with sample data
    conn = create_schema(":memory:")

    # Insert sample entities
    conn.execute("""
        INSERT INTO entity (entity_id, name, description, layer, stereotype)
        VALUES
            ('src_customers', 'raw_customers', 'Source customer data', 'source', 'src_external'),
            ('stg_customers', 'stg_customers', 'Staged customer data', 'staging', 'stg_cleaned'),
            ('int_customers', 'int_customers', 'Integrated customer data', 'integration', NULL),
            ('dim_customer', 'dim_customer', 'Customer dimension', 'business', 'dim_scd2')
    """)

    # Insert sample attributes
    conn.execute("""
        INSERT INTO attribute (attribute_id, entity_id, name, data_type, ordinal_position, is_primary_key)
        VALUES
            ('src_cust_id', 'src_customers', 'customer_id', 'INTEGER', 1, TRUE),
            ('src_cust_name', 'src_customers', 'name', 'VARCHAR', 2, FALSE),
            ('src_cust_email', 'src_customers', 'email', 'VARCHAR', 3, FALSE),

            ('stg_cust_id', 'stg_customers', 'customer_id', 'INTEGER', 1, TRUE),
            ('stg_cust_name', 'stg_customers', 'customer_name', 'VARCHAR', 2, FALSE),
            ('stg_cust_email', 'stg_customers', 'email', 'VARCHAR', 3, FALSE),

            ('dim_cust_sk', 'dim_customer', 'customer_sk', 'INTEGER', 1, TRUE),
            ('dim_cust_id', 'dim_customer', 'customer_id', 'INTEGER', 2, FALSE),
            ('dim_cust_name', 'dim_customer', 'customer_name', 'VARCHAR', 3, FALSE),
            ('dim_valid_from', 'dim_customer', 'valid_from', 'TIMESTAMP', 4, FALSE),
            ('dim_valid_to', 'dim_customer', 'valid_to', 'TIMESTAMP', 5, FALSE)
    """)

    # Insert sample attribute mappings (lineage)
    conn.execute("""
        INSERT INTO attribute_mapping (mapping_id, target_entity_id, target_attribute_id,
                                       source_entity_id, source_attribute_id, mapping_type, transformation)
        VALUES
            ('map_1', 'stg_customers', 'stg_cust_id', 'src_customers', 'src_cust_id', 'direct', NULL),
            ('map_2', 'stg_customers', 'stg_cust_name', 'src_customers', 'src_cust_name', 'rename', NULL),
            ('map_3', 'stg_customers', 'stg_cust_email', 'src_customers', 'src_cust_email', 'direct', NULL),

            ('map_4', 'dim_customer', 'dim_cust_sk', NULL, NULL, 'derived', 'ROW_NUMBER() OVER (ORDER BY customer_id)'),
            ('map_5', 'dim_customer', 'dim_cust_id', 'stg_customers', 'stg_cust_id', 'direct', NULL),
            ('map_6', 'dim_customer', 'dim_cust_name', 'stg_customers', 'stg_cust_name', 'direct', NULL),
            ('map_7', 'dim_customer', 'dim_valid_from', NULL, NULL, 'constant', 'CURRENT_TIMESTAMP'),
            ('map_8', 'dim_customer', 'dim_valid_to', NULL, NULL, 'constant', '''9999-12-31''::TIMESTAMP')
    """)

    # Generate dbt project
    print("\nGenerating dbt project...")
    stats = generate_dbt_project(conn, "workspace/dbt", "mdde_demo")

    print(f"\nGeneration complete:")
    print(f"  Models generated: {stats['models_generated']}")
    print(f"  Sources generated: {stats['sources_generated']}")
    print(f"  Schema files: {stats['schema_files']}")
    print(f"\nFiles created:")
    for f in stats["files"]:
        print(f"  - {f}")

    # Show sample model SQL
    print("\n" + "=" * 50)
    print("Sample generated model (stg_customers.sql):")
    print("-" * 50)

    sample = generate_model_sql(
        "stg_customers",
        ["customer_id", "customer_name", "email", "loaded_at"],
        source_ref="raw_customers",
        source_type="source",
        transformations={
            "customer_name": "TRIM(name)",
            "loaded_at": "CURRENT_TIMESTAMP"
        }
    )
    print(sample)

    conn.close()
