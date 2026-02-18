"""
MDDE Lite - Documentation Generator

Generates documentation from MDDE metadata:
- Entity documentation (markdown)
- Data dictionary
- Lineage documentation
- Model overview

Related articles:
- "From Metadata to Documentation"
- "From Metadata to Living Documentation"
- "Automating Release Notes"

This is a simplified version. The full MDDE framework includes:
- Full HTML documentation site generation
- Confluence/Notion integration
- Auto-updating documentation
- Release notes generation
"""

from pathlib import Path
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import duckdb


@dataclass
class EntityDoc:
    """Documentation for a single entity."""
    entity_id: str
    name: str
    description: str
    entity_type: str
    layer: str
    stereotype: str
    attributes: List[Dict[str, Any]]
    relationships: List[Dict[str, Any]]
    lineage: List[Dict[str, Any]]


def generate_entity_docs(
    conn: duckdb.DuckDBPyConnection,
    output_dir: str = "generated/docs",
    include_lineage: bool = True
) -> Dict[str, Any]:
    """
    Generate markdown documentation for all entities.

    Args:
        conn: DuckDB connection with MDDE metadata
        output_dir: Output directory for documentation
        include_lineage: Whether to include lineage information

    Returns:
        Dictionary with generation statistics
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stats = {
        "entities_documented": 0,
        "files": []
    }

    # Get all entities
    entities = conn.execute("""
        SELECT entity_id, name, description, entity_type, layer, stereotype
        FROM entity
        ORDER BY layer, name
    """).fetchall()

    # Generate individual entity docs
    for entity_row in entities:
        entity_id, name, description, entity_type, layer, stereotype = entity_row

        # Get attributes
        attributes = conn.execute("""
            SELECT name, data_type, description, is_primary_key, is_nullable, is_derived, expression
            FROM attribute
            WHERE entity_id = ?
            ORDER BY ordinal_position
        """, [entity_id]).fetchall()

        # Get relationships
        relationships = conn.execute("""
            SELECT r.name, r.cardinality,
                   se.name as source_entity, te.name as target_entity
            FROM relationship r
            JOIN entity se ON r.source_entity_id = se.entity_id
            JOIN entity te ON r.target_entity_id = te.entity_id
            WHERE r.source_entity_id = ? OR r.target_entity_id = ?
        """, [entity_id, entity_id]).fetchall()

        # Get lineage
        lineage = []
        if include_lineage:
            lineage = conn.execute("""
                SELECT
                    am.target_attribute_id,
                    ta.name as target_attr_name,
                    se.name as source_entity,
                    sa.name as source_attr_name,
                    am.mapping_type,
                    am.transformation
                FROM attribute_mapping am
                JOIN attribute ta ON am.target_attribute_id = ta.attribute_id
                LEFT JOIN entity se ON am.source_entity_id = se.entity_id
                LEFT JOIN attribute sa ON am.source_attribute_id = sa.attribute_id
                WHERE am.target_entity_id = ?
            """, [entity_id]).fetchall()

        # Generate markdown
        md_content = _generate_entity_markdown(
            name, description, entity_type, layer, stereotype,
            attributes, relationships, lineage
        )

        # Write file
        entity_file = output_path / f"{name}.md"
        with open(entity_file, "w") as f:
            f.write(md_content)

        stats["entities_documented"] += 1
        stats["files"].append(str(entity_file))

    # Generate index
    index_content = _generate_index_markdown(entities)
    index_file = output_path / "index.md"
    with open(index_file, "w") as f:
        f.write(index_content)
    stats["files"].append(str(index_file))

    # Generate data dictionary
    dict_content = _generate_data_dictionary(conn)
    dict_file = output_path / "data_dictionary.md"
    with open(dict_file, "w") as f:
        f.write(dict_content)
    stats["files"].append(str(dict_file))

    return stats


def _generate_entity_markdown(
    name: str,
    description: str,
    entity_type: str,
    layer: str,
    stereotype: str,
    attributes: List[tuple],
    relationships: List[tuple],
    lineage: List[tuple]
) -> str:
    """Generate markdown for a single entity."""
    lines = [
        f"# {name}",
        "",
        description or "*No description provided*",
        "",
        "## Overview",
        "",
        "| Property | Value |",
        "|----------|-------|",
        f"| Type | {entity_type or 'table'} |",
        f"| Layer | {layer or '-'} |",
        f"| Stereotype | {stereotype or '-'} |",
        "",
    ]

    # Attributes section
    if attributes:
        lines.extend([
            "## Attributes",
            "",
            "| Name | Data Type | PK | Nullable | Description |",
            "|------|-----------|:--:|:--------:|-------------|"
        ])

        for attr in attributes:
            attr_name, data_type, attr_desc, is_pk, is_nullable, is_derived, expression = attr
            pk_mark = "Yes" if is_pk else ""
            null_mark = "Yes" if is_nullable else "No"
            desc = attr_desc or ""
            if is_derived and expression:
                desc = f"*Derived:* `{expression}`" + (f" - {desc}" if desc else "")

            lines.append(f"| {attr_name} | {data_type or '-'} | {pk_mark} | {null_mark} | {desc} |")

        lines.append("")

    # Relationships section
    if relationships:
        lines.extend([
            "## Relationships",
            "",
            "| Name | Cardinality | Source | Target |",
            "|------|-------------|--------|--------|"
        ])

        for rel in relationships:
            rel_name, cardinality, source_ent, target_ent = rel
            lines.append(f"| {rel_name or '-'} | {cardinality} | {source_ent} | {target_ent} |")

        lines.append("")

    # Lineage section
    if lineage:
        lines.extend([
            "## Column Lineage",
            "",
            "| Target Column | Source Entity | Source Column | Mapping Type | Transformation |",
            "|---------------|---------------|---------------|--------------|----------------|"
        ])

        for lin in lineage:
            target_attr_id, target_name, source_ent, source_attr, map_type, transform = lin
            source_ent = source_ent or "-"
            source_attr = source_attr or "-"
            transform = f"`{transform}`" if transform else "-"
            lines.append(f"| {target_name} | {source_ent} | {source_attr} | {map_type} | {transform} |")

        lines.append("")

    # Footer
    lines.extend([
        "---",
        f"*Generated by MDDE Lite on {datetime.now().strftime('%Y-%m-%d %H:%M')}*"
    ])

    return "\n".join(lines)


def _generate_index_markdown(entities: List[tuple]) -> str:
    """Generate index/overview markdown."""
    lines = [
        "# Data Model Documentation",
        "",
        f"*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        "",
        "## Entities by Layer",
        ""
    ]

    # Group by layer
    layers = {}
    for entity_row in entities:
        entity_id, name, description, entity_type, layer, stereotype = entity_row
        layer = layer or "other"
        if layer not in layers:
            layers[layer] = []
        layers[layer].append((name, description, stereotype))

    # Layer order
    layer_order = ["source", "staging", "integration", "business", "other"]

    for layer in layer_order:
        if layer in layers:
            lines.extend([
                f"### {layer.title()} Layer",
                "",
                "| Entity | Stereotype | Description |",
                "|--------|------------|-------------|"
            ])

            for name, description, stereotype in sorted(layers[layer]):
                desc = (description[:50] + "...") if description and len(description) > 50 else (description or "-")
                lines.append(f"| [{name}]({name}.md) | {stereotype or '-'} | {desc} |")

            lines.append("")

    # Summary
    lines.extend([
        "## Summary",
        "",
        f"- **Total Entities**: {len(entities)}",
    ])

    for layer in layer_order:
        if layer in layers:
            lines.append(f"- **{layer.title()}**: {len(layers[layer])} entities")

    return "\n".join(lines)


def _generate_data_dictionary(conn: duckdb.DuckDBPyConnection) -> str:
    """Generate a complete data dictionary."""
    lines = [
        "# Data Dictionary",
        "",
        f"*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        "",
        "## All Attributes",
        "",
        "| Entity | Attribute | Data Type | Description |",
        "|--------|-----------|-----------|-------------|"
    ]

    # Get all attributes with entity info
    attributes = conn.execute("""
        SELECT e.name as entity_name, a.name as attr_name,
               a.data_type, a.description, a.is_primary_key
        FROM attribute a
        JOIN entity e ON a.entity_id = e.entity_id
        ORDER BY e.name, a.ordinal_position
    """).fetchall()

    for entity_name, attr_name, data_type, description, is_pk in attributes:
        desc = description or "-"
        if is_pk:
            attr_name = f"**{attr_name}** (PK)"
        lines.append(f"| {entity_name} | {attr_name} | {data_type or '-'} | {desc} |")

    # Add statistics
    entity_count = conn.execute("SELECT COUNT(*) FROM entity").fetchone()[0]
    attr_count = len(attributes)
    rel_count = conn.execute("SELECT COUNT(*) FROM relationship").fetchone()[0]

    lines.extend([
        "",
        "## Statistics",
        "",
        f"- **Entities**: {entity_count}",
        f"- **Attributes**: {attr_count}",
        f"- **Relationships**: {rel_count}",
    ])

    return "\n".join(lines)


def generate_lineage_doc(
    conn: duckdb.DuckDBPyConnection,
    entity_name: str
) -> str:
    """
    Generate detailed lineage documentation for an entity.

    Args:
        conn: DuckDB connection
        entity_name: Name of the entity

    Returns:
        Markdown string with lineage documentation
    """
    # Get entity
    entity = conn.execute("""
        SELECT entity_id, name, description
        FROM entity
        WHERE name = ?
    """, [entity_name]).fetchone()

    if not entity:
        return f"# Error\n\nEntity '{entity_name}' not found."

    entity_id, name, description = entity

    lines = [
        f"# Lineage: {name}",
        "",
        description or "",
        "",
        "## Upstream Dependencies",
        ""
    ]

    # Get upstream lineage
    upstream = conn.execute("""
        SELECT DISTINCT se.name as source_entity
        FROM attribute_mapping am
        JOIN entity se ON am.source_entity_id = se.entity_id
        WHERE am.target_entity_id = ?
    """, [entity_id]).fetchall()

    if upstream:
        lines.append("```mermaid")
        lines.append("graph LR")
        for (source_name,) in upstream:
            lines.append(f"    {source_name} --> {name}")
        lines.append("```")
    else:
        lines.append("*No upstream dependencies*")

    # Detailed mappings
    mappings = conn.execute("""
        SELECT
            ta.name as target_attr,
            se.name as source_entity,
            sa.name as source_attr,
            am.mapping_type,
            am.transformation
        FROM attribute_mapping am
        JOIN attribute ta ON am.target_attribute_id = ta.attribute_id
        LEFT JOIN entity se ON am.source_entity_id = se.entity_id
        LEFT JOIN attribute sa ON am.source_attribute_id = sa.attribute_id
        WHERE am.target_entity_id = ?
        ORDER BY ta.ordinal_position
    """, [entity_id]).fetchall()

    if mappings:
        lines.extend([
            "",
            "## Column Mappings",
            "",
            "| Target | Source | Mapping | Transformation |",
            "|--------|--------|---------|----------------|"
        ])

        for target_attr, source_ent, source_attr, map_type, transform in mappings:
            source = f"{source_ent}.{source_attr}" if source_ent and source_attr else "-"
            transform = f"`{transform}`" if transform else "-"
            lines.append(f"| {target_attr} | {source} | {map_type} | {transform} |")

    # Downstream usage
    lines.extend([
        "",
        "## Downstream Usage",
        ""
    ])

    downstream = conn.execute("""
        SELECT DISTINCT te.name as target_entity
        FROM attribute_mapping am
        JOIN entity te ON am.target_entity_id = te.entity_id
        WHERE am.source_entity_id = ?
    """, [entity_id]).fetchall()

    if downstream:
        lines.append("```mermaid")
        lines.append("graph LR")
        for (target_name,) in downstream:
            lines.append(f"    {name} --> {target_name}")
        lines.append("```")
    else:
        lines.append("*No downstream dependencies*")

    return "\n".join(lines)


if __name__ == "__main__":
    from src.mdde_lite.schema import create_schema

    print("MDDE Lite - Documentation Generator Demo")
    print("=" * 50)

    # Create in-memory database with sample data
    conn = create_schema(":memory:")

    # Insert sample data
    conn.execute("""
        INSERT INTO entity (entity_id, name, description, entity_type, layer, stereotype)
        VALUES
            ('src_customers', 'raw_customers', 'Raw customer data from CRM system', 'table', 'source', 'src_external'),
            ('stg_customers', 'stg_customers', 'Cleaned and standardized customer data', 'table', 'staging', 'stg_cleaned'),
            ('dim_customer', 'dim_customer', 'Customer dimension with SCD Type 2 history', 'table', 'business', 'dim_scd2')
    """)

    conn.execute("""
        INSERT INTO attribute (attribute_id, entity_id, name, data_type, ordinal_position, is_primary_key, description)
        VALUES
            ('src_cust_1', 'src_customers', 'customer_id', 'INTEGER', 1, TRUE, 'Unique customer identifier'),
            ('src_cust_2', 'src_customers', 'name', 'VARCHAR(100)', 2, FALSE, 'Customer full name'),
            ('src_cust_3', 'src_customers', 'email', 'VARCHAR(255)', 3, FALSE, 'Email address'),

            ('stg_cust_1', 'stg_customers', 'customer_id', 'INTEGER', 1, TRUE, 'Customer identifier'),
            ('stg_cust_2', 'stg_customers', 'customer_name', 'VARCHAR(100)', 2, FALSE, 'Cleaned customer name'),
            ('stg_cust_3', 'stg_customers', 'email_address', 'VARCHAR(255)', 3, FALSE, 'Validated email'),

            ('dim_cust_1', 'dim_customer', 'customer_sk', 'INTEGER', 1, TRUE, 'Surrogate key'),
            ('dim_cust_2', 'dim_customer', 'customer_id', 'INTEGER', 2, FALSE, 'Natural key'),
            ('dim_cust_3', 'dim_customer', 'customer_name', 'VARCHAR(100)', 3, FALSE, 'Customer name'),
            ('dim_cust_4', 'dim_customer', 'valid_from', 'TIMESTAMP', 4, FALSE, 'SCD2 valid from'),
            ('dim_cust_5', 'dim_customer', 'valid_to', 'TIMESTAMP', 5, FALSE, 'SCD2 valid to')
    """)

    conn.execute("""
        INSERT INTO attribute_mapping (mapping_id, target_entity_id, target_attribute_id,
                                       source_entity_id, source_attribute_id, mapping_type, transformation)
        VALUES
            ('map_1', 'stg_customers', 'stg_cust_1', 'src_customers', 'src_cust_1', 'direct', NULL),
            ('map_2', 'stg_customers', 'stg_cust_2', 'src_customers', 'src_cust_2', 'derived', 'TRIM(UPPER(name))'),
            ('map_3', 'stg_customers', 'stg_cust_3', 'src_customers', 'src_cust_3', 'derived', 'LOWER(email)'),
            ('map_4', 'dim_customer', 'dim_cust_2', 'stg_customers', 'stg_cust_1', 'direct', NULL),
            ('map_5', 'dim_customer', 'dim_cust_3', 'stg_customers', 'stg_cust_2', 'direct', NULL)
    """)

    conn.execute("""
        INSERT INTO relationship (relationship_id, name, source_entity_id, target_entity_id, cardinality)
        VALUES
            ('rel_1', 'staging_to_source', 'stg_customers', 'src_customers', 'many_to_one'),
            ('rel_2', 'dim_to_staging', 'dim_customer', 'stg_customers', 'many_to_one')
    """)

    # Generate documentation
    print("\nGenerating documentation...")
    stats = generate_entity_docs(conn, "workspace/docs")

    print(f"\nGeneration complete:")
    print(f"  Entities documented: {stats['entities_documented']}")
    print(f"\nFiles created:")
    for f in stats["files"]:
        print(f"  - {f}")

    # Show sample lineage doc
    print("\n" + "=" * 50)
    print("Sample Lineage Documentation (stg_customers):")
    print("-" * 50)
    lineage_doc = generate_lineage_doc(conn, "stg_customers")
    print(lineage_doc[:800] + "...")

    conn.close()
