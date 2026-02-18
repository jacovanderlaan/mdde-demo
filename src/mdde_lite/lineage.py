"""
MDDE Lite - Column-Level Lineage Extractor

Extracts column-level lineage from SQL SELECT statements:
- Direct column references (SELECT a FROM t)
- Aliased columns (SELECT a AS b FROM t)
- Expressions (SELECT a + b AS c FROM t)
- Aggregations (SELECT SUM(a) AS total FROM t)

Related articles:
- "Beyond the SELECT Clause: The Hidden Depths of Column Lineage"
- "Dynamic Metadata Lineage: Visualizing Relationships Across Data Models"
- "Context-Aware Lineage: Making Dataflows Conditional and Intelligent"

This is a simplified version. The full MDDE lineage module includes:
- Conditional lineage (CASE WHEN handling)
- Multi-path lineage through CTEs
- Window function lineage
- Subquery correlation
"""

import sqlglot
from sqlglot import exp
from typing import List, Dict, Optional, Set
from dataclasses import dataclass
import duckdb

from .schema import create_schema


@dataclass
class ColumnLineage:
    """Represents lineage for a single target column."""
    target_column: str
    target_alias: Optional[str]
    source_columns: List[str]
    source_tables: List[str]
    mapping_type: str  # direct, rename, derived, aggregation, constant
    expression: Optional[str]

    def __repr__(self):
        sources = ", ".join(f"{t}.{c}" for t, c in zip(self.source_tables, self.source_columns))
        target = self.target_alias or self.target_column
        return f"{sources} -> {target} ({self.mapping_type})"


def extract_lineage(sql_content: str) -> List[ColumnLineage]:
    """
    Extract column-level lineage from a SQL SELECT statement.

    Args:
        sql_content: SQL query string

    Returns:
        List of ColumnLineage objects

    Example:
        >>> sql = "SELECT c.name AS customer_name, SUM(o.amount) AS total FROM customers c JOIN orders o"
        >>> lineage = extract_lineage(sql)
        >>> for l in lineage:
        ...     print(l)
        customers.name -> customer_name (rename)
        orders.amount -> total (aggregation)
    """
    try:
        parsed = sqlglot.parse_one(sql_content)
    except Exception as e:
        return []

    lineage = []

    # Build table alias map
    alias_map = _build_alias_map(parsed)

    # Find the main SELECT
    select = parsed.find(exp.Select)
    if not select:
        return lineage

    # Process each SELECT expression
    for expr in select.expressions:
        col_lineage = _extract_column_lineage(expr, alias_map)
        if col_lineage:
            lineage.append(col_lineage)

    return lineage


def extract_lineage_to_db(
    sql_content: str,
    target_entity_id: str,
    conn: duckdb.DuckDBPyConnection
) -> List[ColumnLineage]:
    """
    Extract lineage and store in attribute_mapping table.

    Args:
        sql_content: SQL query string
        target_entity_id: Entity ID for the target
        conn: Database connection

    Returns:
        List of ColumnLineage objects
    """
    import uuid

    lineage = extract_lineage(sql_content)

    for lin in lineage:
        target_attr_name = lin.target_alias or lin.target_column
        target_attr_id = f"attr_{target_entity_id}_{target_attr_name}"

        for source_table, source_col in zip(lin.source_tables, lin.source_columns):
            source_entity_id = f"ent_{source_table}"
            source_attr_id = f"attr_{source_table}_{source_col}"
            mapping_id = f"map_{str(uuid.uuid4())[:8]}"

            # Ensure source entity exists
            conn.execute("""
                INSERT OR IGNORE INTO entity (entity_id, name, entity_type)
                VALUES (?, ?, 'table')
            """, [source_entity_id, source_table])

            # Insert mapping
            conn.execute("""
                INSERT OR REPLACE INTO attribute_mapping
                (mapping_id, target_entity_id, target_attribute_id,
                 source_entity_id, source_attribute_id, mapping_type, transformation)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                mapping_id,
                target_entity_id,
                target_attr_id,
                source_entity_id,
                source_attr_id,
                lin.mapping_type,
                lin.expression,
            ])

    return lineage


def get_upstream_lineage(
    conn: duckdb.DuckDBPyConnection,
    entity_id: str,
    attribute_name: Optional[str] = None,
    depth: int = 3
) -> Dict:
    """
    Get upstream lineage for an entity or attribute.

    Args:
        conn: Database connection
        entity_id: Starting entity
        attribute_name: Optional specific attribute
        depth: Maximum traversal depth

    Returns:
        Dictionary with lineage tree
    """
    result = {
        "entity_id": entity_id,
        "attributes": [],
        "sources": [],
    }

    # Get entity name
    entity = conn.execute("""
        SELECT name FROM entity WHERE entity_id = ?
    """, [entity_id]).fetchone()
    if entity:
        result["entity_name"] = entity[0]

    # Get attribute mappings
    if attribute_name:
        attr_id = f"attr_{entity_id}_{attribute_name}"
        mappings = conn.execute("""
            SELECT
                am.source_entity_id,
                am.source_attribute_id,
                am.mapping_type,
                am.transformation,
                e.name as source_entity_name,
                a.name as source_attr_name
            FROM attribute_mapping am
            LEFT JOIN entity e ON am.source_entity_id = e.entity_id
            LEFT JOIN attribute a ON am.source_attribute_id = a.attribute_id
            WHERE am.target_entity_id = ?
              AND am.target_attribute_id = ?
        """, [entity_id, attr_id]).fetchall()
    else:
        mappings = conn.execute("""
            SELECT
                am.source_entity_id,
                am.source_attribute_id,
                am.mapping_type,
                am.transformation,
                e.name as source_entity_name,
                a.name as source_attr_name,
                ta.name as target_attr_name
            FROM attribute_mapping am
            LEFT JOIN entity e ON am.source_entity_id = e.entity_id
            LEFT JOIN attribute a ON am.source_attribute_id = a.attribute_id
            LEFT JOIN attribute ta ON am.target_attribute_id = ta.attribute_id
            WHERE am.target_entity_id = ?
        """, [entity_id]).fetchall()

    # Build lineage tree
    for mapping in mappings:
        source_entry = {
            "source_entity_id": mapping[0],
            "source_entity_name": mapping[4],
            "source_attribute": mapping[5],
            "mapping_type": mapping[2],
            "transformation": mapping[3],
        }
        if len(mapping) > 6:
            source_entry["target_attribute"] = mapping[6]

        result["sources"].append(source_entry)

        # Recurse if depth allows
        if depth > 1 and mapping[0]:
            upstream = get_upstream_lineage(conn, mapping[0], None, depth - 1)
            if upstream["sources"]:
                source_entry["upstream"] = upstream

    return result


def get_downstream_lineage(
    conn: duckdb.DuckDBPyConnection,
    entity_id: str,
    attribute_name: Optional[str] = None,
    depth: int = 3
) -> Dict:
    """
    Get downstream lineage (impact analysis) for an entity or attribute.

    Args:
        conn: Database connection
        entity_id: Starting entity
        attribute_name: Optional specific attribute
        depth: Maximum traversal depth

    Returns:
        Dictionary with downstream dependents
    """
    result = {
        "entity_id": entity_id,
        "dependents": [],
    }

    # Get entity name
    entity = conn.execute("""
        SELECT name FROM entity WHERE entity_id = ?
    """, [entity_id]).fetchone()
    if entity:
        result["entity_name"] = entity[0]

    # Get downstream mappings
    if attribute_name:
        attr_id = f"attr_{entity_id}_{attribute_name}"
        mappings = conn.execute("""
            SELECT
                am.target_entity_id,
                am.target_attribute_id,
                am.mapping_type,
                e.name as target_entity_name,
                a.name as target_attr_name
            FROM attribute_mapping am
            LEFT JOIN entity e ON am.target_entity_id = e.entity_id
            LEFT JOIN attribute a ON am.target_attribute_id = a.attribute_id
            WHERE am.source_entity_id = ?
              AND am.source_attribute_id = ?
        """, [entity_id, attr_id]).fetchall()
    else:
        mappings = conn.execute("""
            SELECT DISTINCT
                am.target_entity_id,
                e.name as target_entity_name
            FROM attribute_mapping am
            LEFT JOIN entity e ON am.target_entity_id = e.entity_id
            WHERE am.source_entity_id = ?
        """, [entity_id]).fetchall()

    # Build dependents list
    seen = set()
    for mapping in mappings:
        target_id = mapping[0]
        if target_id and target_id not in seen:
            seen.add(target_id)
            dependent = {
                "target_entity_id": target_id,
                "target_entity_name": mapping[1] if len(mapping) > 1 else None,
            }
            if len(mapping) > 2:
                dependent["target_attribute"] = mapping[4]
                dependent["mapping_type"] = mapping[2]

            # Recurse if depth allows
            if depth > 1:
                downstream = get_downstream_lineage(conn, target_id, None, depth - 1)
                if downstream["dependents"]:
                    dependent["downstream"] = downstream

            result["dependents"].append(dependent)

    return result


# ============================================================
# Helper Functions
# ============================================================


def _build_alias_map(parsed: exp.Expression) -> Dict[str, str]:
    """Build map of table alias -> table name."""
    alias_map = {}

    for table in parsed.find_all(exp.Table):
        table_name = table.name
        alias = table.alias
        if alias:
            alias_map[alias] = table_name
        else:
            alias_map[table_name] = table_name

    return alias_map


def _extract_column_lineage(expr: exp.Expression, alias_map: Dict[str, str]) -> Optional[ColumnLineage]:
    """Extract lineage for a single SELECT expression."""
    target_alias = None
    target_column = None
    source_columns = []
    source_tables = []
    mapping_type = "direct"
    expression_str = None

    # Handle aliased expressions
    if isinstance(expr, exp.Alias):
        target_alias = expr.alias
        inner = expr.this
    else:
        inner = expr

    # Analyze the expression type
    if isinstance(inner, exp.Column):
        # Direct column reference
        target_column = inner.name
        source_columns.append(inner.name)
        table_ref = inner.table if inner.table else None
        if table_ref and table_ref in alias_map:
            source_tables.append(alias_map[table_ref])
        elif table_ref:
            source_tables.append(table_ref)
        else:
            source_tables.append("unknown")

        # Check if it's a rename
        if target_alias and target_alias != inner.name:
            mapping_type = "rename"
        else:
            mapping_type = "direct"

    elif isinstance(inner, exp.Star):
        # SELECT *
        target_column = "*"
        mapping_type = "direct"
        source_columns.append("*")
        source_tables.append("all")

    elif _is_aggregate(inner):
        # Aggregation
        target_column = target_alias or "aggregate"
        mapping_type = "aggregation"
        expression_str = inner.sql()

        # Find source columns within aggregate
        for col in inner.find_all(exp.Column):
            source_columns.append(col.name)
            table_ref = col.table if col.table else None
            if table_ref and table_ref in alias_map:
                source_tables.append(alias_map[table_ref])
            elif table_ref:
                source_tables.append(table_ref)
            else:
                source_tables.append("unknown")

    elif isinstance(inner, exp.Literal):
        # Constant value
        target_column = target_alias or "constant"
        mapping_type = "constant"
        expression_str = inner.sql()

    else:
        # Derived expression
        target_column = target_alias or "expression"
        mapping_type = "derived"
        expression_str = inner.sql()

        # Find all source columns in the expression
        for col in inner.find_all(exp.Column):
            source_columns.append(col.name)
            table_ref = col.table if col.table else None
            if table_ref and table_ref in alias_map:
                source_tables.append(alias_map[table_ref])
            elif table_ref:
                source_tables.append(table_ref)
            else:
                source_tables.append("unknown")

    return ColumnLineage(
        target_column=target_column,
        target_alias=target_alias,
        source_columns=source_columns,
        source_tables=source_tables,
        mapping_type=mapping_type,
        expression=expression_str,
    )


def _is_aggregate(expr: exp.Expression) -> bool:
    """Check if expression is an aggregate function."""
    aggregate_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
    return isinstance(expr, aggregate_types) or any(expr.find(agg) for agg in aggregate_types)


if __name__ == "__main__":
    print("MDDE Lite - Column Lineage Extractor")
    print("=" * 60)

    # Test SQL examples
    examples = [
        # Direct column
        ("Direct", "SELECT customer_id FROM customers"),

        # Rename
        ("Rename", "SELECT customer_id AS cust_id FROM customers"),

        # Expression
        ("Expression", "SELECT first_name || ' ' || last_name AS full_name FROM customers"),

        # Aggregation
        ("Aggregation", "SELECT customer_id, SUM(amount) AS total_amount FROM orders GROUP BY customer_id"),

        # Complex query
        ("Complex", """
            SELECT
                c.customer_id,
                c.name AS customer_name,
                COUNT(o.order_id) AS order_count,
                SUM(o.amount) AS total_spent,
                MAX(o.order_date) AS last_order
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.name
        """),
    ]

    for name, sql in examples:
        print(f"\n--- {name} ---")
        print(f"SQL: {sql.strip()[:60]}...")
        print("Lineage:")
        for lin in extract_lineage(sql):
            print(f"  {lin}")
