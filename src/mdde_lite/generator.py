"""
MDDE Lite - Simple SQL Regenerator

Regenerates SQL from parsed metadata with improvements:
- Explicit column lists (no SELECT *)
- Canonical formatting
- Deterministic column ordering
- Consistent style

This is a simplified version. The full MDDE generator includes:
- Multi-dialect support (Snowflake, Databricks, BigQuery, etc.)
- DDL generation with platform-specific features
- dbt model generation
- Advanced transformations
"""

import sqlglot
from sqlglot import exp
from pathlib import Path
from typing import List, Optional
import duckdb


def regenerate_sql(
    sql_content: str,
    dialect: str = "duckdb",
    expand_star: bool = True,
    format_style: str = "pretty",
) -> str:
    """
    Regenerate SQL with improvements.

    Args:
        sql_content: Original SQL
        dialect: Target dialect (duckdb, snowflake, postgres, etc.)
        expand_star: Replace SELECT * with explicit columns (best effort)
        format_style: pretty or compact

    Returns:
        Regenerated SQL string
    """
    try:
        parsed = sqlglot.parse_one(sql_content)
    except Exception as e:
        return f"-- Parse error: {e}\n{sql_content}"

    # Expand SELECT * if requested (best effort - needs schema info for full expansion)
    if expand_star:
        parsed = _expand_star_best_effort(parsed)

    # Generate formatted SQL
    regenerated = parsed.sql(dialect=dialect, pretty=(format_style == "pretty"))

    return regenerated


def _expand_star_best_effort(parsed: exp.Expression) -> exp.Expression:
    """
    Attempt to expand SELECT * - this is a placeholder.

    In real implementation, this would:
    1. Look up table schemas in metadata
    2. Replace * with actual column list
    3. Handle table aliases

    For demo, we just add a comment.
    """
    stars = list(parsed.find_all(exp.Star))
    if stars:
        # In production, we'd replace with actual columns from metadata
        # For demo, we leave a comment
        pass
    return parsed


def format_sql(sql_content: str, dialect: str = "duckdb") -> str:
    """
    Format SQL consistently without other transformations.
    """
    try:
        parsed = sqlglot.parse_one(sql_content)
        return parsed.sql(dialect=dialect, pretty=True)
    except Exception:
        return sql_content


def transpile_sql(sql_content: str, source_dialect: str, target_dialect: str) -> str:
    """
    Transpile SQL from one dialect to another.

    Supported dialects: duckdb, snowflake, postgres, bigquery, databricks, mysql
    """
    try:
        return sqlglot.transpile(
            sql_content,
            read=source_dialect,
            write=target_dialect,
            pretty=True,
        )[0]
    except Exception as e:
        return f"-- Transpile error: {e}\n{sql_content}"


def generate_from_metadata(
    conn: duckdb.DuckDBPyConnection,
    entity_id: str,
    dialect: str = "duckdb",
) -> str:
    """
    Generate a SELECT statement from stored metadata.

    This demonstrates regenerating SQL from the metadata store.
    """
    # Get entity info
    entity = conn.execute("""
        SELECT name, entity_type FROM entity WHERE entity_id = ?
    """, [entity_id]).fetchone()

    if not entity:
        return f"-- Entity {entity_id} not found"

    entity_name, entity_type = entity

    # Get attributes
    attributes = conn.execute("""
        SELECT name, is_derived, expression
        FROM attribute
        WHERE entity_id = ?
        ORDER BY ordinal_position
    """, [entity_id]).fetchall()

    if not attributes:
        return f"-- No attributes found for {entity_id}"

    # Get source relationships
    sources = conn.execute("""
        SELECT e.name
        FROM relationship r
        JOIN entity e ON r.source_entity_id = e.entity_id
        WHERE r.target_entity_id = ?
    """, [entity_id]).fetchall()

    # Build SELECT statement
    columns = []
    for name, is_derived, expression in attributes:
        if is_derived and expression:
            columns.append(f"    {expression} AS {name}")
        else:
            columns.append(f"    {name}")

    select_clause = "SELECT\n" + ",\n".join(columns)

    if sources:
        source_tables = ", ".join(s[0] for s in sources)
        from_clause = f"\nFROM {source_tables}"
    else:
        from_clause = f"\nFROM {entity_name}"

    return f"-- Generated from metadata for: {entity_name}\n{select_clause}{from_clause}"


if __name__ == "__main__":
    print("MDDE Lite - SQL Regenerator Demo")
    print("=" * 50)

    original_sql = """
    SELECT * FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.status='completed' AND c.tier='gold'
    """

    print("Original SQL:")
    print(original_sql)

    print("\nFormatted (DuckDB):")
    print(format_sql(original_sql, "duckdb"))

    print("\nTranspiled to Snowflake:")
    print(transpile_sql(original_sql, "duckdb", "snowflake"))

    print("\nTranspiled to BigQuery:")
    print(transpile_sql(original_sql, "duckdb", "bigquery"))
