"""
MDDE Lite - Simple SQL Parser

Uses sqlglot to parse SQL files and extract metadata:
- Tables and CTEs as entities
- Columns as attributes
- JOIN conditions as relationships
- Column lineage through SELECT mappings
"""

import sqlglot
from sqlglot import exp
from pathlib import Path
from typing import Optional
import uuid
import duckdb

from .schema import create_schema


def generate_id() -> str:
    """Generate a short unique ID."""
    return str(uuid.uuid4())[:8]


def parse_sql_file(sql_path: str, conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Parse a SQL file and store metadata in the database.

    Returns dict with parsing results.
    """
    sql_path = Path(sql_path)
    sql_content = sql_path.read_text()

    # Derive entity name from filename
    entity_name = sql_path.stem
    entity_id = f"ent_{entity_name}"

    results = {
        "entity_id": entity_id,
        "entity_name": entity_name,
        "source_file": str(sql_path),
        "attributes": [],
        "sources": [],
        "ctes": [],
    }

    try:
        # Parse SQL using sqlglot
        parsed = sqlglot.parse_one(sql_content)

        # Extract CTEs if present
        ctes = list(parsed.find_all(exp.CTE))
        for cte in ctes:
            cte_name = cte.alias
            cte_id = f"cte_{entity_name}_{cte_name}"
            results["ctes"].append(cte_name)

            # Register CTE as entity
            conn.execute("""
                INSERT OR REPLACE INTO entity (entity_id, name, entity_type, source_file)
                VALUES (?, ?, 'cte', ?)
            """, [cte_id, cte_name, str(sql_path)])

        # Find the main SELECT statement
        select = parsed.find(exp.Select)
        if select:
            # Extract columns from SELECT
            for i, col_expr in enumerate(select.expressions):
                attr_name = None
                is_derived = False
                expression = None

                if isinstance(col_expr, exp.Alias):
                    attr_name = col_expr.alias
                    inner = col_expr.this
                    if not isinstance(inner, exp.Column):
                        is_derived = True
                        expression = inner.sql()
                elif isinstance(col_expr, exp.Column):
                    attr_name = col_expr.name
                elif isinstance(col_expr, exp.Star):
                    attr_name = "*"
                    is_derived = False
                else:
                    attr_name = f"col_{i}"
                    is_derived = True
                    expression = col_expr.sql()

                if attr_name:
                    attr_id = f"attr_{entity_name}_{attr_name}"
                    results["attributes"].append({
                        "id": attr_id,
                        "name": attr_name,
                        "is_derived": is_derived,
                        "expression": expression,
                        "ordinal": i + 1,
                    })

            # Extract source tables from FROM clause
            for table in select.find_all(exp.Table):
                table_name = table.name
                if table_name and table_name not in results["ctes"]:
                    results["sources"].append(table_name)

        # Store entity
        conn.execute("""
            INSERT OR REPLACE INTO entity (entity_id, name, entity_type, source_file)
            VALUES (?, ?, 'view', ?)
        """, [entity_id, entity_name, str(sql_path)])

        # Store attributes
        for attr in results["attributes"]:
            conn.execute("""
                INSERT OR REPLACE INTO attribute
                (attribute_id, entity_id, name, ordinal_position, is_derived, expression)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                attr["id"],
                entity_id,
                attr["name"],
                attr["ordinal"],
                attr["is_derived"],
                attr["expression"],
            ])

        # Store relationships to source tables
        for source_table in results["sources"]:
            source_entity_id = f"ent_{source_table}"

            # Ensure source entity exists
            conn.execute("""
                INSERT OR IGNORE INTO entity (entity_id, name, entity_type)
                VALUES (?, ?, 'table')
            """, [source_entity_id, source_table])

            # Create relationship
            rel_id = f"rel_{entity_name}_from_{source_table}"
            conn.execute("""
                INSERT OR REPLACE INTO relationship
                (relationship_id, name, source_entity_id, target_entity_id)
                VALUES (?, ?, ?, ?)
            """, [rel_id, f"{entity_name} from {source_table}", source_entity_id, entity_id])

        results["success"] = True
        results["error"] = None

    except Exception as e:
        results["success"] = False
        results["error"] = str(e)

    return results


def parse_directory(sql_dir: str, db_path: str = "mdde_lite.duckdb") -> list:
    """
    Parse all SQL files in a directory.

    Returns list of parsing results.
    """
    conn = create_schema(db_path)
    sql_dir = Path(sql_dir)

    results = []
    for sql_file in sql_dir.glob("*.sql"):
        result = parse_sql_file(str(sql_file), conn)
        results.append(result)
        status = "OK" if result["success"] else f"ERROR: {result['error']}"
        print(f"  Parsed {sql_file.name}: {status}")

    conn.close()
    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python parser.py <sql_directory>")
        print("Example: python parser.py examples/sales")
        sys.exit(1)

    sql_dir = sys.argv[1]
    print(f"Parsing SQL files in {sql_dir}...")
    results = parse_directory(sql_dir)

    print(f"\nParsed {len(results)} files")
    for r in results:
        print(f"  - {r['entity_name']}: {len(r['attributes'])} attributes, {len(r['sources'])} sources")
