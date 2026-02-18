"""
MDDE Lite - Simple SQL Optimizer

Basic SQL quality checks that detect common anti-patterns:
- SELECT * usage
- Missing table aliases in JOINs
- ORDER BY column number instead of name
- Implicit joins (comma-separated FROM)

This is a simplified version. The full MDDE optimizer includes:
- CTE normalization
- UNION handling
- Advanced dialect rendering
- Performance recommendations
"""

import sqlglot
from sqlglot import exp
from pathlib import Path
from typing import List, Dict
import uuid
import duckdb

from .schema import create_schema


def generate_id() -> str:
    """Generate a short unique ID."""
    return str(uuid.uuid4())[:8]


class SQLDiagnostic:
    """Represents a SQL quality finding."""

    def __init__(
        self,
        diagnostic_type: str,
        message: str,
        severity: str = "warning",
        line_number: int = None,
        suggestion: str = None,
    ):
        self.diagnostic_type = diagnostic_type
        self.message = message
        self.severity = severity
        self.line_number = line_number
        self.suggestion = suggestion

    def __repr__(self):
        return f"[{self.severity.upper()}] {self.diagnostic_type}: {self.message}"


def check_select_star(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect SELECT * usage."""
    diagnostics = []

    for star in parsed.find_all(exp.Star):
        diagnostics.append(SQLDiagnostic(
            diagnostic_type="SELECT_STAR",
            message="SELECT * detected - explicit column list recommended",
            severity="warning",
            suggestion="Replace * with explicit column names for better maintainability and performance",
        ))

    return diagnostics


def check_missing_alias(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect tables without aliases in JOINs."""
    diagnostics = []

    # Find all tables
    tables = list(parsed.find_all(exp.Table))

    if len(tables) > 1:
        for table in tables:
            if not table.alias:
                diagnostics.append(SQLDiagnostic(
                    diagnostic_type="MISSING_ALIAS",
                    message=f"Table '{table.name}' has no alias in multi-table query",
                    severity="info",
                    suggestion=f"Add alias: {table.name} AS {table.name[0].lower()}",
                ))

    return diagnostics


def check_order_by_number(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect ORDER BY column number instead of name."""
    diagnostics = []

    for order in parsed.find_all(exp.Order):
        for expr in order.expressions:
            if isinstance(expr.this, exp.Literal) and expr.this.is_int:
                diagnostics.append(SQLDiagnostic(
                    diagnostic_type="ORDER_BY_NUMBER",
                    message=f"ORDER BY uses column number ({expr.this.this}) instead of name",
                    severity="warning",
                    suggestion="Use column name for clarity and maintainability",
                ))

    return diagnostics


def check_implicit_join(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect implicit joins (comma-separated FROM)."""
    diagnostics = []

    # This is a simplified check - look for multiple tables in FROM without JOIN
    select = parsed.find(exp.Select)
    if select:
        from_clause = select.find(exp.From)
        if from_clause:
            # If there are multiple tables but no JOIN keywords, it might be implicit
            tables = list(select.find_all(exp.Table))
            joins = list(select.find_all(exp.Join))

            if len(tables) > 1 and len(joins) < len(tables) - 1:
                # Check if it's actually comma-separated
                from_sql = from_clause.sql()
                if "," in from_sql:
                    diagnostics.append(SQLDiagnostic(
                        diagnostic_type="IMPLICIT_JOIN",
                        message="Implicit join (comma-separated FROM) detected",
                        severity="warning",
                        suggestion="Use explicit JOIN syntax for clarity",
                    ))

    return diagnostics


def check_where_one_equals_one(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect WHERE 1=1 pattern."""
    diagnostics = []

    where = parsed.find(exp.Where)
    if where:
        where_sql = where.sql().upper()
        if "1=1" in where_sql or "1 = 1" in where_sql:
            diagnostics.append(SQLDiagnostic(
                diagnostic_type="WHERE_1_EQUALS_1",
                message="WHERE 1=1 pattern detected",
                severity="info",
                suggestion="Remove if not needed for dynamic SQL generation",
            ))

    return diagnostics


def analyze_sql(sql_content: str) -> List[SQLDiagnostic]:
    """
    Run all checks on SQL content.

    Returns list of diagnostics.
    """
    try:
        parsed = sqlglot.parse_one(sql_content)
    except Exception as e:
        return [SQLDiagnostic(
            diagnostic_type="PARSE_ERROR",
            message=str(e),
            severity="error",
        )]

    diagnostics = []
    diagnostics.extend(check_select_star(parsed))
    diagnostics.extend(check_missing_alias(parsed))
    diagnostics.extend(check_order_by_number(parsed))
    diagnostics.extend(check_implicit_join(parsed))
    diagnostics.extend(check_where_one_equals_one(parsed))

    return diagnostics


def analyze_file(sql_path: str, conn: duckdb.DuckDBPyConnection = None) -> List[SQLDiagnostic]:
    """
    Analyze a SQL file and optionally store diagnostics in database.
    """
    sql_path = Path(sql_path)
    sql_content = sql_path.read_text()
    entity_name = sql_path.stem
    entity_id = f"ent_{entity_name}"

    diagnostics = analyze_sql(sql_content)

    # Store in database if connection provided
    if conn:
        for diag in diagnostics:
            diag_id = f"diag_{generate_id()}"
            conn.execute("""
                INSERT INTO optimizer_diagnostics
                (diagnostic_id, entity_id, source_file, diagnostic_type, severity, message, suggestion)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                diag_id,
                entity_id,
                str(sql_path),
                diag.diagnostic_type,
                diag.severity,
                diag.message,
                diag.suggestion,
            ])

    return diagnostics


def analyze_directory(sql_dir: str, db_path: str = "mdde_lite.duckdb") -> Dict:
    """
    Analyze all SQL files in a directory.

    Returns summary of diagnostics.
    """
    conn = create_schema(db_path)
    sql_dir = Path(sql_dir)

    results = {
        "files_analyzed": 0,
        "total_diagnostics": 0,
        "by_type": {},
        "by_file": {},
    }

    for sql_file in sql_dir.glob("*.sql"):
        diagnostics = analyze_file(str(sql_file), conn)
        results["files_analyzed"] += 1
        results["by_file"][sql_file.name] = diagnostics

        for diag in diagnostics:
            results["total_diagnostics"] += 1
            if diag.diagnostic_type not in results["by_type"]:
                results["by_type"][diag.diagnostic_type] = 0
            results["by_type"][diag.diagnostic_type] += 1

    conn.close()
    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        # Demo with inline SQL
        print("MDDE Lite - SQL Optimizer Demo")
        print("=" * 50)

        bad_sql = """
        SELECT *
        FROM products p
        JOIN categories ON categories.id = p.category_id
        WHERE 1=1
        ORDER BY 1
        """

        print("Analyzing SQL:")
        print(bad_sql)
        print("\nDiagnostics:")

        for diag in analyze_sql(bad_sql):
            print(f"  {diag}")
            if diag.suggestion:
                print(f"    -> {diag.suggestion}")
    else:
        sql_dir = sys.argv[1]
        print(f"Analyzing SQL files in {sql_dir}...")
        results = analyze_directory(sql_dir)

        print(f"\nAnalyzed {results['files_analyzed']} files")
        print(f"Found {results['total_diagnostics']} diagnostics\n")

        print("By type:")
        for dtype, count in results["by_type"].items():
            print(f"  {dtype}: {count}")

        print("\nBy file:")
        for fname, diags in results["by_file"].items():
            if diags:
                print(f"\n  {fname}:")
                for d in diags:
                    print(f"    - {d}")
