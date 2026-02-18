"""
MDDE Lite - Simple SQL Optimizer

SQL quality checks that detect common anti-patterns.

Checks included (15 total):
- SELECT_STAR: SELECT * usage
- MISSING_ALIAS: Tables without aliases in JOINs
- ORDER_BY_NUMBER: ORDER BY column number instead of name
- IMPLICIT_JOIN: Comma-separated FROM (implicit join)
- WHERE_1_EQUALS_1: WHERE 1=1 pattern
- DISTINCT_STAR: SELECT DISTINCT * pattern
- CARTESIAN_JOIN: JOIN without ON clause
- DUPLICATE_COLUMN: Same column selected twice
- NESTED_SUBQUERY: Deep subquery nesting (3+ levels)
- UNION_COLUMN_MISMATCH: Mismatched column counts in UNION
- LEADING_WILDCARD: LIKE '%...' pattern (non-SARGable)
- FUNCTION_IN_WHERE: Function on column in WHERE (non-SARGable)
- OR_IN_JOIN: OR condition in JOIN ON clause
- HARDCODED_DATE: Hardcoded date literals
- MISSING_GROUP_BY: Aggregate function without GROUP BY

Related article: "Implementing 25 Essential Data Quality Checks Using YAML Metadata"

This is a simplified version. The full MDDE optimizer includes:
- CTE normalization and optimization
- UNION handling and column alignment
- Advanced dialect rendering
- Performance recommendations
- Filter/join pushdown analysis
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


# ============================================================
# NEW CHECKS (expanding from 5 to 15)
# ============================================================


def check_distinct_star(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect SELECT DISTINCT * pattern."""
    diagnostics = []

    for select in parsed.find_all(exp.Select):
        if select.args.get("distinct"):
            for expr in select.expressions:
                if isinstance(expr, exp.Star):
                    diagnostics.append(SQLDiagnostic(
                        diagnostic_type="DISTINCT_STAR",
                        message="SELECT DISTINCT * detected",
                        severity="warning",
                        suggestion="Use explicit column list with DISTINCT for clarity and performance",
                    ))
                    break

    return diagnostics


def check_cartesian_join(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect JOIN without ON clause (potential cartesian product)."""
    diagnostics = []

    for join in parsed.find_all(exp.Join):
        # Check if join has ON condition
        if not join.args.get("on") and not join.args.get("using"):
            # CROSS JOIN is intentional, others are suspicious
            join_type = join.args.get("kind", "").upper() if join.args.get("kind") else ""
            if "CROSS" not in join_type:
                table_name = join.this.name if hasattr(join.this, 'name') else str(join.this)
                diagnostics.append(SQLDiagnostic(
                    diagnostic_type="CARTESIAN_JOIN",
                    message=f"JOIN to '{table_name}' without ON clause - potential cartesian product",
                    severity="warning",
                    suggestion="Add ON clause or use CROSS JOIN if intentional",
                ))

    return diagnostics


def check_duplicate_column(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect same column selected twice in SELECT clause."""
    diagnostics = []

    for select in parsed.find_all(exp.Select):
        seen_columns = {}
        for expr in select.expressions:
            col_name = None
            if isinstance(expr, exp.Column):
                col_name = expr.name.lower()
            elif isinstance(expr, exp.Alias):
                col_name = expr.alias.lower()

            if col_name:
                if col_name in seen_columns:
                    diagnostics.append(SQLDiagnostic(
                        diagnostic_type="DUPLICATE_COLUMN",
                        message=f"Column '{col_name}' appears multiple times in SELECT",
                        severity="warning",
                        suggestion="Remove duplicate or use different alias",
                    ))
                else:
                    seen_columns[col_name] = True

    return diagnostics


def check_nested_subquery(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect deeply nested subqueries (3+ levels)."""
    diagnostics = []

    def count_depth(node, current_depth=0):
        max_depth = current_depth
        for child in node.iter_expressions():
            if isinstance(child, exp.Subquery):
                child_depth = count_depth(child, current_depth + 1)
                max_depth = max(max_depth, child_depth)
            else:
                child_depth = count_depth(child, current_depth)
                max_depth = max(max_depth, child_depth)
        return max_depth

    depth = count_depth(parsed)
    if depth >= 3:
        diagnostics.append(SQLDiagnostic(
            diagnostic_type="NESTED_SUBQUERY",
            message=f"Deeply nested subqueries detected ({depth} levels)",
            severity="info",
            suggestion="Consider using CTEs for better readability",
        ))

    return diagnostics


def check_union_column_mismatch(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect UNION/UNION ALL with different column counts."""
    diagnostics = []

    for union in parsed.find_all(exp.Union):
        left = union.this
        right = union.expression

        # Count columns in each side
        left_cols = len(list(left.find(exp.Select).expressions)) if left.find(exp.Select) else 0
        right_cols = len(list(right.find(exp.Select).expressions)) if right.find(exp.Select) else 0

        if left_cols != right_cols and left_cols > 0 and right_cols > 0:
            diagnostics.append(SQLDiagnostic(
                diagnostic_type="UNION_COLUMN_MISMATCH",
                message=f"UNION has mismatched column counts: {left_cols} vs {right_cols}",
                severity="error",
                suggestion="Ensure both sides of UNION have same number of columns",
            ))

    return diagnostics


def check_leading_wildcard(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect LIKE '%...' pattern (non-SARGable)."""
    diagnostics = []

    for like in parsed.find_all(exp.Like):
        pattern = like.expression
        if isinstance(pattern, exp.Literal) and pattern.is_string:
            pattern_value = pattern.this
            if pattern_value.startswith('%'):
                diagnostics.append(SQLDiagnostic(
                    diagnostic_type="LEADING_WILDCARD",
                    message=f"LIKE pattern starts with '%' - cannot use index",
                    severity="info",
                    suggestion="Consider full-text search or restructure query if performance critical",
                ))

    return diagnostics


def check_function_in_where(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect function applied to column in WHERE clause (non-SARGable)."""
    diagnostics = []

    where = parsed.find(exp.Where)
    if where:
        # Look for common patterns: UPPER(col), LOWER(col), YEAR(col), etc.
        for func in where.find_all(exp.Func):
            # Check if function is applied to a column (not a literal)
            for arg in func.expressions:
                if isinstance(arg, exp.Column):
                    func_name = func.key.upper() if hasattr(func, 'key') else type(func).__name__.upper()
                    diagnostics.append(SQLDiagnostic(
                        diagnostic_type="FUNCTION_IN_WHERE",
                        message=f"Function {func_name} on column in WHERE - cannot use index",
                        severity="info",
                        suggestion="Consider computed column or functional index",
                    ))
                    break

    return diagnostics


def check_or_in_join(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect OR condition in JOIN ON clause."""
    diagnostics = []

    for join in parsed.find_all(exp.Join):
        on_clause = join.args.get("on")
        if on_clause:
            # Check for OR at the top level of the ON clause
            if isinstance(on_clause, exp.Or):
                diagnostics.append(SQLDiagnostic(
                    diagnostic_type="OR_IN_JOIN",
                    message="OR condition in JOIN ON clause",
                    severity="warning",
                    suggestion="Consider UNION of separate JOINs for better optimization",
                ))
            # Also check for OR nested in AND
            for or_expr in on_clause.find_all(exp.Or):
                diagnostics.append(SQLDiagnostic(
                    diagnostic_type="OR_IN_JOIN",
                    message="OR condition in JOIN ON clause",
                    severity="warning",
                    suggestion="Consider UNION of separate JOINs for better optimization",
                ))
                break  # Only report once per join

    return diagnostics


def check_hardcoded_date(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect hardcoded date literals."""
    diagnostics = []

    # Common date patterns
    import re
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$|^\d{8}$")

    for literal in parsed.find_all(exp.Literal):
        if literal.is_string:
            value = str(literal.this)
            if date_pattern.match(value):
                diagnostics.append(SQLDiagnostic(
                    diagnostic_type="HARDCODED_DATE",
                    message=f"Hardcoded date literal: '{value}'",
                    severity="info",
                    suggestion="Consider using parameters or date functions",
                ))

    return diagnostics


def check_missing_group_by(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect aggregate function without GROUP BY."""
    diagnostics = []

    aggregate_funcs = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)

    for select in parsed.find_all(exp.Select):
        has_aggregate = False
        has_non_aggregate = False

        for expr in select.expressions:
            # Check if this expression contains an aggregate
            if any(expr.find(agg) for agg in aggregate_funcs):
                has_aggregate = True
            elif isinstance(expr, exp.Column):
                has_non_aggregate = True
            elif isinstance(expr, exp.Alias):
                inner = expr.this
                if any(inner.find(agg) for agg in aggregate_funcs):
                    has_aggregate = True
                elif isinstance(inner, exp.Column):
                    has_non_aggregate = True

        # If we have both aggregates and non-aggregates, need GROUP BY
        if has_aggregate and has_non_aggregate:
            group = select.args.get("group")
            if not group:
                diagnostics.append(SQLDiagnostic(
                    diagnostic_type="MISSING_GROUP_BY",
                    message="Aggregate function mixed with non-aggregated columns without GROUP BY",
                    severity="error",
                    suggestion="Add GROUP BY clause or wrap non-aggregates in aggregate functions",
                ))

    return diagnostics


def analyze_sql(sql_content: str) -> List[SQLDiagnostic]:
    """
    Run all 15 checks on SQL content.

    Returns list of diagnostics.

    Related article: "Implementing 25 Essential Data Quality Checks Using YAML Metadata"
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

    # Original 5 checks
    diagnostics.extend(check_select_star(parsed))
    diagnostics.extend(check_missing_alias(parsed))
    diagnostics.extend(check_order_by_number(parsed))
    diagnostics.extend(check_implicit_join(parsed))
    diagnostics.extend(check_where_one_equals_one(parsed))

    # New checks (expanding to 15)
    diagnostics.extend(check_distinct_star(parsed))
    diagnostics.extend(check_cartesian_join(parsed))
    diagnostics.extend(check_duplicate_column(parsed))
    diagnostics.extend(check_nested_subquery(parsed))
    diagnostics.extend(check_union_column_mismatch(parsed))
    diagnostics.extend(check_leading_wildcard(parsed))
    diagnostics.extend(check_function_in_where(parsed))
    diagnostics.extend(check_or_in_join(parsed))
    diagnostics.extend(check_hardcoded_date(parsed))
    diagnostics.extend(check_missing_group_by(parsed))

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


def get_all_check_types() -> List[str]:
    """Return list of all check types for documentation."""
    return [
        "SELECT_STAR",
        "MISSING_ALIAS",
        "ORDER_BY_NUMBER",
        "IMPLICIT_JOIN",
        "WHERE_1_EQUALS_1",
        "DISTINCT_STAR",
        "CARTESIAN_JOIN",
        "DUPLICATE_COLUMN",
        "NESTED_SUBQUERY",
        "UNION_COLUMN_MISMATCH",
        "LEADING_WILDCARD",
        "FUNCTION_IN_WHERE",
        "OR_IN_JOIN",
        "HARDCODED_DATE",
        "MISSING_GROUP_BY",
    ]


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        # Demo with inline SQL showcasing multiple checks
        print("MDDE Lite - SQL Optimizer Demo (15 Checks)")
        print("=" * 60)

        # SQL that triggers multiple checks
        bad_sql = """
        SELECT DISTINCT *
        FROM products p
        JOIN categories ON categories.id = p.category_id
        JOIN inventory  -- Missing ON clause
        WHERE 1=1
          AND UPPER(product_name) = 'WIDGET'
          AND created_date > '2024-01-01'
          AND name LIKE '%test%'
        ORDER BY 1
        """

        print("Analyzing SQL:")
        print(bad_sql)
        print("\nDiagnostics Found:")

        diagnostics = analyze_sql(bad_sql)
        for diag in diagnostics:
            print(f"\n  [{diag.severity.upper()}] {diag.diagnostic_type}")
            print(f"    {diag.message}")
            if diag.suggestion:
                print(f"    Suggestion: {diag.suggestion}")

        print(f"\n" + "=" * 60)
        print(f"Total: {len(diagnostics)} issues found")
        print(f"\nAll available checks ({len(get_all_check_types())}):")
        for check in get_all_check_types():
            print(f"  - {check}")

    else:
        sql_dir = sys.argv[1]
        print(f"Analyzing SQL files in {sql_dir}...")
        results = analyze_directory(sql_dir)

        print(f"\nAnalyzed {results['files_analyzed']} files")
        print(f"Found {results['total_diagnostics']} diagnostics\n")

        print("By type:")
        for dtype, count in sorted(results["by_type"].items()):
            print(f"  {dtype}: {count}")

        print("\nBy file:")
        for fname, diags in results["by_file"].items():
            if diags:
                print(f"\n  {fname}:")
                for d in diags:
                    print(f"    - {d}")
