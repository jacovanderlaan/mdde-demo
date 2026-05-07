"""
MDDE Lite - Determinism Checker

Detects non-deterministic SQL patterns that can cause:
- Inconsistent results between query runs
- Failed regression tests during migrations
- Silent data quality issues
- Unreproducible analytics

Non-deterministic patterns detected:
- WINDOW_NO_ORDER: ROW_NUMBER/RANK without ORDER BY
- WINDOW_NON_UNIQUE_ORDER: ORDER BY doesn't guarantee uniqueness
- FIRST_LAST_NO_ORDER: FIRST_VALUE/LAST_VALUE without ORDER BY
- LAG_LEAD_NO_ORDER: LAG/LEAD without ORDER BY
- LIMIT_NO_ORDER: LIMIT/TOP without ORDER BY
- VOLATILE_FUNCTION: NOW(), RANDOM(), UUID() usage
- DISTINCT_NO_ORDER: DISTINCT without deterministic ordering
- GROUP_BY_NON_UNIQUE: Aggregation may produce unstable results

Why This Matters for Regression Testing:
-----------------------------------------
During SQL migrations (e.g., legacy to modern platform), we need to compare
results between old and new systems. Non-deterministic SQL makes this impossible:

1. ROW_NUMBER() without unique ORDER BY may assign different numbers each run
2. FIRST_VALUE() without ORDER BY picks an arbitrary "first" row
3. LIMIT without ORDER BY returns random rows
4. GROUP BY on non-unique keys with ANY_VALUE/FIRST produces unstable output

The solution: Add "tie-breaker" columns to ORDER BY clauses and generate
DQ monitoring columns that detect when non-determinism would have occurred.

Related articles:
- "Testing Query Migrations Using Synthetic Data"
- "When Migrations Meet Reality: Handling Schema Drift"
- "Deterministic SQL Generation Is a Hard Invariant"

This is a simplified version. The full MDDE framework includes:
- Automatic tie-breaker column suggestion
- DQ column generation for monitoring
- Database-driven uniqueness analysis
- Primary key inclusion validation
"""

import sqlglot
from sqlglot import exp
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass
import re


@dataclass
class DeterminismIssue:
    """Represents a determinism problem in SQL."""
    issue_type: str
    severity: str  # error, warning, info
    message: str
    location: str  # e.g., "ROW_NUMBER in SELECT"
    suggestion: str
    tie_breaker_columns: List[str]  # Suggested columns to add
    dq_check_sql: Optional[str]  # SQL to detect non-determinism


# Window functions that REQUIRE ORDER BY for determinism
RANKING_FUNCTIONS = {"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE"}
NAVIGATION_FUNCTIONS = {"LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"}
ALL_ORDERED_WINDOW_FUNCTIONS = RANKING_FUNCTIONS | NAVIGATION_FUNCTIONS

# Functions that are inherently non-deterministic
VOLATILE_FUNCTIONS = {
    "RANDOM", "RAND",
    "UUID", "GEN_RANDOM_UUID", "NEWID", "UUID_GENERATE_V4",
    "NOW", "CURRENT_TIMESTAMP", "SYSDATE", "GETDATE", "SYSTIMESTAMP",
    "CURRENT_DATE", "CURRENT_TIME",
}

# Common tie-breaker column patterns (in priority order)
TIE_BREAKER_PATTERNS = [
    r".*_source_row_id$",
    r".*_row_id$",
    r"^row_id$",
    r".*_load_timestamp$",
    r".*_load_ts$",
    r"^load_ts$",
    r"^created_at$",
    r".*_file_row_number$",
    r"^surrogate_key$",
    r"^sk$",
    r".*_sk$",
    r"^id$",
    r".*_id$",
]


def check_determinism(sql_content: str) -> List[DeterminismIssue]:
    """
    Analyze SQL for non-deterministic patterns.

    Args:
        sql_content: SQL query string

    Returns:
        List of DeterminismIssue objects

    Example:
        >>> sql = "SELECT ROW_NUMBER() OVER (PARTITION BY dept) AS rn FROM emp"
        >>> issues = check_determinism(sql)
        >>> print(issues[0].issue_type)
        WINDOW_NO_ORDER
    """
    try:
        parsed = sqlglot.parse_one(sql_content)
    except Exception as e:
        return [DeterminismIssue(
            issue_type="PARSE_ERROR",
            severity="error",
            message=str(e),
            location="SQL",
            suggestion="Fix syntax error",
            tie_breaker_columns=[],
            dq_check_sql=None,
        )]

    issues = []

    # Check window functions
    issues.extend(_check_window_functions(parsed))

    # Check LIMIT/TOP without ORDER BY
    issues.extend(_check_limit_without_order(parsed))

    # Check volatile functions
    issues.extend(_check_volatile_functions(parsed))

    # Check DISTINCT without ORDER BY context
    issues.extend(_check_distinct_determinism(parsed))

    return issues


def _check_window_functions(parsed: exp.Expression) -> List[DeterminismIssue]:
    """Check window functions for determinism issues."""
    issues = []

    for window in parsed.find_all(exp.Window):
        func = window.this
        func_name = _get_function_name(func)

        if func_name not in ALL_ORDERED_WINDOW_FUNCTIONS:
            continue

        # Check for ORDER BY in window spec
        order_by = window.args.get("order")

        if not order_by:
            # No ORDER BY at all - definitely non-deterministic
            issue_type = "WINDOW_NO_ORDER"
            if func_name in NAVIGATION_FUNCTIONS:
                issue_type = "FIRST_LAST_NO_ORDER" if func_name in {"FIRST_VALUE", "LAST_VALUE"} else "LAG_LEAD_NO_ORDER"

            issues.append(DeterminismIssue(
                issue_type=issue_type,
                severity="error",
                message=f"{func_name}() without ORDER BY - results are non-deterministic",
                location=f"{func_name} window function",
                suggestion=f"Add ORDER BY clause with unique columns to {func_name}()",
                tie_breaker_columns=["primary_key", "created_at", "row_id"],
                dq_check_sql=_generate_dq_check(func_name, window),
            ))
        else:
            # Has ORDER BY - but is it unique enough?
            order_cols = _extract_order_columns(order_by)

            if func_name in RANKING_FUNCTIONS:
                # For ROW_NUMBER etc., ORDER BY should be unique within partition
                partition_cols = _extract_partition_columns(window)

                issues.append(DeterminismIssue(
                    issue_type="WINDOW_NON_UNIQUE_ORDER",
                    severity="warning",
                    message=f"{func_name}() ORDER BY ({', '.join(order_cols)}) may not be unique within partition",
                    location=f"{func_name} window function",
                    suggestion=f"Ensure ORDER BY includes a unique column (PK or tie-breaker)",
                    tie_breaker_columns=["primary_key", "_source_row_id", "created_at"],
                    dq_check_sql=_generate_uniqueness_check(func_name, partition_cols, order_cols),
                ))

    return issues


def _check_limit_without_order(parsed: exp.Expression) -> List[DeterminismIssue]:
    """Check for LIMIT/TOP without ORDER BY."""
    issues = []

    for select in parsed.find_all(exp.Select):
        # Check for LIMIT
        limit = select.args.get("limit")
        if not limit:
            continue

        # Check for ORDER BY
        order = select.args.get("order")
        if not order:
            issues.append(DeterminismIssue(
                issue_type="LIMIT_NO_ORDER",
                severity="error",
                message="LIMIT/TOP without ORDER BY - returns arbitrary rows",
                location="SELECT with LIMIT",
                suggestion="Add ORDER BY clause to ensure consistent row selection",
                tie_breaker_columns=["primary_key", "created_at"],
                dq_check_sql=None,  # Can't easily generate DQ check for this
            ))

    return issues


def _check_volatile_functions(parsed: exp.Expression) -> List[DeterminismIssue]:
    """Check for volatile/non-deterministic functions."""
    issues = []
    seen_functions = set()

    for func in parsed.find_all(exp.Func):
        func_name = _get_function_name(func)

        # Handle Anonymous functions (like NOW() in some dialects)
        if isinstance(func, exp.Anonymous):
            func_name = func.name.upper() if func.name else "ANONYMOUS"

        if func_name in VOLATILE_FUNCTIONS and func_name not in seen_functions:
            seen_functions.add(func_name)

            if func_name in {"RANDOM", "RAND"}:
                severity = "error"
                message = f"{func_name}() produces different values each execution"
            elif func_name in {"UUID", "GEN_RANDOM_UUID", "NEWID", "UUID_GENERATE_V4"}:
                severity = "warning"
                message = f"{func_name}() generates new UUIDs each execution"
            else:
                severity = "info"
                message = f"{func_name}() returns current time - varies between runs"

            issues.append(DeterminismIssue(
                issue_type="VOLATILE_FUNCTION",
                severity=severity,
                message=message,
                location=f"{func_name}() function call",
                suggestion="For regression testing, consider parameterizing time-dependent values",
                tie_breaker_columns=[],
                dq_check_sql=None,
            ))

    return issues


def _check_distinct_determinism(parsed: exp.Expression) -> List[DeterminismIssue]:
    """Check DISTINCT for potential ordering issues."""
    issues = []

    for select in parsed.find_all(exp.Select):
        if not select.args.get("distinct"):
            continue

        # DISTINCT with LIMIT but no ORDER BY is problematic
        limit = select.args.get("limit")
        order = select.args.get("order")

        if limit and not order:
            issues.append(DeterminismIssue(
                issue_type="DISTINCT_NO_ORDER",
                severity="warning",
                message="SELECT DISTINCT with LIMIT but no ORDER BY - arbitrary row selection",
                location="SELECT DISTINCT with LIMIT",
                suggestion="Add ORDER BY to ensure consistent row selection",
                tie_breaker_columns=[],
                dq_check_sql=None,
            ))

    return issues


def _get_function_name(func: exp.Expression) -> str:
    """Extract function name from expression."""
    # Try sql_name first (most reliable for sqlglot functions)
    if hasattr(func, 'sql_name'):
        return func.sql_name().upper()
    # Try key attribute
    if hasattr(func, 'key'):
        return func.key.upper()
    # Try name attribute for Anonymous functions
    if hasattr(func, 'name'):
        return func.name.upper()
    # Fallback to class name
    return type(func).__name__.upper()


def _extract_order_columns(order_by: exp.Expression) -> List[str]:
    """Extract column names from ORDER BY clause."""
    columns = []
    if hasattr(order_by, 'expressions'):
        for expr in order_by.expressions:
            if isinstance(expr, exp.Ordered):
                col = expr.this
            else:
                col = expr

            if isinstance(col, exp.Column):
                columns.append(col.name)
            else:
                columns.append(str(col))
    return columns


def _extract_partition_columns(window: exp.Window) -> List[str]:
    """Extract column names from PARTITION BY clause."""
    columns = []
    partition = window.args.get("partition_by")
    if partition:
        for expr in partition:
            if isinstance(expr, exp.Column):
                columns.append(expr.name)
            else:
                columns.append(str(expr))
    return columns


def _generate_dq_check(func_name: str, window: exp.Window) -> str:
    """Generate SQL to detect when non-determinism would occur."""
    partition_cols = _extract_partition_columns(window)
    partition_str = ", ".join(partition_cols) if partition_cols else "1"

    return f"""-- DQ Check: Detect potential {func_name} non-determinism
-- If this returns rows, the ordering is not unique
SELECT {partition_str}, COUNT(*) as duplicate_count
FROM your_table
GROUP BY {partition_str}
HAVING COUNT(*) > 1"""


def _generate_uniqueness_check(func_name: str, partition_cols: List[str], order_cols: List[str]) -> str:
    """Generate SQL to check if ORDER BY is unique within partitions."""
    all_cols = partition_cols + order_cols
    cols_str = ", ".join(all_cols) if all_cols else "*"

    return f"""-- DQ Check: Verify ORDER BY uniqueness for {func_name}
-- If this returns > 0, ties exist that make {func_name} non-deterministic
SELECT COUNT(*) as tie_count
FROM (
    SELECT {cols_str}, COUNT(*) as cnt
    FROM your_table
    GROUP BY {cols_str}
    HAVING COUNT(*) > 1
) ties"""


def suggest_tie_breakers(column_names: List[str]) -> List[str]:
    """
    Suggest tie-breaker columns from available columns.

    Args:
        column_names: List of available column names

    Returns:
        List of suggested tie-breaker columns in priority order
    """
    suggestions = []

    for pattern in TIE_BREAKER_PATTERNS:
        regex = re.compile(pattern, re.IGNORECASE)
        for col in column_names:
            if regex.match(col) and col not in suggestions:
                suggestions.append(col)

    return suggestions


def generate_deterministic_sql(
    sql_content: str,
    tie_breaker_column: str = "_row_hash",
    add_dq_columns: bool = True
) -> Dict[str, str]:
    """
    Generate deterministic version of SQL with optional DQ columns.

    Args:
        sql_content: Original SQL
        tie_breaker_column: Column to add to ORDER BY clauses
        add_dq_columns: Whether to add DQ monitoring columns

    Returns:
        Dictionary with:
        - 'sql': Modified SQL
        - 'dq_sql': DQ check SQL
        - 'recommendations': List of changes made

    Note: This is a conceptual implementation. Full implementation would
    require deeper AST manipulation.
    """
    issues = check_determinism(sql_content)

    recommendations = []
    dq_checks = []

    for issue in issues:
        if issue.issue_type in ("WINDOW_NO_ORDER", "WINDOW_NON_UNIQUE_ORDER"):
            recommendations.append(
                f"Add {tie_breaker_column} to ORDER BY in {issue.location}"
            )
            if issue.dq_check_sql:
                dq_checks.append(issue.dq_check_sql)

        elif issue.issue_type == "LIMIT_NO_ORDER":
            recommendations.append(
                f"Add ORDER BY clause before LIMIT"
            )

    # Generate DQ monitoring column concept
    dq_column_sql = ""
    if add_dq_columns and any(i.issue_type.startswith("WINDOW") for i in issues):
        dq_column_sql = f"""
-- DQ Monitoring Columns (add to your SELECT)
-- These detect when non-determinism would affect results

-- Count of duplicates in partition+order key (should be 0 or 1)
COUNT(*) OVER (PARTITION BY <partition_cols>, <order_cols>) AS _dq_tie_count,

-- Flag rows where ties exist
CASE
    WHEN COUNT(*) OVER (PARTITION BY <partition_cols>, <order_cols>) > 1
    THEN 'NON_DETERMINISTIC'
    ELSE 'DETERMINISTIC'
END AS _dq_determinism_status
"""

    return {
        "original_sql": sql_content,
        "recommendations": recommendations,
        "dq_checks": "\n\n".join(dq_checks),
        "dq_column_template": dq_column_sql,
        "issues": issues,
    }


def get_all_determinism_checks() -> List[str]:
    """Return list of all determinism check types."""
    return [
        "WINDOW_NO_ORDER",
        "WINDOW_NON_UNIQUE_ORDER",
        "FIRST_LAST_NO_ORDER",
        "LAG_LEAD_NO_ORDER",
        "LIMIT_NO_ORDER",
        "VOLATILE_FUNCTION",
        "DISTINCT_NO_ORDER",
    ]


if __name__ == "__main__":
    print("MDDE Lite - Determinism Checker")
    print("=" * 70)

    # Test cases demonstrating non-deterministic patterns
    test_cases = [
        ("ROW_NUMBER without ORDER BY", """
            SELECT
                customer_id,
                ROW_NUMBER() OVER (PARTITION BY region) AS rn
            FROM customers
        """),

        ("ROW_NUMBER with non-unique ORDER BY", """
            SELECT
                customer_id,
                ROW_NUMBER() OVER (
                    PARTITION BY region
                    ORDER BY created_date
                ) AS rn
            FROM customers
        """),

        ("FIRST_VALUE without ORDER BY", """
            SELECT
                customer_id,
                FIRST_VALUE(name) OVER (PARTITION BY region) AS first_name
            FROM customers
        """),

        ("LIMIT without ORDER BY", """
            SELECT * FROM orders LIMIT 100
        """),

        ("Volatile functions", """
            SELECT
                customer_id,
                RANDOM() AS random_value,
                NOW() AS current_time
            FROM customers
        """),
    ]

    for name, sql in test_cases:
        print(f"\n--- {name} ---")
        print(f"SQL: {sql.strip()[:60]}...")

        issues = check_determinism(sql)
        if issues:
            for issue in issues:
                print(f"\n  [{issue.severity.upper()}] {issue.issue_type}")
                print(f"    {issue.message}")
                print(f"    Location: {issue.location}")
                print(f"    Suggestion: {issue.suggestion}")
                if issue.tie_breaker_columns:
                    print(f"    Tie-breakers: {', '.join(issue.tie_breaker_columns)}")
        else:
            print("  No determinism issues found")

    print("\n" + "=" * 70)
    print("Why Determinism Matters for Regression Testing:")
    print("-" * 70)
    print("""
During SQL migrations, we compare query results between systems.
Non-deterministic SQL makes comparison impossible because:

1. ROW_NUMBER() without unique ORDER BY assigns different numbers each run
2. FIRST_VALUE() without ORDER BY picks arbitrary "first" rows
3. LIMIT without ORDER BY returns random subsets
4. Time functions return different values

Solution: Add tie-breaker columns and DQ monitoring columns to detect
when non-determinism would have affected results.
""")
