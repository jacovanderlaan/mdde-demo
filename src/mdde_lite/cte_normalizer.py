"""
MDDE Lite - CTE Normalizer

Transforms monolithic SQL queries into modular CTE-based queries:
- Extract repeated subqueries into named CTEs
- Standardize CTE naming conventions
- Flatten nested subqueries
- Generate readable, maintainable SQL

Related articles:
- "Modular SQL with CTEs: A Best Practice"
- "From Raw SQL to Logical Building Blocks"
- "From Monolith to Modules"

This is a simplified version. The full MDDE framework includes:
- Full AST-based CTE extraction
- Automatic dependency ordering
- CTE deduplication across queries
- Integration with lineage tracking
"""

import sqlglot
from sqlglot import exp
from typing import List, Dict, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import OrderedDict
import re


@dataclass
class CTEDefinition:
    """Represents a Common Table Expression."""
    name: str
    sql: str
    dependencies: List[str] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    description: str = ""


@dataclass
class NormalizationResult:
    """Result of CTE normalization."""
    original_sql: str
    normalized_sql: str
    ctes_extracted: int
    cte_definitions: List[CTEDefinition]
    transformations: List[str]


def normalize_to_ctes(
    sql: str,
    cte_prefix: str = "cte_",
    extract_subqueries: bool = True,
    extract_repeated: bool = True,
    min_subquery_depth: int = 1
) -> NormalizationResult:
    """
    Transform SQL into CTE-based structure.

    Args:
        sql: Input SQL query
        cte_prefix: Prefix for generated CTE names
        extract_subqueries: Extract nested subqueries into CTEs
        extract_repeated: Extract repeated expressions into shared CTEs
        min_subquery_depth: Minimum nesting depth to extract

    Returns:
        NormalizationResult with normalized SQL and metadata
    """
    try:
        parsed = sqlglot.parse_one(sql)
    except Exception as e:
        return NormalizationResult(
            original_sql=sql,
            normalized_sql=sql,
            ctes_extracted=0,
            cte_definitions=[],
            transformations=[f"Parse error: {e}"]
        )

    transformations = []
    cte_definitions = []
    cte_counter = [0]  # Use list for mutable counter in nested function

    def generate_cte_name(hint: str = "") -> str:
        cte_counter[0] += 1
        if hint:
            # Clean hint for valid identifier
            clean_hint = re.sub(r'[^a-zA-Z0-9_]', '_', hint.lower())[:20]
            return f"{cte_prefix}{clean_hint}_{cte_counter[0]}"
        return f"{cte_prefix}{cte_counter[0]}"

    # Extract existing CTEs
    existing_ctes = {}
    for cte in parsed.find_all(exp.CTE):
        cte_name = cte.alias
        if cte_name:
            existing_ctes[cte_name] = cte.this.sql()

    # Find and extract subqueries
    new_ctes = OrderedDict()

    if extract_subqueries:
        subqueries = list(parsed.find_all(exp.Subquery))

        for i, subquery in enumerate(subqueries):
            # Skip if it's a scalar subquery in SELECT
            parent = subquery.parent
            if isinstance(parent, exp.Select):
                continue

            # Get the inner query
            inner = subquery.this
            if not isinstance(inner, exp.Select):
                continue

            # Generate CTE name based on context
            hint = _get_subquery_hint(subquery)
            cte_name = generate_cte_name(hint)

            # Extract columns
            columns = _extract_select_columns(inner)

            # Store CTE definition
            new_ctes[cte_name] = CTEDefinition(
                name=cte_name,
                sql=inner.sql(),
                columns=columns,
                description=f"Extracted from subquery {i+1}"
            )

            transformations.append(f"Extracted subquery to CTE: {cte_name}")

    # Find repeated table references that could be CTEd
    if extract_repeated:
        table_refs = _find_repeated_table_patterns(parsed)
        for pattern, count in table_refs.items():
            if count > 1:
                transformations.append(
                    f"Found repeated pattern '{pattern}' ({count} times) - consider extracting to CTE"
                )

    # Build normalized SQL
    normalized_sql = _build_normalized_sql(parsed, new_ctes, existing_ctes)

    # Convert to CTEDefinition list
    cte_definitions = list(new_ctes.values())

    return NormalizationResult(
        original_sql=sql,
        normalized_sql=normalized_sql,
        ctes_extracted=len(new_ctes),
        cte_definitions=cte_definitions,
        transformations=transformations
    )


def _get_subquery_hint(subquery: exp.Subquery) -> str:
    """Generate a hint for CTE naming based on subquery context."""
    # Check for alias
    if subquery.alias:
        return subquery.alias

    # Check parent context
    parent = subquery.parent
    if isinstance(parent, exp.From):
        return "source"
    elif isinstance(parent, exp.Join):
        return "joined"
    elif isinstance(parent, exp.Where):
        return "filter"

    # Check for table references in the subquery
    inner = subquery.this
    if isinstance(inner, exp.Select):
        tables = list(inner.find_all(exp.Table))
        if tables:
            return tables[0].name

    return "subq"


def _extract_select_columns(select: exp.Select) -> List[str]:
    """Extract column names/aliases from SELECT clause."""
    columns = []
    for expr in select.expressions:
        if isinstance(expr, exp.Alias):
            columns.append(expr.alias)
        elif isinstance(expr, exp.Column):
            columns.append(expr.name)
        elif isinstance(expr, exp.Star):
            columns.append("*")
        else:
            # For expressions, try to get a reasonable name
            columns.append(str(expr)[:30])
    return columns


def _find_repeated_table_patterns(parsed: exp.Expression) -> Dict[str, int]:
    """Find repeated table access patterns."""
    patterns = {}

    for table in parsed.find_all(exp.Table):
        table_name = table.name
        if table_name:
            patterns[table_name] = patterns.get(table_name, 0) + 1

    return patterns


def _build_normalized_sql(
    parsed: exp.Expression,
    new_ctes: Dict[str, CTEDefinition],
    existing_ctes: Dict[str, str]
) -> str:
    """Build the normalized SQL with CTEs."""
    if not new_ctes:
        return parsed.sql(pretty=True)

    # Build CTE block
    cte_parts = []
    for cte_name, cte_def in new_ctes.items():
        cte_parts.append(f"{cte_name} AS (\n    {cte_def.sql}\n)")

    cte_block = "WITH " + ",\n".join(cte_parts)

    # Get the main query (simplified - in full version would replace subqueries)
    main_query = parsed.sql(pretty=True)

    return f"{cte_block}\n\n{main_query}"


def flatten_nested_subqueries(
    sql: str,
    max_depth: int = 2
) -> NormalizationResult:
    """
    Flatten deeply nested subqueries into CTEs.

    Args:
        sql: Input SQL with nested subqueries
        max_depth: Maximum allowed nesting depth

    Returns:
        NormalizationResult with flattened SQL
    """
    try:
        parsed = sqlglot.parse_one(sql)
    except Exception as e:
        return NormalizationResult(
            original_sql=sql,
            normalized_sql=sql,
            ctes_extracted=0,
            cte_definitions=[],
            transformations=[f"Parse error: {e}"]
        )

    transformations = []
    cte_definitions = []

    # Find nesting depth
    def get_depth(node: exp.Expression, current: int = 0) -> int:
        max_found = current
        if isinstance(node, (exp.Subquery, exp.Select)):
            current += 1
        for child in node.iter_expressions():
            child_depth = get_depth(child, current)
            max_found = max(max_found, child_depth)
        return max_found

    depth = get_depth(parsed)

    if depth > max_depth:
        transformations.append(
            f"Query has nesting depth {depth} (max recommended: {max_depth})"
        )
        transformations.append("Consider extracting inner queries to CTEs")

        # Extract deepest subqueries first
        result = normalize_to_ctes(sql, min_subquery_depth=max_depth)
        return result

    return NormalizationResult(
        original_sql=sql,
        normalized_sql=parsed.sql(pretty=True),
        ctes_extracted=0,
        cte_definitions=[],
        transformations=[f"Nesting depth {depth} is acceptable (max: {max_depth})"]
    )


def standardize_cte_names(
    sql: str,
    naming_convention: str = "snake_case",
    prefix: str = ""
) -> str:
    """
    Standardize CTE names according to naming convention.

    Args:
        sql: Input SQL with CTEs
        naming_convention: "snake_case", "camelCase", or "PascalCase"
        prefix: Optional prefix to add to all CTE names

    Returns:
        SQL with standardized CTE names
    """
    try:
        parsed = sqlglot.parse_one(sql)
    except:
        return sql

    # Find all CTEs and their references
    cte_renames = {}

    for cte in parsed.find_all(exp.CTE):
        old_name = cte.alias
        if old_name:
            new_name = _apply_naming_convention(old_name, naming_convention, prefix)
            if new_name != old_name:
                cte_renames[old_name] = new_name

    if not cte_renames:
        return sql

    # Apply renames (simplified - would need full AST transformation in production)
    result = sql
    for old_name, new_name in cte_renames.items():
        # Replace CTE definition
        result = re.sub(
            rf'\b{old_name}\s+AS\s*\(',
            f'{new_name} AS (',
            result,
            flags=re.IGNORECASE
        )
        # Replace CTE references
        result = re.sub(
            rf'\bFROM\s+{old_name}\b',
            f'FROM {new_name}',
            result,
            flags=re.IGNORECASE
        )
        result = re.sub(
            rf'\bJOIN\s+{old_name}\b',
            f'JOIN {new_name}',
            result,
            flags=re.IGNORECASE
        )

    return result


def _apply_naming_convention(name: str, convention: str, prefix: str) -> str:
    """Apply naming convention to a name."""
    # Split into words
    words = re.split(r'[_\s]+', name)
    words = [w for w in words if w]

    if not words:
        return name

    if convention == "snake_case":
        result = "_".join(w.lower() for w in words)
    elif convention == "camelCase":
        result = words[0].lower() + "".join(w.capitalize() for w in words[1:])
    elif convention == "PascalCase":
        result = "".join(w.capitalize() for w in words)
    else:
        result = name

    if prefix:
        result = f"{prefix}{result}"

    return result


def suggest_cte_structure(sql: str) -> List[str]:
    """
    Analyze SQL and suggest CTE restructuring.

    Args:
        sql: Input SQL to analyze

    Returns:
        List of suggestions for CTE restructuring
    """
    suggestions = []

    try:
        parsed = sqlglot.parse_one(sql)
    except:
        suggestions.append("Could not parse SQL")
        return suggestions

    # Check for existing CTEs
    existing_ctes = list(parsed.find_all(exp.CTE))
    if existing_ctes:
        suggestions.append(f"Query already has {len(existing_ctes)} CTE(s)")

    # Check nesting depth
    def count_subquery_depth(node, depth=0):
        max_depth = depth
        if isinstance(node, exp.Subquery):
            depth += 1
            max_depth = depth
        for child in node.iter_expressions():
            child_depth = count_subquery_depth(child, depth)
            max_depth = max(max_depth, child_depth)
        return max_depth

    depth = count_subquery_depth(parsed)
    if depth > 2:
        suggestions.append(
            f"High nesting depth ({depth}): Extract inner subqueries to CTEs"
        )

    # Check for repeated table references
    table_counts = {}
    for table in parsed.find_all(exp.Table):
        name = table.name
        if name:
            table_counts[name] = table_counts.get(name, 0) + 1

    for table, count in table_counts.items():
        if count > 2:
            suggestions.append(
                f"Table '{table}' referenced {count} times: Consider CTE for shared logic"
            )

    # Check for subqueries in FROM/JOIN
    subquery_count = len(list(parsed.find_all(exp.Subquery)))
    if subquery_count > 0:
        suggestions.append(
            f"Found {subquery_count} subquery(s): Consider extracting to named CTEs"
        )

    # Check for complex aggregations
    agg_count = 0
    for func in parsed.find_all(exp.Func):
        if func.sql_name().upper() in ("SUM", "COUNT", "AVG", "MAX", "MIN"):
            agg_count += 1

    if agg_count > 3:
        suggestions.append(
            f"Multiple aggregations ({agg_count}): Consider staging CTE for base data"
        )

    # Check for UNION/UNION ALL
    union_count = len(list(parsed.find_all(exp.Union)))
    if union_count > 0:
        suggestions.append(
            f"Found {union_count} UNION(s): Consider CTEs for each branch"
        )

    if not suggestions:
        suggestions.append("Query structure looks good - no CTE suggestions")

    return suggestions


if __name__ == "__main__":
    print("MDDE Lite - CTE Normalizer Demo")
    print("=" * 60)

    # Test case 1: Nested subqueries
    print("\n--- Test 1: Nested Subqueries ---")
    sql1 = """
    SELECT
        o.order_id,
        o.customer_id,
        o.total_amount,
        c.customer_name
    FROM (
        SELECT
            order_id,
            customer_id,
            SUM(amount) as total_amount
        FROM order_lines
        GROUP BY order_id, customer_id
    ) o
    JOIN (
        SELECT customer_id, customer_name
        FROM customers
        WHERE status = 'active'
    ) c ON o.customer_id = c.customer_id
    WHERE o.total_amount > 1000
    """

    result = normalize_to_ctes(sql1)
    print(f"CTEs extracted: {result.ctes_extracted}")
    print("Transformations:")
    for t in result.transformations:
        print(f"  - {t}")

    # Test case 2: Suggestions
    print("\n--- Test 2: CTE Suggestions ---")
    sql2 = """
    SELECT
        c.customer_id,
        c.name,
        (SELECT COUNT(*) FROM orders WHERE customer_id = c.customer_id) as order_count,
        (SELECT SUM(amount) FROM orders WHERE customer_id = c.customer_id) as total_spent,
        (SELECT MAX(order_date) FROM orders WHERE customer_id = c.customer_id) as last_order
    FROM customers c
    WHERE c.status = 'active'
    """

    suggestions = suggest_cte_structure(sql2)
    print("Suggestions:")
    for s in suggestions:
        print(f"  - {s}")

    # Test case 3: Standardize names
    print("\n--- Test 3: Standardize CTE Names ---")
    sql3 = """
    WITH CustomerOrders AS (
        SELECT customer_id, COUNT(*) as cnt FROM orders GROUP BY customer_id
    ),
    ActiveCustomers AS (
        SELECT * FROM customers WHERE status = 'active'
    )
    SELECT * FROM ActiveCustomers a JOIN CustomerOrders o ON a.customer_id = o.customer_id
    """

    standardized = standardize_cte_names(sql3, "snake_case", "cte_")
    print("Standardized SQL (snake_case with prefix):")
    print(standardized[:200] + "...")

    # Test case 4: Flatten deeply nested
    print("\n--- Test 4: Flatten Nested Subqueries ---")
    sql4 = """
    SELECT * FROM (
        SELECT * FROM (
            SELECT * FROM (
                SELECT customer_id, SUM(amount) as total
                FROM orders
                GROUP BY customer_id
            ) level3
            WHERE total > 100
        ) level2
        WHERE total > 500
    ) level1
    WHERE total > 1000
    """

    flatten_result = flatten_nested_subqueries(sql4, max_depth=2)
    print("Transformations:")
    for t in flatten_result.transformations:
        print(f"  - {t}")
