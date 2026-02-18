# The Hidden Danger of Non-Deterministic SQL

**Why Your Queries May Return Different Results Every Time — And Why It Matters**

---

## The Query That Worked Until It Didn't

Last month, a client's data team spent three days debugging a "migration bug" that didn't exist.

They were migrating a data warehouse from Oracle to Snowflake. The migration tests kept failing — results didn't match between systems. After extensive investigation, they discovered the root cause:

```sql
SELECT
    customer_id,
    region,
    ROW_NUMBER() OVER (PARTITION BY region) AS rn
FROM customers
```

This query was producing different results on each system. Not because of a migration bug, but because **the query itself is non-deterministic**.

Without an ORDER BY clause, ROW_NUMBER() assigns row numbers arbitrarily. Different database engines, different execution plans, or even different runs on the same system can produce different results.

**The migration was correct. The SQL was broken.**

---

## What Makes SQL Non-Deterministic?

Non-deterministic SQL produces different results with the same input data. The most common patterns:

### 1. Window Functions Without ORDER BY

```sql
-- Non-deterministic: Which row gets rn=1?
SELECT
    ROW_NUMBER() OVER (PARTITION BY department) AS rn,
    employee_name
FROM employees

-- Also problematic: FIRST_VALUE, LAST_VALUE, LAG, LEAD
SELECT FIRST_VALUE(salary) OVER (PARTITION BY department) AS first_salary
FROM employees
```

Without ORDER BY, the database chooses an arbitrary ordering. Different query plans = different results.

### 2. Window Functions With Non-Unique ORDER BY

```sql
-- Still non-deterministic: Multiple employees can share the same hire_date
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY department
        ORDER BY hire_date
    ) AS rn,
    employee_name
FROM employees
```

If two employees have the same `hire_date`, which one gets `rn=1`? The database decides arbitrarily.

### 3. LIMIT Without ORDER BY

```sql
-- Returns different 10 rows each time
SELECT * FROM orders LIMIT 10

-- Correct version
SELECT * FROM orders ORDER BY order_id LIMIT 10
```

### 4. Volatile Functions

```sql
-- Different values every execution
SELECT
    customer_id,
    RANDOM() AS random_value,
    NOW() AS execution_time,
    UUID() AS unique_id
FROM customers
```

---

## Why This Matters: Three Critical Use Cases

### 1. Regression Testing During Migrations

When migrating from one database platform to another, you need to compare results:

```
Source System (Oracle)  →  Compare  ←  Target System (Snowflake)
     Query Results A                        Query Results B
```

If Results A ≠ Results B, is it:
- A migration bug? (Bad)
- Non-deterministic SQL? (Noise)

**With non-deterministic SQL, you can never know.** Teams waste days chasing phantom bugs while real issues hide in the noise.

### 2. Reproducible Analytics

Imagine running a quarterly revenue report:
- Monday: $10.2M
- Tuesday (same data): $10.4M
- Wednesday: $10.1M

If your report uses non-deterministic window functions for deduplication or ranking, results vary randomly. Good luck explaining that to the CFO.

### 3. Incremental Processing

Many pipelines use window functions for deduplication:

```sql
WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at  -- But what if two records have same updated_at?
        ) AS rn
    FROM raw_customers
)
SELECT * FROM deduplicated WHERE rn = 1
```

If `updated_at` isn't unique, today's "winner" might be tomorrow's "loser" — silently changing downstream data.

---

## The Solution: Tie-Breaker Columns

The fix is simple: ensure your ORDER BY is **unique within each partition**.

### Add a Tie-Breaker Column

```sql
-- Before: Non-deterministic
ROW_NUMBER() OVER (PARTITION BY department ORDER BY hire_date)

-- After: Deterministic (employee_id breaks ties)
ROW_NUMBER() OVER (PARTITION BY department ORDER BY hire_date, employee_id)
```

### Common Tie-Breaker Columns

| Column Type | Example | When to Use |
|-------------|---------|-------------|
| Primary Key | `customer_id`, `order_id` | Always available, always unique |
| Source Row ID | `_source_row_id` | For ETL from external systems |
| Load Timestamp | `_load_ts` | When records have load metadata |
| Surrogate Key | `sk`, `row_key` | For data warehouse patterns |
| Composite Key | `customer_id, order_line` | For natural keys |

---

## Beyond Detection: DQ Monitoring Columns

Detection finds problems in code. But what about **data** that causes non-determinism?

Consider: even with a unique ORDER BY, you might want to know when ties *would* have existed. This helps:

1. Validate that your tie-breaker actually breaks ties
2. Monitor for data quality issues that create ties
3. Debug when downstream processes behave unexpectedly

### Adding DQ Monitoring Columns

```sql
SELECT
    customer_id,
    region,
    created_at,

    -- Original window function with tie-breaker
    ROW_NUMBER() OVER (
        PARTITION BY region
        ORDER BY created_at, customer_id
    ) AS rn,

    -- DQ: Count records with same partition+order values (before tie-breaker)
    COUNT(*) OVER (
        PARTITION BY region, created_at
    ) AS _dq_tie_count,

    -- DQ: Flag rows where ties exist
    CASE
        WHEN COUNT(*) OVER (PARTITION BY region, created_at) > 1
        THEN 'TIE_EXISTS'
        ELSE 'NO_TIE'
    END AS _dq_determinism_status

FROM customers
```

Now you can:
- Filter: `WHERE _dq_tie_count > 1` to see all tie scenarios
- Monitor: Track `_dq_tie_count > 1` over time as a DQ metric
- Debug: Understand exactly which rows might have been ordered differently

---

## Implementation: MDDE Lite Determinism Checker

The [MDDE Lite](https://github.com/jacovanderlaan/mdde-demo) framework includes a determinism checker:

```python
from mdde_lite.determinism import check_determinism

sql = """
SELECT
    ROW_NUMBER() OVER (PARTITION BY region) AS rn,
    customer_name
FROM customers
"""

issues = check_determinism(sql)
for issue in issues:
    print(f"[{issue.severity}] {issue.issue_type}")
    print(f"  {issue.message}")
    print(f"  Suggestion: {issue.suggestion}")
```

Output:
```
[ERROR] WINDOW_NO_ORDER
  ROW_NUMBER() without ORDER BY - results are non-deterministic
  Suggestion: Add ORDER BY clause with unique columns to ROW_NUMBER()
```

### Checks Available

| Check | Severity | Pattern |
|-------|----------|---------|
| WINDOW_NO_ORDER | Error | ROW_NUMBER/RANK without ORDER BY |
| WINDOW_NON_UNIQUE_ORDER | Warning | ORDER BY may not be unique |
| FIRST_LAST_NO_ORDER | Error | FIRST_VALUE/LAST_VALUE without ORDER BY |
| LIMIT_NO_ORDER | Error | LIMIT/TOP without ORDER BY |
| VOLATILE_FUNCTION | Warning | RANDOM(), NOW(), UUID() |

---

## Best Practices Summary

### 1. Always Include ORDER BY in Window Functions

```sql
-- Always
ROW_NUMBER() OVER (PARTITION BY x ORDER BY y, z)
FIRST_VALUE(col) OVER (PARTITION BY x ORDER BY y)
```

### 2. Ensure ORDER BY Is Unique

```sql
-- Add primary key or unique column as final ORDER BY element
ORDER BY business_date, transaction_id  -- transaction_id is unique
```

### 3. Add ORDER BY Before LIMIT

```sql
SELECT * FROM orders
ORDER BY order_id  -- Deterministic selection
LIMIT 100
```

### 4. Parameterize Time-Dependent Values

```sql
-- Instead of
WHERE created_at > NOW() - INTERVAL '7 days'

-- Use
WHERE created_at > @report_date - INTERVAL '7 days'
```

### 5. Add DQ Monitoring for Critical Paths

For deduplication or ranking logic that affects downstream systems, add `_dq_tie_count` columns to detect data patterns that test your assumptions.

---

## Conclusion

Non-deterministic SQL is a hidden source of data quality issues. It causes:

- Failed regression tests during migrations
- Unreproducible analytics
- Silent data changes
- Wasted debugging time

The fix is straightforward:

1. **Detect** non-deterministic patterns with static analysis
2. **Fix** by adding unique tie-breaker columns to ORDER BY
3. **Monitor** with DQ columns that detect tie scenarios in data

Your SQL should be a function: same input → same output. Every time.

---

*Try the determinism checker: [github.com/jacovanderlaan/mdde-demo](https://github.com/jacovanderlaan/mdde-demo)*

*More articles on Metadata-Driven Data Engineering: [@jaco.vanderlaan](https://medium.com/@jaco.vanderlaan)*
