# ADR-004: Deterministic SQL Patterns

**Status**: Accepted
**Date**: 2026-02-18
**Category**: analyzer, generator

## Context

SQL queries can produce non-deterministic results that vary between executions, even with identical input data. This causes critical problems in:

1. **Regression Testing** - When migrating SQL from one platform to another (e.g., Oracle to Snowflake), we need to compare results between systems. Non-deterministic SQL makes this comparison meaningless.

2. **Reproducibility** - Analytics and reports must produce the same output when re-run on the same data.

3. **Debugging** - Inconsistent results make it nearly impossible to identify root causes of data issues.

4. **Data Quality** - Silent changes in output create hidden data quality problems.

### Common Non-Deterministic Patterns

| Pattern | Example | Problem |
|---------|---------|---------|
| ROW_NUMBER without ORDER BY | `ROW_NUMBER() OVER (PARTITION BY dept)` | Different row assigned number 1 each run |
| ROW_NUMBER with non-unique ORDER BY | `ROW_NUMBER() OVER (ORDER BY date)` | Ties resolved arbitrarily |
| FIRST_VALUE without ORDER BY | `FIRST_VALUE(name) OVER (PARTITION BY dept)` | Arbitrary "first" row selected |
| LIMIT without ORDER BY | `SELECT * FROM t LIMIT 10` | Random 10 rows returned |
| Volatile functions | `SELECT RANDOM(), NOW()` | Different values each execution |

### Why This Matters for Migration Testing

During SQL migrations, teams compare query results between old and new systems:

```
Legacy System (Oracle)  →  Compare  ←  New System (Snowflake)
     Results A                              Results B
```

If the SQL is non-deterministic:
- Results A and B may differ even if both systems are correct
- Teams waste time investigating "differences" that are just random variation
- Real migration bugs are masked by noise from non-determinism
- Confidence in migration correctness is impossible to achieve

## Decision

We implement determinism checking with three components:

### 1. Detection Rules

Detect and flag non-deterministic patterns:

| Check ID | Pattern | Severity |
|----------|---------|----------|
| WINDOW_NO_ORDER | Window function without ORDER BY | Error |
| WINDOW_NON_UNIQUE_ORDER | ORDER BY columns may not be unique | Warning |
| FIRST_LAST_NO_ORDER | FIRST_VALUE/LAST_VALUE without ORDER BY | Error |
| LAG_LEAD_NO_ORDER | LAG/LEAD without ORDER BY | Error |
| LIMIT_NO_ORDER | LIMIT/TOP without ORDER BY | Error |
| VOLATILE_FUNCTION | RANDOM(), NOW(), UUID() | Warning |

### 2. Tie-Breaker Recommendations

When ORDER BY exists but may not be unique, suggest "tie-breaker" columns:

**Priority order for tie-breaker columns:**
1. `_source_row_id` - Source system row identifier
2. `row_id`, `_row_id` - Generic row identifiers
3. `_load_timestamp`, `load_ts` - Load timestamps
4. `created_at` - Creation timestamps
5. `surrogate_key`, `sk` - Surrogate keys
6. Primary key columns

**Example fix:**
```sql
-- Non-deterministic
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hire_date)

-- Deterministic (added tie-breaker)
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hire_date, employee_id)
```

### 3. DQ Monitoring Columns

Generate data quality columns that detect when non-determinism would have affected results:

```sql
SELECT
    customer_id,
    region,
    ROW_NUMBER() OVER (
        PARTITION BY region
        ORDER BY created_at, customer_id  -- customer_id is tie-breaker
    ) AS rn,

    -- DQ Monitoring: Detect ties
    COUNT(*) OVER (
        PARTITION BY region, created_at
    ) AS _dq_tie_count,

    -- DQ Flag: Would this row be affected by non-determinism?
    CASE
        WHEN COUNT(*) OVER (PARTITION BY region, created_at) > 1
        THEN 'POTENTIAL_TIE'
        ELSE 'DETERMINISTIC'
    END AS _dq_determinism_status

FROM customers
```

The `_dq_tie_count` column shows how many rows share the same partition+order values. If > 1, ties exist and the tie-breaker column determines the order.

## Consequences

### Positive

- **Migration confidence** - Can trust result comparisons during migrations
- **Reproducible analytics** - Same query produces same results
- **Early detection** - Catch non-determinism before it causes production issues
- **DQ monitoring** - Ongoing detection of data that could cause non-determinism

### Negative

- **Additional columns** - DQ monitoring columns add overhead
- **False positives** - Some flagged patterns may be intentionally non-deterministic
- **Performance** - Adding tie-breaker columns may slightly impact window function performance

### Neutral

- **Code changes required** - Existing queries may need modification
- **Developer education** - Teams need to understand determinism concepts

## Implementation

### Detection (analyzer)

```python
from mdde_lite.determinism import check_determinism

issues = check_determinism(sql)
for issue in issues:
    print(f"{issue.issue_type}: {issue.message}")
    print(f"  Suggestion: {issue.suggestion}")
```

### Generation (generator)

When generating SQL with window functions:

1. Always include ORDER BY clause
2. Ensure ORDER BY includes a unique column combination
3. Optionally add `_dq_tie_count` monitoring column

### Validation (validator)

During migration testing:

1. Run determinism check on both source and target SQL
2. Fix any non-deterministic patterns before comparing results
3. Use DQ monitoring columns to identify data that could cause differences

## Related

- [ADR-001: YAML over JSON](ADR-001-yaml-over-json.md) - Model format decision
- MDDE Full Framework: `src/mdde/analyzer/checks/determinism_checker.py`
- MDDE ADR-007: "Deterministic SQL Generation Is a Hard Invariant"

## References

- "Testing Query Migrations Using Synthetic Data" (Medium article)
- "When Migrations Meet Reality: Handling Schema Drift" (Medium article)
- SQL Standard: Window function ordering semantics (ISO/IEC 9075)
