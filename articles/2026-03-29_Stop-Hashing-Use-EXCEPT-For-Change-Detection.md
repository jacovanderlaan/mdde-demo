# Stop Hashing. Use EXCEPT for Change Detection.

**Why the simplest approach is often the best — and when it isn't**

---

## The Hash Habit

Every data engineer has written this pattern:

```sql
SELECT *
FROM source s
LEFT JOIN target t
  ON s.business_key = t.business_key
WHERE t.business_key IS NULL  -- Inserts
   OR HASHBYTES('SHA2_256',
        CONCAT(ISNULL(s.col1, ''), '|',
               ISNULL(CAST(s.col2 AS VARCHAR), ''), '|',
               ISNULL(s.col3, '')))
      <> t.row_hash  -- Updates
```

It works. It's familiar. And it's almost always more complex than it needs to be.

There's a simpler approach that handles edge cases better, runs faster, and requires less maintenance. It's been in SQL since 1992.

```sql
SELECT col1, col2, col3, business_key
FROM source
EXCEPT
SELECT col1, col2, col3, business_key
FROM target
```

Let me explain why this should be your default.

---

## The Case for EXCEPT

### 1. NULLs Just Work

The most fragile part of hash-based change detection is NULL handling.

```sql
-- Hash approach: defensive coding required
HASHBYTES('SHA2_256',
  ISNULL(col1, '') + '|' +
  ISNULL(col2, '') + '|' +
  ISNULL(CAST(col3 AS VARCHAR(50)), '') + '|' +
  ISNULL(CAST(col4 AS VARCHAR(20)), '')
)
```

What happens when `col1` legitimately contains the value `''`? Or `'NULL'`? Or your delimiter `'|'`? You need to pick a sentinel value that will never appear in your data. That's a time bomb.

```sql
-- EXCEPT approach: no sentinel values, no edge cases
SELECT col1, col2, col3, col4 FROM source
EXCEPT
SELECT col1, col2, col3, col4 FROM target
```

`EXCEPT` treats `NULL = NULL` as true for set comparison purposes. This is exactly what you want for change detection: "is this row the same as that row, including NULLs?"

### 2. No Type Casting Gymnastics

Hash functions operate on strings. Your data isn't all strings.

```sql
-- Hash approach: cast everything
CONCAT(
  ISNULL(CAST(id AS VARCHAR(20)), ''),
  '|',
  ISNULL(CAST(amount AS VARCHAR(30)), ''),  -- Precision loss?
  '|',
  ISNULL(CAST(created_at AS VARCHAR(30)), ''),  -- Format?
  '|',
  ISNULL(CAST(is_active AS VARCHAR(5)), '')
)
```

What format do you use for dates? What precision for decimals? Different choices produce different hashes for identical values.

```sql
-- EXCEPT approach: native type comparison
SELECT id, amount, created_at, is_active FROM source
EXCEPT
SELECT id, amount, created_at, is_active FROM target
```

The database compares values in their native types. `DECIMAL(18,2)` compares as `DECIMAL(18,2)`, not as a string representation that might round differently.

### 3. No Hash Computation Overhead

`HASHBYTES` is CPU-intensive. On a table with 100 columns and 10 million rows, you're computing 10 million cryptographic hashes — every single load.

```sql
-- Hash approach: CPU-bound
SELECT business_key,
       HASHBYTES('SHA2_256', CONCAT(...100 columns...)) AS row_hash
FROM source
-- 10 million hash computations
```

```sql
-- EXCEPT approach: optimizer chooses strategy
SELECT col1, col2, ..., col100 FROM source
EXCEPT
SELECT col1, col2, ..., col100 FROM target
-- Hash match or merge join, optimizer's choice
```

The query optimizer can use hash match or merge join operators natively. It chooses the strategy based on statistics, available indexes, and memory. You're not forcing a specific algorithm.

### 4. Zero Collision Risk

SHA-256 collision probability is approximately 1 in 2^128. That's vanishingly small.

But "vanishingly small" is not "impossible." And when you're processing billions of rows across thousands of tables over years of operation, rare events happen.

```sql
-- EXCEPT: compares actual values
-- No collisions, ever
```

More importantly: when something goes wrong with `EXCEPT`, the error is obvious — missing or extra rows. When something goes wrong with hash collision (or hash generation bugs), you get **silent data corruption**.

### 5. Trivial Maintenance

Adding a column to your change detection logic:

**Hash approach:**
```sql
-- Find the CONCAT chain
-- Add the new column in the right position
-- Don't forget ISNULL
-- Don't forget the correct CAST
-- Don't forget the delimiter
-- Test that you didn't break the existing hash values
CONCAT(
  ...,
  ISNULL(CAST(new_column AS VARCHAR(50)), ''),  -- Added
  '|',
  ...
)
```

**EXCEPT approach:**
```sql
SELECT col1, col2, col3, new_column FROM source  -- Added
EXCEPT
SELECT col1, col2, col3, new_column FROM target  -- Added
```

Add it to both SELECT lists. Done.

---

## The Complete Pattern

Here's a production-ready change detection pattern using EXCEPT:

```sql
-- Identify inserts (in source, not in target)
WITH source_keys AS (
  SELECT business_key FROM source
),
target_keys AS (
  SELECT business_key FROM target
),
inserts AS (
  SELECT business_key FROM source_keys
  EXCEPT
  SELECT business_key FROM target_keys
),

-- Identify deletes (in target, not in source)
deletes AS (
  SELECT business_key FROM target_keys
  EXCEPT
  SELECT business_key FROM source_keys
),

-- Identify updates (same key, different values)
updates AS (
  SELECT s.business_key
  FROM source s
  INNER JOIN target t ON s.business_key = t.business_key
  WHERE EXISTS (
    SELECT s.col1, s.col2, s.col3
    EXCEPT
    SELECT t.col1, t.col2, t.col3
  )
)

SELECT 'INSERT' AS operation, business_key FROM inserts
UNION ALL
SELECT 'DELETE' AS operation, business_key FROM deletes
UNION ALL
SELECT 'UPDATE' AS operation, business_key FROM updates
```

Or more concisely for SCD Type 1 loads:

```sql
-- All changed rows (inserts + updates)
SELECT s.*
FROM source s
WHERE EXISTS (
  SELECT s.business_key, s.col1, s.col2, s.col3
  EXCEPT
  SELECT t.business_key, t.col1, t.col2, t.col3
  FROM target t
  WHERE t.business_key = s.business_key
)
```

---

## When Hashing Earns Its Keep

EXCEPT isn't always the answer. There are legitimate cases for hash-based detection:

### 1. Pre-Computed Hash Columns

If you compute the hash **once on ingestion** and store it as a column:

```sql
-- Ingestion: compute once
INSERT INTO staging (business_key, col1, col2, row_hash)
SELECT
  business_key,
  col1,
  col2,
  HASHBYTES('SHA2_256', CONCAT(...)) AS row_hash
FROM source_system

-- Comparison: single column
SELECT s.business_key
FROM staging s
INNER JOIN target t ON s.business_key = t.business_key
WHERE s.row_hash <> t.row_hash
```

This is efficient because you're comparing one column instead of 100. The hash computation happened at ingestion, not at comparison time.

### 2. Cross-Database Comparison

EXCEPT doesn't work across different database engines:

```sql
-- This doesn't work
SELECT * FROM oracle_source.customers
EXCEPT
SELECT * FROM snowflake_target.customers
```

For cross-engine comparison (migration testing, replication validation), you need a portable representation. Hash strings work here.

### 3. Network Bandwidth Constraints

When comparing data across a slow network link, sending a hash column is cheaper than sending all columns:

```sql
-- Send from remote system: small payload
SELECT business_key, row_hash FROM remote_source

-- Compare locally: fast
SELECT r.business_key
FROM remote_hashes r
INNER JOIN local_target t ON r.business_key = t.business_key
WHERE r.row_hash <> t.row_hash
```

---

## The Determinism Problem

There's a deeper issue that affects both approaches: **non-deterministic SQL**.

Consider this change detection query:

```sql
SELECT
  customer_id,
  ROW_NUMBER() OVER (PARTITION BY region) AS row_num,
  first_order_date
FROM customers
EXCEPT
SELECT
  customer_id,
  ROW_NUMBER() OVER (PARTITION BY region) AS row_num,
  first_order_date
FROM customers_snapshot
```

This will **always** show changes, even when the data is identical. Why? Because `ROW_NUMBER()` without `ORDER BY` is non-deterministic — it assigns numbers arbitrarily.

The same problem affects hash-based detection:

```sql
-- This hash will be different every time
SELECT HASHBYTES('SHA2_256',
  CONCAT(customer_id, '|',
         ROW_NUMBER() OVER (PARTITION BY region), '|',  -- Non-deterministic!
         first_order_date))
```

### Common Non-Deterministic Patterns

| Pattern | Problem |
|---------|---------|
| `ROW_NUMBER() OVER (PARTITION BY x)` | No ORDER BY |
| `ROW_NUMBER() OVER (ORDER BY created_at)` | Ties possible |
| `FIRST_VALUE(x) OVER (PARTITION BY y)` | No ORDER BY |
| `SELECT * FROM t LIMIT 10` | No ORDER BY |
| `NOW()`, `RANDOM()`, `UUID()` | Volatile functions |

### The Fix: Deterministic SQL

Every window function needs a **unique** ORDER BY:

```sql
-- Non-deterministic: ties possible on created_at
ROW_NUMBER() OVER (PARTITION BY region ORDER BY created_at)

-- Deterministic: id breaks ties
ROW_NUMBER() OVER (PARTITION BY region ORDER BY created_at, id)
```

The MDDE framework includes a [determinism checker](https://github.com/jacovanderlaan/mdde-demo) that detects these patterns:

```python
from mdde_lite.determinism import check_determinism

sql = """
SELECT ROW_NUMBER() OVER (PARTITION BY region) AS rn
FROM customers
"""

issues = check_determinism(sql)
# Returns: DeterminismIssue(
#   type='WINDOW_NO_ORDER',
#   message='ROW_NUMBER without ORDER BY is non-deterministic',
#   suggestion='Add ORDER BY with unique column'
# )
```

---

## Putting It Together: MDDE's Approach

The [MDDE (Metadata-Driven Data Engineering)](https://github.com/jacovanderlaan/mdde-demo) framework generates change detection SQL based on entity metadata:

```yaml
# Entity definition
entity:
  name: dim_customer
  stereotype: dim_scd2

  change_detection:
    method: except  # or: hash_column
    business_key: [customer_id]
    compare_columns: [customer_name, email, segment]
    exclude: [_loaded_at, _batch_id, _valid_from, _valid_to]
```

The generator produces:

```sql
-- Generated SCD2 merge with EXCEPT-based change detection
WITH source_data AS (
  SELECT customer_id, customer_name, email, segment
  FROM staging.customers
),
current_records AS (
  SELECT customer_id, customer_name, email, segment
  FROM gold.dim_customer
  WHERE _is_current = TRUE
),
changed_records AS (
  SELECT s.customer_id
  FROM source_data s
  INNER JOIN current_records c ON s.customer_id = c.customer_id
  WHERE EXISTS (
    SELECT s.customer_name, s.email, s.segment
    EXCEPT
    SELECT c.customer_name, c.email, c.segment
  )
)
-- MERGE logic follows...
```

For cases where hash is appropriate (pre-computed, cross-database), the configuration switches:

```yaml
change_detection:
  method: hash_column
  hash_column: row_hash
  hash_algorithm: SHA256
```

The framework also validates determinism before generating comparison SQL, preventing the non-deterministic patterns that break both approaches.

---

## Decision Framework

| Scenario | Use EXCEPT | Use Hash |
|----------|-----------|----------|
| Same-database ETL | ✓ | |
| Wide tables (50+ columns) | ✓ | |
| Frequent schema changes | ✓ | |
| Pre-computed hash available | | ✓ |
| Cross-database comparison | | ✓ |
| Network-constrained replication | | ✓ |
| Audit trail of row versions | | ✓ |

Default to EXCEPT. Switch to hash when you have a specific reason.

---

## Summary

1. **EXCEPT handles NULLs correctly** — no ISNULL chains, no sentinel values
2. **EXCEPT preserves types** — no CAST precision loss, no format mismatches
3. **EXCEPT has no computation overhead** — the optimizer chooses the strategy
4. **EXCEPT has zero collision risk** — compares actual values
5. **EXCEPT is trivial to maintain** — add column to both SELECT lists

Hash-based detection earns its place when:
- You pre-compute and store the hash
- You're comparing across different databases
- Network bandwidth is the constraint

But for same-database, source-to-target loads? **EXCEPT is simpler and faster.**

---

**Related reading:**
- [The Hidden Danger of Non-Deterministic SQL](./the-hidden-danger-of-non-deterministic-sql.md)
- [MDDE Demo Repository](https://github.com/jacovanderlaan/mdde-demo)
- [Determinism Checker Module](https://github.com/jacovanderlaan/mdde-demo/blob/main/src/mdde_lite/determinism.py)

---

*This article is part of my series on practical data engineering patterns. The examples use SQL Server syntax, but the concepts apply to any database that supports EXCEPT (which is most of them: PostgreSQL, Snowflake, Databricks, BigQuery, Oracle, SQL Server).*
