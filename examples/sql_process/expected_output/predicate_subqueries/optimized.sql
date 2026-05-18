-- @mdde-entity: predicate_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: filter
-- @mdde-description: Exercises lifting of WHERE IN, WHERE EXISTS, and
-- correlated subqueries. Each predicate's inner SELECT becomes its own
-- CTE; the outer predicate keeps its IN/EXISTS structure but references
-- the new CTE. Correlated cases carry the correlation column up to the
-- lifted CTE's projection so the outer WHERE can re-correlate.

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: predicate_subqueries.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] Modular CTE structure applied.
- [X] Table qualifiers normalised.
*/

-- Source prep: single-table SELECT + renames + single-source value transforms
WITH customer_prepared AS (
  SELECT
    customer_id AS customer_id,
    email AS email,
    country AS country
  FROM schema_identifier_ssf_snapshot.customer
), _sub1 AS (
  SELECT
    o.customer_id
  FROM schema_identifier_ssf_snapshot.orders AS o
  WHERE
    o.channel = 'WEB'
), _sub2 AS (
  SELECT
    customer_id
  FROM schema_identifier_ssf_snapshot.loans AS l
  WHERE
    l.status = 'OPEN'
)
/* @mdde-entity: predicate_subqueries */
/* @mdde-layer: business */
/* @mdde-stereotype: filter */
/* @mdde-description: Exercises lifting of WHERE IN, WHERE EXISTS, and */
/* correlated subqueries. Each predicate's inner SELECT becomes its own */
/* CTE; the outer predicate keeps its IN/EXISTS structure but references */
/* the new CTE. Correlated cases carry the correlation column up to the */
/* lifted CTE's projection so the outer WHERE can re-correlate. */
SELECT
  c.customer_id,
  c.email,
  c.country
FROM customer_prepared AS c
WHERE
  c.customer_id IN (
    SELECT
      customer_id
    FROM _sub1
  )
  AND EXISTS(
    SELECT
      1
    FROM _sub2
    WHERE
      _sub2.customer_id = c.customer_id
  )