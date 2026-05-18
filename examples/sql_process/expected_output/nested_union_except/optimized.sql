-- @mdde-entity: nested_union_except
-- @mdde-layer: business
-- @mdde-stereotype: filter
-- @mdde-description: Three-branch UNION ALL where one branch is itself an
-- EXCEPT. Exercises UNION + EXCEPT composition: each top-level UNION branch
-- gets layered; an EXCEPT branch's two sides become their own CTEs too.

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: nested_union_except.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Lifted each `UNION ALL` branch into its own CTE; top-level statement is a pure `SELECT * FROM cte_a UNION ALL SELECT * FROM cte_b ...`.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] UNION branches lifted to CTEs.
- [X] Table qualifiers normalised.
*/

WITH _sub1 AS (
  SELECT
    customer_id AS customer_id,
    'churned' AS channel
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    order_date >= '2024-01-01'
), orders AS (
  /* @mdde-entity: nested_union_except */
  /* @mdde-layer: business */
  /* @mdde-stereotype: filter */
  /* @mdde-description: Three-branch UNION ALL where one branch is itself an */
  /* EXCEPT. Exercises UNION + EXCEPT composition: each top-level UNION branch */
  /* gets layered; an EXCEPT branch's two sides become their own CTEs too. */
  SELECT
    customer_id AS customer_id,
    'web' AS channel
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    channel = 'WEB'
), orders_2 AS (
  SELECT
    customer_id AS customer_id,
    'store' AS channel
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    channel = 'STORE'
), customer AS (
  SELECT
    customer_id AS customer_id,
    'churned' AS channel
  FROM schema_identifier_ssf_snapshot.customer
)
SELECT
  *
FROM orders
UNION ALL
SELECT
  *
FROM orders_2
UNION ALL
SELECT
  *
FROM customer
EXCEPT
SELECT
  *
FROM _sub1