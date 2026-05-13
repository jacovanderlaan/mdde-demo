-- @mdde-entity: union_revenue_breakdown
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Web vs store revenue breakdown via UNION ALL.
-- Exercises UNION-branch extraction: each branch becomes its own CTE
-- named from its `'X' AS channel` literal tag (`web` / `store`).

/*
Migration Details:
- Original SQL File: union_revenue_breakdown.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted each `UNION ALL` branch into its own CTE; top-level statement is a pure `SELECT * FROM cte_a UNION ALL SELECT * FROM cte_b ...`.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] UNION branches lifted to CTEs.
- [X] Table qualifiers normalised.
*/

WITH web AS (
  -- Source filter: single-table SELECT + WHERE for one source
  WITH orders_filtered AS (
    SELECT
      customer_id AS customer_id,
      order_date AS order_date,
      amount AS amount
    FROM schema_identifier_ssf_snapshot.orders
    WHERE
      channel = 'WEB'
  )
  /* @mdde-entity: union_revenue_breakdown */ /* @mdde-layer: business */ /* @mdde-stereotype: fact */ /* @mdde-description: Web vs store revenue breakdown via UNION ALL. */ /* Exercises UNION-branch extraction: each branch becomes its own CTE */ /* named from its `'X' AS channel` literal tag (`web` / `store`). */
  SELECT
    o.customer_id,
    o.order_date,
    o.amount,
    'web' AS channel
  FROM orders_filtered AS o
), store AS (
  -- Source filter: single-table SELECT + WHERE for one source
  WITH orders_filtered AS (
    SELECT
      customer_id AS customer_id,
      order_date AS order_date,
      amount AS amount
    FROM schema_identifier_ssf_snapshot.orders
    WHERE
      channel = 'STORE'
  )
  SELECT
    o.customer_id,
    o.order_date,
    o.amount,
    'store' AS channel
  FROM orders_filtered AS o
)
SELECT
  *
FROM web
UNION ALL
SELECT
  *
FROM store