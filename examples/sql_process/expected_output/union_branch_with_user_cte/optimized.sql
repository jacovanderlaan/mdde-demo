-- @mdde-entity: union_branch_with_user_cte
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: UNION ALL where ONE branch has its own user-defined WITH
-- clause. Verifies recursive per-branch layering doesn't clobber the user's
-- inner CTEs.

/*
Migration Details:
- Original SQL File: union_branch_with_user_cte.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted each `UNION ALL` branch into its own CTE; top-level statement is a pure `SELECT * FROM cte_a UNION ALL SELECT * FROM cte_b ...`.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] UNION branches lifted to CTEs.
- [X] Table qualifiers normalised.
*/

WITH orders AS (
  /* @mdde-entity: union_branch_with_user_cte */
  /* @mdde-layer: business */
  /* @mdde-stereotype: fact */
  /* @mdde-description: UNION ALL where ONE branch has its own user-defined WITH */
  /* clause. Verifies recursive per-branch layering doesn't clobber the user's */
  /* inner CTEs. */
  SELECT
    customer_id AS customer_id,
    amount AS amount,
    'web' AS channel
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    channel = 'WEB'
), recent_orders AS (
  WITH recent_orders AS (
    SELECT
      customer_id,
      amount
    FROM schema_identifier_ssf_snapshot.orders
    WHERE
      order_date >= '2024-01-01'
)
  -- Source filter: single-table SELECT + WHERE for one source
  , recent_orders_filtered AS (
    SELECT
      customer_id AS customer_id,
      amount AS amount
    FROM recent_orders
    WHERE
      amount > 100
  )
  SELECT
    ro.customer_id,
    ro.amount,
    'recent' AS channel
  FROM recent_orders_filtered AS ro
)
SELECT
  *
FROM orders
UNION ALL
SELECT
  *
FROM recent_orders