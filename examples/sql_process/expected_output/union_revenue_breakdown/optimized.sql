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
  /* @mdde-entity: union_revenue_breakdown */ /* @mdde-layer: business */ /* @mdde-stereotype: fact */ /* @mdde-description: Web vs store revenue breakdown via UNION ALL. */ /* Exercises UNION-branch extraction: each branch becomes its own CTE */ /* named from its `'X' AS channel` literal tag (`web` / `store`). */
  SELECT
    o.customer_id AS customer_id,
    o.order_date AS order_date,
    o.amount AS amount,
    'web' AS channel
  FROM schema_identifier_ssf_snapshot.orders AS o
  WHERE
    o.channel = 'WEB'
), store AS (
  SELECT
    o.customer_id AS customer_id,
    o.order_date AS order_date,
    o.amount AS amount,
    'store' AS channel
  FROM schema_identifier_ssf_snapshot.orders AS o
  WHERE
    o.channel = 'STORE'
)
SELECT
  *
FROM web
UNION ALL
SELECT
  *
FROM store