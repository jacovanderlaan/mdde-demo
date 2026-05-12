-- @mdde-entity: customer_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Subquery shapes that should be lifted to CTEs (plus two that should be left inline)

-- Planted shapes:
--   1. Derived table in FROM        -> lift (alias 'recent_orders')
--   2. Derived table in JOIN        -> lift (alias 'order_totals')
--   3. Scalar subquery in SELECT    -> lift (uncorrelated)
--   4. WHERE IN (SELECT ...)        -> SKIP (predicate -> SUBQUERY_NOT_LIFTED)
--   5. Correlated subquery in SELECT-> SKIP (correlated -> SUBQUERY_NOT_LIFTED)

/*
Migration Details:
- Original SQL File: customer_subqueries.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] Modular CTE structure applied.
*/

/* @mdde-entity: customer_subqueries */ /* @mdde-layer: business */ /* @mdde-stereotype: fact */ /* @mdde-description: Subquery shapes that should be lifted to CTEs (plus two that should be left inline) */ /* Planted shapes: */ /*   1. Derived table in FROM        -> lift (alias 'recent_orders') */ /*   2. Derived table in JOIN        -> lift (alias 'order_totals') */ /*   3. Scalar subquery in SELECT    -> lift (uncorrelated) */ /*   4. WHERE IN (SELECT ...)        -> SKIP (predicate -> SUBQUERY_NOT_LIFTED) */ /*   5. Correlated subquery in SELECT-> SKIP (correlated -> SUBQUERY_NOT_LIFTED) */
CREATE OR REPLACE VIEW customer_subqueries AS
WITH stg_customers_prepared AS (
  SELECT
    customer_id, /* @pk @business_key */
    email /* @pii */
  FROM stg_customers
), _sub1 AS (
  SELECT
    MAX(total_amount) AS value
  FROM raw_orders
), r AS (
  SELECT
    customer_id,
    MAX(order_date) AS last_order_date
  FROM raw_orders
  WHERE
    order_status = 'SHIPPED'
  GROUP BY
    customer_id
), t AS (
  SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue
  FROM raw_orders
  GROUP BY
    customer_id
)
SELECT
  c.customer_id,
  c.email,
  r.last_order_date,
  t.order_count,
  t.total_revenue,
  (
    SELECT
      value
    FROM _sub1
  ) AS max_order_global,
  (
    SELECT
      COUNT(*)
    FROM raw_orders AS o
    WHERE
      o.customer_id = c.customer_id
  ) AS lifetime_order_count
FROM stg_customers_prepared AS c
LEFT JOIN r
  ON r.customer_id = c.customer_id
LEFT JOIN t
  ON t.customer_id = c.customer_id
WHERE
  c.customer_id IN (
    SELECT
      customer_id
    FROM raw_orders
    WHERE
      order_status = 'SHIPPED'
  );