-- @mdde-entity: customer_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Subquery shapes that should be lifted to CTEs (plus two that should be left inline)

-- Planted shapes:
--   1. Derived table in FROM        -> lift (alias 'recent_orders')
--   2. Derived table in JOIN        -> lift (alias 'order_totals')
--   3. Scalar subquery in SELECT    -> lift (uncorrelated)
--   4. WHERE IN (SELECT ...)        -> lift (inner SELECT goes to a CTE; outer keeps the IN against the CTE)
--   5. Correlated subquery in SELECT-> SKIP (correlated scalar subquery in projection — still inline)

/*
Migration Details:
- Original SQL File: customer_subqueries.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Lifted cross-source WHERE predicates into a dedicated `_filtered` CTE so the joined CTE stays single-concern.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
- [X] Cross-source filtering isolated.
*/

/* @mdde-entity: customer_subqueries */ /* @mdde-layer: business */ /* @mdde-stereotype: fact */ /* @mdde-description: Subquery shapes that should be lifted to CTEs (plus two that should be left inline) */ /* Planted shapes: */ /*   1. Derived table in FROM        -> lift (alias 'recent_orders') */ /*   2. Derived table in JOIN        -> lift (alias 'order_totals') */ /*   3. Scalar subquery in SELECT    -> lift (uncorrelated) */ /*   4. WHERE IN (SELECT ...)        -> lift (inner SELECT goes to a CTE; outer keeps the IN against the CTE) */ /*   5. Correlated subquery in SELECT-> SKIP (correlated scalar subquery in projection — still inline) */
CREATE OR REPLACE VIEW customer_subqueries AS
-- Source prep: single-table SELECT + renames + single-source value transforms
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
), _sub2 AS (
  SELECT
    customer_id
  FROM raw_orders
  WHERE
    order_status = 'SHIPPED'
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, customer_subqueries_joined AS (
  SELECT
    customer_id,
    email,
    last_order_date,
    order_count,
    total_revenue,
    value
  FROM stg_customers_prepared AS c
  LEFT JOIN r
    ON r.customer_id = c.customer_id
  LEFT JOIN t
    ON t.customer_id = c.customer_id
)
-- Filtered: cross-source WHERE predicates (no JOIN, no derivation, no aggregation)
, customer_subqueries_filtered AS (
  SELECT
    *
  FROM customer_subqueries_joined
  WHERE
    customer_id IN (
      SELECT
        customer_id
      FROM _sub2
    )
)
SELECT
  customer_id,
  email,
  last_order_date,
  order_count,
  total_revenue,
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
      customer_id = customer_id
  ) AS lifetime_order_count
FROM customer_subqueries_filtered;