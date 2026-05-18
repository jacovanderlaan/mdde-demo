-- @mdde-entity: window_ranked_orders
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Window functions (ROW_NUMBER, LAG, SUM OVER PARTITION) at the
-- outer SELECT. Windows must stay at the outer layer (they reshape rows in ways
-- the agg-CTE pass mustn't intercept). Source / joined CTEs handle the rest.

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: window_ranked_orders.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
- [X] Table qualifiers normalised.
*/

-- Source prep: single-table SELECT + renames + single-source value transforms
WITH customer_prepared AS (
  SELECT
    customer_id AS customer_id
  FROM schema_identifier_ssf_snapshot.customer
)
-- Source filter: single-table SELECT + WHERE for one source
, orders_filtered AS (
  SELECT
    order_id AS order_id,
    order_date AS order_date,
    amount AS amount,
    customer_id
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    amount > 0
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, customer_joined AS (
  SELECT
    customer_id,
    order_id,
    order_date,
    amount
  FROM customer_prepared AS c
  INNER JOIN orders_filtered AS o
    ON o.customer_id = c.customer_id
)
/* @mdde-entity: window_ranked_orders */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Window functions (ROW_NUMBER, LAG, SUM OVER PARTITION) at the */
/* outer SELECT. Windows must stay at the outer layer (they reshape rows in ways */
/* the agg-CTE pass mustn't intercept). Source / joined CTEs handle the rest. */
SELECT
  customer_id,
  order_id,
  order_date,
  amount,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_rank,
  SUM(amount) OVER (PARTITION BY customer_id) AS customer_total,
  LAG(amount, 1, 0) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_amount,
  amount - COALESCE(LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date), 0) AS delta_from_previous
FROM customer_joined