-- @mdde-entity: ordered_top_customers
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: ORDER BY + LIMIT at the outer SELECT. These belong at the
-- final SELECT layer (sorting is a presentation concern, not a layer-1
-- transformation). The layering passes must preserve them at the top.

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: ordered_top_customers.sql
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
    customer_id AS customer_id,
    country AS country
  FROM schema_identifier_ssf_snapshot.customer
)
-- Source filter: single-table SELECT + WHERE for one source
, orders_filtered AS (
  SELECT
    amount AS amount,
    order_date AS order_date,
    customer_id
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    amount > 100
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, customer_joined AS (
  SELECT
    customer_id,
    country,
    amount,
    order_date
  FROM customer_prepared AS c
  INNER JOIN orders_filtered AS o
    ON o.customer_id = c.customer_id
)
/* @mdde-entity: ordered_top_customers */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: ORDER BY + LIMIT at the outer SELECT. These belong at the */
/* final SELECT layer (sorting is a presentation concern, not a layer-1 */
/* transformation). The layering passes must preserve them at the top. */
SELECT
  customer_id,
  country,
  amount,
  order_date
FROM customer_joined
ORDER BY
  amount DESC,
  order_date DESC
LIMIT 100