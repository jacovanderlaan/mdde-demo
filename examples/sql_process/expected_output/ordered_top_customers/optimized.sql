-- @mdde-entity: ordered_top_customers
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: ORDER BY + LIMIT at the outer SELECT. These belong at the
-- final SELECT layer (sorting is a presentation concern, not a layer-1
-- transformation). The layering passes must preserve them at the top.

/*
Migration Details:
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

WITH customer_prepared AS (
  SELECT
    customer_id AS customer_id,
    country AS country
  FROM schema_identifier_ssf_snapshot.customer
), orders_filtered AS (
  SELECT
    amount AS amount,
    order_date AS order_date,
    customer_id
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    amount > 100
), ordered_top_customers_joined AS (
  SELECT
    customer_id,
    country,
    amount,
    order_date
  FROM customer_prepared AS c
  INNER JOIN orders_filtered AS o
    ON o.customer_id = c.customer_id
)
/* @mdde-entity: ordered_top_customers */ /* @mdde-layer: business */ /* @mdde-stereotype: fact */ /* @mdde-description: ORDER BY + LIMIT at the outer SELECT. These belong at the */ /* final SELECT layer (sorting is a presentation concern, not a layer-1 */ /* transformation). The layering passes must preserve them at the top. */
SELECT
  customer_id,
  country,
  amount,
  order_date
FROM ordered_top_customers_joined
ORDER BY
  amount DESC,
  order_date DESC
LIMIT 100