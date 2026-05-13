-- @mdde-entity: filtered_high_value
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Joins customer + orders and filters on a cross-source
-- predicate (customer's country must match the order's payment_method region).
-- Exercises the `<entity>_filtered` CTE: the cross-source WHERE moves OUT of the
-- joined CTE into its own layer so each CTE has a single concern.

/*
Migration Details:
- Original SQL File: filtered_high_value.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Lifted cross-source WHERE predicates into a dedicated `_filtered` CTE so the joined CTE stays single-concern.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
- [X] Cross-source filtering isolated.
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
    order_date AS order_date,
    amount AS amount,
    customer_id,
    payment_method
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    amount > 100
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, customer_joined AS (
  SELECT
    customer_id,
    country,
    order_date,
    amount,
    payment_method
  FROM customer_prepared AS c
  INNER JOIN orders_filtered AS o
    ON o.customer_id = c.customer_id
)
-- Source filter: single-table SELECT + WHERE for one source
, customer_filtered AS (
  SELECT
    *
  FROM customer_joined
  WHERE
    country = payment_method
)
/* @mdde-entity: filtered_high_value */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Joins customer + orders and filters on a cross-source */
/* predicate (customer's country must match the order's payment_method region). */
/* Exercises the `<entity>_filtered` CTE: the cross-source WHERE moves OUT of the */
/* joined CTE into its own layer so each CTE has a single concern. */
SELECT
  customer_id,
  country,
  order_date,
  amount
FROM customer_filtered