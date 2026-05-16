-- @mdde-entity: window_no_order_with_annotation
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Window functions without ORDER BY. The `@mdde-order-by`
-- annotation supplies the deterministic ordering so the auto-fix can add an
-- explicit ORDER BY to every window function in the file.
-- @mdde-order-by: order_date DESC, order_id ASC

/*
Migration Details:
- Original SQL File: window_no_order_with_annotation.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Added `ORDER BY` (from `@mdde-order-by` annotation) to every window function lacking one, producing deterministic results.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Window functions made deterministic via `@mdde-order-by`.
- [X] Table qualifiers normalised.
*/

/* @mdde-entity: window_no_order_with_annotation */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Window functions without ORDER BY. The `@mdde-order-by` */
/* annotation supplies the deterministic ordering so the auto-fix can add an */
/* explicit ORDER BY to every window function in the file. */
/* @mdde-order-by: order_date DESC, order_id ASC */
SELECT
  customer_id,
  order_id,
  order_date,
  amount,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, order_id ASC) AS recency_rank,
  SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date DESC, order_id ASC) AS customer_total,
  LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_date DESC, order_id ASC) AS prev_amount
FROM schema_identifier_ssf_snapshot.orders