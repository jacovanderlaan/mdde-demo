-- @mdde-entity: customer_revenue
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Customer revenue rollup with several planted optimizer issues

-- Planted issues (the optimizer should catch all of these):
--   1. SELECT *           -> warning, auto-fixable
--   2. ORDER BY 1         -> warning
--   3. WHERE 1=1          -> info
--   4. CARTESIAN_JOIN     -> warning (join without ON)
--   5. WINDOW_NO_ORDER    -> ERROR (ROW_NUMBER without ORDER BY)
--   6. HARDCODED_DATE     -> info

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: customer_revenue_bad.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Removed `WHERE 1=1` placeholder.
*/

/* @mdde-entity: customer_revenue */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Customer revenue rollup with several planted optimizer issues */
/* Planted issues (the optimizer should catch all of these): */
/*   1. SELECT *           -> warning, auto-fixable */
/*   2. ORDER BY 1         -> warning */
/*   3. WHERE 1=1          -> info */
/*   4. CARTESIAN_JOIN     -> warning (join without ON) */
/*   5. WINDOW_NO_ORDER    -> ERROR (ROW_NUMBER without ORDER BY) */
/*   6. HARDCODED_DATE     -> info */
CREATE OR REPLACE VIEW customer_revenue AS
WITH ranked AS (
  SELECT
    customer_id, /* @pk */
    total_amount,
    ROW_NUMBER() OVER () AS rn /* @derived */
  FROM raw_orders
  WHERE
    order_date >= '2026-01-01' AND order_status = 'SHIPPED'
), joined AS (
  SELECT
    *
  FROM ranked AS r, stg_customers AS c
)
SELECT
  *
FROM joined
ORDER BY
  1;