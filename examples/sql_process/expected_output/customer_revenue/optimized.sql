-- @mdde-entity: customer_revenue_clean
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Customer revenue rollup — well-formed reference target

/*
Migration Details:
- Original SQL File: customer_revenue.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
*/

/* @mdde-entity: customer_revenue_clean */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Customer revenue rollup — well-formed reference target */
CREATE OR REPLACE VIEW customer_revenue_clean AS
-- Source prep: single-table SELECT + renames + single-source value transforms
WITH stg_customers_prepared AS (
  SELECT
    customer_id, /* @pk @business_key */
    email, /* @pii */
    first_name, /* @pii */
    last_name /* @pii */
  FROM stg_customers
), shipped_orders AS (
  SELECT
    order_id, /* @pk */
    customer_id,
    order_date,
    total_amount
  FROM raw_orders
  WHERE
    order_status = 'SHIPPED'
), customer_totals AS (
  SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value
  FROM shipped_orders
  GROUP BY
    customer_id
)
-- Source prep: single-table SELECT + renames + single-source value transforms
, customer_totals_prepared AS (
  SELECT
    order_count, /* @derived */
    total_revenue, /* @derived */
    avg_order_value, /* @derived */
    customer_id
  FROM customer_totals
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, stg_customers_joined AS (
  SELECT
    customer_id,
    email,
    first_name,
    last_name,
    order_count,
    total_revenue,
    avg_order_value
  FROM stg_customers_prepared AS c
  INNER JOIN customer_totals_prepared AS t
    ON c.customer_id = t.customer_id
)
SELECT
  customer_id,
  email,
  first_name,
  last_name,
  order_count,
  total_revenue,
  avg_order_value
FROM stg_customers_joined
ORDER BY
  total_revenue DESC;