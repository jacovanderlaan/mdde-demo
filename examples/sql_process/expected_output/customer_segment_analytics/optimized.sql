-- @mdde-entity: customer_segment_analytics
-- @mdde-layer: business
-- @mdde-stereotype: fact_aggregate
-- @mdde-description: Customer segment analytics with scalar subqueries and multi-CTE chain

/*
Migration Details:
- Original SQL File: customer_segment_analytics.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
*/

/* @mdde-entity: customer_segment_analytics */ /* @mdde-layer: business */ /* @mdde-stereotype: fact_aggregate */ /* @mdde-description: Customer segment analytics with scalar subqueries and multi-CTE chain */
CREATE OR REPLACE VIEW customer_segment_analytics AS
WITH stg_customers_prepared AS (
  SELECT
    email, /* @pii */
    first_name, /* @pii */
    last_name, /* @pii */
    customer_id
  FROM stg_customers
), shipped_orders AS (
  SELECT
    order_id,
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
    SUM(total_amount) AS total_revenue
  FROM shipped_orders
  GROUP BY
    customer_id
), ranked_customers AS (
  SELECT
    ct.customer_id, /* @pk */
    ct.order_count,
    ct.total_revenue,
    (
      SELECT
        value
      FROM _sub1
    ) AS avg_orders_overall, /* Comparative subqueries against the same table */
    (
      SELECT
        value
      FROM _sub2
    ) AS max_revenue_overall,
    ROW_NUMBER() OVER (ORDER BY ct.total_revenue DESC) AS revenue_rank, /* Per-customer rank */ /* @derived */
    CASE
      WHEN ct.total_revenue >= 10000
      THEN 'platinum'
      WHEN ct.total_revenue >= 5000
      THEN 'gold'
      WHEN ct.total_revenue >= 1000
      THEN 'silver'
      ELSE 'bronze'
    END /* Segment derivation */ AS segment /* @derived */
  FROM customer_totals AS ct
), _sub1 AS (
  SELECT
    AVG(order_count) AS value
  FROM customer_totals
), _sub2 AS (
  SELECT
    MAX(total_revenue) AS value
  FROM customer_totals
), customer_segment_analytics_joined AS (
  SELECT
    customer_id,
    email,
    first_name,
    last_name,
    order_count,
    total_revenue,
    avg_orders_overall,
    max_revenue_overall,
    revenue_rank,
    segment
  FROM ranked_customers AS rc
  LEFT JOIN stg_customers_prepared AS c
    ON rc.customer_id = c.customer_id
)
SELECT
  customer_id,
  email,
  first_name,
  last_name,
  order_count,
  total_revenue,
  avg_orders_overall,
  max_revenue_overall,
  revenue_rank,
  segment
FROM customer_segment_analytics_joined
ORDER BY
  revenue_rank;