-- @mdde-entity: customer_segment_analytics
-- @mdde-layer: business
-- @mdde-stereotype: fact_aggregate
-- @mdde-description: Customer segment analytics with scalar subqueries and multi-CTE chain

/* @mdde-entity: customer_segment_analytics */ /* @mdde-layer: business */ /* @mdde-stereotype: fact_aggregate */ /* @mdde-description: Customer segment analytics with scalar subqueries and multi-CTE chain */
CREATE OR REPLACE VIEW customer_segment_analytics AS
WITH shipped_orders AS (
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
        AVG(order_count)
      FROM customer_totals
    ) AS avg_orders_overall, /* Comparative subqueries against the same table */
    (
      SELECT
        MAX(total_revenue)
      FROM customer_totals
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
)
SELECT
  rc.customer_id, /* @pk @business_key */
  c.email, /* @pii */
  c.first_name, /* @pii */
  c.last_name, /* @pii */
  rc.order_count,
  rc.total_revenue,
  rc.avg_orders_overall,
  rc.max_revenue_overall,
  rc.revenue_rank, /* @derived */
  rc.segment /* @derived */
FROM ranked_customers AS rc
LEFT JOIN stg_customers AS c
  ON rc.customer_id = c.customer_id
ORDER BY
  rc.revenue_rank;