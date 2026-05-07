-- @mdde-entity: customer_revenue_clean
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Customer revenue rollup — well-formed reference target

/* @mdde-entity: customer_revenue_clean */ /* @mdde-layer: business */ /* @mdde-stereotype: fact */ /* @mdde-description: Customer revenue rollup — well-formed reference target */
CREATE OR REPLACE VIEW customer_revenue_clean AS
WITH shipped_orders AS (
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
SELECT
  c.customer_id, /* @pk @business_key */
  c.email, /* @pii */
  c.first_name, /* @pii */
  c.last_name, /* @pii */
  t.order_count, /* @derived */
  t.total_revenue, /* @derived */
  t.avg_order_value /* @derived */
FROM stg_customers AS c
INNER JOIN customer_totals AS t
  ON c.customer_id = t.customer_id
ORDER BY
  t.total_revenue DESC;