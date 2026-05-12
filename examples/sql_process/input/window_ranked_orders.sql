-- @mdde-entity: window_ranked_orders
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Window functions (ROW_NUMBER, LAG, SUM OVER PARTITION) at the
-- outer SELECT. Windows must stay at the outer layer (they reshape rows in ways
-- the agg-CTE pass mustn't intercept). Source / joined CTEs handle the rest.

SELECT
    c.customer_id AS customer_id,
    o.order_id AS order_id,
    o.order_date AS order_date,
    o.amount AS amount,
    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS order_rank,
    SUM(o.amount) OVER (PARTITION BY o.customer_id) AS customer_total,
    LAG(o.amount, 1, 0) OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS previous_amount,
    o.amount - COALESCE(LAG(o.amount) OVER (PARTITION BY o.customer_id ORDER BY o.order_date), 0) AS delta_from_previous
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
WHERE o.amount > 0
