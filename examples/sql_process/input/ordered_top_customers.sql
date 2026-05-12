-- @mdde-entity: ordered_top_customers
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: ORDER BY + LIMIT at the outer SELECT. These belong at the
-- final SELECT layer (sorting is a presentation concern, not a layer-1
-- transformation). The layering passes must preserve them at the top.

SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    o.amount AS amount,
    o.order_date AS order_date
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
WHERE o.amount > 100
ORDER BY o.amount DESC, o.order_date DESC
LIMIT 100
