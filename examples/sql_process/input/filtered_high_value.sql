-- @mdde-entity: filtered_high_value
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Joins customer + orders and filters on a cross-source
-- predicate (customer's country must match the order's payment_method region).
-- Exercises the `<entity>_filtered` CTE: the cross-source WHERE moves OUT of the
-- joined CTE into its own layer so each CTE has a single concern.

SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    o.order_date AS order_date,
    o.amount AS amount
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
WHERE c.country = o.payment_method
  AND o.amount > 100
