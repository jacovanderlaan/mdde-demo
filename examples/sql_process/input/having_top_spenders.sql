-- @mdde-entity: having_top_spenders
-- @mdde-layer: business
-- @mdde-stereotype: aggregate
-- @mdde-description: Aggregate with a HAVING clause. HAVING gates aggregated
-- output and must move INTO the aggregation CTE (it can't be evaluated until
-- after GROUP BY). Outer SELECT applies casting / defaulting only.

SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    CAST(SUM(o.amount) AS DECIMAL(18, 2)) AS total_revenue,
    COUNT(*) AS order_count
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.country
HAVING SUM(o.amount) > 1000
   AND COUNT(*) >= 5
