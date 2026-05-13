-- @mdde-entity: order_by_aggregate
-- @mdde-layer: business
-- @mdde-stereotype: aggregate
-- @mdde-description: ORDER BY an aggregate expression at the outer SELECT. After
-- the aggregation CTE moves SUM(o.amount) → amount_sum, the outer ORDER BY
-- should reference `amount_sum DESC`, not the raw `SUM(o.amount) DESC` (which
-- would need to recompute the aggregate against the agg CTE — invalid in many
-- engines, ugly in all of them).

SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    CAST(SUM(o.amount) AS DECIMAL(18, 2)) AS total_revenue,
    COUNT(*) AS order_count
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.country
ORDER BY SUM(o.amount) DESC, COUNT(*) DESC
LIMIT 100
