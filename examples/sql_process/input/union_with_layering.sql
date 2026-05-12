-- @mdde-entity: union_with_layering
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Web vs store revenue aggregates UNIONed together. Each branch
-- has its own JOIN + aggregation. Exercises recursive layering inside UNION-branch
-- CTEs: each branch CTE becomes a nested layered pipeline.

SELECT
    c.country AS country,
    CAST(SUM(o.amount) AS DECIMAL(18, 2)) AS revenue,
    COUNT(*) AS order_count,
    'web' AS channel
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
WHERE o.channel = 'WEB'
GROUP BY c.country
UNION ALL
SELECT
    c.country AS country,
    CAST(SUM(o.amount) AS DECIMAL(18, 2)) AS revenue,
    COUNT(*) AS order_count,
    'store' AS channel
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
WHERE o.channel = 'STORE'
GROUP BY c.country
