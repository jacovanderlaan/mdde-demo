-- @mdde-entity: combo_union_valuations
-- @mdde-layer: business
-- @mdde-stereotype: fact_aggregate
-- @mdde-description: Three-branch UNION ALL where each branch is its own
-- aggregated rollup with JOINs, WHERE filters, formatting (CAST/CASE), and a
-- literal tag column. Stresses: UNION-branch lifting + recursive per-branch
-- layering (source / joined / filtered / aggregated / outer formatting INSIDE
-- each branch CTE) + branch-name inference from `'X' AS valuation_type`.

SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    CAST(SUM(l.principal_amount) AS DECIMAL(18, 2)) AS amount,
    COUNT(*) AS line_count,
    'principal' AS valuation_type
FROM raw.customer AS c
INNER JOIN raw.loans AS l
  ON l.customer_id = c.customer_id
WHERE l.status = 'OPEN'
GROUP BY c.customer_id, c.country
HAVING SUM(l.principal_amount) > 0
UNION ALL
SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    CAST(SUM(o.amount) AS DECIMAL(18, 2)) AS amount,
    COUNT(*) AS line_count,
    'revenue' AS valuation_type
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
WHERE o.amount > 0
GROUP BY c.customer_id, c.country
HAVING SUM(o.amount) > 0
UNION ALL
SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    CAST(COUNT(DISTINCT o.order_date) AS DECIMAL(18, 2)) AS amount,
    COUNT(*) AS line_count,
    'active_days' AS valuation_type
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.country
