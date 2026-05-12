-- @mdde-entity: union_revenue_breakdown
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Web vs store revenue breakdown via UNION ALL.
-- Exercises UNION-branch extraction: each branch becomes its own CTE
-- named from its `'X' AS channel` literal tag (`web` / `store`).

SELECT
    o.customer_id AS customer_id,
    o.order_date AS order_date,
    o.amount AS amount,
    'web' AS channel
FROM raw.orders AS o
WHERE o.channel = 'WEB'
UNION ALL
SELECT
    o.customer_id AS customer_id,
    o.order_date AS order_date,
    o.amount AS amount,
    'store' AS channel
FROM raw.orders AS o
WHERE o.channel = 'STORE'
