-- @mdde-entity: union_branch_with_user_cte
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: UNION ALL where ONE branch has its own user-defined WITH
-- clause. Verifies recursive per-branch layering doesn't clobber the user's
-- inner CTEs.

SELECT
    customer_id AS customer_id,
    amount AS amount,
    'web' AS channel
FROM raw.orders
WHERE channel = 'WEB'
UNION ALL
WITH recent_orders AS (
    SELECT customer_id, amount FROM raw.orders WHERE order_date >= '2024-01-01'
)
SELECT
    ro.customer_id AS customer_id,
    ro.amount AS amount,
    'recent' AS channel
FROM recent_orders AS ro
WHERE ro.amount > 100
