-- @mdde-entity: except_subscribed_customers
-- @mdde-layer: business
-- @mdde-stereotype: filter
-- @mdde-description: Subscribed customers minus those who unsubscribed.
-- Exercises top-level EXCEPT lifting: each branch becomes its own CTE,
-- the top-level body becomes a pure
-- `SELECT * FROM cte_a EXCEPT SELECT * FROM cte_b`.

SELECT
    c.customer_id AS customer_id,
    c.email AS email
FROM raw.customer AS c
WHERE c.email IS NOT NULL
EXCEPT
SELECT
    o.customer_id AS customer_id,
    o.payment_method AS email
FROM raw.orders AS o
WHERE o.channel = 'WEB'
