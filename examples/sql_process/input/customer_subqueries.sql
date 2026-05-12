-- @mdde-entity: customer_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Subquery shapes that should be lifted to CTEs (plus two that should be left inline)

-- Planted shapes:
--   1. Derived table in FROM        -> lift (alias 'recent_orders')
--   2. Derived table in JOIN        -> lift (alias 'order_totals')
--   3. Scalar subquery in SELECT    -> lift (uncorrelated)
--   4. WHERE IN (SELECT ...)        -> lift (inner SELECT goes to a CTE; outer keeps the IN against the CTE)
--   5. Correlated subquery in SELECT-> SKIP (correlated scalar subquery in projection — still inline)

CREATE OR REPLACE VIEW customer_subqueries AS
SELECT
    c.customer_id,                  -- @pk @business_key
    c.email,                        -- @pii
    r.last_order_date,
    t.order_count,
    t.total_revenue,
    (SELECT MAX(total_amount) FROM raw_orders)         AS max_order_global,
    (SELECT COUNT(*)
       FROM raw_orders o
       WHERE o.customer_id = c.customer_id)            AS lifetime_order_count
FROM stg_customers c
LEFT JOIN (
    SELECT
        customer_id,
        MAX(order_date) AS last_order_date
    FROM raw_orders
    WHERE order_status = 'SHIPPED'
    GROUP BY customer_id
) AS r
    ON r.customer_id = c.customer_id
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*)          AS order_count,
        SUM(total_amount) AS total_revenue
    FROM raw_orders
    GROUP BY customer_id
) AS t
    ON t.customer_id = c.customer_id
WHERE c.customer_id IN (
    SELECT customer_id
    FROM raw_orders
    WHERE order_status = 'SHIPPED'
);
