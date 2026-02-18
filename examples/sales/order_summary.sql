-- Order summary report
-- Aggregates orders by customer with running totals

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.tier,
        o.order_id,
        o.order_date,
        o.total_amount
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.status = 'completed'
),

customer_metrics AS (
    SELECT
        customer_id,
        customer_name,
        tier,
        COUNT(*) AS order_count,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM customer_orders
    GROUP BY customer_id, customer_name, tier
)

SELECT
    customer_id,
    customer_name,
    tier,
    order_count,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    DATEDIFF('day', first_order_date, last_order_date) AS customer_lifetime_days
FROM customer_metrics
ORDER BY total_revenue DESC
