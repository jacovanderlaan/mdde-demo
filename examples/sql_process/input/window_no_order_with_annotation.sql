-- @mdde-entity: window_no_order_with_annotation
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Window functions without ORDER BY. The `@mdde-order-by`
-- annotation supplies the deterministic ordering so the auto-fix can add an
-- explicit ORDER BY to every window function in the file.
-- @mdde-order-by: order_date DESC, order_id ASC

SELECT
    customer_id,
    order_id,
    order_date,
    amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id) AS recency_rank,
    SUM(amount) OVER (PARTITION BY customer_id) AS customer_total,
    LAG(amount) OVER (PARTITION BY customer_id) AS prev_amount
FROM raw.orders
