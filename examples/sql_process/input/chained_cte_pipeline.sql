-- @mdde-entity: chained_cte_pipeline
-- @mdde-layer: business
-- @mdde-stereotype: fact_aggregate
-- @mdde-description: User-defined CTE chain (4 levels deep). Exercises that the
-- layering passes don't accidentally lift / rewrite already-named CTEs that the
-- author put there on purpose. Only the FINAL SELECT gets layered.

WITH base_customers AS (
    SELECT customer_id, country, email
    FROM raw.customer
    WHERE email IS NOT NULL
),
base_orders AS (
    SELECT customer_id, order_id, order_date, amount
    FROM raw.orders
    WHERE amount > 0
),
customer_totals AS (
    SELECT
        customer_id,
        SUM(amount) AS lifetime_revenue,
        COUNT(*) AS order_count
    FROM base_orders
    GROUP BY customer_id
),
ranked_customers AS (
    SELECT
        ct.customer_id,
        ct.lifetime_revenue,
        ct.order_count,
        ROW_NUMBER() OVER (ORDER BY ct.lifetime_revenue DESC) AS revenue_rank
    FROM customer_totals AS ct
)
SELECT
    rc.customer_id AS customer_id,
    bc.country AS country,
    bc.email AS email,
    CAST(rc.lifetime_revenue AS DECIMAL(18, 2)) AS lifetime_revenue,
    rc.order_count AS order_count,
    rc.revenue_rank AS revenue_rank,
    CASE
        WHEN rc.revenue_rank <= 10 THEN 'top-10'
        WHEN rc.revenue_rank <= 100 THEN 'top-100'
        ELSE 'long-tail'
    END AS tier
FROM ranked_customers AS rc
INNER JOIN base_customers AS bc
  ON bc.customer_id = rc.customer_id
WHERE rc.lifetime_revenue >= 100
