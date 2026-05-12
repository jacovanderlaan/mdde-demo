-- @mdde-entity: agg_customer_summary
-- @mdde-layer: business
-- @mdde-stereotype: aggregate
-- @mdde-description: Per-customer revenue rollup that mixes aggregates with casts and defaults.
-- Exercises the `<entity>_aggregated` CTE extraction: SUM / COUNT / MAX become
-- pre-aggregated columns; the outer SELECT only applies CAST, COALESCE, constants.

SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    c.country AS country,
    CAST(SUM(o.amount) AS DECIMAL(18, 2)) AS total_revenue,
    COUNT(*) AS order_count,
    MAX(o.order_date) AS last_order_date,
    COALESCE(c.country, 'unknown') AS country_clean,
    'customer_summary' AS rollup_kind
FROM raw.customer AS c
LEFT JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.email,
    c.country
