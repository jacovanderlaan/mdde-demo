-- @mdde-entity: lateral_unnest
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: LATERAL join — the right-hand side references columns from
-- the left side. Tests that source pushdown and joined-CTE extraction handle
-- LATERAL correctly (or fail-soft and leave the structure alone).
-- Snowflake/Databricks/Postgres syntax: LATERAL FLATTEN / EXPLODE / UNNEST.

SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    o.order_id AS order_id,
    o.amount AS amount
FROM raw.customer AS c
INNER JOIN LATERAL (
    SELECT order_id, amount
    FROM raw.orders o2
    WHERE o2.customer_id = c.customer_id
    ORDER BY o2.order_date DESC
    LIMIT 1
) AS o
  ON TRUE
WHERE c.email IS NOT NULL
