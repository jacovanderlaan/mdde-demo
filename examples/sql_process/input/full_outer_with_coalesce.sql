-- @mdde-entity: full_outer_with_coalesce
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: FULL OUTER JOIN with COALESCE on join keys. Exercises the
-- rewrite into three explicit CTEs (`unique_rows_from_<a>`,
-- `unique_rows_from_<b>`, `matching_rows`) UNION ALLed with a `source_ind` tag.

SELECT
    COALESCE(c.customer_id, o.customer_id) AS customer_id,
    c.country AS country,
    c.email AS email,
    o.order_id AS order_id,
    o.amount AS amount
FROM raw.customer AS c
FULL OUTER JOIN raw.orders AS o
  ON c.customer_id = o.customer_id
