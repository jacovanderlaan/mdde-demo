-- @mdde-entity: distinct_multi_column
-- @mdde-layer: business
-- @mdde-stereotype: dim
-- @mdde-description: Multi-column DISTINCT over a JOIN. Exercises the
-- DISTINCT-to-ROW_NUMBER rewrite with multiple partition columns and ORDER BY
-- + LIMIT at the outer SELECT (which should move to the deduped layer's outer).

SELECT DISTINCT
    c.customer_id AS customer_id,
    c.country AS country,
    l.product_code AS loan_product
FROM raw.customer AS c
INNER JOIN raw.loans AS l
  ON l.customer_id = c.customer_id
WHERE l.status = 'OPEN'
ORDER BY c.country, c.customer_id
LIMIT 500
