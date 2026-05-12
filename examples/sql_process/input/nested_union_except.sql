-- @mdde-entity: nested_union_except
-- @mdde-layer: business
-- @mdde-stereotype: filter
-- @mdde-description: Three-branch UNION ALL where one branch is itself an
-- EXCEPT. Exercises UNION + EXCEPT composition: each top-level UNION branch
-- gets layered; an EXCEPT branch's two sides become their own CTEs too.

SELECT customer_id AS customer_id, 'web' AS channel FROM raw.orders WHERE channel = 'WEB'
UNION ALL
SELECT customer_id AS customer_id, 'store' AS channel FROM raw.orders WHERE channel = 'STORE'
UNION ALL
SELECT customer_id AS customer_id, 'churned' AS channel FROM raw.customer
EXCEPT
SELECT customer_id AS customer_id, 'churned' AS channel FROM raw.orders WHERE order_date >= '2024-01-01'
