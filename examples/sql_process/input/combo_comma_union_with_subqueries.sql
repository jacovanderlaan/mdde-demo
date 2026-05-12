-- @mdde-entity: combo_comma_union_with_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Customer-generated SQL with a BARE COMMA between two top-level
-- SELECTs (instead of UNION ALL) and scalar subqueries inside each branch's
-- projection list. Stresses: comma-as-UNION pre-parse, scalar subquery → CTE
-- lifting, recursive per-branch layering inside the lifted UNION CTEs.

SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    c.email AS email,
    (SELECT MAX(amount) FROM raw.orders) AS max_order_globally,
    'has_email' AS bucket
FROM raw.customer AS c
WHERE c.email IS NOT NULL
,
SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    c.email AS email,
    (SELECT MAX(amount) FROM raw.orders) AS max_order_globally,
    'no_email' AS bucket
FROM raw.customer AS c
WHERE c.email IS NULL
