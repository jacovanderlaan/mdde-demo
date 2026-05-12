-- @mdde-entity: combo_nested_predicate_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: filter
-- @mdde-description: NESTED predicate subqueries — WHERE IN whose body itself
-- contains another WHERE IN, plus a WHERE EXISTS whose body contains a scalar
-- subquery in its projection. Stresses recursive subquery lifting: every level
-- of nesting should produce its own CTE, with correlations promoted as
-- projections where needed.

SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    c.email AS email
FROM raw.customer AS c
WHERE c.customer_id IN (
    SELECT l.customer_id
    FROM raw.loans AS l
    WHERE l.status = 'OPEN'
      AND l.customer_id IN (
          SELECT o.customer_id
          FROM raw.orders AS o
          WHERE o.amount > 5000
      )
)
AND EXISTS (
    SELECT 1
    FROM raw.loans AS l2
    WHERE l2.customer_id = c.customer_id
      AND l2.principal_amount > (
          SELECT AVG(principal_amount)
          FROM raw.loans
      )
)
