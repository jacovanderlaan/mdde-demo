-- @mdde-entity: predicate_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: filter
-- @mdde-description: Exercises lifting of WHERE IN, WHERE EXISTS, and
-- correlated subqueries. Each predicate's inner SELECT becomes its own
-- CTE; the outer predicate keeps its IN/EXISTS structure but references
-- the new CTE. Correlated cases carry the correlation column up to the
-- lifted CTE's projection so the outer WHERE can re-correlate.

SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    c.country AS country
FROM raw.customer AS c
WHERE c.customer_id IN (
    SELECT o.customer_id
    FROM raw.orders AS o
    WHERE o.channel = 'WEB'
)
AND EXISTS (
    SELECT 1
    FROM raw.loans AS l
    WHERE l.customer_id = c.customer_id
      AND l.status = 'OPEN'
)
