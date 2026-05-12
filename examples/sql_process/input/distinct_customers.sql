-- @mdde-entity: distinct_customers
-- @mdde-layer: business
-- @mdde-stereotype: dim
-- @mdde-description: SELECT DISTINCT to deduplicate customers by email domain.
-- Exercises DISTINCT handling: the keyword must survive the layering passes
-- intact so the outer SELECT still de-dupes.

SELECT DISTINCT
    c.customer_id AS customer_id,
    c.email AS email,
    SUBSTRING(c.email, INSTR(c.email, '@') + 1) AS email_domain
FROM raw.customer AS c
WHERE NOT c.email IS NULL
