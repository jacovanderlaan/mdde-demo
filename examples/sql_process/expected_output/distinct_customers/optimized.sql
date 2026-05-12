-- @mdde-entity: distinct_customers
-- @mdde-layer: business
-- @mdde-stereotype: dim
-- @mdde-description: SELECT DISTINCT to deduplicate customers by email domain.
-- Exercises DISTINCT handling: the keyword must survive the layering passes
-- intact so the outer SELECT still de-dupes.

/*
Migration Details:
- Original SQL File: distinct_customers.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] Table qualifiers normalised.
*/

WITH customer_filtered AS (
  SELECT
    customer_id AS customer_id,
    email AS email,
    SUBSTRING(email, STR_POSITION(email, '@') + 1) AS email_domain
  FROM schema_identifier_ssf_snapshot.customer
  WHERE
    NOT email IS NULL
)
/* @mdde-entity: distinct_customers */ /* @mdde-layer: business */ /* @mdde-stereotype: dim */ /* @mdde-description: SELECT DISTINCT to deduplicate customers by email domain. */ /* Exercises DISTINCT handling: the keyword must survive the layering passes */ /* intact so the outer SELECT still de-dupes. */
SELECT DISTINCT
  c.customer_id,
  c.email,
  c.email_domain
FROM customer_filtered AS c