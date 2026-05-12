-- @mdde-entity: combo_nested_predicate_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: filter
-- @mdde-description: NESTED predicate subqueries — WHERE IN whose body itself
-- contains another WHERE IN, plus a WHERE EXISTS whose body contains a scalar
-- subquery in its projection. Stresses recursive subquery lifting: every level
-- of nesting should produce its own CTE, with correlations promoted as
-- projections where needed.

/*
Migration Details:
- Original SQL File: combo_nested_predicate_subqueries.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] Modular CTE structure applied.
- [X] Table qualifiers normalised.
*/

WITH customer_prepared AS (
  SELECT
    customer_id AS customer_id,
    country AS country,
    email AS email
  FROM schema_identifier_ssf_snapshot.customer
), _sub1 AS (
  SELECT
    o.customer_id
  FROM schema_identifier_ssf_snapshot.orders AS o
  WHERE
    o.amount > 5000
), _sub2 AS (
  SELECT
    l.customer_id
  FROM schema_identifier_ssf_snapshot.loans AS l
  WHERE
    l.status = 'OPEN' AND l.customer_id IN (
      SELECT
        customer_id
      FROM _sub1
    )
), _sub3 AS (
  SELECT
    customer_id
  FROM schema_identifier_ssf_snapshot.loans AS l2
  WHERE
    l2.principal_amount > (
      SELECT
        AVG(principal_amount)
      FROM schema_identifier_ssf_snapshot.loans
    )
)
/* @mdde-entity: combo_nested_predicate_subqueries */ /* @mdde-layer: business */ /* @mdde-stereotype: filter */ /* @mdde-description: NESTED predicate subqueries — WHERE IN whose body itself */ /* contains another WHERE IN, plus a WHERE EXISTS whose body contains a scalar */ /* subquery in its projection. Stresses recursive subquery lifting: every level */ /* of nesting should produce its own CTE, with correlations promoted as */ /* projections where needed. */
SELECT
  c.customer_id,
  c.country,
  c.email
FROM customer_prepared AS c
WHERE
  c.customer_id IN (
    SELECT
      customer_id
    FROM _sub2
  )
  AND EXISTS(
    SELECT
      1
    FROM _sub3
    WHERE
      _sub3.customer_id = c.customer_id
  )