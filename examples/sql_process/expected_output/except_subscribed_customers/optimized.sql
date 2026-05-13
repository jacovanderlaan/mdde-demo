-- @mdde-entity: except_subscribed_customers
-- @mdde-layer: business
-- @mdde-stereotype: filter
-- @mdde-description: Subscribed customers minus those who unsubscribed.
-- Exercises top-level EXCEPT lifting: each branch becomes its own CTE,
-- the top-level body becomes a pure
-- `SELECT * FROM cte_a EXCEPT SELECT * FROM cte_b`.

/*
Migration Details:
- Original SQL File: except_subscribed_customers.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] Table qualifiers normalised.
*/

WITH _sub1 AS (
  /* @mdde-entity: except_subscribed_customers */
  /* @mdde-layer: business */
  /* @mdde-stereotype: filter */
  /* @mdde-description: Subscribed customers minus those who unsubscribed. */
  /* Exercises top-level EXCEPT lifting: each branch becomes its own CTE, */
  /* the top-level body becomes a pure */
  /* `SELECT * FROM cte_a EXCEPT SELECT * FROM cte_b`. */
  SELECT
    c.customer_id AS customer_id,
    c.email AS email
  FROM schema_identifier_ssf_snapshot.customer AS c
  WHERE
    NOT c.email IS NULL
), _sub2 AS (
  SELECT
    o.customer_id AS customer_id,
    o.payment_method AS email
  FROM schema_identifier_ssf_snapshot.orders AS o
  WHERE
    o.channel = 'WEB'
)
SELECT
  *
FROM _sub1
EXCEPT
SELECT
  *
FROM _sub2