-- @mdde-entity: combo_comma_union_with_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Customer-generated SQL with a BARE COMMA between two top-level
-- SELECTs (instead of UNION ALL) and scalar subqueries inside each branch's
-- projection list. Stresses: comma-as-UNION pre-parse, scalar subquery → CTE
-- lifting, recursive per-branch layering inside the lifted UNION CTEs.

/*
Migration Details:
- Original SQL File: combo_comma_union_with_subqueries.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Lifted each `UNION ALL` branch into its own CTE; top-level statement is a pure `SELECT * FROM cte_a UNION ALL SELECT * FROM cte_b ...`.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] UNION branches lifted to CTEs.
- [X] Table qualifiers normalised.
*/

WITH _sub1 AS (
  SELECT
    MAX(amount) AS value
  FROM schema_identifier_ssf_snapshot.orders
), _sub2 AS (
  SELECT
    MAX(amount) AS value
  FROM schema_identifier_ssf_snapshot.orders
), customer AS (
  -- Source filter: single-table SELECT + WHERE for one source
  WITH customer_filtered AS (
    SELECT
      customer_id AS customer_id,
      country AS country,
      email AS email
    FROM schema_identifier_ssf_snapshot.customer
    WHERE
      NOT email IS NULL
  )
  /* @mdde-entity: combo_comma_union_with_subqueries */
  /* @mdde-layer: business */
  /* @mdde-stereotype: fact */
  /* @mdde-description: Customer-generated SQL with a BARE COMMA between two top-level */
  /* SELECTs (instead of UNION ALL) and scalar subqueries inside each branch's */
  /* projection list. Stresses: comma-as-UNION pre-parse, scalar subquery → CTE */
  /* lifting, recursive per-branch layering inside the lifted UNION CTEs. */
  SELECT
    c.customer_id,
    c.country,
    c.email,
    (
      SELECT
        value
      FROM _sub1
    ) AS max_order_globally,
    'has_email' AS bucket
  FROM customer_filtered AS c
), customer_2 AS (
  -- Source filter: single-table SELECT + WHERE for one source
  WITH customer_filtered AS (
    SELECT
      customer_id AS customer_id,
      country AS country,
      email AS email
    FROM schema_identifier_ssf_snapshot.customer
    WHERE
      email IS NULL
  )
  SELECT
    c.customer_id,
    c.country,
    c.email,
    (
      SELECT
        value
      FROM _sub2
    ) AS max_order_globally,
    'no_email' AS bucket
  FROM customer_filtered AS c
)
SELECT
  *
FROM customer
UNION ALL
SELECT
  *
FROM customer_2