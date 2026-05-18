-- @mdde-entity: passthrough_with_loans
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Outer SELECT joins two passthrough CTEs that simply wrap a
-- real table. Exercises passthrough-CTE rewrite: the `SELECT *` body of each
-- existing CTE is replaced with the renames the outer SELECT actually uses.

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: passthrough_with_loans.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
- [X] Table qualifiers normalised.
*/

/* @mdde-entity: passthrough_with_loans */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Outer SELECT joins two passthrough CTEs that simply wrap a */
/* real table. Exercises passthrough-CTE rewrite: the `SELECT *` body of each */
/* existing CTE is replaced with the renames the outer SELECT actually uses. */
WITH active_customers AS (
  SELECT
    customer_id AS customer_id,
    email AS email
  FROM schema_identifier_ssf_snapshot.customer
  WHERE
    NOT email IS NULL
), open_loans AS (
  SELECT
    product_code AS loan_product,
    principal_amount AS principal_amount,
    interest_rate AS interest_rate,
    status AS loan_status,
    customer_id
  FROM schema_identifier_ssf_snapshot.loans
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, active_customers_joined AS (
  SELECT
    customer_id,
    email,
    loan_product,
    principal_amount,
    interest_rate,
    loan_status
  FROM active_customers AS c
  LEFT JOIN open_loans AS l
    ON l.customer_id = c.customer_id
)
SELECT
  customer_id,
  email,
  loan_product,
  principal_amount,
  interest_rate,
  loan_status
FROM active_customers_joined