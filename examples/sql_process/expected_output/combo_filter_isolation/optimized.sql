-- @mdde-entity: combo_filter_isolation
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Stresses the filter-isolation rule. The input WHERE clause
-- has FOUR kinds of predicates that should be routed to DIFFERENT layers:
--   1. Single-source on customer (folds to customer_prepared/_filtered)
--   2. Single-source on orders (folds to orders_prepared/_filtered)
--   3. Single-source on loans (folds to loans_prepared/_filtered)
--   4. Cross-source predicate touching customer + orders (must move to a
--      dedicated <entity>_filtered CTE — NEVER stays in the joined CTE)
-- The joined CTE must end up with NO WHERE clause.

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: combo_filter_isolation.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Lifted cross-source WHERE predicates into a dedicated `_filtered` CTE so the joined CTE stays single-concern.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
- [X] Cross-source filtering isolated.
- [X] Table qualifiers normalised.
*/

-- Source filter: single-table SELECT + WHERE for one source
WITH customer_filtered AS (
  SELECT
    customer_id AS customer_id,
    country AS country,
    email AS email
  FROM schema_identifier_ssf_snapshot.customer
  WHERE
    NOT country IS NULL /* single-source customer */
)
-- Source filter: single-table SELECT + WHERE for one source
, orders_filtered AS (
  SELECT
    amount AS amount,
    order_date AS order_date,
    customer_id,
    payment_method
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    amount > 0 /* single-source orders */
)
-- Source filter: single-table SELECT + WHERE for one source
, loans_filtered AS (
  SELECT
    principal_amount AS loan_principal,
    status AS loan_status,
    customer_id,
    principal_amount
  FROM schema_identifier_ssf_snapshot.loans
  WHERE
    status = 'OPEN' /* single-source loans */
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, customer_joined AS (
  SELECT
    customer_id,
    country,
    email,
    amount,
    order_date,
    loan_principal,
    loan_status,
    payment_method,
    principal_amount
  FROM customer_filtered AS c
  INNER JOIN orders_filtered AS o
    ON o.customer_id = c.customer_id
  INNER JOIN loans_filtered AS l
    ON l.customer_id = c.customer_id
), customer_filtered_2 AS (
  SELECT
    *
  FROM customer_joined
  WHERE
    country = payment_method /* cross-source (customer + orders) → filtered CTE */
    AND principal_amount > amount /* cross-source (loans + orders) → filtered CTE */
)
/* @mdde-entity: combo_filter_isolation */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Stresses the filter-isolation rule. The input WHERE clause */
/* has FOUR kinds of predicates that should be routed to DIFFERENT layers: */
/*   1. Single-source on customer (folds to customer_prepared/_filtered) */
/*   2. Single-source on orders (folds to orders_prepared/_filtered) */
/*   3. Single-source on loans (folds to loans_prepared/_filtered) */
/*   4. Cross-source predicate touching customer + orders (must move to a */
/*      dedicated <entity>_filtered CTE — NEVER stays in the joined CTE) */
/* The joined CTE must end up with NO WHERE clause. */
SELECT
  customer_id,
  country,
  email,
  amount,
  order_date,
  loan_principal,
  loan_status
FROM customer_filtered_2