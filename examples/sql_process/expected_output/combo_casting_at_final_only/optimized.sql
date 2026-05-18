-- @mdde-entity: combo_casting_at_final_only
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Exercises that CAST / COALESCE / NULLIF / CASE / constants
-- ALWAYS end up at the final SELECT, never folded into source/joined/agg CTEs.
-- The input deliberately mixes casting and defaulting with:
--   - single-source value transforms that DO fold into source CTEs
--   - multi-source derivations that DO fold into the joined CTE
--   - aggregates that DO fold into the agg CTE
-- Verifies the boundary stays in the right place across all combinations.

/*
Migration Details:
- sql_process Version: 192caf00 (2026-05-18)
- Original SQL File: combo_casting_at_final_only.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Lifted aggregates and GROUP BY into a dedicated `_aggregated` CTE; outer SELECT applies casting / defaulting only.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
- [X] Aggregation isolated from formatting.
- [X] Table qualifiers normalised.
*/

-- Source filter: single-table SELECT + WHERE for one source
WITH customer_filtered AS (
  SELECT
    customer_id AS customer_id,
    UPPER(country) AS country_upper, /* single-source transform (folds to source CTE) */
    country,
    email
  FROM schema_identifier_ssf_snapshot.customer
  WHERE
    NOT email IS NULL
)
-- Source filter: single-table SELECT + WHERE for one source
, orders_filtered AS (
  SELECT
    amount,
    customer_id,
    order_id,
    payment_method
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    amount > 0
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, customer_joined AS (
  SELECT
    customer_id,
    country_upper,
    email,
    c.customer_id /* multi-source derivation (folds to joined CTE) */ || ':' || o.order_id AS composite_key,
    payment_method,
    amount,
    country
  FROM customer_filtered AS c
  INNER JOIN orders_filtered AS o
    ON o.customer_id = c.customer_id
)
-- Aggregated: GROUP BY + aggregates + HAVING (no JOIN, no derivation, no WHERE)
, customer_aggregated AS (
  SELECT
    customer_id,
    country_upper,
    email,
    composite_key,
    payment_method,
    country,
    SUM(amount) AS amount_sum
  FROM customer_joined
  GROUP BY
    customer_id,
    country,
    email,
    order_id,
    payment_method
  HAVING
    amount_sum > 0
)
/* @mdde-entity: combo_casting_at_final_only */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Exercises that CAST / COALESCE / NULLIF / CASE / constants */
/* ALWAYS end up at the final SELECT, never folded into source/joined/agg CTEs. */
/* The input deliberately mixes casting and defaulting with: */
/*   - single-source value transforms that DO fold into source CTEs */
/*   - multi-source derivations that DO fold into the joined CTE */
/*   - aggregates that DO fold into the agg CTE */
/* Verifies the boundary stays in the right place across all combinations. */
SELECT
  customer_id,
  country_upper,
  CAST(UPPER(email) AS VARCHAR(100)) AS email_normalised, /* single-source transform wrapped in CAST (CAST stays outer, the value */
  /* transform UPPER also stays with it because CAST wraps it) */
  composite_key,
  COALESCE(email, payment_method, 'unknown') AS contact_handle, /* multi-source derivation wrapped in COALESCE (COALESCE stays outer) */
  amount_sum AS revenue, /* aggregate (folds to agg CTE) */
  CAST(amount_sum AS DECIMAL(18, 2)) AS revenue_fmt, /* aggregate wrapped in CAST (CAST stays outer, aggregate folds) */
  CASE
    WHEN CAST(amount_sum AS DECIMAL(18, 2)) > 1000
    THEN 'high'
    WHEN CAST(amount_sum AS DECIMAL(18, 2)) > 100
    THEN 'mid'
    ELSE 'low'
  END AS revenue_band, /* nested formatting: CASE wrapping a CAST wrapping an aggregate */
  COALESCE(country, 'unknown') AS country_clean, /* COALESCE around a bare column reference (COALESCE stays outer) */
  NULLIF(UPPER(email), '') AS email_or_null, /* NULLIF around a single-source projection (NULLIF stays outer) */
  'rollup_v1' AS rollup_kind, /* Pure literal (stays outer) */
  1.0 AS scale_factor /* Numeric literal (stays outer) */
FROM customer_aggregated