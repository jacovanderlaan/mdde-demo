-- @mdde-entity: agg_customer_summary
-- @mdde-layer: business
-- @mdde-stereotype: aggregate
-- @mdde-description: Per-customer revenue rollup that mixes aggregates with casts and defaults.
-- Exercises the `<entity>_aggregated` CTE extraction: SUM / COUNT / MAX become
-- pre-aggregated columns; the outer SELECT only applies CAST, COALESCE, constants.

/*
Migration Details:
- sql_process Version: 192caf00 (2026-05-18)
- Original SQL File: agg_customer_summary.sql
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

-- Source prep: single-table SELECT + renames + single-source value transforms
WITH customer_prepared AS (
  SELECT
    customer_id AS customer_id,
    email AS email,
    country AS country
  FROM schema_identifier_ssf_snapshot.customer
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, customer_joined AS (
  SELECT
    customer_id,
    email,
    country,
    amount,
    order_date
  FROM customer_prepared AS c
  LEFT JOIN schema_identifier_ssf_snapshot.orders AS o
    ON o.customer_id = c.customer_id
)
-- Aggregated: GROUP BY + aggregates + HAVING (no JOIN, no derivation, no WHERE)
, customer_aggregated AS (
  SELECT
    customer_id,
    email,
    country,
    SUM(amount) AS amount_sum,
    COUNT(*) AS agg_2,
    MAX(order_date) AS order_date_max
  FROM customer_joined
  GROUP BY
    customer_id,
    email,
    country
)
/* @mdde-entity: agg_customer_summary */
/* @mdde-layer: business */
/* @mdde-stereotype: aggregate */
/* @mdde-description: Per-customer revenue rollup that mixes aggregates with casts and defaults. */
/* Exercises the `<entity>_aggregated` CTE extraction: SUM / COUNT / MAX become */
/* pre-aggregated columns; the outer SELECT only applies CAST, COALESCE, constants. */
SELECT
  customer_id,
  email,
  country,
  CAST(amount_sum AS DECIMAL(18, 2)) AS total_revenue,
  agg_2 AS order_count,
  order_date_max AS last_order_date,
  COALESCE(country, 'unknown') AS country_clean,
  'customer_summary' AS rollup_kind
FROM customer_aggregated