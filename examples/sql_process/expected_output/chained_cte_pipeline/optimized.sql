-- @mdde-entity: chained_cte_pipeline
-- @mdde-layer: business
-- @mdde-stereotype: fact_aggregate
-- @mdde-description: User-defined CTE chain (4 levels deep). Exercises that the
-- layering passes don't accidentally lift / rewrite already-named CTEs that the
-- author put there on purpose. Only the FINAL SELECT gets layered.

/*
Migration Details:
- sql_process Version: 192caf00 (2026-05-18)
- Original SQL File: chained_cte_pipeline.sql
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

/* @mdde-entity: chained_cte_pipeline */
/* @mdde-layer: business */
/* @mdde-stereotype: fact_aggregate */
/* @mdde-description: User-defined CTE chain (4 levels deep). Exercises that the */
/* layering passes don't accidentally lift / rewrite already-named CTEs that the */
/* author put there on purpose. Only the FINAL SELECT gets layered. */
WITH base_customers AS (
  SELECT
    customer_id,
    country,
    email
  FROM schema_identifier_ssf_snapshot.customer
  WHERE
    NOT email IS NULL
)
-- Source prep: single-table SELECT + renames + single-source value transforms
, base_customers_prepared AS (
  SELECT
    country AS country,
    email AS email,
    customer_id
  FROM base_customers
), base_orders AS (
  SELECT
    customer_id,
    order_id,
    order_date,
    amount
  FROM schema_identifier_ssf_snapshot.orders
  WHERE
    amount > 0
), customer_totals AS (
  SELECT
    customer_id,
    SUM(amount) AS lifetime_revenue,
    COUNT(*) AS order_count
  FROM base_orders
  GROUP BY
    customer_id
), ranked_customers AS (
  SELECT
    ct.customer_id,
    ct.lifetime_revenue,
    ct.order_count,
    ROW_NUMBER() OVER (ORDER BY ct.lifetime_revenue DESC) AS revenue_rank
  FROM customer_totals AS ct
)
-- Source filter: single-table SELECT + WHERE for one source
, ranked_customers_filtered AS (
  SELECT
    customer_id AS customer_id,
    order_count AS order_count,
    revenue_rank AS revenue_rank,
    lifetime_revenue
  FROM ranked_customers
  WHERE
    lifetime_revenue >= 100
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, ranked_customers_joined AS (
  SELECT
    customer_id,
    country,
    email,
    lifetime_revenue,
    order_count,
    revenue_rank
  FROM ranked_customers_filtered AS rc
  INNER JOIN base_customers_prepared AS bc
    ON bc.customer_id = rc.customer_id
)
SELECT
  customer_id,
  country,
  email,
  CAST(lifetime_revenue AS DECIMAL(18, 2)) AS lifetime_revenue,
  order_count,
  revenue_rank,
  CASE
    WHEN revenue_rank <= 10
    THEN 'top-10'
    WHEN revenue_rank <= 100
    THEN 'top-100'
    ELSE 'long-tail'
  END AS tier
FROM ranked_customers_joined