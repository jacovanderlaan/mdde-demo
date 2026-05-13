-- @mdde-entity: chained_cte_pipeline
-- @mdde-layer: business
-- @mdde-stereotype: fact_aggregate
-- @mdde-description: User-defined CTE chain (4 levels deep). Exercises that the
-- layering passes don't accidentally lift / rewrite already-named CTEs that the
-- author put there on purpose. Only the FINAL SELECT gets layered.

/*
Migration Details:
- Original SQL File: chained_cte_pipeline.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Lifted cross-source WHERE predicates into a dedicated `_filtered` CTE so the joined CTE stays single-concern.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] JOIN isolated into joined CTE.
- [X] Cross-source filtering isolated.
- [X] Table qualifiers normalised.
*/

/* @mdde-entity: chained_cte_pipeline */ /* @mdde-layer: business */ /* @mdde-stereotype: fact_aggregate */ /* @mdde-description: User-defined CTE chain (4 levels deep). Exercises that the */ /* layering passes don't accidentally lift / rewrite already-named CTEs that the */ /* author put there on purpose. Only the FINAL SELECT gets layered. */
WITH base_customers AS (
  SELECT
    customer_id,
    country,
    email
  FROM schema_identifier_ssf_snapshot.customer
  WHERE
    NOT email IS NULL
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
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, chained_cte_pipeline_joined AS (
  SELECT
    customer_id,
    country,
    email,
    lifetime_revenue,
    order_count,
    revenue_rank
  FROM ranked_customers AS rc
  INNER JOIN base_customers AS bc
    ON bc.customer_id = rc.customer_id
)
-- Filtered: cross-source WHERE predicates (no JOIN, no derivation, no aggregation)
, chained_cte_pipeline_filtered AS (
  SELECT
    *
  FROM chained_cte_pipeline_joined
  WHERE
    lifetime_revenue >= 100
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
FROM chained_cte_pipeline_filtered