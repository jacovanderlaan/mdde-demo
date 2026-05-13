-- @mdde-entity: having_top_spenders
-- @mdde-layer: business
-- @mdde-stereotype: aggregate
-- @mdde-description: Aggregate with a HAVING clause. HAVING gates aggregated
-- output and must move INTO the aggregation CTE (it can't be evaluated until
-- after GROUP BY). Outer SELECT applies casting / defaulting only.

/*
Migration Details:
- Original SQL File: having_top_spenders.sql
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
    country AS country
  FROM schema_identifier_ssf_snapshot.customer
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, customer_joined AS (
  SELECT
    customer_id,
    country,
    amount
  FROM customer_prepared AS c
  INNER JOIN schema_identifier_ssf_snapshot.orders AS o
    ON o.customer_id = c.customer_id
)
-- Aggregated: GROUP BY + aggregates + HAVING (no JOIN, no derivation, no WHERE)
, customer_aggregated AS (
  SELECT
    customer_id,
    country,
    SUM(amount) AS amount_sum,
    COUNT(*) AS agg_2
  FROM customer_joined
  GROUP BY
    customer_id,
    country
  HAVING
    amount_sum > 1000 AND agg_2 >= 5
)
/* @mdde-entity: having_top_spenders */
/* @mdde-layer: business */
/* @mdde-stereotype: aggregate */
/* @mdde-description: Aggregate with a HAVING clause. HAVING gates aggregated */
/* output and must move INTO the aggregation CTE (it can't be evaluated until */
/* after GROUP BY). Outer SELECT applies casting / defaulting only. */
SELECT
  customer_id,
  country,
  CAST(amount_sum AS DECIMAL(18, 2)) AS total_revenue,
  agg_2 AS order_count
FROM customer_aggregated