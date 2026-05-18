-- @mdde-entity: combo_union_valuations
-- @mdde-layer: business
-- @mdde-stereotype: fact_aggregate
-- @mdde-description: Three-branch UNION ALL where each branch is its own
-- aggregated rollup with JOINs, WHERE filters, formatting (CAST/CASE), and a
-- literal tag column. Stresses: UNION-branch lifting + recursive per-branch
-- layering (source / joined / filtered / aggregated / outer formatting INSIDE
-- each branch CTE) + branch-name inference from `'X' AS valuation_type`.

/*
Migration Details:
- sql_process Version: 192caf00 (2026-05-18)
- Original SQL File: combo_union_valuations.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Lifted aggregates and GROUP BY into a dedicated `_aggregated` CTE; outer SELECT applies casting / defaulting only.
  - Lifted each `UNION ALL` branch into its own CTE; top-level statement is a pure `SELECT * FROM cte_a UNION ALL SELECT * FROM cte_b ...`.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] JOIN isolated into joined CTE.
- [X] Aggregation isolated from formatting.
- [X] UNION branches lifted to CTEs.
- [X] Table qualifiers normalised.
*/

WITH customer AS (
  -- Source prep: single-table SELECT + renames + single-source value transforms
  WITH customer_prepared AS (
    SELECT
      customer_id AS customer_id,
      country AS country
    FROM schema_identifier_ssf_snapshot.customer
)
  -- Source filter: single-table SELECT + WHERE for one source
  , loans_filtered AS (
    SELECT
      customer_id,
      principal_amount
    FROM schema_identifier_ssf_snapshot.loans
    WHERE
      status = 'OPEN'
)
  -- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
  , customer_joined AS (
    SELECT
      customer_id,
      country,
      principal_amount
    FROM customer_prepared AS c
    INNER JOIN loans_filtered AS l
      ON l.customer_id = c.customer_id
)
  -- Aggregated: GROUP BY + aggregates + HAVING (no JOIN, no derivation, no WHERE)
  , customer_aggregated AS (
    SELECT
      customer_id,
      country,
      SUM(principal_amount) AS principal_amount_sum,
      COUNT(*) AS agg_2
    FROM customer_joined
    GROUP BY
      customer_id,
      country
    HAVING
      principal_amount_sum > 0
  )
  /* @mdde-entity: combo_union_valuations */
  /* @mdde-layer: business */
  /* @mdde-stereotype: fact_aggregate */
  /* @mdde-description: Three-branch UNION ALL where each branch is its own */
  /* aggregated rollup with JOINs, WHERE filters, formatting (CAST/CASE), and a */
  /* literal tag column. Stresses: UNION-branch lifting + recursive per-branch */
  /* layering (source / joined / filtered / aggregated / outer formatting INSIDE */
  /* each branch CTE) + branch-name inference from `'X' AS valuation_type`. */
  SELECT
    customer_id,
    country,
    CAST(principal_amount_sum AS DECIMAL(18, 2)) AS amount,
    agg_2 AS line_count,
    'principal' AS valuation_type
  FROM customer_aggregated
), customer_2 AS (
  -- Source prep: single-table SELECT + renames + single-source value transforms
  WITH customer_prepared AS (
    SELECT
      customer_id AS customer_id,
      country AS country
    FROM schema_identifier_ssf_snapshot.customer
)
  -- Source filter: single-table SELECT + WHERE for one source
  , orders_filtered AS (
    SELECT
      amount,
      customer_id
    FROM schema_identifier_ssf_snapshot.orders
    WHERE
      amount > 0
)
  -- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
  , customer_joined AS (
    SELECT
      customer_id,
      country,
      amount
    FROM customer_prepared AS c
    INNER JOIN orders_filtered AS o
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
      amount_sum > 0
  )
  SELECT
    customer_id,
    country,
    CAST(amount_sum AS DECIMAL(18, 2)) AS amount,
    agg_2 AS line_count,
    'revenue' AS valuation_type
  FROM customer_aggregated
), customer_3 AS (
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
      order_date
    FROM customer_prepared AS c
    INNER JOIN schema_identifier_ssf_snapshot.orders AS o
      ON o.customer_id = c.customer_id
)
  -- Aggregated: GROUP BY + aggregates + HAVING (no JOIN, no derivation, no WHERE)
  , customer_aggregated AS (
    SELECT
      customer_id,
      country,
      COUNT(DISTINCT order_date) AS order_date_count,
      COUNT(*) AS agg_2
    FROM customer_joined
    GROUP BY
      customer_id,
      country
  )
  SELECT
    customer_id,
    country,
    CAST(order_date_count AS DECIMAL(18, 2)) AS amount,
    agg_2 AS line_count,
    'active_days' AS valuation_type
  FROM customer_aggregated
)
SELECT
  *
FROM customer
UNION ALL
SELECT
  *
FROM customer_2
UNION ALL
SELECT
  *
FROM customer_3