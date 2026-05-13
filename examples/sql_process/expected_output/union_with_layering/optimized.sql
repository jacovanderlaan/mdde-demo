-- @mdde-entity: union_with_layering
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Web vs store revenue aggregates UNIONed together. Each branch
-- has its own JOIN + aggregation. Exercises recursive layering inside UNION-branch
-- CTEs: each branch CTE becomes a nested layered pipeline.

/*
Migration Details:
- Original SQL File: union_with_layering.sql
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

WITH web AS (
  -- Source prep: single-table SELECT + renames + single-source value transforms
  WITH customer_prepared AS (
    SELECT
      country AS country,
      customer_id
    FROM schema_identifier_ssf_snapshot.customer
)
  -- Source filter: single-table SELECT + WHERE for one source
  , orders_filtered AS (
    SELECT
      amount,
      customer_id
    FROM schema_identifier_ssf_snapshot.orders
    WHERE
      channel = 'WEB'
)
  -- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
  , web_joined AS (
    SELECT
      country,
      amount
    FROM customer_prepared AS c
    INNER JOIN orders_filtered AS o
      ON o.customer_id = c.customer_id
)
  -- Aggregated: GROUP BY + aggregates + HAVING (no JOIN, no derivation, no WHERE)
  , web_aggregated AS (
    SELECT
      country,
      SUM(amount) AS amount_sum,
      COUNT(*) AS agg_2
    FROM web_joined
    GROUP BY
      country
  )
  /* @mdde-entity: union_with_layering */ /* @mdde-layer: business */ /* @mdde-stereotype: fact */ /* @mdde-description: Web vs store revenue aggregates UNIONed together. Each branch */ /* has its own JOIN + aggregation. Exercises recursive layering inside UNION-branch */ /* CTEs: each branch CTE becomes a nested layered pipeline. */
  SELECT
    country,
    CAST(amount_sum AS DECIMAL(18, 2)) AS revenue,
    agg_2 AS order_count,
    'web' AS channel
  FROM web_aggregated
), store AS (
  -- Source prep: single-table SELECT + renames + single-source value transforms
  WITH customer_prepared AS (
    SELECT
      country AS country,
      customer_id
    FROM schema_identifier_ssf_snapshot.customer
)
  -- Source filter: single-table SELECT + WHERE for one source
  , orders_filtered AS (
    SELECT
      amount,
      customer_id
    FROM schema_identifier_ssf_snapshot.orders
    WHERE
      channel = 'STORE'
)
  -- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
  , store_joined AS (
    SELECT
      country,
      amount
    FROM customer_prepared AS c
    INNER JOIN orders_filtered AS o
      ON o.customer_id = c.customer_id
)
  -- Aggregated: GROUP BY + aggregates + HAVING (no JOIN, no derivation, no WHERE)
  , store_aggregated AS (
    SELECT
      country,
      SUM(amount) AS amount_sum,
      COUNT(*) AS agg_2
    FROM store_joined
    GROUP BY
      country
  )
  SELECT
    country,
    CAST(amount_sum AS DECIMAL(18, 2)) AS revenue,
    agg_2 AS order_count,
    'store' AS channel
  FROM store_aggregated
)
SELECT
  *
FROM web
UNION ALL
SELECT
  *
FROM store