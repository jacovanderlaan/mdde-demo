-- @mdde-entity: lateral_unnest
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: LATERAL join — the right-hand side references columns from
-- the left side. Tests that source pushdown and joined-CTE extraction handle
-- LATERAL correctly (or fail-soft and leave the structure alone).
-- Snowflake/Databricks/Postgres syntax: LATERAL FLATTEN / EXPLODE / UNNEST.

/*
Migration Details:
- Original SQL File: lateral_unnest.sql
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

-- Source filter: single-table SELECT + WHERE for one source
WITH customer_filtered AS (
  SELECT
    customer_id AS customer_id,
    email AS email
  FROM schema_identifier_ssf_snapshot.customer
  WHERE
    NOT email IS NULL
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, lateral_unnest_joined AS (
  SELECT
    customer_id,
    email,
    order_id,
    amount
  FROM customer_filtered AS c
  INNER JOIN LATERAL (
    SELECT
      order_id,
      amount
    FROM schema_identifier_ssf_snapshot.orders AS o2
    WHERE
      o2.customer_id = c.customer_id
    ORDER BY
      o2.order_date DESC
    LIMIT 1
  ) AS o
    ON TRUE
)
/* @mdde-entity: lateral_unnest */ /* @mdde-layer: business */ /* @mdde-stereotype: fact */ /* @mdde-description: LATERAL join — the right-hand side references columns from */ /* the left side. Tests that source pushdown and joined-CTE extraction handle */ /* LATERAL correctly (or fail-soft and leave the structure alone). */ /* Snowflake/Databricks/Postgres syntax: LATERAL FLATTEN / EXPLODE / UNNEST. */
SELECT
  customer_id,
  email,
  order_id,
  amount
FROM lateral_unnest_joined