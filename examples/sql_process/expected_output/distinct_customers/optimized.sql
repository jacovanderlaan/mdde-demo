-- @mdde-entity: distinct_customers
-- @mdde-layer: business
-- @mdde-stereotype: dim
-- @mdde-description: SELECT DISTINCT to deduplicate customers by email domain.
-- Exercises DISTINCT handling: the keyword must survive the layering passes
-- intact so the outer SELECT still de-dupes.

/*
Migration Details:
- sql_process Version: 192caf00 (2026-05-18)
- Original SQL File: distinct_customers.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Replaced `SELECT DISTINCT` with explicit `ROW_NUMBER() OVER (PARTITION BY <projections>)` + `WHERE rn = 1` dedup pattern (`_ranked` + `_deduped` CTEs).
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] DISTINCT replaced by explicit dedup CTEs.
- [X] Table qualifiers normalised.
*/

-- Ranked: ROW_NUMBER() OVER (PARTITION BY all projection columns) — replaces SELECT DISTINCT, makes duplicates inspectable
WITH customer_ranked AS (
  /* @mdde-entity: distinct_customers */
  /* @mdde-layer: business */
  /* @mdde-stereotype: dim */
  /* @mdde-description: SELECT DISTINCT to deduplicate customers by email domain. */
  /* Exercises DISTINCT handling: the keyword must survive the layering passes */
  /* intact so the outer SELECT still de-dupes. */
  SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    SUBSTRING(c.email, STR_POSITION(c.email, '@') + 1) AS email_domain,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id, email, email_domain
      ORDER BY (
        SELECT
          NULL
      ) NULLS LAST
    ) AS rn
  FROM schema_identifier_ssf_snapshot.customer AS c
  WHERE
    NOT c.email IS NULL
)
-- Deduped: filters WHERE rn = 1 (removes duplicates surfaced by the ranked CTE above)
, customer_deduped AS (
  SELECT
    customer_id,
    email,
    email_domain
  FROM customer_ranked
  WHERE
    rn = 1
)
SELECT
  *
FROM customer_deduped