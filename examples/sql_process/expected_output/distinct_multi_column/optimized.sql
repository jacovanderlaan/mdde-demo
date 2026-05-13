-- @mdde-entity: distinct_multi_column
-- @mdde-layer: business
-- @mdde-stereotype: dim
-- @mdde-description: Multi-column DISTINCT over a JOIN. Exercises the
-- DISTINCT-to-ROW_NUMBER rewrite with multiple partition columns and ORDER BY
-- + LIMIT at the outer SELECT (which should move to the deduped layer's outer).

/*
Migration Details:
- Original SQL File: distinct_multi_column.sql
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
  /* @mdde-entity: distinct_multi_column */
  /* @mdde-layer: business */
  /* @mdde-stereotype: dim */
  /* @mdde-description: Multi-column DISTINCT over a JOIN. Exercises the */
  /* DISTINCT-to-ROW_NUMBER rewrite with multiple partition columns and ORDER BY */
  /* + LIMIT at the outer SELECT (which should move to the deduped layer's outer). */
  SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    l.product_code AS loan_product,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id, country, loan_product
      ORDER BY (
        SELECT
          NULL
      ) NULLS LAST
    ) AS rn
  FROM schema_identifier_ssf_snapshot.customer AS c
  INNER JOIN schema_identifier_ssf_snapshot.loans AS l
    ON l.customer_id = c.customer_id
  WHERE
    l.status = 'OPEN'
)
-- Deduped: filters WHERE rn = 1 (removes duplicates surfaced by the ranked CTE above)
, customer_deduped AS (
  SELECT
    customer_id,
    country,
    loan_product
  FROM customer_ranked
  WHERE
    rn = 1
)
SELECT
  *
FROM customer_deduped
ORDER BY
  country,
  customer_id
LIMIT 500