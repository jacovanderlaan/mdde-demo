-- @mdde-entity: union_distinct_skipped
-- @mdde-layer: business
-- @mdde-stereotype: dim
-- @mdde-description: UNION (set-distinct, not UNION ALL) should be LEFT ALONE by
-- the UNION-branch lifter. UNION's row-count-preserving semantics are different
-- and lifting the branches changes the result for distinct mode. Verifies the
-- transform's distinct/by_name guard.

/*
Migration Details:
- Original SQL File: union_distinct_skipped.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Table qualifiers normalised.
*/

/* @mdde-entity: union_distinct_skipped */
/* @mdde-layer: business */
/* @mdde-stereotype: dim */
/* @mdde-description: UNION (set-distinct, not UNION ALL) should be LEFT ALONE by */
/* the UNION-branch lifter. UNION's row-count-preserving semantics are different */
/* and lifting the branches changes the result for distinct mode. Verifies the */
/* transform's distinct/by_name guard. */
SELECT
  customer_id,
  country
FROM schema_identifier_ssf_snapshot.customer
WHERE
  country = 'NL'
UNION
SELECT
  customer_id,
  country
FROM schema_identifier_ssf_snapshot.customer
WHERE
  country = 'BE'