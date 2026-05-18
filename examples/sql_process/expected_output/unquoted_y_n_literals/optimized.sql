-- @mdde-entity: unquoted_y_n_literals
-- @mdde-layer: business
-- @mdde-stereotype: dim
-- @mdde-description: Customer SQL writes Y / N as bare tokens (interpreted by
-- the SQL author as string literals). sqlglot parses them as columns and the
-- rewrite breaks. The `optimize.unquoted_literals: ['Y', 'N']` pre-parse
-- substitution wraps them in single quotes before parsing.

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: unquoted_y_n_literals.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Table qualifiers normalised.
*/

/* @mdde-entity: unquoted_y_n_literals */
/* @mdde-layer: business */
/* @mdde-stereotype: dim */
/* @mdde-description: Customer SQL writes Y / N as bare tokens (interpreted by */
/* the SQL author as string literals). sqlglot parses them as columns and the */
/* rewrite breaks. The `optimize.unquoted_literals: ['Y', 'N']` pre-parse */
/* substitution wraps them in single quotes before parsing. */
SELECT
  customer_id,
  email,
  CASE WHEN NOT email IS NULL THEN 'Y' ELSE 'N' END AS has_email,
  CASE WHEN country = 'NL' THEN 'Y' WHEN country = 'BE' THEN 'Y' ELSE 'N' END AS is_benelux
FROM schema_identifier_ssf_snapshot.customer