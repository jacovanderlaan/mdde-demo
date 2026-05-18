-- @mdde-entity: full_outer_with_coalesce
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: FULL OUTER JOIN with COALESCE on join keys. Exercises the
-- rewrite into three explicit CTEs (`unique_rows_from_<a>`,
-- `unique_rows_from_<b>`, `matching_rows`) UNION ALLed with a `source_ind` tag.

/*
Migration Details:
- sql_process Version: 192caf00 (2026-05-18)
- Original SQL File: full_outer_with_coalesce.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Replaced `FULL OUTER JOIN` + `COALESCE` with three explicit CTEs (`unique_rows_from_<a>`, `unique_rows_from_<b>`, `matching_rows`) UNION ALLed with a `source_ind` tag column.
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] FULL OUTER + COALESCE restructured into 3 CTEs.
- [X] Table qualifiers normalised.
*/

WITH unique_rows_from_customer AS (
  SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    c.email AS email,
    NULL AS order_id,
    NULL AS amount,
    'a' AS source_ind
  FROM schema_identifier_ssf_snapshot.customer AS c
  LEFT JOIN schema_identifier_ssf_snapshot.orders AS o
    ON c.customer_id = o.customer_id
  WHERE
    o.customer_id IS NULL
), unique_rows_from_orders AS (
  SELECT
    o.customer_id AS customer_id,
    NULL AS country,
    NULL AS email,
    o.order_id AS order_id,
    o.amount AS amount,
    'b' AS source_ind
  FROM schema_identifier_ssf_snapshot.orders AS o
  LEFT JOIN schema_identifier_ssf_snapshot.customer AS c
    ON c.customer_id = o.customer_id
  WHERE
    c.customer_id IS NULL
), matching_rows AS (
  SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    c.email AS email,
    o.order_id AS order_id,
    o.amount AS amount,
    'both' AS source_ind
  FROM schema_identifier_ssf_snapshot.customer AS c
  INNER JOIN schema_identifier_ssf_snapshot.orders AS o
    ON c.customer_id = o.customer_id
)
SELECT
  *
FROM unique_rows_from_customer
UNION ALL
SELECT
  *
FROM unique_rows_from_orders
UNION ALL
SELECT
  *
FROM matching_rows