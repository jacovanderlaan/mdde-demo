-- @mdde-entity: combo_at_risk_customers
-- @mdde-layer: business
-- @mdde-stereotype: filter_with_ranking
-- @mdde-description: Customers with overdue loans, excluding any who already paid
-- off recently. Stresses: correlated WHERE IN, correlated WHERE EXISTS, top-level
-- EXCEPT (with both sides Select), window function (ROW_NUMBER), CAST/CASE in
-- outer formatting, metadata column stripping, ORDER BY at outer.

/*
Migration Details:
- sql_process Version: 192caf00 (2026-05-18)
- Original SQL File: combo_at_risk_customers.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Excluded metadata columns from outputs and WHERE (`snapshot_date`).
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] Metadata columns excluded.
- [X] Table qualifiers normalised.
*/

WITH _sub1 AS (
  SELECT
    l.customer_id
  FROM schema_identifier_ssf_snapshot.loans AS l
  WHERE
    l.status = 'OPEN' AND l.opened_at < '2023-01-01'
), _sub2 AS (
  SELECT
    customer_id
  FROM schema_identifier_ssf_snapshot.orders AS o
  WHERE
    o.amount > 1000
), _sub3 AS (
  /* @mdde-entity: combo_at_risk_customers */
  /* @mdde-layer: business */
  /* @mdde-stereotype: filter_with_ranking */
  /* @mdde-description: Customers with overdue loans, excluding any who already paid */
  /* off recently. Stresses: correlated WHERE IN, correlated WHERE EXISTS, top-level */
  /* EXCEPT (with both sides Select), window function (ROW_NUMBER), CAST/CASE in */
  /* outer formatting, metadata column stripping, ORDER BY at outer. */
  SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    c.country AS country,
    ROW_NUMBER() OVER (PARTITION BY c.country ORDER BY c.customer_id) AS country_rank,
    CASE WHEN c.country = 'NL' THEN 'domestic' ELSE 'international' END AS region
  FROM schema_identifier_ssf_snapshot.customer AS c
  WHERE
    c.customer_id IN (
      SELECT
        customer_id
      FROM _sub1
    )
    AND EXISTS(
      SELECT
        1
      FROM _sub2
      WHERE
        _sub2.customer_id = c.customer_id
    )
), _sub4 AS (
  SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    c.country AS country,
    1 AS country_rank,
    'recently_paid' AS region,
    '2026-05-12' AS snapshot_date
  FROM schema_identifier_ssf_snapshot.customer AS c
  INNER JOIN schema_identifier_ssf_snapshot.loans AS l
    ON l.customer_id = c.customer_id
  WHERE
    l.status = 'CLOSED' AND l.opened_at >= '2024-01-01'
)
SELECT
  *
FROM _sub3
EXCEPT
SELECT
  *
FROM _sub4
ORDER BY
  country_rank