-- @mdde-entity: source_derivations
-- @mdde-layer: business
-- @mdde-stereotype: int_consolidated
-- @mdde-description: Single-source value transforms (UPPER, TRIM, arithmetic) get
-- folded into the source CTE alongside renames. Multi-source derivations stay in
-- the joined CTE. CAST / CASE / COALESCE / constants stay at the outer SELECT.

/*
Migration Details:
- Original SQL File: source_derivations.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
*/

WITH customer_prepared AS (
  SELECT
    customer_id AS customer_id,
    UPPER(TRIM(email)) AS email_clean,
    SUBSTRING(email, STR_POSITION(email, '@') + 1) AS email_domain,
    country AS country
  FROM customer
), orders_prepared AS (
  SELECT
    order_date AS order_date,
    amount * 1.21 AS amount_with_vat,
    amount + amount * 0.05 AS amount_plus_tip,
    amount - amount * 0.10 AS amount_after_discount,
    amount,
    customer_id,
    order_id
  FROM orders
), source_derivations_joined AS (
  SELECT
    customer_id,
    email_clean,
    email_domain,
    country,
    order_date,
    amount_with_vat,
    amount_plus_tip,
    amount_after_discount,
    c.customer_id || ':' || o.order_id AS composite_key,
    amount
  FROM customer_prepared AS c
  LEFT JOIN orders_prepared AS o
    ON o.customer_id = c.customer_id
)
/* @mdde-entity: source_derivations */ /* @mdde-layer: business */ /* @mdde-stereotype: int_consolidated */ /* @mdde-description: Single-source value transforms (UPPER, TRIM, arithmetic) get */ /* folded into the source CTE alongside renames. Multi-source derivations stay in */ /* the joined CTE. CAST / CASE / COALESCE / constants stay at the outer SELECT. */
SELECT
  customer_id,
  email_clean,
  email_domain,
  country,
  order_date,
  amount_with_vat,
  amount_plus_tip,
  amount_after_discount,
  composite_key,
  CASE WHEN amount > 100 THEN 'large' ELSE 'small' END AS amount_tier,
  CAST(amount AS DECIMAL(18, 2)) AS amount_fmt,
  COALESCE(country, 'unknown') AS country_clean,
  'derivation_demo' AS rollup_kind
FROM source_derivations_joined