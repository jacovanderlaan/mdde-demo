-- @mdde-entity: source_derivations
-- @mdde-layer: business
-- @mdde-stereotype: int_consolidated
-- @mdde-description: Single-source value transforms (UPPER, TRIM, arithmetic) get
-- folded into the source CTE alongside renames. Multi-source derivations stay in
-- the joined CTE. CAST / CASE / COALESCE / constants stay at the outer SELECT.

SELECT
    c.customer_id AS customer_id,
    UPPER(TRIM(c.email)) AS email_clean,
    SUBSTRING(c.email, INSTR(c.email, '@') + 1) AS email_domain,
    c.country AS country,
    o.order_date AS order_date,
    o.amount * 1.21 AS amount_with_vat,
    o.amount + o.amount * 0.05 AS amount_plus_tip,
    o.amount - o.amount * 0.10 AS amount_after_discount,
    c.customer_id || ':' || o.order_id AS composite_key,
    CASE WHEN o.amount > 100 THEN 'large' ELSE 'small' END AS amount_tier,
    CAST(o.amount AS DECIMAL(18, 2)) AS amount_fmt,
    COALESCE(c.country, 'unknown') AS country_clean,
    'derivation_demo' AS rollup_kind
FROM raw.customer AS c
LEFT JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
