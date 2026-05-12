-- @mdde-entity: combo_casting_at_final_only
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Exercises that CAST / COALESCE / NULLIF / CASE / constants
-- ALWAYS end up at the final SELECT, never folded into source/joined/agg CTEs.
-- The input deliberately mixes casting and defaulting with:
--   - single-source value transforms that DO fold into source CTEs
--   - multi-source derivations that DO fold into the joined CTE
--   - aggregates that DO fold into the agg CTE
-- Verifies the boundary stays in the right place across all combinations.

SELECT
    c.customer_id AS customer_id,
    -- single-source transform (folds to source CTE)
    UPPER(c.country) AS country_upper,
    -- single-source transform wrapped in CAST (CAST stays outer, the value
    -- transform UPPER also stays with it because CAST wraps it)
    CAST(UPPER(c.email) AS VARCHAR(100)) AS email_normalised,
    -- multi-source derivation (folds to joined CTE)
    c.customer_id || ':' || o.order_id AS composite_key,
    -- multi-source derivation wrapped in COALESCE (COALESCE stays outer)
    COALESCE(c.email, o.payment_method, 'unknown') AS contact_handle,
    -- aggregate (folds to agg CTE)
    SUM(o.amount) AS revenue,
    -- aggregate wrapped in CAST (CAST stays outer, aggregate folds)
    CAST(SUM(o.amount) AS DECIMAL(18, 2)) AS revenue_fmt,
    -- nested formatting: CASE wrapping a CAST wrapping an aggregate
    CASE
        WHEN CAST(SUM(o.amount) AS DECIMAL(18, 2)) > 1000 THEN 'high'
        WHEN CAST(SUM(o.amount) AS DECIMAL(18, 2)) > 100 THEN 'mid'
        ELSE 'low'
    END AS revenue_band,
    -- COALESCE around a bare column reference (COALESCE stays outer)
    COALESCE(c.country, 'unknown') AS country_clean,
    -- NULLIF around a single-source projection (NULLIF stays outer)
    NULLIF(UPPER(c.email), '') AS email_or_null,
    -- Pure literal (stays outer)
    'rollup_v1' AS rollup_kind,
    -- Numeric literal (stays outer)
    1.0 AS scale_factor
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
WHERE c.email IS NOT NULL
  AND o.amount > 0
GROUP BY c.customer_id, c.country, c.email, o.order_id, o.payment_method
HAVING SUM(o.amount) > 0
