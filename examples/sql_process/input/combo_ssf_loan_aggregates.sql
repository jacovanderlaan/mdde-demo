-- @mdde-entity: combo_ssf_loan_aggregates
-- @mdde-layer: business
-- @mdde-stereotype: aggregate
-- @mdde-description: SSF-style banking aggregate over loans + customers. Stresses:
-- passthrough-CTE rewrite, source-CTE pushdown with single-source derivations,
-- joined CTE for multi-source derivations, agg CTE with HAVING, outer CAST/CASE,
-- metadata column stripping, schema-qualifier rewrite.

WITH cust AS (
    SELECT * FROM raw.customer
),
loans AS (
    SELECT * FROM raw.loans WHERE status = 'OPEN'
)
SELECT
    c.customer_id AS customer_id,
    UPPER(c.country) AS country_code,
    c.email AS email,
    CAST(SUM(l.principal_amount) AS DECIMAL(18, 2)) AS total_principal,
    CAST(AVG(l.interest_rate * 100) AS DECIMAL(6, 3)) AS avg_interest_pct,
    COUNT(*) AS loan_count,
    MAX(l.opened_at) AS most_recent_loan,
    CASE
        WHEN SUM(l.principal_amount) > 1000000 THEN 'tier-1'
        WHEN SUM(l.principal_amount) > 100000 THEN 'tier-2'
        ELSE 'tier-3'
    END AS exposure_tier,
    COALESCE(c.country, 'unknown') AS country_clean,
    snapshot_date AS snapshot_date,
    'loan_rollup' AS rollup_kind
FROM cust AS c
INNER JOIN loans AS l
  ON l.customer_id = c.customer_id
WHERE c.country IS NOT NULL
  AND l.principal_amount > 0
GROUP BY c.customer_id, c.country, c.email
HAVING SUM(l.principal_amount) > 0
ORDER BY total_principal DESC
LIMIT 1000
