-- @mdde-entity: combo_ssf_loan_aggregates
-- @mdde-layer: business
-- @mdde-stereotype: aggregate
-- @mdde-description: SSF-style banking aggregate over loans + customers. Stresses:
-- passthrough-CTE rewrite, source-CTE pushdown with single-source derivations,
-- joined CTE for multi-source derivations, agg CTE with HAVING, outer CAST/CASE,
-- metadata column stripping, schema-qualifier rewrite.

/*
Migration Details:
- Original SQL File: combo_ssf_loan_aggregates.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Lifted aggregates and GROUP BY into a dedicated `_aggregated` CTE; outer SELECT applies casting / defaulting only.
  - Excluded metadata columns from outputs and WHERE (`snapshot_date`).
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
- [X] Aggregation isolated from formatting.
- [X] Metadata columns excluded.
- [X] Table qualifiers normalised.
*/

/* @mdde-entity: combo_ssf_loan_aggregates */ /* @mdde-layer: business */ /* @mdde-stereotype: aggregate */ /* @mdde-description: SSF-style banking aggregate over loans + customers. Stresses: */ /* passthrough-CTE rewrite, source-CTE pushdown with single-source derivations, */ /* joined CTE for multi-source derivations, agg CTE with HAVING, outer CAST/CASE, */ /* metadata column stripping, schema-qualifier rewrite. */
WITH cust AS (
  SELECT
    customer_id AS customer_id,
    UPPER(country) AS country_code,
    email AS email,
    country
  FROM schema_identifier_ssf_snapshot.customer
  WHERE
    NOT country IS NULL
), loans AS (
  SELECT
    customer_id,
    interest_rate,
    opened_at,
    principal_amount
  FROM schema_identifier_ssf_snapshot.loans
  WHERE
    status = 'OPEN' AND principal_amount > 0
), combo_ssf_loan_aggregates_joined AS (
  SELECT
    customer_id,
    country_code,
    email,
    principal_amount,
    interest_rate,
    opened_at,
    country
  FROM cust AS c
  INNER JOIN loans AS l
    ON l.customer_id = c.customer_id
), combo_ssf_loan_aggregates_aggregated AS (
  SELECT
    customer_id,
    country_code,
    email,
    country,
    SUM(principal_amount) AS principal_amount_sum,
    AVG(interest_rate * 100) AS interest_rate_avg,
    COUNT(*) AS agg_3,
    MAX(opened_at) AS opened_at_max
  FROM combo_ssf_loan_aggregates_joined
  GROUP BY
    customer_id,
    country,
    email
  HAVING
    SUM(principal_amount) > 0
)
SELECT
  customer_id,
  country_code,
  email,
  CAST(principal_amount_sum AS DECIMAL(18, 2)) AS total_principal,
  CAST(interest_rate_avg AS DECIMAL(6, 3)) AS avg_interest_pct,
  agg_3 AS loan_count,
  opened_at_max AS most_recent_loan,
  CASE
    WHEN principal_amount_sum > 1000000
    THEN 'tier-1'
    WHEN principal_amount_sum > 100000
    THEN 'tier-2'
    ELSE 'tier-3'
  END AS exposure_tier,
  COALESCE(country, 'unknown') AS country_clean,
  'loan_rollup' AS rollup_kind
FROM combo_ssf_loan_aggregates_aggregated
ORDER BY
  total_principal DESC
LIMIT 1000