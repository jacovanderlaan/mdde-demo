-- @mdde-entity: combo_at_risk_customers
-- @mdde-layer: business
-- @mdde-stereotype: filter_with_ranking
-- @mdde-description: Customers with overdue loans, excluding any who already paid
-- off recently. Stresses: correlated WHERE IN, correlated WHERE EXISTS, top-level
-- EXCEPT (with both sides Select), window function (ROW_NUMBER), CAST/CASE in
-- outer formatting, metadata column stripping, ORDER BY at outer.

SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    c.country AS country,
    ROW_NUMBER() OVER (PARTITION BY c.country ORDER BY c.customer_id) AS country_rank,
    CASE
        WHEN c.country = 'NL' THEN 'domestic'
        ELSE 'international'
    END AS region,
    snapshot_date AS snapshot_date
FROM raw.customer AS c
WHERE c.customer_id IN (
    SELECT l.customer_id
    FROM raw.loans AS l
    WHERE l.status = 'OPEN'
      AND l.opened_at < '2023-01-01'
)
AND EXISTS (
    SELECT 1
    FROM raw.orders AS o
    WHERE o.customer_id = c.customer_id
      AND o.amount > 1000
)
EXCEPT
SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    c.country AS country,
    1 AS country_rank,
    'recently_paid' AS region,
    '2026-05-12' AS snapshot_date
FROM raw.customer AS c
INNER JOIN raw.loans AS l
  ON l.customer_id = c.customer_id
WHERE l.status = 'CLOSED'
  AND l.opened_at >= '2024-01-01'
ORDER BY country_rank
