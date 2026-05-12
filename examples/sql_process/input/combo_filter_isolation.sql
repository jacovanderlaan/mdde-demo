-- @mdde-entity: combo_filter_isolation
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Stresses the filter-isolation rule. The input WHERE clause
-- has FOUR kinds of predicates that should be routed to DIFFERENT layers:
--   1. Single-source on customer (folds to customer_prepared/_filtered)
--   2. Single-source on orders (folds to orders_prepared/_filtered)
--   3. Single-source on loans (folds to loans_prepared/_filtered)
--   4. Cross-source predicate touching customer + orders (must move to a
--      dedicated <entity>_filtered CTE — NEVER stays in the joined CTE)
-- The joined CTE must end up with NO WHERE clause.

SELECT
    c.customer_id AS customer_id,
    c.country AS country,
    c.email AS email,
    o.amount AS amount,
    o.order_date AS order_date,
    l.principal_amount AS loan_principal,
    l.status AS loan_status
FROM raw.customer AS c
INNER JOIN raw.orders AS o
  ON o.customer_id = c.customer_id
INNER JOIN raw.loans AS l
  ON l.customer_id = c.customer_id
WHERE c.country IS NOT NULL        -- single-source customer
  AND o.amount > 0                  -- single-source orders
  AND l.status = 'OPEN'             -- single-source loans
  AND c.country = o.payment_method  -- cross-source (customer + orders) → filtered CTE
  AND l.principal_amount > o.amount -- cross-source (loans + orders) → filtered CTE
