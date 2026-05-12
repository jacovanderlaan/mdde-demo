-- @mdde-entity: passthrough_with_loans
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Outer SELECT joins two passthrough CTEs that simply wrap a
-- real table. Exercises passthrough-CTE rewrite: the `SELECT *` body of each
-- existing CTE is replaced with the renames the outer SELECT actually uses.

WITH active_customers AS (
    SELECT *
    FROM raw.customer
    WHERE NOT email IS NULL
), open_loans AS (
    SELECT *
    FROM raw.loans
)
SELECT
    c.customer_id AS customer_id,
    c.email AS email,
    l.product_code AS loan_product,
    l.principal_amount AS principal_amount,
    l.interest_rate AS interest_rate,
    l.status AS loan_status
FROM active_customers AS c
LEFT JOIN open_loans AS l
  ON l.customer_id = c.customer_id
