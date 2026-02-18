-- analytics_bad.sql
-- Example SQL with multiple anti-patterns for optimizer demo
-- This file intentionally contains bad practices to demonstrate the quality checks

SELECT DISTINCT *
FROM orders o
JOIN customers  -- Missing ON clause (CARTESIAN_JOIN)
JOIN products p ON p.product_id = o.product_id
    OR p.alt_product_id = o.product_id  -- OR in JOIN (OR_IN_JOIN)
WHERE 1=1
  AND UPPER(customer_name) = 'ACME'  -- Function on column (FUNCTION_IN_WHERE)
  AND order_date > '2024-01-01'  -- Hardcoded date (HARDCODED_DATE)
  AND product_name LIKE '%widget%'  -- Leading wildcard (LEADING_WILDCARD)
ORDER BY 1, 2, 3  -- ORDER BY number (ORDER_BY_NUMBER)
