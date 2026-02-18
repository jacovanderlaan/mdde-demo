-- Products table - INTENTIONALLY BAD SQL for optimizer demo
-- This file contains anti-patterns that the optimizer should detect

SELECT *
FROM raw_products p
JOIN raw_categories ON raw_categories.category_id = p.category_id
WHERE 1=1
  AND p.is_active = 1
ORDER BY 1
