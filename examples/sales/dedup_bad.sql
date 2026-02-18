-- dedup_bad.sql
-- Example of non-deterministic SQL patterns
-- These patterns will cause different results on different runs or platforms

-- Pattern 1: ROW_NUMBER without ORDER BY
-- This is the most common non-determinism bug
WITH customer_dedup AS (
    SELECT
        customer_id,
        customer_name,
        email,
        region,
        -- PROBLEM: No ORDER BY - arbitrary row gets rn=1
        ROW_NUMBER() OVER (PARTITION BY email) AS rn
    FROM raw_customers
)
SELECT * FROM customer_dedup WHERE rn = 1;

-- Pattern 2: ROW_NUMBER with non-unique ORDER BY
-- This looks correct but isn't
WITH order_latest AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        -- PROBLEM: Multiple orders can have same order_date
        -- Which one gets rn=1 is arbitrary
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) AS rn
    FROM orders
)
SELECT * FROM order_latest WHERE rn = 1;

-- Pattern 3: FIRST_VALUE without ORDER BY
SELECT
    customer_id,
    region,
    -- PROBLEM: Which customer's name is "first"? Arbitrary!
    FIRST_VALUE(customer_name) OVER (PARTITION BY region) AS region_representative
FROM customers;

-- Pattern 4: LIMIT without ORDER BY
-- Returns different rows each execution
SELECT * FROM products LIMIT 10;

-- Pattern 5: Sample query using RANDOM()
-- Intentionally non-deterministic, but should be flagged
SELECT
    customer_id,
    customer_name,
    RANDOM() AS random_sort_key
FROM customers
ORDER BY RANDOM()
LIMIT 100;
