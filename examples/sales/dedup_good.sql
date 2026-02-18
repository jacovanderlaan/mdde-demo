-- dedup_good.sql
-- Deterministic versions of the patterns from dedup_bad.sql
-- These will produce consistent results across runs and platforms

-- Pattern 1: ROW_NUMBER with unique ORDER BY
-- Added customer_id as tie-breaker
WITH customer_dedup AS (
    SELECT
        customer_id,
        customer_name,
        email,
        region,
        -- FIXED: ORDER BY with unique tie-breaker
        ROW_NUMBER() OVER (
            PARTITION BY email
            ORDER BY created_at DESC, customer_id  -- customer_id breaks ties
        ) AS rn,

        -- DQ Monitoring: Detect when ties would have existed
        COUNT(*) OVER (PARTITION BY email, created_at) AS _dq_tie_count
    FROM raw_customers
)
SELECT
    customer_id,
    customer_name,
    email,
    region,
    rn,
    _dq_tie_count,
    CASE
        WHEN _dq_tie_count > 1 THEN 'TIE_RESOLVED'
        ELSE 'NO_TIE'
    END AS _dq_determinism_status
FROM customer_dedup
WHERE rn = 1;

-- Pattern 2: ROW_NUMBER with unique ORDER BY
-- Added order_id as tie-breaker
WITH order_latest AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        -- FIXED: order_id ensures uniqueness
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC, order_id DESC
        ) AS rn
    FROM orders
)
SELECT * FROM order_latest WHERE rn = 1;

-- Pattern 3: FIRST_VALUE with explicit ORDER BY
SELECT
    customer_id,
    region,
    -- FIXED: Explicit ordering by customer_id
    FIRST_VALUE(customer_name) OVER (
        PARTITION BY region
        ORDER BY customer_id
    ) AS region_representative
FROM customers;

-- Pattern 4: LIMIT with ORDER BY
-- Deterministic selection of first 10 products by ID
SELECT * FROM products
ORDER BY product_id
LIMIT 10;

-- Pattern 5: Parameterized sampling (for reproducibility)
-- Use a seed or filter instead of RANDOM()
SELECT
    customer_id,
    customer_name,
    -- For reproducible sampling, use modulo on a stable column
    MOD(customer_id, 100) AS sample_bucket
FROM customers
WHERE MOD(customer_id, 100) < 10  -- 10% sample, deterministic
ORDER BY customer_id;
