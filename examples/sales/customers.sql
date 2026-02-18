-- Customers dimension table
-- Source: CRM system export

SELECT
    customer_id,
    customer_name,
    email,
    phone,
    country,
    tier,
    created_at,
    updated_at
FROM raw_customers
WHERE is_active = true
