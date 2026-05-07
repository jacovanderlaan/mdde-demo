-- Model: int_customer_master
-- Description: Generated from MDDE metadata
-- Stereotype: int_master

SELECT
    customer_id,
    first_name,
    last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    email,
    phone,
    onboarding_date,
    CASE WHEN c.created_at >= CURRENT_DATE - INTERVAL '90' DAY THEN 'new' WHEN c.created_at >= CURRENT_DATE - INTERVAL '365' DAY THEN 'active' ELSE 'tenured' END AS customer_tenure_segment,
    source_system,
    _ingested_at
-- TODO: Add source reference
FROM source_table