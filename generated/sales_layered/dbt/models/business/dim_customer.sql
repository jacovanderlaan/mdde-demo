-- Model: dim_customer
-- Description: Generated from MDDE metadata
-- Stereotype: dim_scd2

SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id, _valid_from) AS customer_sk,
    customer_id,
    full_name,
    email,
    phone,
    onboarding_date,
    customer_tenure_segment,
    source_system,
    _valid_from,
    _valid_to,
    _is_current
-- TODO: Add source reference
FROM source_table