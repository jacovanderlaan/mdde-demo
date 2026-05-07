-- Model: stg_customers
-- Description: Generated from MDDE metadata
-- Stereotype: stg_cleaned

SELECT
    CAST(customer_id AS BIGINT) AS customer_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    LOWER(TRIM(email)) AS email,
    NULLIF(TRIM(phone), '') AS phone,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    source_system,
    _ingested_at
-- TODO: Add source reference
FROM source_table