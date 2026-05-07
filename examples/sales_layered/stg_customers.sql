-- Layer: staging
-- Stereotype: stg_cleaned
-- Description: Cleansed customer staging — null-handling, type-casts, basic dedup

CREATE OR REPLACE VIEW stg_customers AS
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
FROM raw_customers
WHERE customer_id IS NOT NULL
  AND email IS NOT NULL;
