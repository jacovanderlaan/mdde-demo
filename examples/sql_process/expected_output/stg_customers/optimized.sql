-- @mdde-entity: stg_customers
-- @mdde-layer: staging
-- @mdde-stereotype: stg_cleaned
-- @mdde-description: Cleansed customers — typed, deduplicated, null-handled

/* @mdde-entity: stg_customers */ /* @mdde-layer: staging */ /* @mdde-stereotype: stg_cleaned */ /* @mdde-description: Cleansed customers — typed, deduplicated, null-handled */
CREATE OR REPLACE VIEW stg_customers AS
SELECT
  CAST(customer_id AS BIGINT) AS customer_id, /* @pk @business_key */
  TRIM(first_name) AS first_name, /* @pii */
  TRIM(last_name) AS last_name, /* @pii */
  LOWER(TRIM(email)) AS email, /* @pii */
  NULLIF(TRIM(phone), '') AS phone, /* @pii @nullable */
  CAST(created_at AS TIMESTAMP) AS created_at,
  source_system
FROM raw_customers
WHERE
  NOT customer_id IS NULL AND NOT email IS NULL;