-- Layer: source
-- Stereotype: src_raw
-- Description: Raw landing table from CRM system, no transformations
-- Source system: CRM export (daily dump)

CREATE OR REPLACE VIEW raw_customers AS
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    created_at,
    updated_at,
    source_system,
    _ingested_at
FROM raw.crm_customers_landing;
