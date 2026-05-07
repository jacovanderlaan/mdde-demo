-- @mdde-entity: raw_customers
-- @mdde-layer: source
-- @mdde-stereotype: src_raw
-- @mdde-description: Raw customer landing from CRM

CREATE OR REPLACE VIEW raw_customers AS
SELECT
    customer_id,            -- @pk @business_key
    first_name,             -- @pii
    last_name,              -- @pii
    email,                  -- @pii @nullable
    phone,                  -- @pii @nullable
    created_at,
    source_system
FROM landing.crm_customers_export
WHERE _ingested_at IS NOT NULL;
