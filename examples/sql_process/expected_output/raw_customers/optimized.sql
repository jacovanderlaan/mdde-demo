-- @mdde-entity: raw_customers
-- @mdde-layer: source
-- @mdde-stereotype: src_raw
-- @mdde-description: Raw customer landing from CRM

/*
Migration Details:
- Original SQL File: raw_customers.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Table qualifiers normalised.
*/

/* @mdde-entity: raw_customers */ /* @mdde-layer: source */ /* @mdde-stereotype: src_raw */ /* @mdde-description: Raw customer landing from CRM */
CREATE OR REPLACE VIEW raw_customers AS
SELECT
  customer_id, /* @pk @business_key */
  first_name, /* @pii */
  last_name, /* @pii */
  email, /* @pii @nullable */
  phone, /* @pii @nullable */
  created_at,
  source_system
FROM schema_identifier_ssf_snapshot.crm_customers_export
WHERE
  NOT _ingested_at IS NULL;