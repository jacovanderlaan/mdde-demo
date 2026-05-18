-- @mdde-entity: raw_orders
-- @mdde-layer: source
-- @mdde-stereotype: src_raw
-- @mdde-description: Raw orders landing from order management system

/*
Migration Details:
- sql_process Version: 192caf00 (2026-05-18)
- Original SQL File: raw_orders.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Table qualifiers normalised.
*/

/* @mdde-entity: raw_orders */
/* @mdde-layer: source */
/* @mdde-stereotype: src_raw */
/* @mdde-description: Raw orders landing from order management system */
CREATE OR REPLACE VIEW raw_orders AS
SELECT
  order_id, /* @pk @business_key */
  customer_id, /* @fk(raw_customers.customer_id) */
  order_date,
  order_status,
  total_amount,
  currency,
  source_system
FROM schema_identifier_ssf_snapshot.oms_orders_export;