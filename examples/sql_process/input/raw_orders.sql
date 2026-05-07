-- @mdde-entity: raw_orders
-- @mdde-layer: source
-- @mdde-stereotype: src_raw
-- @mdde-description: Raw orders landing from order management system

CREATE OR REPLACE VIEW raw_orders AS
SELECT
    order_id,                   -- @pk @business_key
    customer_id,                -- @fk(raw_customers.customer_id)
    order_date,
    order_status,
    total_amount,
    currency,
    source_system
FROM landing.oms_orders_export;
