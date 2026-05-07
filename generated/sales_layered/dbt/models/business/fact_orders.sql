-- Model: fact_orders
-- Description: Generated from MDDE metadata
-- Stereotype: fact

SELECT
    order_id,
    order_line_id,
    customer_sk,
    product_id,
    order_date,
    quantity,
    unit_price,
    o.quantity * o.unit_price AS line_amount,
    currency_code,
    order_status,
    _ingested_at
-- TODO: Add source reference
FROM source_table