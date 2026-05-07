-- Layer: business
-- Stereotype: fact
-- Description: Order fact at order-line grain, joined to customer dimension

CREATE OR REPLACE VIEW fact_orders AS
SELECT
    o.order_id,
    o.order_line_id,
    dc.customer_sk,
    o.product_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS line_amount,
    o.currency_code,
    o.order_status,
    o._ingested_at
FROM stg_orders o
JOIN dim_customer dc
  ON o.customer_id = dc.customer_id
 AND dc._is_current = TRUE;
