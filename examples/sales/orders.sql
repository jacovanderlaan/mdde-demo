-- Orders fact table
-- Source: Order management system

SELECT
    order_id,
    customer_id,
    order_date,
    status,
    payment_method,
    subtotal,
    tax_amount,
    shipping_amount,
    total_amount,
    currency
FROM raw_orders
WHERE order_date >= '2024-01-01'
