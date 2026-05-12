Produce a result set equivalent to `customer_latest_orders`.

Sources:
- `stg_customers`
- `raw_orders`

Joins:
- LEFT JOIN `latest_per_customer AS l` on `c.customer_id = l.customer_id`

Return columns:
- `customer_id` = c.customer_id
- `email` = c.email
- `latest_order_id` = l.order_id
- `latest_order_date` = l.order_date
- `latest_order_amount` = l.total_amount
- `days_since_last_order` = DATEDIFF(CURRENT_DATE, l.order_date)
