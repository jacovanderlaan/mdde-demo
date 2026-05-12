Produce a result set equivalent to `customer_revenue_clean`.

Sources:
- `stg_customers`
- `raw_orders`

Joins:
- INNER JOIN `customer_totals AS t` on `c.customer_id = t.customer_id`

Return columns:
- `customer_id` = c.customer_id
- `email` = c.email
- `first_name` = c.first_name
- `last_name` = c.last_name
- `order_count` = t.order_count
- `total_revenue` = t.total_revenue
- `avg_order_value` = t.avg_order_value

Sort: t.total_revenue DESC
