Produce a result set equivalent to `customer_segment_analytics`.

Sources:
- `stg_customers`
- `raw_orders`

Joins:
- LEFT JOIN `stg_customers AS c` on `rc.customer_id = c.customer_id`

Return columns:
- `customer_id` = rc.customer_id
- `email` = c.email
- `first_name` = c.first_name
- `last_name` = c.last_name
- `order_count` = rc.order_count
- `total_revenue` = rc.total_revenue
- `avg_orders_overall` = rc.avg_orders_overall
- `max_revenue_overall` = rc.max_revenue_overall
- `revenue_rank` = rc.revenue_rank
- `segment` = rc.segment

Sort: rc.revenue_rank ASC
