Produce a result set equivalent to `customer_revenue`.

Sources:
- `raw_orders`
- `stg_customers`

Return columns:
- `customer_id` = joined.customer_id
- `total_amount` = joined.total_amount
- `rn` = joined.rn
- `customer_id` = joined.customer_id
- `first_name` = joined.first_name
- `last_name` = joined.last_name
- `email` = joined.email
- `phone` = joined.phone
- `created_at` = joined.created_at
- `source_system` = joined.source_system

Sort: customer_id ASC
