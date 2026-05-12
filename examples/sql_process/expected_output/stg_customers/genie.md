Produce a result set equivalent to `stg_customers`.

Sources:
- `raw_customers`

Filters:
- NOT raw_customers.customer_id IS NULL
- NOT raw_customers.email IS NULL

Return columns:
- `customer_id` = CAST(raw_customers.customer_id AS BIGINT)
- `first_name` = TRIM(raw_customers.first_name)
- `last_name` = TRIM(raw_customers.last_name)
- `email` = LOWER(TRIM(raw_customers.email))
- `phone` = NULLIF(TRIM(raw_customers.phone), '')
- `created_at` = CAST(raw_customers.created_at AS TIMESTAMP)
- `source_system` = raw_customers.source_system
