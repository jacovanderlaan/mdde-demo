Produce a result set equivalent to `raw_customers`.

Sources:
- `landing.crm_customers_export`

Filters:
- NOT crm_customers_export._ingested_at IS NULL

Return columns:
- `customer_id` = crm_customers_export.customer_id
- `first_name` = crm_customers_export.first_name
- `last_name` = crm_customers_export.last_name
- `email` = crm_customers_export.email
- `phone` = crm_customers_export.phone
- `created_at` = crm_customers_export.created_at
- `source_system` = crm_customers_export.source_system
