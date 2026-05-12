Produce a result set equivalent to `customer_subqueries`.

Sources:
- `stg_customers`
- `raw_orders`

Joins:
- LEFT JOIN `(SELECT raw_orders.customer_id AS customer_id, MAX(raw_orders.order_date) AS last_order_date FROM raw_orders AS raw_orders WHERE raw_orders.order_status = 'SHIPPED' GROUP BY raw_orders.customer_id) AS r` on `r.customer_id = c.customer_id`
- LEFT JOIN `(SELECT raw_orders.customer_id AS customer_id, COUNT(*) AS order_count, SUM(raw_orders.total_amount) AS total_revenue FROM raw_orders AS raw_orders GROUP BY raw_orders.customer_id) AS t` on `t.customer_id = c.customer_id`

Filters:
- c.customer_id IN (SELECT raw_orders.customer_id AS customer_id FROM raw_orders AS raw_orders WHERE raw_orders.order_status = 'SHIPPED')

Return columns:
- `customer_id` = c.customer_id
- `email` = c.email
- `last_order_date` = r.last_order_date
- `order_count` = t.order_count
- `total_revenue` = t.total_revenue
- `max_order_global` = (SELECT MAX(raw_orders.total_amount) AS _col_0 FROM raw_orders AS raw_orders)
- `lifetime_order_count` = (SELECT COUNT(*) AS _col_0 FROM raw_orders AS o WHERE o.customer_id = c.customer_id)
