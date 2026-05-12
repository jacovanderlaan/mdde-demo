-- @mdde-entity: customer_latest_orders
-- @mdde-layer: business
-- @mdde-stereotype: fact_dedup
-- @mdde-description: Per-customer latest order via window dedup (QUALIFY pattern)

/*
Migration Details:
- Original SQL File: customer_latest_orders.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.

Validation Checklist:
- [X] Modular CTE structure applied.
*/

/* @mdde-entity: customer_latest_orders */ /* @mdde-layer: business */ /* @mdde-stereotype: fact_dedup */ /* @mdde-description: Per-customer latest order via window dedup (QUALIFY pattern) */
CREATE OR REPLACE VIEW customer_latest_orders AS
WITH stg_customers_prepared AS (
  SELECT
    customer_id, /* @pk @business_key */
    email /* @pii */
  FROM stg_customers
), ordered_orders AS (
  SELECT
    order_id, /* @pk */
    customer_id, /* @fk(stg_customers.customer_id) */
    order_date,
    order_status,
    total_amount,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, order_id DESC) AS recency_rank
  FROM raw_orders
  WHERE
    order_status IN ('SHIPPED', 'DELIVERED')
), latest_per_customer AS (
  SELECT
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount
  FROM ordered_orders
  WHERE
    recency_rank = 1
)
SELECT
  c.customer_id,
  c.email,
  l.order_id AS latest_order_id, /* @fk(raw_orders.order_id) */
  l.order_date AS latest_order_date,
  l.total_amount AS latest_order_amount,
  DATEDIFF(CURRENT_DATE, l.order_date) AS days_since_last_order /* @derived */
FROM stg_customers_prepared AS c
LEFT JOIN latest_per_customer AS l
  ON c.customer_id = l.customer_id;