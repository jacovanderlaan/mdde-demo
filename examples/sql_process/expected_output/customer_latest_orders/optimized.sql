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
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.

Validation Checklist:
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
*/

/* @mdde-entity: customer_latest_orders */
/* @mdde-layer: business */
/* @mdde-stereotype: fact_dedup */
/* @mdde-description: Per-customer latest order via window dedup (QUALIFY pattern) */
CREATE OR REPLACE VIEW customer_latest_orders AS
-- Source prep: single-table SELECT + renames + single-source value transforms
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
-- Source prep: single-table SELECT + renames + single-source value transforms
, latest_per_customer_prepared AS (
  SELECT
    order_id AS latest_order_id, /* @fk(raw_orders.order_id) */
    order_date AS latest_order_date,
    total_amount AS latest_order_amount,
    DATEDIFF(CURRENT_DATE, order_date) AS days_since_last_order, /* @derived */
    customer_id
  FROM latest_per_customer
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, stg_customers_joined AS (
  SELECT
    customer_id,
    email,
    latest_order_id,
    latest_order_date,
    latest_order_amount,
    days_since_last_order
  FROM stg_customers_prepared AS c
  LEFT JOIN latest_per_customer_prepared AS l
    ON c.customer_id = l.customer_id
)
SELECT
  customer_id,
  email,
  latest_order_id,
  latest_order_date,
  latest_order_amount,
  days_since_last_order
FROM stg_customers_joined;