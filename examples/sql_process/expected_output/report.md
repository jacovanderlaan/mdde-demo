# sql_process — run report

Files processed: **7**

## Files

| File | Entity | Layer | Qualified | CTEs | Sources | Output cols | Quality issues |
|---|---|---|---|---|---|---|---|
| `customer_latest_orders.sql` | customer_latest_orders | business | yes | 2 | 2 | 6 | 5 |
| `customer_revenue.sql` | customer_revenue_clean | business | yes | 2 | 2 | 7 | 4 |
| `customer_revenue_bad.sql` | customer_revenue | business | yes | 2 | 2 | 10 | 10 |
| `customer_segment_analytics.sql` | customer_segment_analytics | business | yes | 3 | 2 | 10 | 8 |
| `raw_customers.sql` | raw_customers | source | yes | 0 | 1 | 7 | 2 |
| `raw_orders.sql` | raw_orders | source | yes | 0 | 1 | 7 | 2 |
| `stg_customers.sql` | stg_customers | staging | yes | 0 | 1 | 7 | 2 |

## Quality findings

**Total:** 33 (error=2, warning=8, info=23)  
**Auto-fixed:** 1

| File | Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|---|
| `customer_latest_orders.sql` | <file> | MISSING_ALIAS | info | no | Table 'customer_latest_orders' has no alias in multi-table query |
| `customer_latest_orders.sql` | <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| `customer_latest_orders.sql` | <file> | MISSING_ALIAS | info | no | Table 'ordered_orders' has no alias in multi-table query |
| `customer_latest_orders.sql` | <file> | WINDOW_NON_UNIQUE_ORDER | warning | no | ROW_NUMBER() ORDER BY (order_date, order_id) may not be unique within partition |
| `customer_latest_orders.sql` | <file> | VOLATILE_FUNCTION | info | no | CURRENT_DATE() returns current time - varies between runs |
| `customer_revenue.sql` | <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| `customer_revenue.sql` | <file> | MISSING_ALIAS | info | no | Table 'customer_revenue_clean' has no alias in multi-table query |
| `customer_revenue.sql` | <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| `customer_revenue.sql` | <file> | MISSING_ALIAS | info | no | Table 'shipped_orders' has no alias in multi-table query |
| `customer_revenue_bad.sql` | <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| `customer_revenue_bad.sql` | <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| `customer_revenue_bad.sql` | <file> | MISSING_ALIAS | info | no | Table 'customer_revenue' has no alias in multi-table query |
| `customer_revenue_bad.sql` | <file> | MISSING_ALIAS | info | no | Table 'joined' has no alias in multi-table query |
| `customer_revenue_bad.sql` | <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| `customer_revenue_bad.sql` | <file> | ORDER_BY_NUMBER | warning | no | ORDER BY uses column number (1) instead of name |
| `customer_revenue_bad.sql` | <file> | WHERE_1_EQUALS_1 | info | yes | WHERE 1=1 pattern detected |
| `customer_revenue_bad.sql` | <file> | CARTESIAN_JOIN | warning | no | JOIN to 'stg_customers' without ON clause - potential cartesian product |
| `customer_revenue_bad.sql` | <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2026-01-01' |
| `customer_revenue_bad.sql` | <file> | WINDOW_NO_ORDER | error | no | ROW_NUMBER() without ORDER BY - results are non-deterministic |
| `customer_segment_analytics.sql` | <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| `customer_segment_analytics.sql` | <file> | MISSING_ALIAS | info | no | Table 'customer_segment_analytics' has no alias in multi-table query |
| `customer_segment_analytics.sql` | <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| `customer_segment_analytics.sql` | <file> | MISSING_ALIAS | info | no | Table 'shipped_orders' has no alias in multi-table query |
| `customer_segment_analytics.sql` | <file> | MISSING_ALIAS | info | no | Table 'customer_totals' has no alias in multi-table query |
| `customer_segment_analytics.sql` | <file> | MISSING_ALIAS | info | no | Table 'customer_totals' has no alias in multi-table query |
| `customer_segment_analytics.sql` | <file> | MISSING_GROUP_BY | error | no | Aggregate function mixed with non-aggregated columns without GROUP BY |
| `customer_segment_analytics.sql` | <file> | WINDOW_NON_UNIQUE_ORDER | warning | no | ROW_NUMBER() ORDER BY (total_revenue) may not be unique within partition |
| `raw_customers.sql` | <file> | MISSING_ALIAS | info | no | Table 'raw_customers' has no alias in multi-table query |
| `raw_customers.sql` | <file> | MISSING_ALIAS | info | no | Table 'crm_customers_export' has no alias in multi-table query |
| `raw_orders.sql` | <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| `raw_orders.sql` | <file> | MISSING_ALIAS | info | no | Table 'oms_orders_export' has no alias in multi-table query |
| `stg_customers.sql` | <file> | MISSING_ALIAS | info | no | Table 'stg_customers' has no alias in multi-table query |
| `stg_customers.sql` | <file> | MISSING_ALIAS | info | no | Table 'raw_customers' has no alias in multi-table query |

## Mapping coverage

**54/54** output columns have a resolved source attribute (100%)

## Cross-file lineage

**9** producer-consumer edge(s) stitched across the file set.

| Producer | Consumer | Via table |
|---|---|---|
| `raw_customers.sql` | `stg_customers.sql` | `raw_customers` |
| `raw_orders.sql` | `customer_latest_orders.sql` | `raw_orders` |
| `raw_orders.sql` | `customer_revenue.sql` | `raw_orders` |
| `raw_orders.sql` | `customer_revenue_bad.sql` | `raw_orders` |
| `raw_orders.sql` | `customer_segment_analytics.sql` | `raw_orders` |
| `stg_customers.sql` | `customer_latest_orders.sql` | `stg_customers` |
| `stg_customers.sql` | `customer_revenue.sql` | `stg_customers` |
| `stg_customers.sql` | `customer_revenue_bad.sql` | `stg_customers` |
| `stg_customers.sql` | `customer_segment_analytics.sql` | `stg_customers` |

```mermaid
flowchart LR
  subgraph source
    raw_customers["raw_customers<br/><i>raw_customers.sql</i>"]
    raw_orders["raw_orders<br/><i>raw_orders.sql</i>"]
  end
  subgraph staging
    stg_customers["stg_customers<br/><i>stg_customers.sql</i>"]
  end
  subgraph business
    customer_latest_orders["customer_latest_orders<br/><i>customer_latest_orders.sql</i>"]
    customer_revenue_clean["customer_revenue_clean<br/><i>customer_revenue.sql</i>"]
    customer_revenue["customer_revenue<br/><i>customer_revenue_bad.sql</i>"]
    customer_segment_analytics["customer_segment_analytics<br/><i>customer_segment_analytics.sql</i>"]
  end
  raw_customers --> stg_customers
  raw_orders --> customer_latest_orders
  raw_orders --> customer_revenue_clean
  raw_orders --> customer_revenue
  raw_orders --> customer_segment_analytics
  stg_customers --> customer_latest_orders
  stg_customers --> customer_revenue_clean
  stg_customers --> customer_revenue
  stg_customers --> customer_segment_analytics
  subgraph external ["external sources"]
    ext_landing_crm_customers_export[("landing.crm_customers_export")]
    ext_landing_oms_orders_export[("landing.oms_orders_export")]
  end
  ext_landing_crm_customers_export --> raw_customers
  ext_landing_oms_orders_export --> raw_orders
```

## SQL-First annotations

| File | Entity | Annotated columns | Tags found |
|---|---|---|---|
| `customer_latest_orders.sql` | customer_latest_orders | 3 | derived, fk, pk |
| `customer_revenue.sql` | customer_revenue_clean | 1 | pk |
| `customer_revenue_bad.sql` | customer_revenue | 1 | pk |
| `customer_segment_analytics.sql` | customer_segment_analytics | 1 | derived |
| `raw_customers.sql` | raw_customers | 5 | business_key, nullable, pii, pk |
| `raw_orders.sql` | raw_orders | 2 | business_key, fk, pk |
| `stg_customers.sql` | stg_customers | 3 | business_key, pii, pk |
