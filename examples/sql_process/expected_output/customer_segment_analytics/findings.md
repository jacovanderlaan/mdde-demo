# Findings — `customer_segment_analytics.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 10 (error=1, warning=2, info=7)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_ALIAS | info | no | Table 'customer_segment_analytics' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'shipped_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'customer_totals' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'customer_totals' has no alias in multi-table query |
| <file> | MISSING_GROUP_BY | error | no | Aggregate function mixed with non-aggregated columns without GROUP BY |
| <file> | PK_DEDUP_CHECK_MISSING | info | no | @pk annotations present but no ROW_NUMBER PARTITION BY <pk> for dedup verification |
| <file> | WINDOW_NON_UNIQUE_ORDER | warning | no | ROW_NUMBER() ORDER BY (total_revenue) may not be unique within partition |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'customer_segment_analytics.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will b... |
