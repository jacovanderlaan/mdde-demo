# Findings — `window_ranked_orders.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 3 (error=1, warning=1, info=1)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_GROUP_BY | error | no | Aggregate function mixed with non-aggregated columns without GROUP BY |
| <file> | WINDOW_NON_UNIQUE_ORDER | warning | no | ROW_NUMBER() ORDER BY (order_date) may not be unique within partition |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'window_ranked_orders.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
