# Findings — `window_no_order_with_annotation.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 4 (error=3, warning=0, info=1)  
**Auto-fixed:** 1

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_GROUP_BY | error | no | Aggregate function mixed with non-aggregated columns without GROUP BY |
| <file> | WINDOW_NO_ORDER | error | yes | ROW_NUMBER() without ORDER BY - results are non-deterministic |
| <file> | LAG_LEAD_NO_ORDER | error | no | LAG() without ORDER BY - results are non-deterministic |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'window_no_order_with_annotation.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version w... |
