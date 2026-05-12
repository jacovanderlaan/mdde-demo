# Findings — `raw_orders.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 4 (error=0, warning=0, info=4)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'oms_orders_export' has no alias in multi-table query |
| <file> | PK_DEDUP_CHECK_MISSING | info | no | @pk annotations present but no ROW_NUMBER PARTITION BY <pk> for dedup verification |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'raw_orders.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
