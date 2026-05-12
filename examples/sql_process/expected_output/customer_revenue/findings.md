# Findings — `customer_revenue.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 6 (error=0, warning=1, info=5)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_ALIAS | info | no | Table 'customer_revenue_clean' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'shipped_orders' has no alias in multi-table query |
| <file> | PK_DEDUP_CHECK_MISSING | info | no | @pk annotations present but no ROW_NUMBER PARTITION BY <pk> for dedup verification |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'customer_revenue.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
