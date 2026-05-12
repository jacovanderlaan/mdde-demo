# Findings — `chained_cte_pipeline.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 7 (error=0, warning=2, info=5)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_ALIAS | info | no | Table 'customer' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'base_orders' has no alias in multi-table query |
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'email' inside WHERE |
| <file> | WINDOW_NON_UNIQUE_ORDER | warning | no | ROW_NUMBER() ORDER BY (lifetime_revenue) may not be unique within partition |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'chained_cte_pipeline.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
