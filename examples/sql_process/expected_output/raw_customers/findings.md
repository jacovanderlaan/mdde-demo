# Findings — `raw_customers.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 5 (error=0, warning=0, info=5)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'raw_customers' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'crm_customers_export' has no alias in multi-table query |
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column '_ingested_at' inside WHERE |
| <file> | PK_DEDUP_CHECK_MISSING | info | no | @pk annotations present but no ROW_NUMBER PARTITION BY <pk> for dedup verification |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'raw_customers.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
