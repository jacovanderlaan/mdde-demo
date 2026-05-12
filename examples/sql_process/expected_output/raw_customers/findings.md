# Findings — `raw_customers.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 4 (error=0, warning=0, info=4)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'raw_customers' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'crm_customers_export' has no alias in multi-table query |
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column '_ingested_at' inside WHERE |
| <file> | PK_DEDUP_CHECK_MISSING | info | no | @pk annotations present but no ROW_NUMBER PARTITION BY <pk> for dedup verification |
