# Findings — `passthrough_with_loans.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 6 (error=0, warning=2, info=4)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_ALIAS | info | no | Table 'customer' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'loans' has no alias in multi-table query |
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'email' inside WHERE |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'passthrough_with_loans.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
