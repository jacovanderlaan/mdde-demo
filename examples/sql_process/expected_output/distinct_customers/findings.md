# Findings — `distinct_customers.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 3 (error=0, warning=0, info=3)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'email' inside WHERE |
| <file> | DISTINCT_WITHOUT_JUSTIFICATION | info | no | DISTINCT used without a justifying comment |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'distinct_customers.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
