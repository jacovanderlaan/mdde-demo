# Findings — `except_subscribed_customers.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 2 (error=0, warning=0, info=2)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'email' inside WHERE |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'except_subscribed_customers.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will ... |
