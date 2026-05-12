# Findings — `combo_filter_isolation.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 3 (error=0, warning=0, info=3)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'country' inside WHERE |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 3 sources (c, l, o) |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'combo_filter_isolation.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
