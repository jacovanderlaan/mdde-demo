# Findings — `lateral_unnest.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 4 (error=0, warning=0, info=4)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'email' inside WHERE |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 2 sources (c, o2) |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'lateral_unnest.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
| <predicate> | SUBQUERY_NOT_LIFTED | info | no | Subquery inside Lateral left inline — non-IN/EXISTS predicate subquery; lifting would require synthesising a join/DIS... |
