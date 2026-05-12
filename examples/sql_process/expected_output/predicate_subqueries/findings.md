# Findings — `predicate_subqueries.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 3 (error=0, warning=0, info=3)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 3 sources (c, l, o) |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 2 sources (c, l) |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'predicate_subqueries.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
