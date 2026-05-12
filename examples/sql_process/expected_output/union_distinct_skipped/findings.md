# Findings — `union_distinct_skipped.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 7 (error=0, warning=2, info=5)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'customer' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'customer' has no alias in multi-table query |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | UNION_MISSING_SOURCE_TAG | info | no | UNION ALL branch lacks a source-identifying literal column |
| <file> | UNION_MISSING_SOURCE_TAG | info | no | UNION ALL branch lacks a source-identifying literal column |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'union_distinct_skipped.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
