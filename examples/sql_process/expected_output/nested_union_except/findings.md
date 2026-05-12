# Findings — `nested_union_except.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 9 (error=0, warning=3, info=6)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'customer' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'orders' has no alias in multi-table query |
| <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2024-01-01' |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'nested_union_except.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
