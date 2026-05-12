# Findings — `union_branch_with_user_cte.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 6 (error=0, warning=2, info=4)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'orders' has no alias in multi-table query |
| <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2024-01-01' |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'union_branch_with_user_cte.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will b... |
