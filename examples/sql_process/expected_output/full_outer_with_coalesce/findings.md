# Findings — `full_outer_with_coalesce.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 2 (error=0, warning=1, info=1)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | FULL_OUTER_WITH_COALESCE | warning | no | FULL OUTER JOIN with COALESCE — replace with LEFT JOIN/IS NULL + INNER JOIN + UNION ALL pattern |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'full_outer_with_coalesce.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be ... |
