# Findings — `combo_casting_at_final_only.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 4 (error=0, warning=0, info=4)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'email' inside WHERE |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 2 sources (c, o) |
| <file> | GROUPBY_NOT_ISOLATED | info | no | GROUP BY combined with WHERE and JOINs in a single SELECT |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'combo_casting_at_final_only.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will ... |
