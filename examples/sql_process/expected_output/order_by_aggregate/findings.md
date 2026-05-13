# Findings — `order_by_aggregate.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 3 (error=0, warning=2, info=1)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'order_by_aggregate.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
