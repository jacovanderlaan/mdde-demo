# Findings — `agg_customer_summary.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 2 (error=0, warning=1, info=1)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'agg_customer_summary.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
