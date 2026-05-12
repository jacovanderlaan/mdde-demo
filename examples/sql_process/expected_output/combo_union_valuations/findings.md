# Findings — `combo_union_valuations.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 13 (error=0, warning=10, info=3)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | INLINE_UNION_TRANSFORM | warning | no | Transformation inside UNION ALL branch: CAST(SUM(l.principal_amount) AS DECIMAL(18, 2)) AS amount |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | INLINE_UNION_TRANSFORM | warning | no | Transformation inside UNION ALL branch: CAST(COUNT(DISTINCT o.order_date) AS DECIMAL(18, 2)) AS amount |
| <file> | INLINE_UNION_TRANSFORM | warning | no | Transformation inside UNION ALL branch: CAST(SUM(l.principal_amount) AS DECIMAL(18, 2)) AS amount |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | INLINE_UNION_TRANSFORM | warning | no | Transformation inside UNION ALL branch: CAST(SUM(o.amount) AS DECIMAL(18, 2)) AS amount |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | GROUPBY_NOT_ISOLATED | info | no | GROUP BY combined with WHERE and JOINs in a single SELECT |
| <file> | GROUPBY_NOT_ISOLATED | info | no | GROUP BY combined with WHERE and JOINs in a single SELECT |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'combo_union_valuations.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
