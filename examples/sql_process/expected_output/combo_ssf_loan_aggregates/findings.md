# Findings — `combo_ssf_loan_aggregates.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 10 (error=0, warning=4, info=6)  
**Auto-fixed:** 1

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_ALIAS | info | no | Table 'customer' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'loans' has no alias in multi-table query |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'snapshot_date' exposed in output (as 'snapshot_date') |
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'country' inside WHERE |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 2 sources (c, l) |
| <file> | GROUPBY_NOT_ISOLATED | info | no | GROUP BY combined with WHERE and JOINs in a single SELECT |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'combo_ssf_loan_aggregates.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be... |
