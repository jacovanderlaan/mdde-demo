# Findings — `combo_at_risk_customers.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 9 (error=0, warning=3, info=6)  
**Auto-fixed:** 2

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2026-05-12' |
| <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2024-01-01' |
| <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2023-01-01' |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'snapshot_date' exposed in output (as 'snapshot_date') |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'snapshot_date' exposed in output |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 3 sources (c, l, o) |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 2 sources (c, o) |
| <file> | WINDOW_NON_UNIQUE_ORDER | warning | no | ROW_NUMBER() ORDER BY (customer_id) may not be unique within partition |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'combo_at_risk_customers.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be e... |
