# Findings — `customer_revenue_bad.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 11 (error=1, warning=4, info=6)  
**Auto-fixed:** 1

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_ALIAS | info | no | Table 'customer_revenue' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'joined' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | ORDER_BY_NUMBER | warning | no | ORDER BY uses column number (1) instead of name |
| <file> | WHERE_1_EQUALS_1 | info | yes | WHERE 1=1 pattern detected |
| <file> | CARTESIAN_JOIN | warning | no | JOIN to 'stg_customers' without ON clause - potential cartesian product |
| <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2026-01-01' |
| <file> | PK_DEDUP_CHECK_MISSING | info | no | @pk annotations present but no ROW_NUMBER PARTITION BY <pk> for dedup verification |
| <file> | WINDOW_NO_ORDER | error | no | ROW_NUMBER() without ORDER BY - results are non-deterministic |
