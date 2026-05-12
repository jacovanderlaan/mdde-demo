# Findings — `combo_nested_predicate_subqueries.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 8 (error=0, warning=0, info=8)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'loans' has no alias in multi-table query |
| <file> | DERIVATION_IN_WHERE | info | no | Function call (AVG) on column inside WHERE |
| <file> | DERIVATION_IN_WHERE | info | no | Function call (AVG) on column inside WHERE |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 4 sources (c, l, l2, o) |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 2 sources (c, l2) |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 2 sources (l, o) |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'combo_nested_predicate_subqueries.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version... |
| <predicate> | SUBQUERY_NOT_LIFTED | info | no | Subquery inside GT left inline — non-IN/EXISTS predicate subquery; lifting would require synthesising a join/DISTINCT... |
