# Findings — `customer_subqueries.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 13 (error=1, warning=2, info=10)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | SELECT_STAR | warning | no | SELECT * detected - explicit column list recommended |
| <file> | MISSING_ALIAS | info | no | Table 'customer_subqueries' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | MISSING_GROUP_BY | error | no | Aggregate function mixed with non-aggregated columns without GROUP BY |
| <file> | COMBINED_SOURCE_FILTERS | info | no | WHERE clause references columns from 2 sources (c, o) |
| <file> | PK_DEDUP_CHECK_MISSING | info | no | @pk annotations present but no ROW_NUMBER PARTITION BY <pk> for dedup verification |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'customer_subqueries.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
| <predicate> | SUBQUERY_NOT_LIFTED | info | no | Subquery inside In left inline — lifting an IN/EXISTS/comparison subquery would require synthesising a join/DISTINCT ... |
| <correlated> | SUBQUERY_NOT_LIFTED | info | no | Correlated subquery left inline — references an outer scope that a CTE cannot see. |
