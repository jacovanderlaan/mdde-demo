# Findings — `combo_comma_union_with_subqueries.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 11 (error=2, warning=4, info=5)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'orders' has no alias in multi-table query |
| <file> | MISSING_GROUP_BY | error | no | Aggregate function mixed with non-aggregated columns without GROUP BY |
| <file> | MISSING_GROUP_BY | error | no | Aggregate function mixed with non-aggregated columns without GROUP BY |
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'email' inside WHERE |
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'email' inside WHERE |
| <file> | INLINE_UNION_TRANSFORM | warning | no | Transformation inside UNION ALL branch: (SELECT MAX(amount) FROM raw.orders) AS max_order_globally |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | INLINE_UNION_TRANSFORM | warning | no | Transformation inside UNION ALL branch: (SELECT MAX(amount) FROM raw.orders) AS max_order_globally |
| <file> | INLINE_UNION_TRANSFORM | warning | no | WHERE clause inside UNION ALL branch |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'combo_comma_union_with_subqueries.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version... |
