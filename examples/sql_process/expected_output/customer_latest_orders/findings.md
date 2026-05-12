# Findings — `customer_latest_orders.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 5 (error=0, warning=1, info=4)  
**Auto-fixed:** 0

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | MISSING_ALIAS | info | no | Table 'customer_latest_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'raw_orders' has no alias in multi-table query |
| <file> | MISSING_ALIAS | info | no | Table 'ordered_orders' has no alias in multi-table query |
| <file> | WINDOW_NON_UNIQUE_ORDER | warning | no | ROW_NUMBER() ORDER BY (order_date, order_id) may not be unique within partition |
| <file> | VOLATILE_FUNCTION | info | no | CURRENT_DATE() returns current time - varies between runs |
