# CTE Regression Analysis - DuckDB

Lightweight local testing for CTE regression analysis using DuckDB.

## Requirements

```bash
pip install duckdb pandas
```

## Files

| File | Purpose |
|------|---------|
| `demo.ipynb` | Interactive demo notebook for VS Code |

## Quick Start

1. Open `demo.ipynb` in VS Code
2. Run all cells

The demo:
- Creates sample e-commerce tables (customers, orders, products)
- Compares two query versions with intentional differences
- Shows column-level analysis of which columns cause regressions

## Why DuckDB?

- No Java installation required
- Fast startup (no Spark JVM overhead)
- Same SQL syntax as Databricks/Spark
- Returns Pandas DataFrames directly
- Perfect for local development and testing

## See Also

- [Databricks version](../databricks/cte_regression/) - For production use in Databricks
