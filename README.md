# MDDE Lite - Educational Edition

> *"Is MDDE just a concept — or is it something we can actually try?"*
>
> **It's real. And this is a minimal demo you can run yourself.**

A minimal implementation of **Metadata-Driven Data Engineering** concepts. This repository demonstrates core MDDE functionality with working code.

Read the full story: [Is MDDE Just a Concept?](articles/is-mdde-just-a-concept.md)

---

## What is MDDE?

MDDE (Metadata-Driven Data Engineering) is an approach where you:
1. **Parse** SQL files to extract metadata (entities, attributes, lineage)
2. **Store** metadata in a structured schema
3. **Analyze** SQL for quality issues and optimization opportunities
4. **Generate** improved SQL, documentation, and diagrams

## Quick Start

```bash
# Clone and install
git clone https://github.com/jacovanderlaan/mdde-demo.git
cd mdde-demo
pip install -r requirements.txt

# Run the SQL parser
python -m src.mdde_lite.parser examples/sales

# Run the SQL optimizer
python -m src.mdde_lite.optimizer examples/sales

# Run the SQL regenerator
python -m src.mdde_lite.generator
```

## Repository Structure

```
mdde-demo/
├── src/mdde_lite/           # MDDE Lite Python package
│   ├── schema.py            # Minimal metadata schema (5 tables)
│   ├── parser.py            # SQL parser using sqlglot
│   ├── optimizer.py         # SQL quality checks (15 anti-patterns)
│   ├── generator.py         # SQL regenerator
│   ├── diagrams.py          # Mermaid diagram generation
│   └── lineage.py           # Column-level lineage extraction
├── examples/
│   └── sales/               # Sample SQL files
│       ├── customers.sql
│       ├── orders.sql
│       ├── order_summary.sql
│       ├── products_bad.sql  # Intentionally bad SQL for optimizer demo
│       └── analytics_bad.sql # More anti-patterns for testing
├── workspace/
│   └── sales/               # Generated output
│       ├── model.conceptual.yaml
│       └── diagrams/
├── models/                   # YAML model examples
│   ├── ecommerce/           # E-commerce pattern
│   └── regulatory/          # Public regulatory frameworks (AnaCredit, RRE)
├── docs/
│   └── adr/                 # Architecture Decision Records
└── schemas/                 # JSON schemas for validation
```

## MDDE Lite Components

### 1. Minimal Metadata Schema

Five core tables that capture essential metadata:

| Table | Purpose |
|-------|---------|
| `entity` | Tables, views, CTEs |
| `attribute` | Columns within entities |
| `relationship` | Links between entities |
| `attribute_mapping` | Column-level lineage |
| `optimizer_diagnostics` | SQL quality findings |

```python
from src.mdde_lite.schema import create_schema

conn = create_schema("my_metadata.duckdb")
```

### 2. SQL Parser

Parses SQL files using [sqlglot](https://github.com/tobymao/sqlglot) and stores metadata:

```python
from src.mdde_lite.parser import parse_directory

results = parse_directory("examples/sales")
# Extracts: entities, attributes, CTEs, source tables, lineage
```

### 3. SQL Optimizer (15 Checks)

Detects common SQL anti-patterns:

| Check | Severity | Description |
|-------|----------|-------------|
| `SELECT_STAR` | warning | SELECT * usage |
| `MISSING_ALIAS` | info | Tables without aliases in JOINs |
| `ORDER_BY_NUMBER` | warning | ORDER BY 1 instead of column name |
| `IMPLICIT_JOIN` | warning | Comma-separated FROM |
| `WHERE_1_EQUALS_1` | info | WHERE 1=1 pattern |
| `DISTINCT_STAR` | warning | SELECT DISTINCT * pattern |
| `CARTESIAN_JOIN` | warning | JOIN without ON clause |
| `DUPLICATE_COLUMN` | warning | Same column selected twice |
| `NESTED_SUBQUERY` | info | Deep subquery nesting (3+) |
| `UNION_COLUMN_MISMATCH` | error | Mismatched columns in UNION |
| `LEADING_WILDCARD` | info | LIKE '%...' (non-SARGable) |
| `FUNCTION_IN_WHERE` | info | Function on column in WHERE |
| `OR_IN_JOIN` | warning | OR condition in JOIN ON |
| `HARDCODED_DATE` | info | Hardcoded date literals |
| `MISSING_GROUP_BY` | error | Aggregate without GROUP BY |

```python
from src.mdde_lite.optimizer import analyze_directory, get_all_check_types

results = analyze_directory("examples/sales")
print(f"Checks available: {len(get_all_check_types())}")  # 15
print(results["by_type"])  # {"SELECT_STAR": 1, "CARTESIAN_JOIN": 1, ...}
```

### 4. Mermaid Diagram Generation

Generate diagrams from metadata:

```python
from src.mdde_lite.diagrams import generate_erd, generate_dataflow, generate_lineage

# Generate ERD from entities
erd = generate_erd(conn)

# Generate data flow diagram
flow = generate_dataflow(conn)

# Generate lineage for specific entity
lineage_diagram = generate_lineage(conn, "ent_order_summary")
```

### 5. Column-Level Lineage

Extract lineage from SQL SELECT statements:

```python
from src.mdde_lite.lineage import extract_lineage

sql = """
SELECT
    c.customer_id,
    c.name AS customer_name,
    SUM(o.amount) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
"""

for lin in extract_lineage(sql):
    print(lin)
# customers.customer_id -> customer_id (direct)
# customers.name -> customer_name (rename)
# orders.amount -> total_spent (aggregation)
```

### 6. SQL Regenerator

Formats and transpiles SQL:

```python
from src.mdde_lite.generator import format_sql, transpile_sql

# Format consistently
formatted = format_sql("SELECT * FROM orders WHERE status='active'")

# Transpile between dialects
snowflake_sql = transpile_sql(sql, "duckdb", "snowflake")
bigquery_sql = transpile_sql(sql, "duckdb", "bigquery")
```

## Example: Full Pipeline

```python
from src.mdde_lite.schema import create_schema
from src.mdde_lite.parser import parse_directory
from src.mdde_lite.optimizer import analyze_directory

# 1. Create metadata database
conn = create_schema("sales_metadata.duckdb")

# 2. Parse SQL files
parse_results = parse_directory("examples/sales", "sales_metadata.duckdb")
print(f"Parsed {len(parse_results)} files")

# 3. Analyze for issues
optimize_results = analyze_directory("examples/sales", "sales_metadata.duckdb")
print(f"Found {optimize_results['total_diagnostics']} issues")

# 4. Query the metadata
entities = conn.execute("SELECT * FROM entity").fetchall()
diagnostics = conn.execute("SELECT * FROM optimizer_diagnostics").fetchall()
```

## Architecture Decision Records (ADRs)

We document architectural decisions using ADRs. See [docs/adr/](docs/adr/) for:

| ADR | Decision |
|-----|----------|
| [ADR-001](docs/adr/ADR-001-yaml-over-json.md) | YAML over JSON for model files |
| [ADR-002](docs/adr/ADR-002-three-layer-modeling.md) | Three-layer modeling approach |
| [ADR-003](docs/adr/ADR-003-stereotype-driven-generation.md) | Stereotype-driven code generation |

ADRs help teams remember *why* decisions were made, not just *what* was decided.

## Sample YAML Models

### E-commerce Model

```yaml
# models/ecommerce/entities/customer/entity.logical.yaml
entity:
  entity_id: customer
  name: Customer
  stereotype: dim_scd2
  layer: business

attributes:
  - attribute_id: customer_id
    data_type: integer
    is_primary_key: true
  - attribute_id: email
    data_type: varchar(255)
    pii_classification: email
```

### Regulatory Models

Public regulatory frameworks modeled in MDDE format:

- **AnaCredit** - ECB credit data regulation (counterparty, instrument, protection)
- **RRE** - Residential real estate mortgages (LTV, household income, property)

See [models/regulatory/](models/regulatory/) for complete models.

## What This Demo Shows

| Concept | Demo | Related Articles |
|---------|------|------------------|
| SQL parsing | `parser.py` extracts metadata | "Extracting Hidden Metadata Inside SQL" |
| Metadata storage | 5-table schema in DuckDB | "From ERDs and Lineage to Executable Metadata" |
| Quality checks | `optimizer.py` (15 checks) | "Implementing 25 Essential DQ Checks" |
| SQL generation | `generator.py` formats/transpiles | "From YAML to SQL" |
| Diagram generation | `diagrams.py` Mermaid output | "Diagram Generation & Auto-Docs" |
| Column lineage | `lineage.py` traces columns | "Beyond the SELECT Clause" |
| YAML models | Entity definitions | "From ERDs to Executable Metadata" |
| ADRs | Decision documentation | N/A |

## What Remains Private

The full MDDE framework includes features not in this demo:

- Full optimizer pipeline with CTE normalization
- Advanced UNION handling and dialect rendering
- Complete 60+ table metadata schema
- VS Code extension with visualization
- GenAI-powered modeling assistance
- BEAM integration and enterprise imports
- Migration tooling

## Want the Full Framework?

This educational edition demonstrates the concepts. For the full MDDE framework:

- **Workshops** - Hands-on training sessions
- **Consulting** - Implementation in your organization
- **Enterprise License** - Full framework access

Contact: [jacovanderlaan on LinkedIn](https://linkedin.com/in/jacovanderlaan)

## License

MIT License - See [LICENSE](LICENSE) for details.

---

**MDDE Lite** - Proving it's real, one SQL file at a time.
