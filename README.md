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
│   ├── optimizer.py         # SQL quality checks (20 checks)
│   ├── generator.py         # SQL regenerator
│   ├── diagrams.py          # Mermaid diagram generation
│   ├── lineage.py           # Column-level lineage extraction
│   ├── determinism.py       # Non-deterministic SQL detection
│   ├── dbt_generator.py     # Generate dbt models from metadata
│   ├── temporal.py          # SCD2 pattern detection and generation
│   ├── documenter.py        # Markdown documentation generation
│   ├── cte_normalizer.py    # CTE extraction and SQL modularization
│   ├── glossary.py          # Business glossary management
│   ├── datavault.py         # Data Vault pattern detection
│   └── dimensional.py       # Dimensional model generation
├── examples/
│   └── sales/               # Sample SQL files
│       ├── customers.sql
│       ├── orders.sql
│       ├── order_summary.sql
│       ├── products_bad.sql  # Intentionally bad SQL for optimizer demo
│       ├── analytics_bad.sql # More anti-patterns for testing
│       ├── dedup_bad.sql     # Non-deterministic patterns (ROW_NUMBER, etc.)
│       └── dedup_good.sql    # Deterministic versions with tie-breakers
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

### 3. SQL Optimizer (20 Checks)

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

**Determinism Checks** (critical for regression testing):

| Check | Severity | Description |
|-------|----------|-------------|
| `WINDOW_NO_ORDER` | error | ROW_NUMBER/RANK without ORDER BY |
| `WINDOW_NON_UNIQUE_ORDER` | warning | ORDER BY may not be unique |
| `FIRST_LAST_NO_ORDER` | error | FIRST_VALUE/LAST_VALUE without ORDER BY |
| `LAG_LEAD_NO_ORDER` | error | LAG/LEAD without ORDER BY |
| `LIMIT_NO_ORDER` | error | LIMIT/TOP without ORDER BY |

```python
from src.mdde_lite.optimizer import analyze_directory, get_all_check_types

results = analyze_directory("examples/sales")
print(f"Checks available: {len(get_all_check_types())}")  # 20
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

### 6. Determinism Checker (Critical for Migrations)

Detects non-deterministic SQL patterns that cause issues during regression testing:

```python
from src.mdde_lite.determinism import check_determinism, suggest_tie_breakers

sql = """
SELECT
    ROW_NUMBER() OVER (PARTITION BY region) AS rn,
    customer_name
FROM customers
"""

issues = check_determinism(sql)
for issue in issues:
    print(f"[{issue.severity}] {issue.issue_type}")
    print(f"  {issue.message}")
    print(f"  Suggestion: {issue.suggestion}")

# Output:
# [error] WINDOW_NO_ORDER
#   ROW_NUMBER() without ORDER BY - results are non-deterministic
#   Suggestion: Add ORDER BY clause with unique columns to ROW_NUMBER()
```

**Why This Matters:**

During SQL migrations (Oracle → Snowflake, etc.), you need to compare results between systems. Non-deterministic SQL makes this impossible:
- `ROW_NUMBER()` without unique ORDER BY assigns different numbers each run
- `FIRST_VALUE()` without ORDER BY picks arbitrary "first" rows
- `LIMIT` without ORDER BY returns random subsets

**Solution:** Add "tie-breaker" columns to ensure unique ordering:

```sql
-- Non-deterministic
ROW_NUMBER() OVER (PARTITION BY region ORDER BY created_at)

-- Deterministic (customer_id breaks ties)
ROW_NUMBER() OVER (PARTITION BY region ORDER BY created_at, customer_id)
```

See [ADR-004](docs/adr/ADR-004-deterministic-sql-patterns.md) and the [article on deterministic SQL](articles/the-hidden-danger-of-non-deterministic-sql.md) for details.

### 7. dbt Model Generator

Generate complete dbt projects from MDDE metadata:

```python
from src.mdde_lite.dbt_generator import generate_dbt_project

# Generate dbt project structure
stats = generate_dbt_project(conn, "generated/dbt", "my_project")

# Creates:
# - dbt_project.yml
# - models/sources.yml
# - models/staging/*.sql + schema.yml
# - models/business/*.sql + schema.yml
```

Generated models include `{{ ref() }}` and `{{ source() }}` macros, proper materializations by layer, and schema.yml with column documentation.

### 8. SCD2 Pattern Detection

Detect and generate Slowly Changing Dimension patterns:

```python
from src.mdde_lite.temporal import detect_scd_pattern, generate_scd2_merge

# Detect SCD pattern from column names
columns = ["customer_sk", "customer_id", "name", "valid_from", "valid_to", "is_current"]
pattern = detect_scd_pattern("dim_customer", columns)

print(f"SCD Type: {pattern.scd_type.value}")  # type_2
print(f"Bi-temporal: {pattern.is_bi_temporal}")
print(f"Recommendations: {pattern.recommendations}")

# Generate MERGE statement for SCD2 loads
merge_sql = generate_scd2_merge(
    target_table="dim_customer",
    source_table="stg_customers",
    natural_key_columns=["customer_id"],
    tracking_columns=["name", "email", "region"]
)
```

### 9. Documentation Generator

Generate markdown documentation from metadata:

```python
from src.mdde_lite.documenter import generate_entity_docs, generate_lineage_doc

# Generate complete documentation
stats = generate_entity_docs(conn, "generated/docs")

# Creates:
# - index.md (overview by layer)
# - data_dictionary.md (all attributes)
# - {entity}.md (per-entity documentation with lineage)

# Generate lineage-specific documentation
lineage_md = generate_lineage_doc(conn, "dim_customer")
```

### 10. CTE Normalizer

Transform monolithic SQL into modular CTE-based queries:

```python
from src.mdde_lite.cte_normalizer import normalize_to_ctes, suggest_cte_structure

# Get suggestions for restructuring
suggestions = suggest_cte_structure(complex_sql)
# ["Found 3 subquery(s): Consider extracting to named CTEs",
#  "Table 'orders' referenced 3 times: Consider CTE for shared logic"]

# Normalize to CTEs
result = normalize_to_ctes(sql_with_subqueries)
print(f"Extracted {result.ctes_extracted} CTEs")
```

### 11. Business Glossary

Manage business terms and link them to technical metadata:

```python
from src.mdde_lite.glossary import BusinessGlossary, GlossaryTerm

glossary = BusinessGlossary()

glossary.add_term(GlossaryTerm(
    term_id="revenue",
    name="Revenue",
    definition="Total income generated from sales",
    category=TermCategory.METRIC,
    synonyms=["Sales", "Income"]
))

# Search and generate documentation
results = glossary.search_terms("sales")
markdown = glossary.generate_glossary_markdown()
```

### 12. Data Vault Pattern Detection

Detect and validate Data Vault patterns (Hub, Link, Satellite):

```python
from src.mdde_lite.datavault import detect_dv_construct, validate_dv_model

# Detect construct type from table structure
hub = detect_dv_construct("hub_customer", columns)
print(f"Type: {hub.construct_type.value}")  # "hub"
print(f"Business Keys: {hub.business_keys}")

# Validate complete model
report = validate_dv_model([hub, link, satellite])
```

### 13. Dimensional Model Generator

Generate and detect dimensional model patterns:

```python
from src.mdde_lite.dimensional import detect_dimensional_construct, generate_star_schema

# Detect fact/dimension from table structure
fact = detect_dimensional_construct("fact_sales", columns)
print(f"Measures: {[m.name for m in fact.measures]}")
print(f"Grain: {fact.grain}")

# Generate star schema from source entities
schema = generate_star_schema(source_entities)
print(f"Dimensions: {[d['name'] for d in schema['dimensions']]}")
```

### 14. SQL Regenerator

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
| [ADR-004](docs/adr/ADR-004-deterministic-sql-patterns.md) | Deterministic SQL patterns for migrations |

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
| Quality checks | `optimizer.py` (20 checks) | "Implementing 25 Essential DQ Checks" |
| Determinism | `determinism.py` detects non-determinism | "The Hidden Danger of Non-Deterministic SQL" |
| SQL generation | `generator.py` formats/transpiles | "From YAML to SQL" |
| Diagram generation | `diagrams.py` Mermaid output | "Diagram Generation & Auto-Docs" |
| Column lineage | `lineage.py` traces columns | "Beyond the SELECT Clause" |
| dbt generation | `dbt_generator.py` creates dbt projects | "Automating Dimensional Models with Metadata & dbt" |
| SCD2 patterns | `temporal.py` detects/generates SCD2 | "Data Historization - Making Time a First-Class Citizen" |
| Documentation | `documenter.py` generates markdown | "From Metadata to Living Documentation" |
| CTE normalization | `cte_normalizer.py` modularizes SQL | "Modular SQL with CTEs: A Best Practice" |
| Business glossary | `glossary.py` term management | "Integrating the Business Glossary" |
| Data Vault | `datavault.py` Hub/Link/Sat detection | "From DataVault Tooling to Bi-Temporal SCD2" |
| Dimensional | `dimensional.py` star schema generation | "Generating Dimensional Models Automatically" |
| YAML models | Entity definitions | "From ERDs to Executable Metadata" |
| ADRs | Decision documentation | N/A |

## What Remains Private

The full MDDE framework includes features not in this demo:

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
