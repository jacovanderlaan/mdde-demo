# MDDE Demo - Technical Architecture

Technical design of the MDDE Lite educational framework.

---

## Overview

MDDE Lite is a minimal implementation of Metadata-Driven Data Engineering concepts, designed for education and demonstration.

```
┌─────────────────────────────────────────────────────────────┐
│                     MDDE Lite Architecture                   │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │   SQL    │───►│  Parser  │───►│ Metadata │              │
│  │  Files   │    │          │    │   DB     │              │
│  └──────────┘    └──────────┘    └────┬─────┘              │
│                                       │                     │
│       ┌───────────────────────────────┼───────────────┐    │
│       │                               │               │    │
│       ▼                               ▼               ▼    │
│  ┌──────────┐                   ┌──────────┐    ┌────────┐ │
│  │ Optimizer│                   │ Generator│    │Diagrams│ │
│  │ (Checks) │                   │  (SQL)   │    │(Mermaid│ │
│  └──────────┘                   └──────────┘    └────────┘ │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Metadata Schema (`schema.py`)

5 essential tables that capture SQL metadata:

```sql
-- Core entities (tables, views, CTEs)
entity (
    entity_id PRIMARY KEY,
    name, description, entity_type,
    layer, stereotype, source_file
)

-- Columns within entities
attribute (
    attribute_id PRIMARY KEY,
    entity_id REFERENCES entity,
    name, data_type, ordinal_position,
    is_nullable, is_primary_key,
    is_derived, expression
)

-- Foreign key relationships
relationship (
    relationship_id PRIMARY KEY,
    source_entity_id REFERENCES entity,
    target_entity_id REFERENCES entity,
    cardinality, name
)

-- Column-level lineage
attribute_mapping (
    mapping_id PRIMARY KEY,
    target_entity_id, target_attribute_id,
    source_entity_id, source_attribute_id,
    mapping_type, transformation
)

-- SQL quality findings
optimizer_diagnostics (
    diagnostic_id PRIMARY KEY,
    entity_id, source_file,
    diagnostic_type, severity,
    message, suggestion
)
```

### 2. SQL Parser (`parser.py`)

Parses SQL files using sqlglot and extracts metadata:

```python
parse_sql_file(sql_path, conn) -> dict
    # Extracts:
    # - Entity (table/view name from filename)
    # - Attributes (columns from SELECT)
    # - CTEs (named subqueries)
    # - Source tables (FROM clause)
    # - Relationships (entity dependencies)

parse_directory(sql_dir, db_path) -> list
    # Parses all .sql files in directory
```

### 3. SQL Optimizer (`optimizer.py`)

Detects common SQL anti-patterns:

| Check | Severity | Detection Method |
|-------|----------|------------------|
| SELECT_STAR | warning | Find `exp.Star` nodes |
| MISSING_ALIAS | info | Tables without alias in multi-table queries |
| ORDER_BY_NUMBER | warning | `exp.Literal` in ORDER BY |
| IMPLICIT_JOIN | warning | Comma in FROM clause |
| WHERE_1_EQUALS_1 | info | String match in WHERE |

```python
analyze_sql(sql_content) -> List[SQLDiagnostic]
analyze_file(sql_path, conn) -> List[SQLDiagnostic]
analyze_directory(sql_dir, db_path) -> Dict
```

### 4. SQL Generator (`generator.py`)

Regenerates and transpiles SQL:

```python
regenerate_sql(sql, dialect, expand_star, format_style) -> str
format_sql(sql, dialect) -> str
transpile_sql(sql, source_dialect, target_dialect) -> str
generate_from_metadata(conn, entity_id, dialect) -> str
```

---

## Data Flow

### Parse Flow

```
SQL File (.sql)
    │
    ▼
sqlglot.parse_one()
    │
    ├──► CTE nodes ──► entity (type='cte')
    │
    ├──► SELECT columns ──► attribute
    │
    ├──► FROM tables ──► entity (type='table')
    │
    └──► Dependencies ──► relationship
```

### Analyze Flow

```
SQL Content
    │
    ▼
sqlglot.parse_one()
    │
    ├──► check_select_star()
    │
    ├──► check_missing_alias()
    │
    ├──► check_order_by_number()
    │
    ├──► check_implicit_join()
    │
    └──► check_where_one_equals_one()
    │
    ▼
List[SQLDiagnostic] ──► optimizer_diagnostics table
```

### Generate Flow

```
Metadata Tables
    │
    ├──► entity ──► FROM clause
    │
    ├──► attribute ──► SELECT columns
    │
    └──► attribute_mapping ──► Expressions
    │
    ▼
sqlglot AST ──► dialect.sql() ──► Formatted SQL
```

---

## Directory Structure

```
mdde-demo/
├── src/mdde_lite/           # Core Python package
│   ├── __init__.py
│   ├── schema.py            # Metadata schema (5 tables)
│   ├── parser.py            # SQL parser
│   ├── optimizer.py         # Quality checks
│   └── generator.py         # SQL regenerator
│
├── examples/                 # Input SQL files
│   └── sales/               # Sales domain example
│       ├── customers.sql
│       ├── orders.sql
│       ├── order_summary.sql
│       └── products_bad.sql
│
├── models/                   # YAML model definitions
│   ├── ecommerce/           # E-commerce pattern
│   └── regulatory/          # Public regulatory models
│       └── ecb/
│           ├── anacredit/
│           └── rre/
│
├── workspace/                # Generated outputs
│   └── sales/
│       ├── model.conceptual.yaml
│       └── diagrams/
│
├── generated/                # Generated code
│   ├── dbt/                 # dbt models
│   └── ddl/                 # DDL scripts
│
├── docs/                     # Documentation
│   └── adr/                 # Architecture Decision Records
│
├── articles/                 # Medium article index
│
└── schemas/                  # JSON validation schemas
```

---

## Dependencies

Minimal dependency set for portability:

| Package | Version | Purpose |
|---------|---------|---------|
| sqlglot | >=20.0.0 | SQL parsing and transpilation |
| duckdb | >=0.9.0 | Metadata storage |
| pyyaml | >=6.0 | YAML model processing |

Optional:
| Package | Version | Purpose |
|---------|---------|---------|
| jupyter | >=1.0.0 | Interactive notebooks |

---

## Extension Points

### Adding New Quality Checks

```python
def check_new_pattern(parsed: exp.Expression) -> List[SQLDiagnostic]:
    """Detect [pattern description]."""
    diagnostics = []

    for node in parsed.find_all(exp.NodeType):
        if condition:
            diagnostics.append(SQLDiagnostic(
                diagnostic_type="NEW_CHECK",
                message="Description",
                severity="warning",
                suggestion="How to fix",
            ))

    return diagnostics

# Add to analyze_sql():
diagnostics.extend(check_new_pattern(parsed))
```

### Adding New Dialects

sqlglot supports: duckdb, snowflake, postgres, bigquery, databricks, mysql, spark, etc.

```python
# Transpile to any supported dialect
transpile_sql(sql, "duckdb", "snowflake")
transpile_sql(sql, "duckdb", "bigquery")
```

### Adding New Metadata Tables

```python
# In schema.py, add new table:
conn.execute("""
    CREATE TABLE IF NOT EXISTS new_table (
        id VARCHAR PRIMARY KEY,
        entity_id VARCHAR REFERENCES entity(entity_id),
        -- additional columns
    )
""")
```

---

## Design Principles

### 1. Simplicity First
- Each module should be < 300 lines
- No complex inheritance hierarchies
- Clear, linear data flow

### 2. Educational Focus
- Generous comments explaining concepts
- Reference articles in docstrings
- Working examples over abstractions

### 3. Minimal Dependencies
- Only 3 core dependencies
- No framework lock-in
- Portable across environments

### 4. Metadata as Truth
- All derived outputs come from metadata
- Metadata is queryable SQL
- No hidden state

---

## Comparison with Full MDDE

| Aspect | MDDE Lite | Full MDDE |
|--------|-----------|-----------|
| Tables | 5 | 60+ |
| Checks | 5-15 | 50+ |
| Dialects | Basic support | Full with DDL |
| Lineage | Basic | Conditional, context-aware |
| UI | None | VS Code extension |
| AI | None | GenAI module |
| Lines of code | ~1,000 | 100,000+ |

---

*Last updated: 2026-02-18*
