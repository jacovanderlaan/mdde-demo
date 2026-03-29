# MDDE Demo - Roadmap

Planned features and demo code additions for the public mdde-demo repository.

---

## Current State (v0.1.0)

| Component | Status | Description |
|-----------|--------|-------------|
| Metadata Schema | Done | 5 tables (entity, attribute, relationship, attribute_mapping, optimizer_diagnostics) |
| SQL Parser | Done | Extract entities, attributes, CTEs, relationships |
| Quality Checks | Done | 5 checks (SELECT_STAR, MISSING_ALIAS, ORDER_BY_NUMBER, IMPLICIT_JOIN, WHERE_1_EQUALS_1) |
| SQL Generator | Done | Format and transpile SQL |
| YAML Models | Done | E-commerce, AnaCredit, RRE samples |
| ADRs | Done | 3 sample ADRs |
| Articles | Done | 165+ article index |

---

## Roadmap

### Phase 1: Quick Wins (v0.2.0)

**Target**: Maximize educational value with low effort

| Feature | Effort | Impact | Articles Supported |
|---------|--------|--------|-------------------|
| Expand Quality Checks to 15 | Low | High | "Implementing 25 Essential DQ Checks" |
| Mermaid ERD Generation | Low | High | "Diagram Generation & Auto-Docs" |
| Basic Column Lineage | Medium | High | "Beyond the SELECT Clause" |

**New Files:**
```
src/mdde_lite/diagrams.py      # Mermaid generation
src/mdde_lite/lineage.py       # Column lineage extraction
```

**Enhanced Files:**
```
src/mdde_lite/optimizer.py     # Add 10 more checks
```

---

### Phase 2: Core Features (v0.3.0)

**Target**: Demonstrate key MDDE workflows

| Feature | Effort | Impact | Articles Supported |
|---------|--------|--------|-------------------|
| dbt Model Generation | Medium | High | "Automating Dimensional Models with dbt" |
| Markdown Documentation | Low | Medium | "From Metadata to Documentation" |
| SCD2 Pattern Detection | Medium | Medium | "Data Historization" |

**New Files:**
```
src/mdde_lite/dbt_generator.py    # dbt schema.yml + models
src/mdde_lite/documenter.py       # Markdown docs
src/mdde_lite/temporal.py         # SCD2 detection
```

**New Examples:**
```
examples/temporal/                 # SCD2 SQL examples
generated/dbt/                     # Sample dbt output
generated/docs/                    # Sample documentation
```

---

### Phase 3: Advanced Concepts (v0.4.0)

**Target**: Showcase advanced patterns

| Feature | Effort | Impact | Articles Supported |
|---------|--------|--------|-------------------|
| Business Glossary | Low | Medium | "Integrating the Business Glossary" |
| Basic CTE Normalization | Medium | Medium | "Modular SQL with CTEs" |
| Data Vault Examples | Low | Low | "Inmon, Data Vault, and Dimensional" |

**New Files:**
```
src/mdde_lite/glossary.py         # Glossary management
src/mdde_lite/cte_normalizer.py   # CTE extraction
models/datavault/                  # DV example models
```

---

## Feature Details

### 1. Expand Quality Checks (v0.2.0)

Add 10 more anti-pattern detections:

| Check | Severity | Description |
|-------|----------|-------------|
| CARTESIAN_JOIN | warning | JOIN without ON clause |
| DUPLICATE_COLUMN | warning | Same column selected twice |
| FUNCTION_ON_JOIN | warning | Function applied to join column |
| HARDCODED_DATE | info | Hardcoded date literals |
| MISSING_GROUP_BY | error | Aggregate without GROUP BY |
| NESTED_SUBQUERY | info | Deep subquery nesting (3+) |
| OR_IN_JOIN | warning | OR condition in JOIN |
| SELECT_DISTINCT_STAR | warning | SELECT DISTINCT * pattern |
| UNION_ALL_MISMATCH | error | Mismatched columns in UNION |
| WILDCARD_LIKE | info | Leading wildcard in LIKE |

---

### 2. Mermaid Diagram Generation (v0.2.0)

Generate diagrams from metadata:

```python
# ERD from entities
generate_erd(conn) -> str  # Mermaid erDiagram

# Data flow from relationships
generate_dataflow(conn) -> str  # Mermaid flowchart

# Lineage from attribute_mapping
generate_lineage(conn, entity_id) -> str  # Mermaid flowchart
```

---

### 3. Column Lineage (v0.2.0)

Extract column-level lineage from SQL:

```python
# Parse SELECT to trace columns
extract_lineage(sql) -> List[AttributeMapping]

# Handle:
# - Direct columns: SELECT a FROM t
# - Aliases: SELECT a AS b FROM t
# - Expressions: SELECT a + b AS c FROM t
# - Aggregations: SELECT SUM(a) AS total FROM t
```

---

### 4. dbt Generation (v0.3.0)

Generate dbt project files from metadata:

```python
# Generate schema.yml
generate_schema_yml(conn, entities) -> str

# Generate SQL model
generate_dbt_model(conn, entity_id) -> str

# Generate sources.yml
generate_sources_yml(conn) -> str
```

---

### 5. SCD2 Detection (v0.3.0)

Detect SCD2 patterns in models:

```python
# Detect temporal columns
detect_scd2_pattern(entity) -> SCD2Info

# Columns to detect:
# - valid_from, effective_date, start_date
# - valid_to, expiry_date, end_date
# - is_current, is_active, current_flag
```

---

## Release Plan

| Version | Target | Focus |
|---------|--------|-------|
| v0.1.0 | Done | Initial release - core features |
| v0.2.0 | Q1 2026 | Quality checks, diagrams, lineage |
| v0.3.0 | Q2 2026 | dbt, docs, temporal |
| v0.4.0 | Q3 2026 | Glossary, CTE, patterns |

---

## Out of Scope

These features will **not** be added to mdde-demo (see [INCLUSION_RULES.md](INCLUSION_RULES.md)):

- Full CTE normalization with advanced rewrites
- Dual SCD2 / bi-temporal modeling
- PIT table generation
- Cost-based optimization
- VS Code extension features
- GenAI integration
- Enterprise importers
- PowerBI integration
- MCP server

---

## Contributing

When adding new features:

1. Check [INCLUSION_RULES.md](INCLUSION_RULES.md) first
2. Identify supporting article(s)
3. Keep code simple and educational
4. Add examples that demonstrate the feature
5. Update this roadmap
6. Reference article in docstrings

---

*Last updated: 2026-02-18*
