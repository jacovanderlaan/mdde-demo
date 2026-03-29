# MDDE Demo - Feature to Article Mapping

Maps demo features to supporting Medium articles, identifying opportunities for new demo code.

---

## Current Coverage

### Implemented Features

| Demo Feature | Supporting Articles | Status |
|--------------|---------------------|--------|
| SQL Parsing | "Extracting the Hidden Metadata Inside SQL", "From Legacy SQL to Business-Friendly Mapping" | Done |
| Quality Checks (5) | "Implementing 25 Essential Data Quality Checks" (partial) | Done |
| SQL Formatting | "From YAML to SQL: Generating Physical Models" | Done |
| Dialect Transpilation | "Metadata-Driven SQL Optimization and Migration" | Done |
| YAML Models | "From ERDs and Lineage to Executable Metadata" | Done |
| Sample ADRs | N/A (framework documentation) | Done |

---

## Gap Analysis

### Priority 1: High Value, Fits Demo Scope

| Feature | Articles | Effort | Recommended |
|---------|----------|--------|-------------|
| Column-Level Lineage | "Beyond the SELECT Clause", "Dynamic Metadata Lineage", "Context-Aware Lineage" | Medium | Yes |
| Expand Quality Checks | "Implementing 25 Essential DQ Checks", "Metadata-Driven Data Quality" | Low | Yes |
| Mermaid Diagrams | "Diagram Generation & Auto-Docs", "Building the Metadata Star Generator" | Low | Yes |
| dbt Generation | "Automating Dimensional Models with dbt", "Business-Friendly Mapping Meets dbt" | Medium | Yes |

### Priority 2: Medium Value, Moderate Effort

| Feature | Articles | Effort | Recommended |
|---------|----------|--------|-------------|
| Markdown Docs | "From Metadata to Documentation", "From Metadata to Living Documentation" | Low | Yes |
| SCD2 Detection | "Data Historization", "Time Travel vs Bi-Temporal SCD2" | Medium | Yes |
| Business Glossary | "Integrating the Business Glossary", "The Semantic Layer of Metadata" | Low | Yes |

### Priority 3: Limited Scope for Demo

| Feature | Articles | Effort | Recommended |
|---------|----------|--------|-------------|
| Basic CTE Normalization | "Modular SQL with CTEs" | Medium | Maybe |
| Data Vault Examples | "Inmon, Data Vault, and Dimensional" | Low | Maybe |
| Dimensional Examples | "Generating Dimensional Models Automatically" | Low | Maybe |

### Out of Scope (Full MDDE Only)

| Feature | Articles | Reason for Exclusion |
|---------|----------|---------------------|
| Full CTE Normalization | "From Monolith to Modules" | Complex, enterprise feature |
| Dual SCD2 | "Bi-Temporal SCD2 with Redelivery" | Advanced, requires full framework |
| PIT Tables | "Goodbye PIT Tables? ASOF Joins" | Complex, requires full analyzer |
| Cost Optimization | "Cost-Based Optimization" | Enterprise feature |
| PowerBI Integration | "PowerBI Integration" | Enterprise feature |
| VS Code Extension | "Building the Integrated VS Code Environment" | Requires full framework |

---

## Article Categories and Demo Opportunities

### Getting Started (4 articles)
- Covered by existing demo ✓

### Core Concepts (4 articles)
- Covered by existing schema and models ✓

### Column-Level Lineage (5 articles)
- **Gap**: No lineage extraction code
- **Opportunity**: Add `lineage.py` for basic lineage

### SQL Parsing & Transformation (5 articles)
- Partially covered by parser ✓
- **Gap**: No CTE normalization
- **Opportunity**: Add basic CTE extraction

### Temporal Modeling (6 articles)
- **Gap**: No SCD2 detection
- **Opportunity**: Add `temporal.py` for pattern detection

### dbt & SQL Generation (5 articles)
- **Gap**: No dbt generation
- **Opportunity**: Add `dbt_generator.py`

### Diagram Generation (5 articles)
- **Gap**: Only static ERD markdown
- **Opportunity**: Add Mermaid generation

### Data Quality (5 articles)
- Partially covered (5 of 25 checks)
- **Opportunity**: Expand to 15 checks

### Orchestration & Pipelines (6 articles)
- Out of scope for demo (requires DAG infrastructure)

### VS Code & Tooling (5 articles)
- Out of scope for demo (requires extension)

### Documentation & Publishing (6 articles)
- **Gap**: No documentation generation
- **Opportunity**: Add `documenter.py`

### Regulatory & Financial Services (7 articles)
- Covered by AnaCredit/RRE models ✓

### GenAI & AI-Assisted Modeling (7 articles)
- Out of scope for demo (requires GenAI module)

### Enterprise Architecture (6 articles)
- Partially covered by ADRs ✓

### Testing & Migration (3 articles)
- Out of scope for demo (requires full migrator)

### SQL Best Practices (5 articles)
- Partially covered by optimizer ✓
- **Opportunity**: Add UNION checks

### Advanced Topics (6 articles)
- Out of scope for demo (visionary content)

### Industry Patterns (5 articles)
- **Gap**: No DV/Dimensional examples
- **Opportunity**: Add pattern YAML models

---

## Implementation Recommendations

### Quick Wins (Low Effort, High Impact)

1. **Expand Quality Checks** (optimizer.py)
   - Add 10 more checks
   - References: "Implementing 25 Essential DQ Checks"
   - Effort: 2-3 hours

2. **Mermaid ERD Generation** (diagrams.py)
   - Generate ERD from entity metadata
   - References: "Diagram Generation & Auto-Docs"
   - Effort: 2-3 hours

3. **Basic Column Lineage** (lineage.py)
   - Extract lineage from SELECT
   - References: "Beyond the SELECT Clause"
   - Effort: 4-6 hours

### Medium Effort, High Value

4. **dbt Model Generation** (dbt_generator.py)
   - Generate schema.yml and SQL models
   - References: "Automating Dimensional Models with dbt"
   - Effort: 6-8 hours

5. **Markdown Documentation** (documenter.py)
   - Generate entity documentation
   - References: "From Metadata to Documentation"
   - Effort: 3-4 hours

### Nice to Have

6. **SCD2 Pattern Detection** (temporal.py)
   - Detect temporal columns
   - References: "Data Historization"
   - Effort: 4-6 hours

7. **Business Glossary** (glossary.py)
   - Link terms to attributes
   - References: "Integrating the Business Glossary"
   - Effort: 3-4 hours

---

## Article Reference Quick Lookup

| When adding... | Reference these articles |
|----------------|-------------------------|
| Lineage code | "Beyond the SELECT Clause", "Dynamic Metadata Lineage" |
| Quality checks | "Implementing 25 Essential DQ Checks" |
| Diagrams | "Diagram Generation & Auto-Docs", "Building the Metadata Star Generator" |
| dbt code | "Automating Dimensional Models with dbt", "Business-Friendly Mapping Meets dbt" |
| Temporal code | "Data Historization", "Time Travel vs Bi-Temporal SCD2" |
| Documentation | "From Metadata to Documentation" |
| Glossary | "Integrating the Business Glossary" |

---

*Last updated: 2026-02-18*
