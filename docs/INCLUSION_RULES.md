# MDDE Demo - Inclusion Rules

Clear guidelines for what belongs in the public mdde-demo repository vs. what stays private in the full MDDE framework.

---

## Golden Rule

> **Include concepts that teach. Exclude implementations that compete.**

The demo should make readers understand MDDE and want to learn more, not replace the need for the full framework.

---

## INCLUDE in mdde-demo

### Core Concepts (Educational)

| Category | Include | Example |
|----------|---------|---------|
| **Metadata Schema** | Simplified 5-table schema | entity, attribute, relationship, attribute_mapping, optimizer_diagnostics |
| **SQL Parsing** | Basic parsing with sqlglot | Extract entities, attributes, CTEs from SQL |
| **Quality Checks** | Up to 15 common anti-patterns | SELECT *, missing alias, ORDER BY number |
| **SQL Generation** | Basic formatting and transpilation | Pretty print, dialect conversion |
| **Diagram Generation** | Mermaid diagrams | ERD, data flow, simple lineage |
| **YAML Models** | Sample models | E-commerce, public regulatory (AnaCredit, RRE) |
| **Column Lineage** | Basic lineage extraction | Direct mappings, simple expressions |
| **dbt Generation** | Basic schema.yml and models | Source definitions, simple transformations |
| **Documentation** | Markdown generation | Entity docs, data dictionary |

### Supporting Materials

| Category | Include | Example |
|----------|---------|---------|
| **ADRs** | Sample decisions with explanations | YAML over JSON, three-layer modeling |
| **Articles** | Full article index with links | 165+ Medium articles |
| **Examples** | Working SQL and YAML files | Sales model, regulatory models |
| **README** | Clear setup and usage instructions | Quick start guide |

### Public Regulatory Models

These are based on **public specifications** and can be included:
- ECB AnaCredit (credit reporting)
- ECB RRE (residential real estate)
- BIRD (reporting dictionary concepts)
- FIBO concepts (financial ontology basics)

---

## EXCLUDE from mdde-demo

### Enterprise Features (Private)

| Category | Exclude | Reason |
|----------|---------|--------|
| **Full Metadata Schema** | 60+ tables | Core IP, complex dependencies |
| **Advanced Optimizer** | CTE normalization, UNION handling, filter pushdown | Competitive advantage |
| **Dialect Rendering** | Full multi-dialect DDL generation | Enterprise feature |
| **VS Code Extension** | Interactive modeling, webviews | Requires full framework |
| **GenAI Module** | AI-powered modeling assistance | Enterprise feature |
| **Enterprise Importers** | PowerDesigner, erwin, SSDT import | Client-specific |
| **PowerBI Integration** | Bidirectional Power BI sync | Enterprise feature |
| **MCP Server** | Claude/AI tool integration | Requires full MDDE |
| **Work Management** | WIP workflows, Jira/ADO integration | Enterprise feature |
| **Compliance Scoring** | Governance framework | Enterprise feature |
| **Advanced Temporal** | Dual SCD2, PIT generation, ASOF queries | Complex IP |
| **Migration Tooling** | DV to 3NF migration, proposal management | Enterprise feature |

### Internal Models

| Category | Exclude | Reason |
|----------|---------|--------|
| **MDDE Internal Model** | The metadata schema as MDDE model | Core IP (dogfooding) |
| **Full Stereotype Library** | All 50+ stereotypes | Enterprise feature |
| **Client Models** | Any client-specific work | Confidential |
| **Proprietary Domains** | Internal domain definitions | Core IP |

### Advanced Code

| Category | Exclude | Reason |
|----------|---------|--------|
| **Hash Generation** | Multi-dialect hash expressions | Competitive advantage |
| **Cost Estimation** | Query cost calculation | Enterprise feature |
| **Materialized View Advisor** | Performance recommendations | Enterprise feature |
| **PII Detection** | Automated sensitive data identification | Enterprise feature |
| **Naming Standards** | Auto-fix naming conventions | Enterprise feature |

---

## Decision Framework

When deciding whether to include something, ask:

### 1. Is it educational?
- YES: Demonstrates a concept readers can learn from
- NO: Implementation detail that doesn't teach

### 2. Is it complete without MDDE?
- YES: Works standalone with minimal dependencies
- NO: Requires full MDDE infrastructure

### 3. Does it reference an article?
- YES: Can link demo to published content
- NO: Consider whether it's worth adding

### 4. Is it public knowledge?
- YES: Based on public standards (ECB, ISO, etc.)
- NO: Internal methodology or client work

### 5. Does it undermine the full framework?
- YES: Would replace need for enterprise features
- NO: Whets appetite for more capabilities

---

## Complexity Guidelines

### Keep It Simple

| Aspect | Demo Limit | Full MDDE |
|--------|------------|-----------|
| Metadata tables | 5-10 | 60+ |
| Quality checks | 15-20 | 50+ |
| Dialects | 3-4 basic | 10+ with full features |
| Stereotypes | 5-10 common | 50+ with inheritance |
| Lines per module | < 300 | Unlimited |

### Code Style

- Clear, readable code over optimization
- Generous comments explaining concepts
- Reference articles in docstrings
- Working examples over stubs

---

## Example: Feature Classification

| Feature | Decision | Rationale |
|---------|----------|-----------|
| SELECT * detection | INCLUDE | Simple, educational, article-referenced |
| Full CTE normalization | EXCLUDE | Complex, competitive advantage |
| Mermaid ERD generation | INCLUDE | Visual, educational, low complexity |
| D3.js interactive diagrams | EXCLUDE | Requires VS Code extension |
| Basic column lineage | INCLUDE | Core concept, simplified version |
| Conditional/context lineage | EXCLUDE | Advanced, requires full analyzer |
| SCD2 pattern detection | INCLUDE | Common pattern, well-documented |
| Dual SCD2 with re-historization | EXCLUDE | Complex, enterprise feature |
| dbt schema.yml generation | INCLUDE | Popular, educational |
| Full dbt project generation | EXCLUDE | Requires full generator |

---

## Adding New Features

Before adding a new feature to mdde-demo:

1. **Check this document** - Is the feature type allowed?
2. **Identify the article** - Which article does it support?
3. **Simplify ruthlessly** - What's the minimal educational version?
4. **Update ROADMAP.md** - Document the addition
5. **Link in README** - Reference the supporting article

---

## Version Alignment

The demo should stay **behind** the full framework:

| MDDE Version | Demo Version | Gap |
|--------------|--------------|-----|
| 3.21.0 | 0.1.0 | Intentional |

The demo version number is independent and reflects demo-specific releases.

---

*Last updated: 2026-02-18*
