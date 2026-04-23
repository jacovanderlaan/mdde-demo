# erwin Data Modeler Replacement Demo

**Build enterprise data models in VS Code with YAML, Git, and MDDE.**

This demo shows how to replace expensive enterprise modeling tools (erwin, PowerDesigner) with a modern, Git-native workflow.

## Why Replace erwin?

| Traditional (erwin) | Modern (VS Code + MDDE) |
|---------------------|-------------------------|
| $5,000+/user/year | Free |
| Binary .erwin files | Human-readable YAML |
| File locking | Git branches |
| Limited history | Full audit trail |
| Manual DDL export | Multi-platform generation |

## What's Included

```
erwin-replacement/
├── .vscode/
│   ├── settings.json      # Schema associations for autocomplete
│   └── extensions.json    # Recommended VS Code extensions
├── models/
│   ├── entities/          # Entity definitions
│   │   ├── dim_customer.yaml    # Customer dimension (SCD2)
│   │   ├── dim_account.yaml     # Account dimension (SCD2)
│   │   ├── dim_product.yaml     # Product dimension (SCD1)
│   │   ├── dim_date.yaml        # Date dimension (reference)
│   │   ├── fact_transaction.yaml # Transaction fact
│   │   └── fact_balance.yaml    # Daily balance snapshot
│   ├── domains/
│   │   └── banking.yaml   # Reusable data types
│   └── relationships/
│       └── banking.yaml   # Foreign key definitions
├── generated/
│   ├── ddl/
│   │   ├── databricks/    # Databricks SQL
│   │   ├── snowflake/     # Snowflake SQL
│   │   └── duckdb/        # DuckDB SQL (local testing)
│   └── docs/
│       └── erd.md         # Generated ERD diagram
└── README.md
```

## Quick Start

### 1. Open in VS Code

```bash
code examples/erwin-replacement/
```

### 2. Install Recommended Extensions

VS Code will prompt you to install:
- **YAML** (Red Hat) - Schema validation and autocomplete
- **Mermaid Preview** - ERD visualization
- **Draw.io Integration** - Complex diagrams
- **GitLens** - Enhanced Git history

### 3. Explore the Model

Open any entity file in `models/entities/`. You'll get:
- **Autocomplete** for all properties
- **Validation** against JSON Schema
- **Hover documentation**

### 4. Generate DDL

```bash
# From repository root
python -m mdde_lite.generator examples/erwin-replacement/

# Or use the MDDE CLI
mdde generate ddl --platform databricks --output generated/ddl/databricks/
```

### 5. View ERD

Open `generated/docs/erd.md` and use Mermaid preview (Ctrl+Shift+V).

## The Banking Data Model

This demo includes a realistic banking domain model:

### Dimensions

| Entity | Stereotype | Description |
|--------|------------|-------------|
| `dim_customer` | SCD2 | Customer with full history |
| `dim_account` | SCD2 | Banking accounts |
| `dim_product` | SCD1 | Banking products |
| `dim_date` | Reference | Calendar dimension |

### Facts

| Entity | Stereotype | Description |
|--------|------------|-------------|
| `fact_transaction` | Fact | Atomic transactions |
| `fact_balance` | Snapshot | Daily balance snapshots |

### ERD

```mermaid
erDiagram
    dim_customer ||--o{ dim_account : "has"
    dim_customer ||--o{ fact_transaction : "makes"
    dim_customer ||--o{ fact_balance : "has"

    dim_account ||--o{ fact_transaction : "contains"
    dim_account ||--o{ fact_balance : "has"

    dim_product ||--o{ dim_account : "defines"
    dim_product ||--o{ fact_transaction : "for"
    dim_product ||--o{ fact_balance : "for"

    dim_date ||--o{ fact_transaction : "transaction_date"
    dim_date ||--o{ fact_transaction : "posting_date"
    dim_date ||--o{ fact_transaction : "value_date"
    dim_date ||--o{ fact_balance : "balance_date"

    dim_customer {
        bigint customer_sk PK
        string customer_id BK
        string customer_name
        string segment
        timestamp _valid_from
        timestamp _valid_to
    }

    dim_account {
        bigint account_sk PK
        string account_id BK
        bigint customer_sk FK
        string status
        string product_category
    }

    dim_product {
        bigint product_sk PK
        string product_code BK
        string product_name
        string product_category
    }

    fact_transaction {
        bigint transaction_sk PK
        string transaction_id BK
        bigint account_sk FK
        bigint customer_sk FK
        int transaction_date_sk FK
        decimal amount
        string transaction_type
    }

    fact_balance {
        bigint account_sk PK,FK
        int balance_date_sk PK,FK
        decimal balance_closing
        int transaction_count
    }
```

## Key Features Demonstrated

### 1. Schema-Driven Editing

The `.vscode/settings.json` configures JSON Schema for all YAML files:
- Entity schema validates stereotypes, data types, keys
- Domain schema validates type definitions
- Relationship schema validates FK definitions

### 2. Reusable Domains

Instead of repeating type definitions, reference domains:

```yaml
# In domains/banking.yaml
- domain:
    name: monetary_amount
    base_type: decimal
    precision: 18
    scale: 2

# In entities/fact_transaction.yaml
- name: amount
  data_type: $domain.monetary_amount
```

### 3. PII Classification

Mark sensitive columns for data governance:

```yaml
- name: email
  data_type: string
  pii: true
  masking: hash
```

### 4. SCD2 Tracking

Automatic history tracking columns:

```yaml
entity:
  stereotype: dim_scd2
  columns:
    - name: _valid_from
      scd2_from: true
    - name: _valid_to
      scd2_to: true
    - name: _is_current
      scd2_current: true
```

### 5. Multi-Platform DDL

Generate DDL for any platform:
- **Databricks**: Delta tables with liquid clustering
- **Snowflake**: Native Snowflake DDL
- **DuckDB**: Local development/testing
- **PostgreSQL**: Traditional RDBMS

## Workflow: Making Changes

### Add a Column

1. Edit the entity YAML
2. Git commit with clear message
3. Create PR for review
4. Regenerate DDL
5. Apply migration

```bash
# Edit
vim models/entities/dim_customer.yaml

# Commit
git add .
git commit -m "Add loyalty_tier column to dim_customer"

# PR
git push -u origin feature/loyalty-tier
gh pr create --title "Add loyalty tier to customer"
```

### Review Changes

PR diff shows exactly what changed:

```diff
  columns:
    - name: segment
      data_type: $domain.customer_segment
+   - name: loyalty_tier
+     data_type: string
+     allowed_values: [bronze, silver, gold, platinum]
+     description: Customer loyalty program tier
```

No more "what changed in the .erwin file?"

## Next Steps

1. **Add more entities** - Extend the model with your domain
2. **Connect to CI/CD** - Validate models on PR
3. **Generate documentation** - Auto-publish data dictionary
4. **Integrate with dbt** - Generate dbt models from YAML

## Where transformations + mappings live

This VSCode workflow handles the **data model** — entities,
relationships, domains. It does not author the transformation logic
that populates the warehouse, nor the mappings that link targets to
sources.

Those live in the **CTE notebook** (
[`mdde-migrations/common/tools/databricks/cte_notebook`](https://github.com/jacovanderlaan/mdde-migrations/tree/main/common/tools/databricks/cte_notebook)
), MDDE's second authoring tool. The two are paired by persona:

| Persona | Tool | Authors |
|---|---|---|
| Data modeler | VSCode (this demo) | Entities, relationships, domains |
| Analyst + engineer | CTE notebook | Pipeline transformations (via DSL recipe cells) |

**Mappings** — the manifests that declare "this target entity is
produced from these sources via this pipeline" — are *generated* by
the notebook when a pipeline is saved, and committed alongside the
pipeline YAML. VSCode reads these manifests to show "produced by ..."
navigation in the entity pane; it does not edit them.

See [ADR-026 — Mappings live in notebooks, not VSCode](https://github.com/jacovanderlaan/mdde-migrations/blob/main/common/tools/databricks/cte_notebook/docs/adrs/adr-026-mappings-live-in-notebooks.md)
for the design record.

## Related Resources

- [Medium Article: Building Your Own erwin in VS Code](../articles/2026-03-29_Building-Your-Own-Erwin-Data-Modeler-in-VS-Code.md)
- [MDDE Documentation](https://github.com/jacovanderlaan/mdde)
- [JSON Schema Specification](https://json-schema.org/)
