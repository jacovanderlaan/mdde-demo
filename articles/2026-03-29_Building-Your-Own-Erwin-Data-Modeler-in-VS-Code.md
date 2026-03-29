# Building Your Own erwin Data Modeler in VS Code

**Why pay $5,000/user when you can model data in Git?**

Enterprise data modeling tools like erwin Data Modeler and SAP PowerDesigner have dominated the market for decades. They're powerful, mature, and... expensive. They also come with baggage:

- Proprietary binary file formats
- Limited version control
- Complex merge conflicts
- Vendor lock-in
- Steep learning curves

What if you could get the same capabilities using tools you already know?

This article shows how to build an erwin-equivalent data modeling workflow using **VS Code**, **YAML**, **Git**, and **MDDE** (Metadata-Driven Data Engineering).

---

## The Problem with Traditional Tools

```
┌─────────────────────────────────────────────────────────────────┐
│                    Traditional Workflow                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  erwin Desktop ─► .erwin file ─► Shared drive                  │
│       │                │               │                        │
│       ▼                ▼               ▼                        │
│  $5,000+/user    Binary format    No history                   │
│  Per seat        Can't diff       File locking                 │
│  Training        Can't merge      "Who changed this?"          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

I've seen teams struggle with these tools:

- **The merge nightmare**: Two modelers edit the same model. Someone's work gets lost.
- **The history gap**: "Why did we add this column?" No one knows — the .erwin file doesn't remember.
- **The license bottleneck**: Only three people can model because only three licenses exist.

---

## The Modern Alternative

```
┌─────────────────────────────────────────────────────────────────┐
│                    Modern Workflow                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  VS Code (free) ─► YAML models ─► Git repository               │
│       │                │               │                        │
│       ▼                ▼               ▼                        │
│  Extensions      Human-readable   Full history                 │
│  JSON Schema     Line-by-line     Branching                    │
│  Live preview    Git diff         Pull requests                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

The idea is simple:

1. **Define models in YAML** — human-readable, diff-friendly
2. **Validate with JSON Schema** — autocomplete, inline errors
3. **Generate outputs** — DDL, documentation, diagrams
4. **Version in Git** — full history, branches, PRs

---

## What erwin Does vs. What We'll Build

| erwin Capability | Our Approach | How |
|------------------|--------------|-----|
| Entity modeling | YAML files | `entities/*.yaml` |
| Attribute editor | VS Code + Schema | Autocomplete, validation |
| ERD diagrams | Mermaid/Draw.io | Auto-generated |
| Forward engineering | SQL generation | `mdde generate ddl` |
| Reverse engineering | Discovery | `mdde discover` |
| Compare/merge | Git | Native workflow |
| Version history | Git log | Full audit trail |
| Data dictionary | Markdown | `mdde docs generate` |

---

## The Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         VS Code                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ YAML        │  │ Form        │  │ Mermaid     │             │
│  │ Language    │  │ Editor      │  │ Preview     │             │
│  │ Server      │  │ Extension   │  │             │             │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘             │
│         │                │                │                     │
│         └────────────────┼────────────────┘                     │
│                          │                                      │
│  ┌───────────────────────┴───────────────────────┐             │
│  │              JSON Schemas                      │             │
│  │   (entity, relationship, domain)              │             │
│  └───────────────────────────────────────────────┘             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MDDE CLI / Library                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Parse ─► Validate ─► Generate ─► Export                       │
│                                                                 │
│  Outputs:                                                       │
│  • DDL (Databricks, Snowflake, DuckDB, Postgres)               │
│  • Documentation (Markdown, HTML)                               │
│  • Diagrams (Mermaid, Draw.io)                                 │
│  • Tests (dbt, Great Expectations)                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Step 1: Define Entities in YAML

Here's what a dimension table looks like:

```yaml
# models/entities/dim_customer.yaml
entity:
  name: dim_customer
  stereotype: dim_scd2
  description: Customer dimension with full history tracking
  owner: customer-domain-team

  columns:
    - name: customer_sk
      data_type: bigint
      primary_key: true
      description: Surrogate key (auto-generated)

    - name: customer_id
      data_type: string
      business_key: true
      not_null: true
      description: Natural key from source CRM

    - name: customer_name
      data_type: string
      not_null: true
      pii: true

    - name: email
      data_type: string
      pii: true
      masking: hash

    - name: segment
      data_type: string
      allowed_values: [Enterprise, SMB, Consumer]

    - name: _valid_from
      data_type: timestamp
      scd2_from: true

    - name: _valid_to
      data_type: timestamp
      scd2_to: true
```

**Why YAML?**

- Human-readable (unlike .erwin binary)
- Git-friendly (line-by-line diffs)
- Easy to template and generate
- Schema-validatable

---

## Step 2: Schema-Driven Editing

With JSON Schema, VS Code provides:

- **Autocomplete** for all properties
- **Inline validation** (red squiggles for errors)
- **Hover documentation**
- **Go-to-definition** for references

```json
// .vscode/settings.json
{
  "yaml.schemas": {
    "./schemas/entity.schema.json": "models/entities/**/*.yaml"
  }
}
```

The schema defines valid stereotypes, data types, and constraints:

```json
{
  "properties": {
    "stereotype": {
      "enum": [
        "dim_scd1", "dim_scd2", "dim_reference",
        "fact", "fact_snapshot",
        "dv_hub", "dv_link", "dv_satellite"
      ]
    },
    "data_type": {
      "enum": ["string", "int", "bigint", "decimal", "boolean", "date", "timestamp"]
    }
  }
}
```

---

## Step 3: Generate DDL

From a single YAML definition, generate DDL for any platform:

```bash
# Databricks
mdde generate ddl --platform databricks --output generated/ddl/databricks/

# Snowflake
mdde generate ddl --platform snowflake --output generated/ddl/snowflake/

# DuckDB (for local testing)
mdde generate ddl --platform duckdb --output generated/ddl/duckdb/
```

**Generated Databricks DDL:**

```sql
-- Entity: dim_customer
-- Stereotype: dim_scd2
-- Generated: 2026-03-29

CREATE TABLE IF NOT EXISTS catalog.sales.dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id STRING NOT NULL,
    customer_name STRING NOT NULL,
    email STRING,
    segment STRING,
    _valid_from TIMESTAMP,
    _valid_to TIMESTAMP,

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
)
USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- Current view (active records only)
CREATE OR REPLACE VIEW catalog.sales.dim_customer_current AS
SELECT customer_sk, customer_id, customer_name, email, segment
FROM catalog.sales.dim_customer
WHERE _valid_to IS NULL;
```

**Generated Snowflake DDL:**

```sql
CREATE TABLE IF NOT EXISTS sales.dim_customer (
    customer_sk INTEGER AUTOINCREMENT,
    customer_id VARCHAR NOT NULL,
    customer_name VARCHAR NOT NULL,
    email VARCHAR,
    segment VARCHAR,
    _valid_from TIMESTAMP_NTZ,
    _valid_to TIMESTAMP_NTZ,

    PRIMARY KEY (customer_sk)
);
```

One model. Multiple platforms. No copy-paste errors.

---

## Step 4: Auto-Generate ERD Diagrams

```bash
mdde diagram erd --entities "dim_*,fact_*" --output docs/erd.md
```

**Generated Mermaid:**

```mermaid
erDiagram
    dim_customer ||--o{ fact_orders : "customer_sk"
    dim_product ||--o{ fact_orders : "product_sk"
    dim_date ||--o{ fact_orders : "order_date_sk"

    dim_customer {
        bigint customer_sk PK
        string customer_id BK
        string customer_name
        string email
        string segment
    }

    fact_orders {
        bigint order_sk PK
        bigint customer_sk FK
        bigint product_sk FK
        int order_date_sk FK
        decimal order_amount
    }
```

Live preview in VS Code with the Mermaid extension.

---

## Step 5: Collaborate with Git

**The erwin way:**
- Alice opens model.erwin
- Bob can't edit (file locked)
- Alice finishes, Bob starts
- No way to work in parallel

**The Git way:**

```bash
# Alice works on customers
git checkout -b feature/customer-segment
vim models/entities/dim_customer.yaml
git commit -m "Add segment column to customer"
git push -u origin feature/customer-segment
gh pr create

# Bob works on products (in parallel!)
git checkout -b feature/product-category
vim models/entities/dim_product.yaml
git commit -m "Add category hierarchy to product"
git push -u origin feature/product-category
gh pr create
```

**Pull request shows clean diff:**

```diff
   columns:
     - name: customer_name
       data_type: string
+    - name: segment
+      data_type: string
+      allowed_values: [Enterprise, SMB, Consumer]
```

Reviewers can see exactly what changed. No binary blob mysteries.

---

## Step 6: Generate Documentation

```bash
mdde docs generate --output docs/
```

Creates:

- **Data dictionary** (all entities, columns, types)
- **Business glossary** (linked terms)
- **Lineage diagrams** (data flow)
- **Governance report** (PII classification, owners)

All from the same YAML source. Single source of truth.

---

## Cost Comparison

| Aspect | erwin | VS Code + MDDE |
|--------|-------|----------------|
| **License** | $5,000+/user/year | Free |
| **Collaboration** | erwin Model Manager ($$$) | Git (free) |
| **Version control** | Limited | Full history |
| **CI/CD** | Manual export | Native |
| **Platform lock-in** | High | None |
| **Learning curve** | erwin-specific | Standard tools |

For a team of 10 modelers:
- erwin: ~$50,000/year in licenses alone
- VS Code + MDDE: $0

---

## Getting Started

The [mdde-demo repository](https://github.com/jacovanderlaan/mdde-demo) includes a complete example:

```bash
# Clone the demo
git clone https://github.com/jacovanderlaan/mdde-demo.git
cd mdde-demo

# Install dependencies
pip install -e .

# Open in VS Code
code .

# Explore the banking model
ls examples/erwin-replacement/models/

# Generate DDL
python -m mdde_lite.generator examples/erwin-replacement/
```

The `examples/erwin-replacement/` folder contains:

```
erwin-replacement/
├── .vscode/
│   └── settings.json          # Schema associations
├── models/
│   ├── entities/
│   │   ├── dim_customer.yaml
│   │   ├── dim_account.yaml
│   │   ├── dim_product.yaml
│   │   └── fact_transaction.yaml
│   ├── domains/
│   │   └── banking.yaml       # Reusable types
│   └── relationships/
│       └── banking.yaml       # FK definitions
├── generated/
│   ├── ddl/
│   │   ├── databricks/
│   │   ├── snowflake/
│   │   └── duckdb/
│   └── docs/
│       └── erd.md
└── README.md                  # Walkthrough
```

---

## What's Next?

This is the foundation. The full MDDE framework adds:

- **Discovery**: Reverse engineer existing databases to YAML
- **Impact analysis**: See what breaks when you change a column
- **Quality checks**: Define and enforce data quality rules
- **Lineage tracking**: Column-level data flow
- **dbt integration**: Generate dbt models from YAML

The point isn't to replicate erwin feature-for-feature.

The point is to **model data using tools that fit modern engineering practices**.

---

## Conclusion

Enterprise data modeling doesn't require enterprise pricing.

With YAML models, JSON Schema validation, and Git workflows, you get:

- Free tooling
- Better collaboration
- Full version history
- Multi-platform generation
- CI/CD integration

The erwin era is ending. The Git-native era is beginning.

---

**Try it yourself**: [github.com/jacovanderlaan/mdde-demo](https://github.com/jacovanderlaan/mdde-demo)

**Read more**: [MDDE article series on Medium](https://medium.com/@jaco.vanderlaan)

---

*This article is part of my ongoing series on Metadata-Driven Data Engineering. If you're interested in practical approaches to data modeling, governance, and platform migration, follow along.*
