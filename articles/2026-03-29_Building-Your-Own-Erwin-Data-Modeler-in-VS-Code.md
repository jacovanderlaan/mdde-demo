# Building Your Own erwin Data Modeler in VS Code

**The real problem isn't the tool — it's the loss of continuity**

---

## The Continuity Problem

Enterprises don't fail because their individual tools are flawed.

They fail because **nothing maintains continuity between them**.

Dr. Nicolas Figay calls this the challenge of "semantic cartography" — the need for a dynamic map that preserves alignment across domains and time. His observation resonates deeply with data modeling:

> "Teams align in planning, then diverge during execution."

I see this constantly in data engineering:

- Product model changes don't propagate to the data warehouse
- Business rules in the conceptual model disconnect from physical implementation
- Regulatory constraints are discovered too late
- Documentation drifts from reality within weeks

The symptoms are familiar. The root cause is **continuity loss**.

```
┌─────────────────────────────────────────────────────────────────┐
│                    The Continuity Gap                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Conceptual    Logical       Physical       Runtime             │
│  Model         Model         Schema         Data                │
│     │             │             │              │                │
│     ▼             ▼             ▼              ▼                │
│  [erwin]      [erwin]       [DDL]          [Databricks]        │
│     │             │             │              │                │
│     └─────────────┴─────────────┴──────────────┘                │
│                         │                                       │
│                    No continuity                                │
│              "What changed? Who knows?"                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Why Traditional Tools Break Continuity

Enterprise modeling tools like erwin and PowerDesigner were built for a different era:

- **Binary formats** that can't be diffed or merged
- **File locking** that prevents parallel work
- **Isolation** from the systems they're supposed to describe
- **Manual synchronization** between model layers

The result? Models become artifacts — snapshots frozen in time — rather than living representations that evolve with the enterprise.

| Traditional Tool Problem | Continuity Impact |
|--------------------------|-------------------|
| Binary .erwin files | Can't track what changed or why |
| File locking | Teams diverge while waiting |
| Manual DDL export | Physical schema drifts from model |
| No Git integration | No audit trail across time |
| Per-seat licensing | Knowledge siloed to few modelers |

The cost isn't just $5,000/user/year.

The cost is **lost alignment** — decisions made in planning that don't survive execution.

---

## A Different Approach: Models as Living Maps

What if data models weren't static documents, but **semantic maps** that maintain continuity?

This requires three types of interoperability:

1. **Semantic** — Shared meaning across domains
2. **Structural** — Shared elements and relationships
3. **Operational** — Shared capacity to act accordingly

A YAML-based, Git-native approach delivers all three:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Continuity Through Git                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Conceptual    Logical       Physical       Runtime             │
│  (YAML)        (YAML)        (Generated)    (Applied)           │
│     │             │             │              │                │
│     └─────────────┴─────────────┴──────────────┘                │
│                         │                                       │
│              Single source of truth                             │
│         Full history │ Traceability │ Automation               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

The model becomes a **living map** — not a document to be exported, but a source that generates everything downstream.

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
│  │   (entity, relationship, domain, glossary)    │             │
│  └───────────────────────────────────────────────┘             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MDDE: Generation Layer                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  YAML Models ─► Validate ─► Generate ─► Apply                  │
│                                                                 │
│  Outputs (all derived, all traceable):                         │
│  • DDL (Databricks, Snowflake, DuckDB, Postgres)               │
│  • Documentation (Markdown, HTML, Confluence)                   │
│  • Diagrams (Mermaid, Draw.io, PlantUML)                       │
│  • Tests (dbt, Great Expectations, DQOps)                      │
│  • Lineage (column-level, impact analysis)                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Git                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Every change tracked │ Every decision auditable               │
│  Branching for experiments │ PRs for governance                │
│  CI/CD for validation │ Continuity across time                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## What This Enables

| erwin Capability | Our Approach | Continuity Benefit |
|------------------|--------------|-------------------|
| Entity modeling | YAML files | Human-readable, versionable |
| Attribute editor | VS Code + Schema | Validation at edit time |
| ERD diagrams | Mermaid (generated) | Always current, never stale |
| Forward engineering | DDL generation | Physical always matches logical |
| Reverse engineering | Discovery | Existing systems → models |
| Compare/merge | Git | Parallel work, clean merges |
| Version history | Git log | "Why did we add this column?" |
| Data dictionary | Generated docs | Single source of truth |
| Impact analysis | Lineage tracking | "What breaks if I change this?" |

---

## Defining Entities with Semantic Richness

Here's what a dimension looks like — not just structure, but meaning:

```yaml
# models/entities/dim_customer.yaml
entity:
  name: dim_customer
  stereotype: dim_scd2
  description: |
    Customer dimension with full history tracking.
    Business key is customer_id from the CRM system.
  owner: customer-domain-team

  # Governance
  tags: [pii, gdpr-relevant]
  data_classification: confidential
  retention_days: 2555  # 7 years

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
      glossary_term: customer_identifier

    - name: customer_name
      data_type: string
      not_null: true
      pii: true
      description: Full legal name

    - name: segment
      data_type: $domain.customer_segment
      description: Customer segmentation tier

    - name: kyc_status
      data_type: string
      allowed_values: [pending, verified, expired, rejected]
      description: Know Your Customer verification status

    - name: _valid_from
      data_type: timestamp
      scd2_from: true

    - name: _valid_to
      data_type: timestamp
      scd2_to: true
```

This isn't just a table definition. It's a **semantic declaration**:

- **Stereotype** (`dim_scd2`) — pattern that drives generation
- **Owner** — accountability
- **Tags** — classification for governance
- **Glossary terms** — business meaning linked to columns
- **Domains** — reusable types with validation rules
- **PII markers** — data protection requirements

The model carries meaning, not just structure.

---

## Reusable Domains: Shared Semantics

Instead of repeating definitions, reference domains:

```yaml
# models/domains/banking.yaml
domains:
  - domain:
      name: monetary_amount
      base_type: decimal
      precision: 18
      scale: 2
      description: Monetary amount with 2 decimal places

  - domain:
      name: customer_segment
      base_type: string
      allowed_values: [retail, premium, private_banking, corporate]
      description: Customer segmentation tier

  - domain:
      name: iban
      base_type: string
      length: 34
      pattern: "^[A-Z]{2}[0-9]{2}[A-Z0-9]{1,30}$"
      description: International Bank Account Number
      pii: true
```

When used in entities:

```yaml
- name: balance
  data_type: $domain.monetary_amount
```

The domain carries:
- Type constraints
- Validation rules
- Documentation
- Governance tags

Change the domain once → all usages update. **Semantic continuity**.

---

## Generation: One Model, Many Outputs

From a single YAML source, generate platform-specific outputs:

```bash
# Databricks
mdde generate ddl --platform databricks --output generated/ddl/databricks/

# Snowflake
mdde generate ddl --platform snowflake --output generated/ddl/snowflake/

# Documentation
mdde docs generate --output generated/docs/
```

**Generated Databricks DDL:**

```sql
CREATE TABLE IF NOT EXISTS catalog.banking.dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id STRING NOT NULL,
    customer_name STRING NOT NULL,
    segment STRING,
    kyc_status STRING,
    _valid_from TIMESTAMP NOT NULL,
    _valid_to TIMESTAMP,

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk),
    CONSTRAINT chk_kyc CHECK (kyc_status IN ('pending', 'verified', 'expired', 'rejected'))
)
USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'quality.classification' = 'pii,gdpr-relevant'
);
```

The physical schema is derived, not manually created. **Structural continuity**.

---

## Git: Continuity Across Time

Every model change is tracked:

```bash
# Alice adds a segment column
git checkout -b feature/customer-segment
vim models/entities/dim_customer.yaml
git commit -m "Add segment column to customer dimension"
git push -u origin feature/customer-segment
gh pr create --title "Add customer segmentation"
```

**The PR shows exactly what changed:**

```diff
  columns:
    - name: customer_name
      data_type: string
+   - name: segment
+     data_type: $domain.customer_segment
+     description: Customer segmentation tier
```

Six months later, someone asks: "Why do we have a segment column?"

```bash
git log --oneline -p models/entities/dim_customer.yaml | grep -A5 "segment"
```

The answer is there. **Temporal continuity**.

---

## Impact Analysis: What Changes? Who Is Impacted?

Figay asks the governance questions that matter:

> What changes? Who is impacted? What becomes inconsistent? What should adapt versus stabilize? What are the costs?

With lineage tracking, these become answerable:

```bash
# What depends on the customer_id column?
mdde impact --entity dim_customer --column customer_id
```

```
Impact Analysis: dim_customer.customer_id
─────────────────────────────────────────
Downstream entities:
  → dim_account.customer_sk (FK)
  → fact_transaction.customer_sk (FK)
  → fact_balance.customer_sk (FK)

Downstream reports:
  → Customer 360 Dashboard
  → Regulatory Report: AnaCredit

Estimated impact: HIGH
Recommendation: Coordinate with account-domain-team, transaction-domain-team
```

Before making changes, you see the blast radius. **Operational continuity**.

---

## Collaboration Without Conflict

**The erwin way:**
- Alice opens model.erwin
- Bob can't edit (file locked)
- Alice finishes, Bob starts
- Work happens sequentially

**The Git way:**

```bash
# Alice and Bob work in parallel
git checkout -b alice/customer-segment
git checkout -b bob/product-category

# Both submit PRs
# Both get reviewed
# Both merge cleanly (different files)
```

No file locking. No waiting. No lost work.

For conflicts (same file edited):

```diff
<<<<<<< alice/customer-segment
    - name: segment
      data_type: string
=======
    - name: tier
      data_type: string
>>>>>>> bob/customer-tier
```

Git's merge tools resolve it. The team decides together. **Social continuity**.

---

## Cost Comparison

| Aspect | erwin | VS Code + MDDE |
|--------|-------|----------------|
| **License** | $5,000+/user/year | Free |
| **Collaboration** | erwin Model Manager ($$$) | Git (free) |
| **Version control** | Limited | Full history |
| **CI/CD** | Manual export | Native |
| **Continuity** | Manual effort | Built-in |
| **Learning curve** | erwin-specific | Standard tools |

For a team of 10:
- erwin: ~$50,000/year + Model Manager
- VS Code + MDDE: $0

But the real savings? **No more lost alignment.**

---

## Getting Started

The [mdde-demo repository](https://github.com/jacovanderlaan/mdde-demo) includes a complete banking example:

```bash
git clone https://github.com/jacovanderlaan/mdde-demo.git
cd mdde-demo/examples/erwin-replacement

# Open in VS Code
code .

# Explore the model
ls models/entities/
```

```
erwin-replacement/
├── models/
│   ├── entities/
│   │   ├── dim_customer.yaml    # SCD2 customer
│   │   ├── dim_account.yaml     # SCD2 account
│   │   ├── dim_product.yaml     # SCD1 product
│   │   ├── dim_date.yaml        # Reference
│   │   ├── fact_transaction.yaml
│   │   └── fact_balance.yaml
│   ├── domains/
│   │   └── banking.yaml         # 15 reusable types
│   └── relationships/
│       └── banking.yaml         # FK definitions
├── generated/
│   ├── ddl/                     # Multi-platform DDL
│   └── docs/                    # ERD, data dictionary
└── README.md
```

---

## Beyond Tool Replacement

The point isn't to replicate erwin feature-for-feature.

The point is to solve the **continuity problem** that traditional tools ignore:

- **Semantic continuity** — Shared meaning through domains, glossaries, and governance tags
- **Structural continuity** — Generated outputs always match the source model
- **Operational continuity** — Impact analysis before changes, not surprises after
- **Temporal continuity** — Full Git history of every decision
- **Social continuity** — Parallel collaboration without conflicts

As Figay observes, enterprises are social systems. The tools we use should support that reality, not fight it.

---

## Conclusion

Enterprise data modeling doesn't require enterprise pricing.

More importantly, it doesn't require accepting discontinuity as inevitable.

With YAML models, JSON Schema validation, Git workflows, and generation pipelines, you get:

- A **semantic map** that evolves with your enterprise
- **Traceability** from business concept to physical schema
- **Alignment** that survives execution, not just planning
- **Collaboration** that scales with your team

The erwin era is ending.

The era of **living data models** is beginning.

---

**Try it yourself**: [github.com/jacovanderlaan/mdde-demo](https://github.com/jacovanderlaan/mdde-demo)

**Read more**: [MDDE article series on Medium](https://medium.com/@jaco.vanderlaan)

**Inspiration**: Nicolas Figay's work on [Semantic Cartography](https://medium.com/@nfigay)

---

*This article is part of my ongoing series on Metadata-Driven Data Engineering. The ideas here build on Figay's insight that enterprise continuity requires more than good individual tools — it requires a paradigm that maintains alignment across domains and time.*
