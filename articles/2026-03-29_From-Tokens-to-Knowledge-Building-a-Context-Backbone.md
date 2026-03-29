# From Tokens to Knowledge: Building a Context Backbone for Enterprise AI

**Why throwing more tokens at the problem isn't working — and what to build instead**

---

## The Token Trap

Enterprise AI programs are hitting a structural ceiling.

The symptom looks like a capacity problem: models need more context, so we expand token windows, add retrieval, stuff more documents into prompts. Vendors bill by the token, so "more context" becomes a line item that grows every quarter.

But the actual problem isn't capacity. It's structure.

As Joe Honton observes in his "From Tokens to Knowledge" analysis:

> "Context has been commoditized as a token-billed capacity metric, while the enterprise need is context as **relational structure** that stabilizes meaning, accountability, and reuse."

You can feed an LLM 100,000 tokens of documentation about your data warehouse. It will still hallucinate metric definitions, because it's pattern-matching on text, not traversing a knowledge structure.

The control variable that matters is not "how many tokens fit," but whether the organization can **persist meaning** across time, systems, and decisions.

---

## The Knowledge Decay Problem

Every time an AI agent generates an answer, it strips provenance and regenerates from scratch.

Ask the agent "What's our customer churn rate?" on Monday. It reads documentation, infers a definition, generates SQL, returns a number.

Ask the same question on Friday. It reads the same documentation, infers a *slightly different* definition (because LLMs are probabilistic), generates different SQL, returns a different number.

Neither answer is wrong in isolation. But they're inconsistent, and no one knows why they differ, because the reasoning wasn't persisted.

This is **knowledge decay**: the organization's understanding of its own metrics erodes with each AI interaction, because meaning is regenerated rather than retrieved.

The symptoms:
- Numbers don't match between AI-generated reports
- Teams lose trust in AI answers
- Analysts spend time validating AI outputs instead of acting on them
- "The AI said" becomes a disclaimer, not a citation

---

## The Context Backbone Architecture

The solution isn't more tokens. It's a structural backbone that AI agents can traverse.

Honton proposes three layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Context Backbone                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  UPPER-BOUND ONTOLOGY (Stem)                                    │
│  ───────────────────────────                                    │
│  Domain-neutral categories that standardize what kinds of       │
│  things exist: Entity, Event, Agent, Decision, Metric           │
│                                                                 │
│  LOWER-BOUND TAXONOMY (Spine)                                   │
│  ───────────────────────────                                    │
│  Controlled vocabulary rooted in business language:             │
│  Customer, Order, Churn Rate, Lifetime Value, Campaign          │
│                                                                 │
│  CONTEXT GRAPHS (Connective Tissue)                             │
│  ──────────────────────────────────                              │
│  Decision traces and execution flows that link operational      │
│  events to the semantic model: who decided, when, based on what │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

This isn't a documentation system. It's a **queryable knowledge structure** that agents can traverse.

When an agent needs to answer "What's our churn rate?", it doesn't search documents. It traverses:

```
metric:churn_rate
    │
    ├── definition: "Percentage of customers who..."
    │
    ├── computation: fact_customer.churned / fact_customer.total
    │
    ├── interpretation_rules:
    │       ├── threshold_warning: 0.05
    │       ├── threshold_critical: 0.10
    │       └── comparison_allowed: [month_over_month]
    │
    └── decision_trace:
            ├── approved_by: data-governance-council
            ├── approved_date: 2026-01-15
            └── rationale: "Aligned with industry standard..."
```

The agent doesn't infer meaning. It retrieves it. And the meaning is stable, auditable, and traceable.

---

## Three Problems This Solves

### 1. Semantic Stability

Without a backbone:
- Agent infers "active customer" means "logged in within 30 days"
- Different run: agent infers "active customer" means "has positive balance"
- Both are reasonable. Neither is authoritative.

With a backbone:
- Agent retrieves: `glossary.active_customer.definition`
- Definition is versioned, approved, and consistent across all queries

### 2. Decision Provenance

Without a backbone:
- Agent recommends changing churn threshold from 5% to 3%
- No one knows why the current threshold is 5%
- Change is made (or rejected) without context

With a backbone:
- Agent retrieves: `decision.dec-2025-06-15.churn_threshold`
- Sees: approved by VP Customer Success, based on Q2 retention initiative
- Recommendation includes: "Current threshold was set in Q2 2025 based on..."

### 3. Audit Trail

Without a backbone:
- Regulator asks: "How was this risk score calculated?"
- Answer: "The AI generated it based on our data"
- Regulator: "Show me the methodology"
- Answer: "We can regenerate it, but it might be slightly different"

With a backbone:
- Every AI decision traces back to:
  - Which metric definitions were used
  - Which rules were applied
  - Who approved those rules
  - When they were approved

Provenance is not a "nice to have" metadata feature. It's the substrate of auditability, reproducibility, and legal defensibility.

---

## Mapping to MDDE's Semantic API

The context backbone architecture maps directly to what we've been building with MDDE:

| Backbone Layer | MDDE Component | Implementation |
|----------------|----------------|----------------|
| **Upper-bound ontology** | Stereotypes | `dim_scd2`, `fact`, `dv_hub`, `dv_sat` |
| **Lower-bound taxonomy** | Glossary + Domains | `glossary.yaml`, `domains/banking.yaml` |
| **Context graphs** | Lineage + Decision Traces | `entity_mapping`, `decision.yaml` |

### The Stem: Stereotypes as Ontology

MDDE's stereotypes define domain-neutral categories:

```yaml
# Upper-bound ontology: what kinds of things exist
stereotypes:
  - dim_scd2      # A dimension with history
  - fact          # A measurable event
  - dv_hub        # A business entity (Data Vault)
  - dv_satellite  # Descriptive attributes (Data Vault)
```

These are abstract enough to apply across any domain, concrete enough to drive generation.

### The Spine: Glossary as Taxonomy

MDDE's glossary provides business vocabulary:

```yaml
# Lower-bound taxonomy: controlled business vocabulary
glossary:
  - term: customer
    definition: |
      An individual or organization that has purchased products
      or services, or has an active account relationship.
    synonyms: [client, account holder, buyer]
    primary_entity: dim_customer
    owner: customer-domain-team

  - term: churn_rate
    definition: |
      Percentage of customers who stopped being customers
      during a given period.
    calculation: churned_customers / total_customers
    entity: fact_customer_metrics
    owner: retention-team
```

This isn't documentation. It's a **contract** that AI agents must follow.

### The Connective Tissue: Decision Traces

This is the new layer that completes the backbone:

```yaml
# Context graph: decision provenance
decision:
  id: dec-2026-01-15-churn-definition
  type: metric_definition_approval

  subject:
    metric: churn_rate
    change: definition_update

  participants:
    - role: proposer
      agent: analytics-team
    - role: approver
      agent: data-governance-council

  context:
    - type: analysis
      reference: reports/2025-Q4-churn-methodology.md
    - type: industry_benchmark
      reference: saas-benchmarks/churn-definitions

  rationale: |
    Standardized on 90-day inactivity window to align with
    industry benchmarks and enable external comparison.

  effective_date: 2026-02-01
```

Now when an agent uses `churn_rate`, it can trace:
- **What** the definition is (glossary)
- **How** to compute it (metric rules)
- **Why** it's defined that way (decision trace)
- **Who** approved it (participants)
- **When** it became effective (timestamp)

---

## From Semantic Layer to Semantic API

Earlier articles in this series argued that the semantic layer must become an API — not descriptions, but machine-enforceable rules. The context backbone extends this:

| Evolution | What It Provides |
|-----------|-----------------|
| **Semantic Layer (old)** | Descriptions for humans |
| **Semantic API** | Rules for machines |
| **Context Backbone** | Provenance for decisions |

The semantic API tells the agent *what* to do. The context backbone tells it *why*, and records *that it did*.

```
┌─────────────────────────────────────────────────────────────────┐
│                    AI Agent Interaction                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Query: "What's our churn rate?"                                │
│                                                                 │
│  Semantic API provides:                                         │
│  ├── Definition (what it means)                                 │
│  ├── Computation (how to calculate)                             │
│  └── Rules (when it applies)                                    │
│                                                                 │
│  Context Backbone provides:                                     │
│  ├── Provenance (why it's defined this way)                     │
│  ├── Authority (who approved)                                   │
│  └── Trace (this query used this definition at this time)       │
│                                                                 │
│  Result: Auditable, reproducible, defensible                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Implementation: What to Build

### Phase 1: Vocabulary Layer (MDDE Today)

- Entity definitions with stereotypes
- Glossary terms with owners
- Domain types with constraints
- Lineage (data provenance)

### Phase 2: Rule Layer (Semantic API)

- Interpretation rules (when metrics apply)
- Comparison constraints (what's allowed)
- Threshold definitions (normal/warning/critical)
- Agent permissions (what actions are allowed)

### Phase 3: Decision Layer (Context Backbone)

- Decision traces (who approved what, when)
- Execution logs (which definitions were used)
- Change history (how definitions evolved)
- Audit export (regulatory compliance)

---

## The Economics

Token-based context expansion scales linearly with cost and sublinearly with reliability.

```
Tokens     Cost      Reliability
10K        $0.01     60%
100K       $0.10     75%
1M         $1.00     82%
10M        $10.00    85%
```

You can spend 1000x more and get 40% more reliability. That's a budget sink.

A context backbone inverts this:

```
Structure Investment    Reliability
Glossary (1 week)       +20%
Rules (2 weeks)         +30%
Decision traces (1 mo)  +40%
```

The investment is front-loaded, but reliability compounds. And unlike token costs, it doesn't scale with query volume.

More importantly: token-stuffed context gives you **plausible** answers. Structured context gives you **defensible** answers. In enterprise AI, defensibility is the constraint that matters.

---

## Conclusion

The enterprise AI ceiling isn't about model capability or token capacity.

It's about meaning.

Organizations that treat context as a billing metric will keep hitting the ceiling: more spend, brittle agents, knowledge decay, answers that can't be trusted.

Organizations that build a context backbone will have:
- **Stable semantics** that don't drift between queries
- **Decision provenance** that enables accountability
- **Audit trails** that satisfy regulators
- **Reliable AI** that teams actually trust

The architecture is clear:
- **Stem** (ontology): what kinds of things exist
- **Spine** (taxonomy): what they're called in your business
- **Connective tissue** (context graphs): how decisions were made

This is the infrastructure that makes enterprise AI actually work.

---

**Related reading:**
- [Building Your Own erwin Data Modeler in VS Code](./2026-03-29_Building-Your-Own-Erwin-Data-Modeler-in-VS-Code.md)
- [The Semantic Layer Is Dead. Now It's an API for AI Agents](https://medium.com/@sergeygromov) — Sergey Gromov
- [From Tokens to Knowledge](https://www.linkedin.com/in/joehonton/) — Joe Honton

**Implementation:**
- [MDDE Demo Repository](https://github.com/jacovanderlaan/mdde-demo)
- [ADR-413: Semantic Contract Pattern](https://github.com/jacovanderlaan/mdde)
- [ADR-414: Decision Traces](https://github.com/jacovanderlaan/mdde)

---

*This article is part of my series on Metadata-Driven Data Engineering. It builds on the semantic API concept and extends it with decision provenance — the missing layer that makes AI answers auditable and trustworthy.*
