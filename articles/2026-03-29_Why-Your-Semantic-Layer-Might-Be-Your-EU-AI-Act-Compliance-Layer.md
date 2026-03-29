# Why Your Semantic Layer Might Be Your EU AI Act Compliance Layer

**The architecture you need for AI governance already exists — you just need to recognize it**

---

## The Compliance Problem

The EU AI Act came into force in August 2024. By August 2026, organizations deploying high-risk AI systems must demonstrate compliance with seven mandatory obligations:

1. **Risk Management** — Identify, assess, mitigate AI risks
2. **Data Governance** — Training data quality and documentation
3. **Technical Documentation** — System architecture and behavior
4. **Logging** — Record AI decisions for audit
5. **Transparency** — Explain decisions to affected parties
6. **Human Oversight** — Meaningful human control over high-risk decisions
7. **Accuracy** — Appropriate levels of accuracy and robustness

Most organizations are treating this as a new compliance workstream — hiring consultants, building dashboards, creating documentation portals.

But here's the insight that Pankaj Kumar's recent architecture analysis reveals: **the infrastructure you need already exists in your semantic layer**.

---

## The Neuro-Symbolic Insight

Kumar's architecture for EU AI Act compliance is built on a critical distinction:

> "LLMs reason probabilistically. Rules execute deterministically. Never confuse the two."

This is the neuro-symbolic separation pattern. The LLM generates hypotheses. A deterministic layer validates and constrains those hypotheses before they become actions.

What sits in that deterministic layer?

- Business definitions
- Interpretation rules
- Allowed actions
- Decision provenance

This is exactly what a semantic layer provides — or should provide.

---

## From Semantic Layer to Compliance Layer

The traditional semantic layer was documentation: descriptions, glossaries, lineage diagrams. Useful for humans, invisible to machines.

The semantic API (as I've written about previously) transforms this into machine-enforceable rules. AI agents don't just read descriptions — they follow contracts.

The EU AI Act requires the same transformation. What was previously "nice to have" documentation becomes **mandatory compliance artifacts**.

| EU AI Act Requirement | Traditional Semantic Layer | Semantic API |
|-----------------------|---------------------------|--------------|
| Technical Documentation | Word documents | Versioned YAML schemas |
| Logging | Application logs | Decision traces with provenance |
| Transparency | Reports | Machine-readable explanations |
| Human Oversight | Manual review | Formal approval workflows |
| Accuracy | Testing reports | Interpretation rule validation |

---

## The Ontology Firewall

Kumar introduces the concept of an "Ontology Firewall" — a semantic layer that sits between the LLM and execution, validating every AI-generated action against formal constraints.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Neuro-Symbolic Architecture                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐    ┌─────────────────┐    ┌─────────────────┐ │
│  │   LLM       │    │ Ontology        │    │   Execution     │ │
│  │  (Neural)   │───►│ Firewall        │───►│  (Deterministic)│ │
│  │             │    │ (Semantic API)  │    │                 │ │
│  └─────────────┘    └─────────────────┘    └─────────────────┘ │
│        │                    │                      │           │
│   Probabilistic      Validates against       Only valid        │
│   reasoning          formal rules            actions execute   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

This is the same architecture as the Semantic API pattern — MDDE as the formal layer between AI reasoning and data operations.

The difference is regulatory: under EU AI Act, this isn't optional for high-risk systems.

---

## Mapping MDDE to EU AI Act Obligations

### Article 9: Risk Management

MDDE's interpretation rules formalize risk thresholds:

```yaml
metric:
  name: benefit_eligibility_score

  rules:
    thresholds:
      approved: ">= 0.8"
      review_required: "0.5 - 0.8"
      denied: "< 0.5"

    risk_classification:
      eu_ai_act: high_risk
      article: 9
      rationale: "Automated decisions affecting access to public services"
```

The `risk_classification` field transforms implicit risk assessment into auditable documentation.

### Article 10: Data Governance

MDDE's glossary and lineage already document data provenance:

```yaml
glossary_entry:
  term: applicant_income
  definition: |
    Annual gross income from all sources, verified against
    tax records or employer certification.

  data_quality:
    sources: [tax_authority_api, employer_verification]
    validation: income_verification_rule
    completeness_requirement: 0.99

  governance:
    owner: benefits-data-team
    classification: personal_data
    gdpr_basis: legal_obligation
```

### Article 11: Technical Documentation

The YAML model **is** the technical documentation — machine-readable, versioned, complete:

```bash
# Generate EU AI Act Article 11 documentation
mdde compliance export \
  --regulation eu-ai-act \
  --article 11 \
  --format pdf \
  --output technical-documentation.pdf
```

The export includes:
- System architecture (entity relationships)
- Data specifications (domains, constraints)
- Algorithm descriptions (interpretation rules)
- Quality metrics (validation rules)

### Article 12: Logging

Decision traces (ADR-414) provide the required audit trail:

```yaml
execution:
  id: exec-2026-03-29-benefit-decision-001
  timestamp: 2026-03-29T14:30:00Z

  agent: benefits-processor
  action: evaluate_eligibility

  decision_basis:
    - metric: applicant_income (dec-2026-01-15)
    - metric: household_size (dec-2026-01-15)
    - rule: income_threshold_calculation (dec-2026-02-01)

  output:
    eligibility: approved
    confidence: 0.92
    explanation: "Income below threshold for household size"

  compliance:
    eu_ai_act_logged: true
    retention_period: 10 years
```

### Article 13: Transparency

MDDE's glossary provides the vocabulary for explanations:

```yaml
explanation_template:
  decision_type: benefit_eligibility

  template: |
    Your application was {decision} based on:
    - Household income: {income} (threshold: {threshold})
    - Household size: {household_size}
    - Applicable rule: {rule_reference}

    You may appeal this decision within 30 days.

  variables:
    - name: income
      source: applicant_income
      glossary_term: applicant_income
    - name: threshold
      source: income_threshold_calculation
      decision_reference: dec-2026-02-01
```

The explanation traces back to formal definitions, not regenerated text.

### Article 14: Human Oversight

Agent permissions formalize oversight requirements:

```yaml
agent_permissions:
  benefits_processor:

    can_act:
      - action: approve_benefit
        condition: eligibility_score >= 0.95
        requires_approval: false
        oversight: automated_with_audit

      - action: approve_benefit
        condition: eligibility_score >= 0.8 AND eligibility_score < 0.95
        requires_approval: true
        oversight: human_review_required
        reviewer_role: benefits_officer

      - action: deny_benefit
        requires_approval: true
        oversight: human_review_required
        reviewer_role: senior_benefits_officer
        appeal_required: true

    cannot_act:
      - modify_eligibility_rules
      - override_denial_without_appeal
```

### Article 15: Accuracy

Interpretation rules define accuracy requirements:

```yaml
metric:
  name: eligibility_score

  accuracy:
    minimum_precision: 0.95
    minimum_recall: 0.98  # High recall for benefits (don't miss eligible)
    validation_frequency: weekly

    bias_monitoring:
      protected_attributes: [age, gender, ethnicity, disability]
      fairness_metric: demographic_parity
      threshold: 0.05

  rules:
    temporal_lag: 0 days  # Real-time decisions required
    must_use: [verified_income, verified_household_size]
    must_exclude: [estimated_income, self_reported_household]
```

---

## Implementation: The Compliance Extension

MDDE already provides the structural foundation. EU AI Act compliance requires extending it with:

### 1. Risk Classification Metadata

```yaml
entity:
  name: dim_applicant

  compliance:
    eu_ai_act:
      system_type: high_risk
      category: "public_services_access"
      article_6_reference: "Annex III, 5(a)"
```

### 2. SHACL-Style Constraints

Inspired by Kumar's architecture, constraints as first-class validation:

```yaml
constraint:
  name: benefit_decision_explainability
  target: benefit_decision

  rules:
    - path: explanation
      required: true
      min_length: 50
      message: "Every benefit decision must have meaningful explanation"

    - path: human_reviewer
      required_when: "decision = 'denied'"
      message: "Denials require human review documentation"

    - path: decision_basis
      min_count: 1
      message: "Decision must reference at least one formal rule"
```

### 3. Compliance Export

```bash
# Generate full compliance package
mdde compliance package \
  --regulation eu-ai-act \
  --system benefits-processor \
  --output-dir compliance/2026-Q1/

# Outputs:
# - technical-documentation.pdf (Article 11)
# - data-governance-report.pdf (Article 10)
# - decision-logs.json (Article 12)
# - accuracy-validation.pdf (Article 15)
# - human-oversight-summary.pdf (Article 14)
```

---

## The Economic Argument

Kumar's article focuses on public benefits — $140 billion in unclaimed benefits that AI could help surface. The EU AI Act classifies benefits AI as high-risk, requiring compliance infrastructure.

The traditional approach: build separate compliance systems, documentation portals, audit tools.

The semantic API approach: extend your existing metadata layer to serve compliance.

The cost difference is substantial:
- Separate compliance infrastructure: 6-12 month project, dedicated team
- Semantic layer extension: 2-3 month enhancement to existing system

More importantly, the semantic API approach is **self-maintaining**. When interpretation rules change, documentation updates automatically. When decisions execute, logs generate automatically. When accuracy drifts, alerts trigger automatically.

Separate documentation systems drift from reality. Integrated semantic layers stay synchronized.

---

## What This Means for MDDE

MDDE was designed as metadata infrastructure for data engineering. The EU AI Act reveals that this same infrastructure serves regulatory compliance.

The additions required:
1. **Risk classification** — tag entities and metrics by regulatory category
2. **Constraint formalization** — SHACL-style validation rules
3. **Decision traces** — provenance for AI actions (ADR-414)
4. **Compliance export** — generate regulatory artifacts

The architecture doesn't change. The semantic API pattern already separates:
- Observations (what the data shows)
- Interpretations (what conclusions are valid)
- Actions (what the agent can do)

EU AI Act compliance is a specific application of this pattern: **certain interpretations and actions require additional documentation, logging, and human oversight**.

---

## Conclusion

The EU AI Act creates mandatory requirements for AI governance. Most organizations are building compliance as a separate workstream.

But the infrastructure needed — formal definitions, interpretation rules, decision provenance, human oversight workflows — is exactly what a semantic API provides.

If you're already building a semantic layer for AI agents, you're building compliance infrastructure. The question is whether you recognize it and extend it appropriately.

MDDE's roadmap now includes:
- **ADR-413**: Semantic Contract Pattern (interpretation rules, agent permissions)
- **ADR-414**: Decision Traces (provenance, audit trails)
- **ADR-415**: Compliance Integration (risk classification, SHACL constraints, regulatory export)

The semantic layer isn't dead. It's becoming the compliance layer.

---

**Related reading:**
- [The Semantic Layer Is Dead. Now It's an API for AI Agents](./2026-03-29_From-Tokens-to-Knowledge-Building-a-Context-Backbone.md)
- [From Tokens to Knowledge: Building a Context Backbone](./2026-03-29_From-Tokens-to-Knowledge-Building-a-Context-Backbone.md)
- [ADR-415: Compliance Integration](https://github.com/jacovanderlaan/mdde)

**References:**
- Kumar, P. (2026). "EU AI Act Meets $140 Billion in Unclaimed Benefits." LinkedIn.
- EU AI Act (Regulation 2024/1689)
- Gromov, S. (2026). "The Semantic Layer Is Dead." Medium.

---

*This article is part of my series on Metadata-Driven Data Engineering. It explores how the semantic API pattern naturally extends to regulatory compliance — the same infrastructure that makes AI reliable also makes it auditable.*
