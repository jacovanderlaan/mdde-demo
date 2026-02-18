# ADR-002: Three-Layer Modeling Approach

## Status

Accepted

## Date

2024-01-20

## Context

Data modeling can be done at different levels of abstraction:
- **Conceptual**: Business entities and relationships (what data exists)
- **Logical**: Attributes, types, keys, constraints (how data is structured)
- **Technical**: Physical implementation details (where/how data is stored)

Traditional tools often conflate these layers, leading to:
- Business users seeing technical details they don't need
- Technical implementations leaking into business discussions
- Difficulty supporting multiple target platforms

## Decision

We will use a **three-layer modeling approach** where each layer has a separate YAML file:

```
entities/
  customer/
    entity.conceptual.yaml  # Business view
    entity.logical.yaml     # Data structure
    entity.technical.yaml   # Platform details
```

### Layer Responsibilities

| Layer | Audience | Contains | Excludes |
|-------|----------|----------|----------|
| **Conceptual** | Business users, analysts | Entity names, descriptions, relationships | Data types, keys, constraints |
| **Logical** | Data engineers, architects | Attributes, types, keys, domains | Physical storage, partitioning |
| **Technical** | Platform engineers, DBAs | Partitioning, clustering, indexes | Business descriptions |

### Inheritance

Properties cascade down: Conceptual → Logical → Technical

```yaml
# entity.conceptual.yaml
entity:
  name: Customer
  description: Our valued customers

# entity.logical.yaml (inherits from conceptual)
attributes:
  - attribute_id: customer_id
    data_type: integer
    is_primary_key: true

# entity.technical.yaml (inherits from logical)
physical:
  partitioning:
    type: hash
    columns: [customer_id]
  clustering: [country, tier]
```

## Consequences

### Positive

1. **Separation of concerns** - Each audience sees relevant details
2. **Reusability** - Same logical model, multiple technical implementations
3. **Governance** - Business definitions separate from implementation
4. **Multi-platform** - One logical model → Snowflake, Databricks, BigQuery
5. **Documentation** - Conceptual layer serves as data dictionary

### Negative

1. **More files** - Each entity has up to 3 files
2. **Synchronization** - Must keep layers consistent
3. **Complexity** - More concepts to learn upfront
4. **Partial definitions** - Need tooling to merge layers

### Neutral

1. **Flexibility** - Not all layers required (can have only logical)
2. **Migration** - Existing models can be split over time

## Alternatives Considered

### Alternative 1: Single File Per Entity

```yaml
# entity.yaml - everything in one file
entity:
  name: Customer
  description: Business description
  attributes: [...]
  physical:
    partitioning: [...]
```

Pros:
- Simpler file structure
- Everything in one place

Cons:
- Business and technical details mixed
- Harder to generate platform-specific output
- Business users see technical noise

**Not chosen** because separation is valuable for governance and multi-platform support.

### Alternative 2: Two Layers (Logical + Physical)

Like many traditional modeling tools.

Pros:
- Simpler than three layers
- Familiar to data modelers

Cons:
- No pure business view
- Business descriptions mixed with technical details

**Not chosen** because conceptual layer valuable for business glossary and governance.

### Alternative 3: View-Based Filtering

Single file with metadata tags, filtered by role.

Pros:
- One file to maintain

Cons:
- Complex filtering logic
- No clear separation
- Harder to version control changes

**Not chosen** because explicit separation is clearer and more maintainable.

## References

- [The Three Schema Architecture](https://en.wikipedia.org/wiki/Three-schema_approach)
- [Conceptual, Logical, Physical Data Models](https://www.guru99.com/data-modelling-conceptual-logical.html)
- [TOGAF Data Architecture](https://pubs.opengroup.org/architecture/togaf9-doc/arch/chap10.html)
