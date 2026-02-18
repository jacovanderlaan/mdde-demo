# ADR-003: Stereotype-Driven Code Generation

## Status

Accepted

## Date

2024-02-01

## Context

When generating code (DDL, dbt models, documentation) from metadata, entities of different architectural patterns require different treatment:

- A **Data Vault Hub** needs hash keys, business keys, load timestamps
- A **Dimensional Fact** needs surrogate keys, foreign keys, measures
- An **SCD Type 2** dimension needs effective dates, current flags
- A **Staging table** needs raw columns, load metadata

Without pattern awareness, generators would either:
1. Generate generic code missing pattern-specific features
2. Require extensive manual configuration per entity
3. Need separate generators per pattern

## Decision

We will use **stereotypes** to classify entities by their architectural pattern. Generators will read the stereotype and apply pattern-specific templates.

### Supported Stereotypes

| Pattern | Stereotypes |
|---------|-------------|
| Data Vault | `dv_hub`, `dv_link`, `dv_satellite`, `dv_pit`, `dv_bridge` |
| Dimensional | `dim_fact`, `dim_dimension`, `dim_scd1`, `dim_scd2`, `dim_reference` |
| Staging | `stg_raw`, `stg_cleaned`, `stg_persistent` |
| Delivery | `del_api`, `del_report`, `del_view` |

### Usage

```yaml
# entity.logical.yaml
entity:
  entity_id: hub_customer
  name: Hub Customer
  stereotype: dv_hub        # <- Stereotype drives generation
  layer: integration

  attributes:
    - attribute_id: customer_bk
      name: Customer Business Key
      data_type: varchar(50)
      is_business_key: true  # Required for dv_hub
```

### Generator Behavior

```python
# Pseudocode
def generate_ddl(entity):
    stereotype = entity.stereotype

    if stereotype == "dv_hub":
        return generate_hub_ddl(entity)  # Adds hash key, load_dts
    elif stereotype == "dim_scd2":
        return generate_scd2_ddl(entity)  # Adds valid_from/to, is_current
    else:
        return generate_standard_ddl(entity)
```

### Generated Output Example

For `stereotype: dv_hub`:

```sql
CREATE TABLE integration.hub_customer (
    -- Auto-generated hash key
    hub_customer_hk BINARY(32) NOT NULL,

    -- Business key (from attributes)
    customer_bk VARCHAR(50) NOT NULL,

    -- Auto-generated audit columns
    load_dts TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    record_source VARCHAR(100) NOT NULL,

    PRIMARY KEY (hub_customer_hk)
);
```

## Consequences

### Positive

1. **Consistency** - All hubs look the same, all facts look the same
2. **Productivity** - Define once, generate everywhere
3. **Best practices** - Stereotypes encode architectural patterns
4. **Validation** - Can enforce required attributes per stereotype
5. **Documentation** - Stereotype implies expected structure

### Negative

1. **Rigidity** - Must fit into predefined patterns
2. **Learning curve** - Need to understand available stereotypes
3. **Custom patterns** - Extending requires new stereotype definitions
4. **Over-generation** - May add columns not needed in all cases

### Neutral

1. **Opt-in** - Entities without stereotype get generic generation
2. **Override** - Can always override generated code
3. **Versioning** - Stereotypes evolve with ADRs

## Alternatives Considered

### Alternative 1: Configuration Flags

```yaml
entity:
  add_hash_key: true
  add_load_timestamp: true
  add_record_source: true
```

Pros:
- Maximum flexibility

Cons:
- Verbose configuration
- No pattern enforcement
- Easy to misconfigure

**Not chosen** because stereotypes capture intent, not just features.

### Alternative 2: Template Selection

```yaml
entity:
  template: "data_vault/hub.jinja2"
```

Pros:
- Direct control over output

Cons:
- Couples model to generation
- Templates may drift
- No semantic meaning

**Not chosen** because templates are an implementation detail, not a modeling concept.

### Alternative 3: Inheritance

```yaml
entity:
  extends: base_hub
```

Pros:
- Familiar OO concept

Cons:
- Complex inheritance hierarchies
- Diamond problem
- Harder to understand

**Not chosen** because stereotypes are simpler and more explicit.

## References

- [UML Stereotypes](https://www.uml-diagrams.org/stereotype.html)
- [Data Vault 2.0 Patterns](https://www.vertabelo.com/blog/data-vault-series-data-vault-2-0-modeling-basics/)
- [Dimensional Modeling Patterns](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
