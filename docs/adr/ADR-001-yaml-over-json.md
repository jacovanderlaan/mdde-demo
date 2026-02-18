# ADR-001: YAML over JSON for Model Files

## Status

Accepted

## Date

2024-01-15

## Context

MDDE needs a human-readable format for defining data models. The models need to be:
- Easy to read and write by data engineers
- Version-controllable in Git
- Editable in any text editor
- Suitable for code review
- Able to include comments and documentation

Two primary formats were considered: **JSON** and **YAML**.

## Decision

We will use **YAML** as the primary format for all MDDE model definitions.

Model files follow the pattern:
```
entity.{layer}.yaml  (e.g., entity.logical.yaml)
domain.yaml
model.yaml
```

## Consequences

### Positive

1. **Readability** - YAML is significantly more readable than JSON for complex nested structures
2. **Comments** - YAML supports comments (`#`), essential for documenting decisions
3. **Multi-line strings** - YAML handles multi-line descriptions naturally with `|` and `>`
4. **Less syntax** - No quotes required for most strings, no trailing commas
5. **Industry alignment** - Kubernetes, Ansible, GitHub Actions, dbt all use YAML

Example comparison:

```yaml
# YAML - Clean and readable
entity:
  entity_id: customer
  name: Customer
  description: |
    Master customer data including
    demographics and tier classification
  stereotype: dim_scd2

  attributes:
    - attribute_id: customer_id
      name: Customer ID
      data_type: integer
      is_primary_key: true
```

```json
{
  "entity": {
    "entity_id": "customer",
    "name": "Customer",
    "description": "Master customer data including\ndemographics and tier classification",
    "stereotype": "dim_scd2",
    "attributes": [
      {
        "attribute_id": "customer_id",
        "name": "Customer ID",
        "data_type": "integer",
        "is_primary_key": true
      }
    ]
  }
}
```

### Negative

1. **Whitespace sensitivity** - Indentation errors can cause parsing failures
2. **Multiple YAML specs** - YAML 1.1 vs 1.2 differences (we standardize on 1.2)
3. **Type coercion** - `yes`/`no` becoming booleans can be surprising
4. **Slower parsing** - YAML parsing is slower than JSON (negligible for our use case)

### Neutral

1. **IDE Support** - Both formats have excellent editor support
2. **Schema validation** - JSON Schema works for both via converters
3. **Tooling** - Libraries available in all languages for both formats

## Alternatives Considered

### Alternative 1: JSON

Pros:
- Stricter syntax reduces ambiguity
- Native JavaScript support
- Faster parsing

Cons:
- No comments
- Verbose for nested structures
- Poor multi-line string support

**Not chosen** because readability and comments are essential for model documentation.

### Alternative 2: TOML

Pros:
- Clean syntax
- Good for configuration

Cons:
- Poor support for deeply nested structures
- Less familiar to data engineers
- Limited multi-line handling

**Not chosen** because data models are inherently nested and TOML struggles with this.

### Alternative 3: Custom DSL

Pros:
- Could be perfectly tailored to our needs

Cons:
- Requires building parser
- No existing tooling
- Learning curve for users

**Not chosen** because existing formats are sufficient and well-supported.

## References

- [YAML Specification 1.2](https://yaml.org/spec/1.2/spec.html)
- [YAML vs JSON Comparison](https://www.cloudbees.com/blog/yaml-tutorial-everything-you-need-get-started)
- [dbt YAML Files](https://docs.getdbt.com/docs/build/projects)
