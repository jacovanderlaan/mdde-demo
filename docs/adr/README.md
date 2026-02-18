# Architecture Decision Records (ADRs)

This folder contains Architecture Decision Records for the MDDE project. ADRs document significant architectural decisions along with their context and consequences.

## What is an ADR?

An **Architecture Decision Record (ADR)** is a document that captures an important architectural decision made along with its context and consequences.

ADRs help teams:
- **Remember** why decisions were made
- **Onboard** new team members quickly
- **Evaluate** if past decisions still apply
- **Learn** from both successes and failures

## ADR Format

We use a modified version of [Michael Nygard's ADR template](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions):

```markdown
# ADR-{number}: {title}

## Status
{Proposed | Accepted | Deprecated | Superseded by ADR-XXX}

## Context
What is the issue that we're seeing that is motivating this decision or change?

## Decision
What is the change that we're proposing and/or doing?

## Consequences
What becomes easier or more difficult to do because of this change?

## Alternatives Considered
What other options were evaluated?

## References
Links to relevant documentation, issues, or discussions.
```

## ADR Lifecycle

1. **Proposed** - Initial draft for discussion
2. **Accepted** - Approved and implemented
3. **Deprecated** - No longer relevant (but kept for history)
4. **Superseded** - Replaced by a newer ADR

## How MDDE Uses ADRs

In the full MDDE project, we use ADRs to document:

- **Schema Design** - Why specific metadata tables exist
- **Pattern Support** - How Data Vault, Dimensional, etc. are modeled
- **Generator Design** - Multi-dialect generation decisions
- **Tool Integration** - VS Code extension, CLI design
- **Performance** - Optimization strategies

## Sample ADRs

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-001](ADR-001-yaml-over-json.md) | YAML over JSON for Model Files | Accepted |
| [ADR-002](ADR-002-three-layer-modeling.md) | Three-Layer Modeling Approach | Accepted |
| [ADR-003](ADR-003-stereotype-driven-generation.md) | Stereotype-Driven Code Generation | Accepted |

## Creating New ADRs

1. Copy the template from `_template.md`
2. Number sequentially (ADR-XXX)
3. Write the decision and context
4. Submit for team review
5. Update status when accepted

## Tips for Writing Good ADRs

1. **Be specific** - Include concrete examples
2. **Be honest** - Document trade-offs and limitations
3. **Be timely** - Write ADRs when decisions are made, not months later
4. **Be brief** - One page is usually enough
5. **Link related ADRs** - Show how decisions connect

## References

- [Michael Nygard - Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
- [ADR GitHub Organization](https://adr.github.io/)
- [Markdown Any Decision Records (MADR)](https://adr.github.io/madr/)
