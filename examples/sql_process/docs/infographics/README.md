# Infographic briefs

Source material for designer-ready infographics describing the
`sql_process` pipeline. Each brief is a single markdown file
describing one infographic — concept, audience, key takeaways, visual
layout, anchor examples, and source content references.

Use these for slide decks, customer handovers, or commissioned
visuals (Figma / external designer).

## Index

| # | Brief | Audience |
|---|---|---|
| 01 | [Layer model](01-layer-model.md) | Anyone touching the refactored SQL — engineers, analysts, reviewers |
| 02 | [CTE regression](02-cte-regression.md) | SQL authors + reviewers who need to prove refactor equivalence |
| 03 | [CTE notebook (mapping.cte.yaml)](03-cte-notebook.md) | Governance, catalog ingest, dbt / OpenLineage tooling |

## Suggested handover deck flow

1. **Brief 01** — Show the canonical refactored SQL shape. *What the agent produces.*
2. **Brief 02** — Show how to prove the refactor is correct. *How we trust it.*
3. **Brief 03** — Show how the artefact feeds governance. *How it integrates.*

Together: shape → verification → integration.

## Conventions

Each brief follows the same outline:

- **Format** — paper size or slide aspect ratio.
- **Audience** — who sees this and what they bring.
- **Goal** — what the reader takes away in 30 seconds.
- **One-line takeaway** — the headline.
- **Visual concept** — ASCII mockup + colour/typography guidance.
- **Anchor example** — concrete snippet showing before/after or a worked case.
- **Where to source content** — code / doc references for the designer.
- **ABN-AMRO handover framing** — how the brief lands in the handover deck.
