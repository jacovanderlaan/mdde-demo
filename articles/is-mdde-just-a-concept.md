# Is MDDE Just a Concept?

**A Minimal Demo You Can Run Today**

Over the past months I've written extensively about Metadata-Driven Data Engineering (MDDE):

- Extracting structure from SQL
- Making lineage explicit
- Separating observation from enforcement
- Generating deterministic SQL
- Treating metadata as executable architecture

A recurring question I received was simple and fair:

> Is MDDE just a concept — or is it something we can actually try?

The answer is:

**It's real.**

And today I'm publishing a minimal public demo you can run yourself.

🔗 **GitHub**: https://github.com/jacovanderlaan/mdde-demo

---

## Why a Demo?

The full MDDE framework I'm building and using in migration and production contexts is larger and more advanced than what I want to publish publicly.

It includes:

- Advanced optimizer passes
- Structured diagnostics pipelines
- Deterministic SQL generation
- Multi-dialect DDL rendering
- Governance and compliance scoring
- Enterprise model import capabilities

But instead of open-sourcing everything prematurely, I decided to publish a focused, educational subset.

**The goal of the demo is not completeness.**

**The goal is clarity.**

---

## What the Demo Shows

The mdde-demo repository demonstrates the core MDDE idea:

> SQL is necessary. But SQL alone hides structure.

The demo includes:

- A simplified metadata schema
- SQL → structured metadata parsing
- Basic optimizer diagnostics
- Canonical SQL regeneration
- A small example model

In other words:

You can see how SQL is decomposed into:

- **Entities**
- **Attributes**
- **Relationships**
- **Attribute mappings** (lineage)

And how that metadata can be queried, inspected, and regenerated.

---

## The Core Workflow

The demo illustrates a simplified version of the MDDE loop:

```
SQL
  ↓
Parse
  ↓
Metadata
  ↓
Analyze
  ↓
Regenerate SQL
```

You can:

1. Provide example SQL files
2. Run the parser
3. Inspect the generated metadata
4. Run basic diagnostics
5. Generate canonical SQL output

It is intentionally lightweight.

But it is real.

---

## What It Does Not Include

The demo is not the full engine.

It does not include:

- Full optimizer pipeline (function extraction, filter normalization, seed detection, etc.)
- Advanced CTE normalization and UNION handling
- Complete metadata schema (60+ tables)
- Compliance scoring framework
- Enterprise importers (PowerDesigner, erwin, etc.)
- Advanced dialect-aware DDL generation

Those capabilities are part of the broader framework and ongoing work.

The demo focuses on the essential mechanism:

**SQL → structured metadata → controlled regeneration.**

---

## Why Not Open Source Everything?

Two reasons:

1. The full framework is still evolving rapidly.
2. It forms part of my workshop, training, and consultancy work.

Publishing a minimal, stable core allows experimentation and discussion without freezing the architecture prematurely.

I believe that is a healthier approach than releasing a half-finished "full" engine.

---

## What You Can Learn From the Demo

Even in simplified form, the demo demonstrates something important:

> Architecture does not have to live in diagrams.
> It can live in data.

Once SQL is parsed into structured metadata:

- **Lineage** becomes queryable
- **Structure** becomes measurable
- **Diagnostics** become systematic
- **Refactoring** becomes explainable

That is the foundation of MDDE.

---

## Try It

If you're curious:

1. Clone the repo
2. Run the demo scripts
3. Inspect the metadata tables
4. Modify the example SQL
5. Re-run and observe the changes

And ask yourself:

> What happens if your entire SQL platform were structured this way?

---

## Where This Is Going

The demo is not the end.

It's the visible tip of a deeper engine.

Upcoming articles will continue covering:

- Optimizer internals
- Deterministic SQL generation
- Metadata schema design
- Governance and CI gating
- Migration use cases

**MDDE is not about replacing SQL.**

**It's about making the architecture behind SQL explicit.**

---

If you try the demo, I'd genuinely love to hear your feedback — especially where it feels incomplete or too rigid.

That's where the most interesting conversations usually start.

---

*Published on Medium: [@jaco.vanderlaan](https://medium.com/@jaco.vanderlaan)*
