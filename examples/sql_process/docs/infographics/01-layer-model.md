# Infographic Brief 01 — The Five-Layer SQL Refactor Pipeline

**Format:** Single-page diagram, landscape A3. Suitable for a slide or a printed handout.
**Audience:** Data engineers and platform leads at ABN-AMRO who write SSF-style SQL today and need to understand what the refactor agent produces.
**Goal:** In 30 seconds the reader sees that every CTE has exactly one concern, and that messy hand-written SQL becomes a self-documenting pipeline.

---

## One-line takeaway

> Each CTE answers one question. The reader can stop at the layer they care about.

---

## Visual concept

A vertical flow from top to bottom, five layers stacked. Each layer is one horizontal band. Inside each band:

1. Left third — layer name + CTE naming pattern, large
2. Middle third — one-sentence concern statement
3. Right third — a minimal SQL snippet (3–5 lines) showing what's typical in that layer

Connections between layers are downward arrows. The visual must communicate "input flows down through the layers; each layer only adds its own concern".

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. SOURCE LAYER                                                        │
│     <table>_prepared / <table>_filtered                                 │
│     "What does this table expose?"                                      │
│     • Bare columns + renames                                            │
│     • Single-table WHERE filters                                        │
│     • Nothing else                                                      │
│                                                                         │
│     WITH customer_prepared AS (                                         │
│       SELECT customer_id, email, country                                │
│       FROM raw.customer                                                 │
│     )                                                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  2. JOINED LAYER                                                        │
│     <entity>_joined                                                     │
│     "How do these tables relate?"                                       │
│     • JOIN clauses                                                      │
│     • Multi-source derivations                                          │
│     • No aggregation, no formatting                                     │
│                                                                         │
│     <entity>_joined AS (                                                │
│       SELECT c.*, o.amount, o.order_date                                │
│       FROM customer_prepared c                                          │
│       LEFT JOIN orders_prepared o ON o.customer_id = c.customer_id      │
│     )                                                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  3. AGGREGATED LAYER                                                    │
│     <entity>_aggregated                                                 │
│     "What's the rollup?"                                                │
│     • GROUP BY + aggregates                                             │
│     • Reads from joined CTE (single-table input)                        │
│     • No JOINs, no derivations                                          │
│                                                                         │
│     <entity>_aggregated AS (                                            │
│       SELECT customer_id, country,                                      │
│              SUM(amount) AS amount_sum,                                 │
│              COUNT(*) AS order_count                                    │
│       FROM <entity>_joined                                              │
│       GROUP BY customer_id, country                                     │
│     )                                                                   │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  4. BRANCH LAYER  (only when UNION ALL is present)                      │
│     <branch_tag> / <entity>_<n>                                         │
│     "Which variant of the result?"                                      │
│     • One CTE per UNION ALL branch                                      │
│     • Names inferred from 'X' AS tag literals                           │
│     • Top-level body becomes:                                           │
│       SELECT * FROM branch_a UNION ALL SELECT * FROM branch_b           │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  5. FINAL LAYER (the top-level SELECT, no CTE)                          │
│     "How is the result formatted?"                                      │
│     • CAST                                                              │
│     • COALESCE / IFNULL                                                 │
│     • CASE WHEN ... THEN ... ELSE 'default' END                         │
│     • String / numeric constants                                        │
│                                                                         │
│     SELECT                                                              │
│       customer_id, country,                                             │
│       CAST(amount_sum AS DECIMAL(18, 2)) AS total_revenue,              │
│       COALESCE(country, 'unknown') AS country_clean,                    │
│       'customer_summary' AS rollup_kind                                 │
│     FROM <entity>_aggregated                                            │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Anchor example to show side-by-side

Print this in a callout panel to the right of the layer stack:

**Before** (180 lines of customer SSF SQL, single SELECT mixing concerns):
```sql
SELECT
  CAST(SUM(app.MarketValueAmountTransactionAmount) AS DECIMAL(25, 5)) AS appraisal_amount,
  MAX(app.AppraisalDate) AS appraisal_date,
  CASE WHEN NOT col.CollateralValuationType IS NULL
       THEN col.CollateralValuationType
       ELSE 'unknown' END AS asset_valuation_type,
  ...23 more projections...
FROM fnriu_p.ssf.app AS app
LEFT JOIN fnriu_p.ssf.col AS col ON col.asset_id = app.appraisal_asset_id
WHERE File_Delivery_Entity = 'GRIPReporter'
  AND ...6 more metadata predicates...
GROUP BY ...11 keys...
```

**After** (auto-refactor):
```sql
WITH app_prepared    AS ( SELECT ... FROM app ),       -- source
     col_prepared    AS ( SELECT ... FROM col ),       -- source
     appraisal_joined    AS ( ...JOIN... ),            -- joined
     appraisal_aggregated AS ( ...GROUP BY... )        -- aggregated
SELECT CAST(...) AS appraisal_amount,
       CASE WHEN ... END AS asset_valuation_type
FROM appraisal_aggregated;                              -- final
```

Caption: *Same query. Same result. Each CTE has one concern. A reader can stop at the layer they care about.*

---

## Colour & typography guidance

- Layer 1 (Source) — light blue. Source-of-truth metaphor.
- Layer 2 (Joined) — purple. The "joining" / weaving metaphor.
- Layer 3 (Aggregated) — orange. Heat / compression metaphor (many rows → few rows).
- Layer 4 (Branch) — green. Branching metaphor.
- Layer 5 (Final) — red/coral. "Output" / "result" metaphor.
- Each layer's CTE-name pattern in monospace.

Use whitespace generously. The point is calmness — these concerns are NOT entangled.

---

## Where to source content

- README §"Layer / concern model" — confirms exact names and order.
- `DECISIONS.md` D46–D49 — rationale per layer.
- `expected_output/agg_customer_summary/optimized.sql` — full worked example.
- `expected_output/union_revenue_breakdown/optimized.sql` — UNION branch case.
- `expected_output/passthrough_with_loans/optimized.sql` — passthrough variant.

---

## ABN-AMRO handover framing

In the handover deck, this infographic answers the question: *"What does the refactor agent actually do to my SQL?"*. Pair it with a small caption: *"This is the canonical output shape. Every transform — pushdown, JOIN extraction, aggregation isolation, UNION lifting — produces a CTE that fits this five-layer model."*
