# Infographic Brief 02 — CTE Regression: Catching Refactor Bugs Early

**Format:** Single-page diagram, landscape A3 or 16:9 slide.
**Audience:** SQL authors and reviewers at ABN-AMRO who refactor existing queries (the agent's output, a human edit, a Genie-generated variant) and need to prove the new query is equivalent to the old.
**Goal:** Show how `tools/cte-regression/` runs both query versions side-by-side in DuckDB and surfaces every column-level discrepancy in seconds.

---

## One-line takeaway

> Refactor your SQL freely. Prove equivalence in seconds, not weeks.

---

## Why this exists

Refactor work (whether human or agent-driven) is risky because:

- Sample-based testing misses edge-case rows.
- Reviewing 200 lines of pretty CTEs is slower than the original 60 lines of soup.
- Stakeholders won't sign off on "looks right".

**CTE regression analysis** is an objective check: given two query versions, both produce result-sets; the harness diffs them column-by-column on the same input data. If a column matches, ship it. If not, the harness shows exactly which rows and columns disagree.

---

## Visual concept

Two-column comparison flow, with a "DIFF ENGINE" in the middle.

```
┌─────────────────────────────────┐         ┌─────────────────────────────────┐
│        VERSION A                │         │        VERSION B                │
│   (current production SQL)      │         │   (refactored / agent output)   │
│                                 │         │                                 │
│   SELECT                        │         │   WITH app_prepared AS (...),   │
│     CAST(SUM(...) AS ...) AS x, │         │        appraisal_joined AS(...),│
│     CASE WHEN ... END AS y      │         │        appraisal_agg AS (...)   │
│   FROM ssf.app                  │         │   SELECT CAST(...), CASE...     │
│   JOIN ssf.col ON ...           │         │   FROM appraisal_agg            │
│   GROUP BY ...                  │         │                                 │
└─────────────────────────────────┘         └─────────────────────────────────┘
              │                                            │
              │  load both into                            │
              │  DuckDB sandbox                            │
              ▼                                            ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                                                             │
   │              CTE-REGRESSION  DIFF ENGINE                    │
   │              (DuckDB + Pandas + column-level)               │
   │                                                             │
   │     ▶ Run A → result_a (DataFrame)                          │
   │     ▶ Run B → result_b (DataFrame)                          │
   │     ▶ Align rows on the natural key                         │
   │     ▶ Per column:                                           │
   │         match?   delta count   first 5 mismatched rows      │
   │                                                             │
   └─────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
   ┌─────────────────────────────────────────────────────────────┐
   │   COLUMN  │  MATCH RATE  │  ROWS DIFFERING  │  SAMPLE       │
   ├───────────┼──────────────┼──────────────────┼───────────────┤
   │ customer  │    100 %     │        0         │   —           │
   │ amount    │    100 %     │        0         │   —           │
   │ status    │     99.7 %   │       12         │  Pending /    │
   │           │              │                  │  PENDING      │
   │ tier      │    100 %     │        0         │   —           │
   └─────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
        Author fixes the one divergent column;
        re-runs; harness reports 100 % across all columns;
        ships with confidence.
```

---

## Anchor walkthrough (place to the right or below the main diagram)

1. **Author refactors** an existing 200-line query using the SQL refactor agent.
2. **Author opens** `tools/cte-regression/demo.ipynb` in VS Code.
3. **Author pastes** the two query versions into the notebook's `query_a` / `query_b` cells.
4. **Author runs all cells** — DuckDB executes both queries against the sample tables.
5. **Notebook prints** a per-column match-rate table + a sample of mismatched rows.
6. **Author iterates** until every column reports 100 % match.
7. **Author commits** the refactored version with the column-level evidence pasted into the PR description.

The whole loop runs locally in seconds — no Spark, no Databricks cluster, no waiting on CI.

---

## Why DuckDB

- Single `pip install duckdb pandas` — works on a developer laptop.
- SQL dialect close enough to Databricks SQL that 95 % of customer queries run unchanged.
- Returns Pandas DataFrames so the per-column diff is a one-liner.
- Test data lives in the notebook (CREATE TABLE … VALUES (…), …) so the harness is fully reproducible.

For production-scale validation, the same flow runs on a Databricks cluster via `tools/databricks/cte_regression/`. The DuckDB notebook is the fast feedback loop; the Databricks version is the final gate.

---

## What "column-level" means

Most diff tools say "X rows in A are not in B". That's coarse — it doesn't tell you WHICH column drifted. CTE regression goes deeper:

| Granularity | Question answered | Where used |
|---|---|---|
| Row count | "Same number of rows?" | Smoke test |
| Row hash | "Same rows present?" | Quick equality check |
| **Column-by-column** | **"Which COLUMN drifted, and on which rows?"** | **CTE regression** |

When column `status` has 99.7 % match rate, the harness shows the 12 rows that disagree and the specific values: `Pending` vs `PENDING` — a casing bug in a `COALESCE`, not a logic error. Author fixes it in one line.

---

## Where to source content

- `tools/cte-regression/README.md` — installation + run instructions.
- `tools/cte-regression/demo.ipynb` — the notebook itself.
- `tools/databricks/cte_regression/` — production-scale variant.

---

## ABN-AMRO handover framing

This infographic answers: *"How do I trust the refactor?"*. Pair it with the layer-model infographic to show the full story — **agent restructures the SQL → regression harness proves the result didn't drift → ship with confidence**. Without the regression step, every refactor is a guess; with it, every refactor is a verified equivalence.
