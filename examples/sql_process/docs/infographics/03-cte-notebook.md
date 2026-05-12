# Infographic Brief 03 — CTE Notebook: Pipeline-Level Mapping in One YAML

**Format:** Single-page diagram, landscape A3. Pairs well with the layer-model brief.
**Audience:** Data governance, lineage tooling owners, and downstream consumers (catalog, dbt, OpenLineage) at ABN-AMRO who need a structured artefact describing the rewritten SQL pipeline.
**Goal:** Show that for every refactored SQL file, the agent emits `mapping.cte.yaml` — a contract-aware, pipeline-level description with CTE-by-CTE intent, source attribution, primary keys, and target columns. Designed to feed into a catalog or governance tool without a human in the loop.

---

## One-line takeaway

> Every refactored query produces a machine-readable pipeline contract. No screen-scraping required.

---

## Why this exists

A pretty CTE structure is useful for human reviewers. But governance tools need MORE than pretty text:

- Which sources does this query depend on?
- What does each CTE produce?
- What's the primary key of the output?
- What's the derivation logic for each output column?
- Who owns the artefact?
- How fresh does the result need to be?

`mapping.cte.yaml` answers all of these as structured data. It's emitted automatically alongside `optimized.sql` whenever `outputs.cte_mapping: true` is set in `sql_process.config.yaml`.

---

## Visual concept

A "before / parallel-after" layout. Left side = the rewritten SQL (compact, syntax-highlighted). Right side = the matching `mapping.cte.yaml` (with arrows linking each CTE to its YAML entry).

```
┌──────────────────────────────────────────┐   ┌───────────────────────────────────────────┐
│  optimized.sql                           │   │  mapping.cte.yaml                         │
│  ──────────────                          │   │  ──────────────────                       │
│                                          │   │                                           │
│  WITH customer_prepared AS (           ◀─┼───┼──● target: agg_customer_summary           │
│    SELECT customer_id, email, country    │   │    version: 1.0.0                         │
│    FROM raw.customer                     │   │    layer: business                        │
│  ),                                      │   │    stereotype: aggregate                  │
│                                          │   │                                           │
│  agg_customer_summary_joined AS (      ◀─┼───┼──● ctes:                                  │
│    SELECT customer_id, email, country,   │   │      - customer_prepared                  │
│           amount, order_date             │   │      - agg_customer_summary_joined        │
│    FROM customer_prepared c              │   │      - agg_customer_summary_aggregated    │
│    LEFT JOIN orders_prepared o ON ...    │   │                                           │
│  ),                                      │   │    sources:                               │
│                                          │   │      - name: raw.customer                 │
│  agg_customer_summary_aggregated AS (  ◀─┼───┼──●     type: table                        │
│    SELECT customer_id, country,          │   │      - name: raw.orders                   │
│           SUM(amount) AS amount_sum,     │   │        type: table                        │
│           COUNT(*) AS order_count,       │   │                                           │
│           MAX(order_date) AS lod         │   │    outputs:                               │
│    FROM agg_customer_summary_joined      │   │      - name: customer_id                  │
│    GROUP BY customer_id, country         │   │        from_sources:                      │
│  )                                       │   │          - raw.customer.customer_id       │
│                                          │   │        derivation: PASSTHROUGH            │
│  SELECT                                  │   │      - name: total_revenue                │
│    customer_id, email, country,        ◀─┼───┼──●     from_sources:                      │
│    CAST(amount_sum AS DECIMAL(18, 2))    │   │          - raw.orders.amount              │
│      AS total_revenue,                   │   │        derivation: AGGREGATE_THEN_CAST    │
│    COUNT(*) AS order_count,              │   │      - ...                                │
│    COALESCE(country, 'unknown')          │   │                                           │
│      AS country_clean                    │   │    contract:                              │
│  FROM agg_customer_summary_aggregated    │   │      owner: <unknown>                     │
│                                          │   │      freshness_minutes: 60                │
│                                          │   │      primary_key:                         │
│                                          │   │        - customer_id                      │
└──────────────────────────────────────────┘   └───────────────────────────────────────────┘
```

The arrows show: each CTE in the SQL maps to one entry in the `ctes` list; each output column gets a row in `outputs` with explicit `from_sources` lineage + a derivation classification.

---

## What's in `mapping.cte.yaml`

| Field | Purpose | Example |
|---|---|---|
| `target` | Entity name the file produces | `agg_customer_summary` |
| `version` | Schema version of the mapping format | `1.0.0` |
| `layer` | dbt/MDDE layer the entity belongs to | `business` |
| `stereotype` | Modelling stereotype | `aggregate`, `dim`, `fact`, `int_consolidated` |
| `ctes` | Ordered list of CTE names in the file | `[customer_prepared, ..._joined, ..._aggregated]` |
| `sources` | Tables / views the pipeline reads from | `[{name: raw.customer, type: table}, ...]` |
| `outputs` | One entry per final-SELECT column | `name`, `from_sources[]`, `derivation` |
| `contract` | Governance metadata | `owner`, `freshness_minutes`, `primary_key[]` |

### Derivation classification

The `derivation` field for each output column is one of a small fixed set:

| Class | Meaning |
|---|---|
| `PASSTHROUGH` | The output is exactly a source column (`customer_id` → `c.customer_id`) |
| `RENAME` | Same value, different name (`amount AS total`) |
| `AGGREGATE` | A pure aggregate (`SUM(x)`) |
| `AGGREGATE_THEN_CAST` | Aggregate wrapped in cast/coalesce/case |
| `EXPRESSION` | Multi-column derivation (`a + b`, `CASE WHEN ...`) |
| `LITERAL` | Hard-coded value (`'PROCESSED' AS status`) |
| `WINDOW` | Window function (`ROW_NUMBER() OVER ...`) |

A governance system can index every output column by derivation class to answer queries like *"show me every aggregate output column that wraps a CAST"* or *"list every PASSTHROUGH from `raw.customer.email`"* — questions that are otherwise impossible without re-parsing SQL.

---

## How `mapping.cte.yaml` differs from `mapping.bfm.yaml`

The pipeline emits two mapping flavours; users pick the one their tooling expects.

| Aspect | `*.bfm.yaml` (Business-Friendly) | `*.cte.yaml` (CTE-Notebook) |
|---|---|---|
| Audience | Business stewards, data analysts | Engineering tooling, governance platforms |
| Granularity | One entity → many source columns | One CTE per row + columns-with-sources |
| Surface area | Compact, human-scannable | Verbose, structured, machine-friendly |
| Use case | Stewardship reviews, audit trails | Catalog ingest, lineage graphs, contract validation |

Both come out of the same parse. The CTE-notebook variant is the deeper, structured representation; the BFM variant is the executive summary.

---

## Where to source content

- `examples/sql_process/sql_process.py` line ~3147 — `emit_cte_mapping()` function (source of truth).
- `sql_process.config.yaml` — `outputs.cte_mapping: true` switch.
- `examples/sql_process/expected_output/<file>/mapping.cte.yaml` — sample outputs (when enabled).

---

## ABN-AMRO handover framing

This infographic answers: *"How does the refactor agent's output plug into our governance / catalog / lineage infrastructure?"*. The answer is `mapping.cte.yaml`: a contract-aware YAML that travels alongside every refactored SQL file, ready to be ingested by Collibra / Datahub / dbt / OpenLineage / a custom catalog without any extra parsing step.

Pair this brief with brief 01 (layer model) and brief 02 (CTE regression) for the full handover narrative:

1. **Brief 01** — what the refactored SQL LOOKS LIKE.
2. **Brief 02** — how to PROVE the refactor preserved semantics.
3. **Brief 03** — how the refactored SQL FEEDS GOVERNANCE.
