# sql_process — folder of SQL in, optimised SQL + mapping out

A standalone, sqlglot-only pipeline (no DuckDB, no metadata DB) that
reads every `*.sql` file in a folder and produces:

- **Optimised SQL** — quality-checked, auto-fixed where safe, and
  format-normalised, with the original `-- @mdde-*` annotation header
  preserved at the top
- **Mapping metadata** — two flavours per file:
  - `*.bfm.yaml` — Business-Friendly Mapping (entity-level, target ←
    sources, with derivation classification and tags)
  - `*.cte.yaml` — CTE-notebook style (pipeline-level, contract-aware,
    with primary-key extraction)
- **Annotations** — `*.entity.yaml` per file, the SQL-First metadata
  promoted out of the SQL into a YAML representation
- **Lineage** — a single `lineage.json` OpenLineage roll-up across
  all files
- **Report** — `report.md` summarising files processed, quality
  findings, auto-fixes, and mapping coverage

The script is the **lite, demonstrable** counterpart to the full
DuckDB-backed parsers and optimisers in the private MDDE framework.
It runs anywhere Python + sqlglot run — laptop, CI, or a Databricks
notebook.

---

## Files in this folder

| File | Purpose |
|---|---|
| `sql_process.py` | Main pipeline. Run as a CLI. |
| `_optimizer.py` | Vendored quality-check rule set (no DuckDB). |
| `_determinism.py` | Vendored determinism checker (no DuckDB). |
| `sql_process_databricks.py` | Databricks notebook (source format). |
| `input/` | 7 sample SQL files exercising the surface. |
| `input/_metadata.yaml` | Optional source-table schema for the qualify pass. |
| `expected_output/` | Snapshot of what the script produces from `input/`. |
| `README.md` | This file. |

The two underscore-prefixed files are **vendored copies** of
`src/mdde_lite/optimizer.py` and `src/mdde_lite/determinism.py` with
the DuckDB persistence layer stripped. They're committed alongside
`sql_process.py` so the four code files together are self-contained:
drop them anywhere — laptop folder, Databricks Workspace folder,
S3 prefix, anywhere — and the script runs without external paths
or imports beyond `sqlglot` and `pyyaml`.

If `mdde_lite/optimizer.py` upstream evolves, re-vendor by copying
those two files into this folder and removing the `duckdb` /
`from .schema` / `analyze_file` / `analyze_directory` bits.

---

## Usage — laptop / CI

```bash
pip install sqlglot pyyaml

# Top-level folder. Auto-uses input/_metadata.yaml if present.
python sql_process.py input/ --out output/

# Recurse into subdirectories (preserves folder structure in output)
python sql_process.py path/to/sql_corpus --out output/ --recursive

# Override the metadata file (point at a shared schema YAML)
python sql_process.py input/ --out output/ --metadata path/to/schemas.yaml

# Diff mode — compare current run against a previous snapshot
python sql_process.py input/ --out output/ --diff path/to/previous_output/
```

Works from any current working directory — the script locates its
sibling vendored modules via `__file__`, not via the CWD.

---

## Usage — Databricks notebook

Open `sql_process_databricks.py` in a Databricks Workspace. It's a
notebook in source format with eight cells:

1. `%pip install sqlglot pyyaml`
2. Locate the module folder (defaults to the notebook's own folder)
3. Configure widgets: `input_dir`, `output_dir`, `recursive`
4. Run `process_folder(...)`
5. Render `report.md` inline via `displayHTML(...)`
6. Browse the generated artefact tree
7. Open one mapping YAML
8. Open the OpenLineage roll-up

### Getting the four files into Databricks

Three options, in order of ease:

1. **Repos**: clone `mdde-demo` via the Repos UI; navigate to
   `examples/sql_process/`. The notebook + vendored modules are
   already side-by-side.
2. **Workspace upload**: upload `sql_process.py`, `_optimizer.py`,
   `_determinism.py`, and `sql_process_databricks.py` into the same
   Workspace folder. Open the `_databricks.py` file — Databricks
   recognises the `# Databricks notebook source` magic and renders it
   as a notebook.
3. **Volumes / DBFS**: place the files under
   `/Volumes/<catalog>/<schema>/<volume>/sql_process/` and edit the
   `module_dir` cell to point there.

### Path conventions in Databricks

The pipeline uses plain `pathlib.Path` so any of these work as
`input_dir` or `output_dir`:

- `/Volumes/main/default/code/sql_inputs` — Unity Catalog volumes
- `/Workspace/Users/me@example.com/sql_inputs` — Workspace files (DBR 14+)
- `/dbfs/mnt/data/sql_inputs` — DBFS-mounted storage
- `/tmp/sql_outputs` — local cluster scratch (ephemeral)

---

## Output structure

```
output/
├── optimized/          # rewritten SQL, header annotations re-attached
│   └── <filename>.sql
├── mapping/
│   ├── <filename>.bfm.yaml
│   └── <filename>.cte.yaml
├── annotations/
│   └── <filename>.entity.yaml
├── lineage.json        # OpenLineage events + cross-file stitching block
├── report.md           # run summary (incl. cross-file Mermaid graph)
└── diff.md             # only when --diff is passed; summary vs previous run
```

In `--recursive` mode, the same subfolder structure is preserved
inside `optimized/`, `mapping/`, and `annotations/`.

---

## What gets parsed

### SQL-First annotations (header)

```sql
-- @mdde-entity: customer_revenue
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Customer revenue rollup
```

### Column-level annotations (trailing comments)

| Annotation | Meaning |
|---|---|
| `@pk` | primary-key column |
| `@business_key` | business key (natural key) |
| `@fk(table.column)` | foreign-key reference |
| `@pii` | personally-identifiable information |
| `@nullable` | column may be NULL |
| `@derived` | column is computed, not raw |
| `@scd2_from`, `@scd2_to`, `@scd2_current` | SCD2 tracking columns |
| `@decimal(p,s)` | decimal precision + scale |

Used like:

```sql
SELECT
    customer_id,            -- @pk @business_key
    email,                  -- @pii
    total_amount,           -- @decimal(18,2)
    ROW_NUMBER() OVER ()    AS rn   -- @derived
FROM ...
```

---

## Metadata input (optional, but recommended)

The script accepts an optional metadata YAML file that describes
the source-table schemas. When provided, it runs sqlglot's
`qualify()` pass on every parsed file. That gives three concrete
wins over the no-metadata path:

1. **`SELECT *` is expanded** into the explicit column list, so the
   mapping output for `SELECT * FROM joined` becomes a per-column
   lineage record instead of a single `*` row.
2. **Bare column references** like `email` or `total_amount` are
   resolved to their owning table (`c.email`, `o.total_amount`),
   so the `(table, column)` pairs in the mapping are accurate
   rather than guessed from the closest alias.
3. **Missing-column errors surface early** — a column referenced
   in the SQL but absent from the metadata appears as a qualify
   skip in `report.md`, pointing at the gap.

### Format

```yaml
# input/_metadata.yaml
sources:
  landing.crm_customers_export:
    customer_id: BIGINT
    email: VARCHAR
    name: VARCHAR
  raw_customers:
    customer_id: BIGINT
    email: VARCHAR
    name: VARCHAR
```

- Top-level key is `sources:`, mapping table name → column dict
- Table names may be qualified (`catalog.db.table` or `db.table`)
  or bare (`raw_customers`); the script normalises mixed depths
  for sqlglot
- Column types are dialect-agnostic — sqlglot uses them for name
  resolution, not type validation. Whatever your team writes works.

### How the script finds the file

In order of precedence:

1. Explicit `--metadata <path>` argument
2. `<input_dir>/_metadata.yaml` if it exists
3. No metadata; script runs without qualification

When no metadata is found, the script still produces all output
artefacts — they just have the limitations the previous section
documents (`SELECT *` rows, less precise source column attribution).

### Per-file fail-soft

`qualify()` is opinionated. If two source tables both have
`customer_id` and a SELECT references it bare, qualify refuses
(genuinely ambiguous). When that happens for a single file:

- The file's lineage extraction falls back to the unqualified AST
- The reason appears in `report.md` under "Qualify skipped"
- Other files keep being processed normally

Reading `report.md`'s Qualify-skipped table is the fastest way to
diagnose what's missing from the metadata file.

### Where the metadata file should come from

Three reasonable sources, in increasing rigour:

- **Hand-authored** for a small input set — what `input/_metadata.yaml` does today
- **Extracted from `INFORMATION_SCHEMA`** of your warehouse — a few lines of SQL
- **Reuse this script's `annotations/*.entity.yaml` outputs** — run on the upstream files first, then assemble their entity YAMLs into a metadata file for the dependent files. (Auto-import of those entity YAMLs as input is on the roadmap; today it's a small manual step.)

---

## Cross-file lineage stitching

When one file in the input set produces an entity that another file
reads, the script automatically records a producer→consumer edge.
Concretely: if file A has `@mdde-entity: stg_customers` (or simply
`CREATE VIEW stg_customers`) and file B contains
`FROM stg_customers`, the script adds an edge `A → B` to the run.

The stitching surfaces in two places:

1. **`lineage.json`** gets a new `stitching` block with an edge
   list — downstream OpenLineage consumers can pre-link the
   per-file events without inferring connections themselves.
2. **`report.md`** gains a "Cross-file lineage" section with an
   edge table plus a Mermaid flowchart grouping nodes by layer
   (`source` → `staging` → `integration` → `business`) and
   showing external sources hanging off the upstream layer.

The matching logic uses **entity name** as the join key. A file's
`@mdde-entity` annotation (or the SQL filename if no annotation is
present) becomes its produced-table identifier. The `entity_name`
appears both as the OpenLineage `outputs.name` for that file and is
matched against every other file's `source_tables` list.

In the sample input set, the chain is:

```
landing.crm_customers_export  -> raw_customers
                                  └-> stg_customers
                                       ├-> customer_revenue (clean)
                                       ├-> customer_revenue (bad)
                                       ├-> customer_segment_analytics
                                       └-> customer_latest_orders
landing.oms_orders_export    -> raw_orders
                                  ├-> customer_revenue (clean / bad)
                                  ├-> customer_segment_analytics
                                  └-> customer_latest_orders
```

Nine cross-file edges across seven files, plus two external sources.

### Limits of the matcher

- **Bare-name matching only.** A file declaring entity
  `customer_revenue_clean` is matched against a `FROM
  customer_revenue_clean` reference. Schema-qualified writes
  (`gold.customer_revenue_clean`) are normalised to their last
  segment so they match the entity name.
- **Duplicate entity names** across files (rare; usually a
  copy-paste bug) cause the later file to win as the producer.
  The earlier file becomes orphaned in the graph. Surface this by
  reading the entity column in `report.md`'s file table.
- **Recursive references** are not specially handled — a file
  reading itself is silently skipped.

---

## Diff mode

Re-run the script on a folder you've processed before and compare
against the previous snapshot:

```bash
# First run
python sql_process.py input/ --out v1_output/

# ...analyst tweaks SQL files...

# Second run, diffing against v1
python sql_process.py input/ --out v2_output/ --diff v1_output/
```

`v2_output/diff.md` summarises what changed between the two runs:

- **Optimised SQL** — added/removed/modified files
- **Mapping (BFM)** — per-target column changes (added/removed/derivation flip/source-set change)
- **Cross-file stitching** — added/removed producer→consumer edges

Sample output:

```
## Mapping (BFM)

**Modified mappings:**

### `customer_revenue.bfm.yaml`

- + added column `revenue_per_order`

## Cross-file stitching

_No changes._
```

The mode is **report-only**: it tells you what changed; it doesn't
generate patches or attempt to "apply" a diff. Pair with Git for
full history (commit each output snapshot, then `git diff` between
revisions does the same job at file level — diff mode is the
domain-aware view on top).

When the previous-output folder doesn't exist, `diff.md` says so
and the run continues normally.

---

## Quality checks (re-using `mdde_lite/optimizer.py`)

The 20 checks already shipped in the educational edition fire on
each file:

| Class | Examples |
|---|---|
| **Errors** | `WINDOW_NO_ORDER`, `MISSING_GROUP_BY`, `UNION_COLUMN_MISMATCH` |
| **Warnings** | `SELECT_STAR`, `CARTESIAN_JOIN`, `OR_IN_JOIN`, `DISTINCT_STAR` |
| **Info** | `MISSING_ALIAS`, `WHERE_1_EQUALS_1`, `LEADING_WILDCARD`, `HARDCODED_DATE` |

### Auto-fixes

Conservative — only rewrites that preserve semantics on every
engine. Currently:

- `WHERE 1=1 AND ...` → `WHERE ...`
- Format normalisation via `sqlglot.transpile(pretty=True)`

`ROW_NUMBER()` without `ORDER BY` is **not** auto-fixed — we can't
infer the right ordering key. Flagged as an error in the report.

---

## What's in `input/` (sample corpus)

Seven files exercising the surface area:

| File | Shape | Stresses |
|---|---|---|
| `raw_customers.sql` | source view | `@pii`, `@nullable` annotations |
| `raw_orders.sql` | source view | `@fk()` foreign-key annotation |
| `stg_customers.sql` | staging view | type casts + cleansing |
| `customer_revenue.sql` | clean fact | multi-CTE chain, GROUP BY, INNER JOIN |
| `customer_revenue_bad.sql` | broken fact (deliberate) | 6 planted optimiser issues |
| `customer_segment_analytics.sql` | complex fact | scalar subqueries, ROW_NUMBER, CASE-based segmentation, LEFT JOIN |
| `customer_latest_orders.sql` | dedup fact | window-based per-customer dedup, derived computation |

Run the script against `input/` and the resulting `expected_output/`
shows the full output shape. Both folders are committed for
diff-ability.

---

## Speaker walkthrough (optional team demo, 20–30 min)

If you want to run this in a meeting:

### Block 1 — The problem (3 min)

> "How do you turn a folder of legacy SQL into something a data
> catalogue can index? Today: open every file, read it, write a
> Confluence page. Or run a vendor extractor and accept whatever
> shape it produces."

### Block 2 — The mechanism (10 min)

Run:

```bash
python sql_process.py input/ --out demo_output/
```

Walk through the printed output:

```
Processed 7 file(s) -> demo_output
  optimized/   7 SQL files
  mapping/     14 YAML files (BFM + CTE shapes)
  annotations/ 7 entity YAML files
  lineage.json OpenLineage roll-up
  report.md    Run summary
```

Open `demo_output/report.md` first — that's the executive view.
Show the per-file table, the quality-findings rollup, the
mapping-coverage percentage.

Then open `demo_output/mapping/customer_segment_analytics.bfm.yaml`
side-by-side with the input SQL. Point at:

- Each output column traced back to source columns
- Derivation classifier (`direct` / `aggregate` / `expression` /
  `constant`)
- Annotation tags propagated (`@pii` columns flagged as `tags: [pii]`)

### Block 3 — Auto-fixes + round-tripping (5 min)

Open `demo_output/optimized/customer_revenue_bad.sql`. Show the
header annotation block was preserved. Show the `WHERE 1=1` was
removed. Show the `ROW_NUMBER() OVER ()` is **still there** — flagged
in `report.md`, not silently rewritten.

The point: the script is **opinionated where it's safe**, and
**explicit where it's not**.

### Block 4 — Recursive on the real corpus (5 min)

```bash
python sql_process.py "C:/Repos/mdde/workspace/exchange/in/dialect/standard/code" \
       --out big_output -r
```

The mdde framework ships >1000 SQL test fixtures across CTEs,
subqueries, joins, windows, aggregations. The script handles them
all in one pass. Show the size of `big_output/` and a sample
mapping from `big_output/mapping/ctes/`.

### Block 5 — Q&A (5 min)

Common questions:

**Why two mapping shapes?** Different consumers — BFM is for
business-glossary tooling, CTE-notebook shape is for the cte_notebook
authoring framework. Both come from the same parse.

**Where's the heavy optimiser?** This is the lite edition. The
private MDDE framework has DuckDB-backed planners (`filter_pushdown`,
`projection_pushdown`, `prune_unused_cte_columns`) that need a
metadata DB. This script trades depth for portability.

**Can I extend the annotations?** Yes — add a flag to
`_COLUMN_ANNOTATION_FLAGS` in `sql_process.py`; the entity YAML
emitter will pick it up automatically.

---

## Limitations (honest)

- Annotations are extracted lexically from comments. A column
  declared without an annotation is silent — there's no inference.
- Auto-fix set is small on purpose. The script flags rather than
  rewrites whenever an engine could behave differently.

**Closed earlier:**

- `SELECT *` projections used to produce empty mappings. The
  `--metadata` schema input + sqlglot's `qualify()` pass solve
  this — `*` is expanded into the explicit column list before
  lineage extraction runs.
- Lineage used to stop at the file boundary. The script now
  builds a cross-file graph: when one file's source matches
  another's entity name, an edge is recorded in `lineage.json`
  under `stitching.edges` and rendered as a Mermaid diagram in
  `report.md`. Multi-hop chains (raw → staging → business) are
  visible end-to-end.

For deeper coverage, use the full MDDE framework's parser/optimiser
chain (DuckDB-backed, ADR-094 onward).

---

## Why this exists

The MDDE methodology has separate stories for **annotation
extraction** (`sql_first` module), **SQL parsing** (the parser
module), **quality checks** (mdde-lite optimiser, full optimiser
in mdde), **mapping** (BFM + CTE-notebook), and **lineage**
(OpenLineage exporter). All five live in different repos and CLIs.

This script is the **single-command demonstration** that all five
hang together. Hand it to a prospect, point them at their own SQL
folder, and they see the unified output in one run.
