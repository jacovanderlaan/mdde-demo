# sql_process — rules, optimizations, and Genie instruction mapping

This file is the rule reference for the `sql_process` script. For
each best-practice / quality check / auto-fix / SQL rewrite the
script applies, it shows:

- **What the rule detects or transforms**
- **Severity** (when applicable)
- **Whether the script auto-fixes it**
- **Which Genie instruction (if any) reflects the rule in the
  `*.genie.md` prompt**

The Genie prompt is derived from the **original** query's AST, so
it captures the analyst's intent — not the optimised rewrite.

---

## 1. SQL rewrites (auto-fix transforms)

These actively change the optimised SQL. They run in the order
listed below.

### 1.1 `WHERE 1=1` removal

| | |
|---|---|
| **Rule** | `WHERE_1_EQUALS_1` |
| **Severity** | info |
| **Auto-fixed** | yes |
| **Trigger** | `WHERE 1=1 AND ...` or `WHERE 1=1\n` |
| **Action** | strips the `1=1` predicate; keeps the rest of the WHERE intact |
| **Why** | placeholder used during interactive SQL authoring; ships to production by accident |
| **Genie instruction** | none — the prompt's `Filters:` section already excludes the placeholder because it walks the original AST and never emits `1=1` |

### 1.2 Subquery → CTE lifting

Three shapes are lifted into named CTEs at the top of the query.

#### 1.2.1 Derived table in FROM/JOIN

| | |
|---|---|
| **Trigger** | `FROM (SELECT ...) AS x` or `JOIN (SELECT ...) AS y` |
| **Action** | move the inner SELECT into `WITH x AS (...) WITH y AS (...)`; outer FROM/JOIN references the CTE by name |
| **CTE name** | the existing alias (`x`, `y`); falls back to `_sub1`, `_sub2` if no alias |
| **Genie instruction** | the original derived table appears in the `Joins:` section verbatim. Example: `LEFT JOIN \`(SELECT customer_id, MAX(order_date) AS last_order_date FROM raw_orders WHERE order_status = 'SHIPPED' GROUP BY customer_id) AS r\` on \`r.customer_id = c.customer_id\`` |

#### 1.2.2 Scalar subquery in SELECT

| | |
|---|---|
| **Trigger** | `SELECT ..., (SELECT MAX(t) FROM o) AS m FROM ...` |
| **Action** | inner SELECT becomes a CTE; outer projection rewrites to `(SELECT value FROM <cte>)` so the slot still receives a scalar |
| **CTE name** | `_sub1`, `_sub2`, ... |
| **Constraint** | only fires for single-column scalar subqueries; complex inner projections trigger a `SUBQUERY_NOT_LIFTED` finding instead |
| **Genie instruction** | shows up under `Return columns:` as the full subquery. Example: `\`max_order_global\` = (SELECT MAX(raw_orders.total_amount) AS _col_0 FROM raw_orders AS raw_orders)` |

#### 1.2.3 UNION wrapped in FROM

| | |
|---|---|
| **Trigger** | `FROM ((SELECT ...) UNION ALL (SELECT ...)) AS x` |
| **Action** | the wrapped UNION moves into a single CTE; outer FROM references it |
| **Genie instruction** | the UNION appears in the `Joins:` section as part of the source description |

#### 1.2.4 Not lifted (with finding)

| | |
|---|---|
| **Rule** | `SUBQUERY_NOT_LIFTED` |
| **Severity** | info |
| **Auto-fixed** | n/a (intentional skip) |
| **Skipped shapes** | correlated subqueries (any column references an outer scope) — the outer reference would no longer resolve from inside a CTE; predicate subqueries (`WHERE id IN (SELECT ...)`, `WHERE EXISTS (...)`, comparison subqueries) — the rewrite would require synthesising a join with DISTINCT and may change row counts |
| **Genie instruction** | the skipped subquery appears verbatim in the relevant `Filters:` or `Return columns:` line so Genie sees the same predicate logic the analyst wrote |

### 1.3 Single-table projection pushdown

| | |
|---|---|
| **Trigger** | the outer SELECT references a base table (not a CTE) and at least one projection or WHERE predicate uses **only** that one table's columns |
| **Action** | create a `<table>_proj` CTE containing the picked columns, renames, single-table derivations, and single-table WHERE predicates. Outer SELECT references `<table>_proj <alias>` and uses the new column names. |
| **CTE name** | `<table>_proj` (e.g. `stg_customers_proj`, `raw_orders_proj`); appends `_2`, `_3` on collision |
| **Pushed expressions** | direct column refs, renames (`AS new_name`), single-column derivations (`UPPER(c.email)`, `c.amount * 0.85`, `CAST(...)`), single-table multi-column CASE/AND expressions |
| **Not pushed** | aggregates (`SUM/COUNT/AVG/...`), window functions (`OVER (...)`), expressions containing subqueries or `*`, expressions with unqualified columns (ambiguous in multi-table query), cross-table expressions |
| **Identity skip** | if a source has nothing to push (no renames, no derivations, no single-table filters), no CTE is created — the source stays inline |
| **CTE source skip** | if the outer FROM references an existing CTE in this file's WITH, pushdown leaves it alone |
| **Genie instruction** | the prompt is derived from the ORIGINAL query, so it shows the analyst's original projections and filters — not the pushed-down rewrite. This is intentional: Genie should see the intent, not the optimisation. |

---

## 2. Anti-pattern quality checks

These detect issues but don't auto-fix. Findings appear in
`report.md`.

| Rule | Severity | Trigger | Genie instruction |
|---|---|---|---|
| `SELECT_STAR` | warning | `SELECT *` | shown as `- all columns` in `Return columns:`. When metadata is present and `qualify()` expanded the star into explicit columns, each appears as its own bullet. |
| `MISSING_ALIAS` | info | table without alias in a multi-table query | column refs in the prompt use whatever qualifier the original used (table name or alias); MISSING_ALIAS doesn't change the prompt |
| `ORDER_BY_NUMBER` | warning | `ORDER BY 1` instead of column name | the `Sort:` section uses the original token (`1 ASC/DESC`) — Genie should infer the column from context |
| `IMPLICIT_JOIN` | warning | `FROM a, b` (comma join) | the `Joins:` section shows whatever JOIN syntax the AST captured; implicit joins surface here as multiple table entries |
| `DISTINCT_STAR` | warning | `SELECT DISTINCT *` | `Return columns:` shows `- all columns`; DISTINCT marker visible in the goal line if the AST preserved it |
| `CARTESIAN_JOIN` | warning | `JOIN` without `ON` | the `Joins:` line shows the JOIN with no `on \`...\`` clause — Genie can see the cartesian intent (or accident) |
| `DUPLICATE_COLUMN` | warning | same column selected twice | both projections appear in `Return columns:` |
| `NESTED_SUBQUERY` | info | 3+ levels of nested subqueries | the nested structure appears literally in the relevant section (Joins/Return columns/Filters) — Genie sees the depth |
| `UNION_COLUMN_MISMATCH` | error | UNION with different column counts on each side | shown as part of the source / join description |
| `LEADING_WILDCARD` | info | `LIKE '%...'` | appears in `Filters:` as `<col> LIKE '%...'` — Genie can recognise the SARGability issue |
| `FUNCTION_IN_WHERE` | info | `WHERE UPPER(col) = ...` | appears in `Filters:` |
| `OR_IN_JOIN` | warning | `JOIN ... ON a = b OR c = d` | the OR predicate appears in the `Joins:` line's `on \`...\`` clause |
| `HARDCODED_DATE` | info | `'2026-01-01'` style literal | appears in `Filters:` as part of the predicate literal |
| `MISSING_GROUP_BY` | error | aggregate mixed with non-aggregated columns, no GROUP BY | the aggregate appears in `Return columns:` but no `Group by:` section — Genie can detect the mismatch |

---

## 2b. Customer rule-pack checks

These implement the customer's consolidated migration rule pack
(see [CUSTOMER_RULES.md](CUSTOMER_RULES.md) for the authoritative
8-rule structure). Each check maps to one of the customer's 8
numbered rules.

| Rule | Severity | Customer rule | Trigger | Auto-fix |
|---|---|---|---|---|
| `LEGACY_SCHEMA` | warning | 1 | Table reference uses a legacy schema (e.g. `bodm`, `csz`, `hz`, `cz`) | yes (opt-in via `--legacy-schemas`) |
| `LEGACY_DATE_VARIABLE` | warning | 2.2 | Source contains `{reporting_date}` template variable | yes (rewritten to `{process_date}`) |
| `BETWEEN_FOR_SCD2` | warning | 2 | `BETWEEN` predicate on SCD2 columns (`_valid_from`/`_valid_to`/`start_dts`/`end_dts`) | no — manual rewrite to explicit `>=`/`<` predicates |
| `OBSOLETE_CTE` | warning | 3 | CTE name matches obsolete blacklist (`extract_dates`, `create_timeline`, `finalize_timeline`) | yes — CTE removed when no downstream reference remains |
| `METADATA_COLUMN_EXPOSED` | warning | 4 | Output projection includes a metadata column (`snapshot_date`, `insert_dts`, `update_dts`, `current_flag`, `delete_flag`, `delta_flag`, `create_timestamp`, `start_dts`, `end_dts`) | no — manual review |
| `UNUSED_LEFT_JOIN` | info | 5 | `LEFT JOIN` contributes no columns to the outer SELECT (referenced only in its own ON clause) | no — verify intent before removing |
| `INLINE_CAST_IN_JOIN` | warning | 7 (Pre-Processed Joins) | `CAST(...)` inside a JOIN ON clause | no — materialise the transformed key in a CTE |
| `LITERAL_IN_JOIN` | info | 7 (Pre-Processed Joins) | String/integer literal inside a JOIN ON clause | no — move literal into a CTE filter |
| `DERIVATION_IN_WHERE` | info | 7 (No Derivations in WHERE) | Function call or `IS [NOT] NULL` on a raw column inside WHERE | no — pre-compute a Boolean column |
| `INLINE_UNION_TRANSFORM` | warning | 7 (Pure UNION ALL) | UNION ALL branch contains a CAST / function / WHERE | no — move transforms to per-source CTEs |
| `COMBINED_SOURCE_FILTERS` | info | 7 (Per-Source Filtering) | WHERE clause references columns from ≥ 2 sources | no — split into per-source initial CTEs |
| `GROUPBY_NOT_ISOLATED` | info | 7 (Aggregation Separate) | SELECT has `GROUP BY` + `WHERE` + JOINs together | no — split into filtered_data + aggregated_data CTEs |
| `DISTINCT_WITHOUT_JUSTIFICATION` | info | 6 (Avoid DISTINCT) | `SELECT DISTINCT` without a justifying comment | no — replace with explicit deduplication or add justification comment |
| `UNION_MISSING_SOURCE_TAG` | info | 7 (UNION source-tag) | UNION ALL branch has no literal string projection identifying the source | no — add `'<SOURCE_NAME>' AS source_ind` |
| `FULL_OUTER_WITH_COALESCE` | warning | 7 (LEFT JOIN/IS NULL pattern) | `FULL OUTER JOIN` combined with `COALESCE(a.x, b.x)` projections | no — restructure as `unique_rows_from_a` + `unique_rows_from_b` + `matching_rows` CTEs |
| `NON_DESCRIPTIVE_CTE_NAME` | info | 7 (Descriptive Naming) | CTE name matches a non-descriptive list (`t1`, `cte1`, `tmp`, `foo`, etc.) | no — rename per customer convention |
| `PK_DEDUP_CHECK_MISSING` | info | (user-added) | `@pk` annotations present but no `ROW_NUMBER() OVER (PARTITION BY <pk>)` for dedup verification | no — manually add the dedup-check columns |

Configurable via CLI flags:
- `--legacy-schemas <comma_list>` — populates the `LEGACY_SCHEMA`
  blacklist; off by default
- `--replacement-schema <name>` — what `LEGACY_SCHEMA` replaces to;
  default `automatically_inferred_qualifier`
- `--date-variable <name>` — target name for `LEGACY_DATE_VARIABLE`
  replacement; default `process_date`
- `--metadata-blacklist <comma_list>` — `METADATA_COLUMN_EXPOSED`
  column list; default = the customer's published 9-column list
- `--obsolete-ctes <comma_list>` — `OBSOLETE_CTE` blacklist; default
  = `extract_dates,create_timeline,finalize_timeline`

---

## 3. Determinism checks

These flag non-deterministic SQL — same query, different results
across runs.

| Rule | Severity | Trigger | Genie instruction |
|---|---|---|---|
| `WINDOW_NO_ORDER` | error | `ROW_NUMBER()`, `RANK()`, etc. with `OVER ()` (no ORDER BY) | the window function appears in `Return columns:` exactly as written — Genie sees the missing tie-breaker |
| `WINDOW_NON_UNIQUE_ORDER` | warning | `OVER (ORDER BY col)` where `col` isn't unique | window appears in `Return columns:`; tie-breakers are not synthesised |
| `LIMIT_NO_ORDER` | warning | `LIMIT N` without `ORDER BY` | `Limit:` section appears with no `Sort:` |
| `VOLATILE_FUNCTION` | info | `RANDOM()`, `NOW()`, `UUID()`, `CURRENT_DATE()` | function appears in the relevant `Return columns:` / `Filters:` line |
| `FIRST_LAST_NO_ORDER` | info | `FIRST_VALUE(...)`/`LAST_VALUE(...)` over an unordered partition | window appears in `Return columns:` exactly as written |

---

## 4. SQL-First annotation rules

These are extraction rules — the script reads them, it doesn't
enforce them.

| Annotation | Location | Effect |
|---|---|---|
| `-- @mdde-entity: <name>` | header | becomes the file's entity name (overrides filename); appears in the Genie goal line |
| `-- @mdde-layer: <layer>` | header | classifies the file (source/staging/integration/business); used to colour-group nodes in the report's Mermaid diagram |
| `-- @mdde-stereotype: <kind>` | header | shape classifier (fact/dimension/lookup) |
| `-- @mdde-description: <text>` | header | free-text description; appears in the BFM mapping |
| `-- @mdde-domain: <name>` | header | domain classifier (CRM/orders/billing) |
| `-- @pk` | trailing | primary-key flag in entity YAML and BFM mapping |
| `-- @business_key` | trailing | business key flag |
| `-- @fk(table.column)` | trailing | foreign-key target |
| `-- @pii` | trailing | PII flag |
| `-- @nullable` | trailing | nullability flag |
| `-- @derived` | trailing | computed column flag |
| `-- @scd2_from`, `-- @scd2_to`, `-- @scd2_current` | trailing | SCD2 tracking columns |
| `-- @decimal(p,s)` | trailing | decimal precision/scale |

**Genie instruction:** annotations are NOT pushed into the Genie
prompt today. The prompt only describes the query's behaviour
(sources, joins, filters, columns) — not the semantic tags. If
Genie needs to understand PII or PK constraints, it should query
the entity YAML separately.

---

## 5. Cross-file lineage stitching

Not a rule per se, but worth listing alongside.

| | |
|---|---|
| **Trigger** | one file's `entity_name` matches another file's `source_tables` |
| **Action** | records a producer→consumer edge in `lineage.json` under `stitching.edges`; renders a Mermaid diagram in `report.md` |
| **Genie instruction** | not exposed in the per-file prompts. The cross-file graph is run-level metadata, not query-level. |

---

## 6. Mapping coverage

Computed as: of all non-`*` output columns across all files, what
percentage resolve to at least one `(table, column)` source pair?

| | |
|---|---|
| **100%** | every output column has a known source — no SELECT * survived unqualified, no expressions evaluated to constants |
| **<100%** | listed columns are dead-ends in `report.md`'s mapping section |

**Genie instruction:** none — mapping coverage is a meta-metric
across the run, not a per-query instruction.

---

## 7. Movement CSV export

| Column | Source / rule |
|---|---|
| `target_model_name` | configurable, default `SSF` (CLI: `--target-model`) |
| `target_table_name` | filename stem, or everything left of the first `-` if present |
| `target_column_name` | the output column name (empty for join-only source rows) |
| `source_model_name` | configurable, default `SSF_SOURCE` (CLI: `--source-model`) |
| `source_table_name` | alias resolved to the underlying table |
| `source_column_name` | the source column (empty for join-only rows) |
| `derived_indicator` | `true` when lineage classifier returns anything other than `direct` or `rename`; `false` otherwise |
| `movement_expression` | the projection's SQL fragment, with MDDE annotation comments stripped and identifier quoting removed |
| `dependency_type` | configurable, default `strict` (CLI: `--dependency-type`) |

**Quoting:** every cell is double-quoted (`csv.QUOTE_ALL`).

**Row generation:**
- One row per `(target_column, source_column)` pair the lineage
  extractor produced.
- Constants and empty-source projections produce one row each with
  empty source fields.
- Join-only tables (referenced in FROM/JOIN, contributing no
  projection) produce one row with empty `target_column_name` and
  `source_column_name`.

**Output locations:**
- `<output>/<query>/movement.csv` — per-query slice
- `<output>/movement.csv` — run-level rollup, all rows in input order

---

## 8. What the Genie prompt deliberately does NOT include

To keep prompts tight and Genie-actionable:

- **No metadata headers** — no front-matter, no `id`, no `version`
- **No annotation tags** (`@pii`, `@pk`, etc.) — those are stored in
  the entity YAML and queryable separately
- **No schema dumps** — Genie has access to the warehouse's
  INFORMATION_SCHEMA and shouldn't need a list pasted in
- **No commentary about why the query was written** — the goal line
  states the result; "why" belongs in PR descriptions
- **No optimisation hints** — the prompt describes intent, not
  implementation
- **No instructions for sections that don't apply** — no
  "Group by: none", no "Limit: none", no "Sort: unsorted". Sections
  appear only when they have content.
