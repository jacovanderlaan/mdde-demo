# Customer SQL Migration Rule Set

This is the consolidated, customer-authoritative rule set for SQL query
migration and optimization. It mirrors the customer's master
"Restructured and AI Optimized Rule Set" page — 8 numbered rules, in
the order ChatGPT (or our `sql_process` pipeline) should apply them,
with validation checklists and explicit before/after examples.

For our internal per-detection reference, see [RULES.md](RULES.md).
For implementation decisions and rejected alternatives, see
[DECISIONS.md](DECISIONS.md).

---

## Workflow

Apply rules in this order:

1. **Schema Replacement** — replace old schemas with `automatically_inferred_qualifier`
2. **Initial Filtering** — apply SCD2 filtering logic in the initial CTEs of each source
3. **Remove Obsolete Logic** — delete redundant timeline-related CTEs and update downstream queries
4. **Optimize Joins** — remove unused `LEFT JOIN`s and associated CTEs
5. **Exclude Metadata** — remove unnecessary metadata columns
6. **Explicit Column Selection** — replace `SELECT *` with explicit column lists
7. **Modular Design** — break the query into logical, named CTEs
8. **Add Comment Header** — include a summary of changes as a comment header

---

## 1. Schema Replacement

**Objective:** Replace old schemas with the new schema structure
`automatically_inferred_qualifier`.

**Steps:**
1. Identify all occurrences of the legacy schemas (default blacklist:
   `bodm`, `csz`, `hz`, `cz`)
2. Replace them with the configured replacement schema (default:
   `automatically_inferred_qualifier`)

**Example:**
- Before: `SELECT * FROM bodm.table_name`
- After: `SELECT * FROM automatically_inferred_qualifier.table_name`

**Validation Checklist:**
- [ ] All legacy schema references are replaced
- [ ] No legacy schema names remain in the query

**Configurable:**
- `--legacy-schemas bodm,csz,hz,cz` — comma-separated list of schemas to replace
- `--replacement-schema automatically_inferred_qualifier` — what to replace with
- Off by default; the rule fires only when `--legacy-schemas` is passed

---

## 2. SCD2 Filtering Logic

**Objective:** Apply point-in-time filtering to ensure only valid
records for the `{process_date}` are included.

**Steps:**
1. Add the following filtering logic to the **initial CTEs** of each
   source:
   ```sql
   WHERE CAST('{process_date}' AS DATE) >= _valid_from
     AND CAST('{process_date}' AS DATE) <  COALESCE(_valid_to, CAST('9999-12-31' AS DATE))
   ```
2. Replace any occurrences of legacy `{reporting_date}` with `{process_date}`
3. **Do not** use `BETWEEN` for filtering

**Example:**
- Before: `WHERE snapshot_date BETWEEN start_dts AND end_dts`
- After: the SCD2 predicate above

**Validation Checklist:**
- [ ] Point-in-time filtering is applied in all initial CTEs
- [ ] Legacy date variable names replaced with `{process_date}`
- [ ] No `BETWEEN` is used for filtering
- [ ] `_valid_from` / `_valid_to` references appear only in initial CTEs

**Configurable:**
- `--date-variable process_date` — name of the template variable to use

---

## 3. Removal of Obsolete Logic

**Objective:** Remove redundant CTEs and logic that are no longer
necessary due to SCD2 filtering.

**Steps:**
1. Identify and remove CTEs related to timeline creation or redundant
   filtering logic, such as:
   - `extract_dates`
   - `create_timeline`
   - `finalize_timeline`
2. Update downstream queries to eliminate dependencies on removed CTEs

**Example (Before):**
```sql
extract_dates AS (
  SELECT DISTINCT collateral_id, start_date AS change_date FROM base_cre_collateral
  UNION
  SELECT DISTINCT collateral_id, DATEADD(day, 1, end_date) AS change_date FROM base_cre_collateral
),
create_timeline AS (
  SELECT collateral_id,
         change_date AS start_dts,
         LEAD(change_date) OVER (PARTITION BY collateral_id ORDER BY change_date) AS end_dts
  FROM extract_dates
),
finalize_timeline AS (
  SELECT collateral_id,
         start_dts,
         DATEADD(day, -1, end_dts) AS end_dts
  FROM create_timeline
  WHERE end_dts IS NOT NULL
)
```

**After:** Remove all CTEs above.

**Validation Checklist:**
- [ ] All redundant timeline-related CTEs are removed
- [ ] Downstream queries are updated to remove dependencies on obsolete logic

---

## 4. Exclusion of Unnecessary Metadata

**Objective:** Remove unnecessary metadata columns from outputs and
transformations.

**Steps:**
1. Identify and remove the following metadata columns from output
   columns of CTEs, the final SELECT query, and intermediate
   transformations:
   - `snapshot_date`
   - `insert_dts`
   - `update_dts`
   - `current_flag`
   - `delete_flag`
   - `delta_flag`
   - `create_timestamp`
   - `start_dts`
   - `end_dts`
2. Retain `_valid_from` and `_valid_to` **only in the WHERE clause of
   the initial CTEs**

**Validation Checklist:**
- [ ] Metadata columns are excluded from all outputs and transformations
- [ ] `_valid_from` and `_valid_to` are used only for filtering in initial CTEs

**Configurable:**
- `--metadata-blacklist snapshot_date,insert_dts,...` — column names to exclude

---

## 5. Removal of Unused Joins

**Objective:** Remove `LEFT JOIN`s that do not contribute to the final
result.

**Steps:**
1. Identify `LEFT JOIN`s that:
   - Do not contribute columns to the SELECT clause
   - Are not used in downstream joins or transformations
2. Remove unused `LEFT JOIN`s
3. If the removed `LEFT JOIN` references a CTE, check if the CTE is
   used elsewhere. If not, delete the CTE.

**Example:**
- Before: `LEFT JOIN unused_table AS u ON main_table.id = u.id`
- After: `-- Join removed`

**Validation Checklist:**
- [ ] All unused `LEFT JOIN`s are removed
- [ ] Unreferenced CTEs are deleted

---

## 6. Explicit Column Selection

**Objective:** Avoid `SELECT *` and specify only the required columns
to improve performance and readability.

**Steps:**
1. Replace any `SELECT *` with explicit column lists
2. List only the columns required for the output or downstream
   transformations

**Example:**
- Before: `SELECT * FROM table_name`
- After: `SELECT column1, column2, column3 FROM table_name`

**Validation Checklist:**
- [ ] All `SELECT *` are replaced with explicit column lists
- [ ] Only necessary columns are included

---

## 7. Modular Query Design with CTEs

**Objective:** Break the query into logical and modular steps using
CTEs.

**Steps:**

1. Separate the query into three tiers:
   - **Preparation CTEs** — apply filtering (e.g., SCD2 filtering) to
     source data. Named `<source_table>_filtered`.
   - **Transformation CTEs** — perform joins, unions, and
     transformations. Named descriptively (`transformed_data`,
     `deduplicated_records`, `aggregated_data`, etc.).
   - **Final Query** — select the required fields for output.
2. Ensure each CTE performs a single, well-defined task

**Example:**
```sql
WITH source_filtered AS (
  SELECT column1, column2
  FROM source_table
  WHERE CAST('{process_date}' AS DATE) >= _valid_from
    AND CAST('{process_date}' AS DATE) <  COALESCE(_valid_to, CAST('9999-12-31' AS DATE))
),
transformed_data AS (
  SELECT column1, column2, column3
  FROM source_filtered
  JOIN other_table
    ON source_filtered.column1 = other_table.column1
)
SELECT column1, column2, column3
FROM transformed_data
```

**Validation Checklist:**
- [ ] Query is broken into logical CTEs (Preparation, Transformation, Final)
- [ ] Each CTE performs a single, well-defined task

**Naming Conventions:**

| CTE type | Pattern | Examples |
|---|---|---|
| Source-filtered preparation | `<source_table>_filtered` | `cre_resourceitempledgedtocollateral_filtered`, `samcre_ctf_cre_resourceitem_filtered` |
| Filter step (generic) | `filtered_data` | when there's a single filtered intermediate |
| Aggregation step | `aggregated_data` or `<purpose>_aggregated` | `aggregated_data` |
| Combination | `combined_data` | union of multiple filtered sources |
| Transformation | descriptive purpose-named | `transformed_data`, `deduplicated_records`, `ranked_customers` |
| Non-match extraction | `unique_rows_from_<source>` | when implementing the FULL OUTER → LEFT JOIN/IS NULL pattern |
| Match extraction | `matching_rows` | INNER JOIN result in the same pattern |

**Sub-rules that flow into rule 7:**

- **Push derivations to the earliest possible step.** Single-source
  derivations belong in the source's `_filtered` CTE. Cross-table
  derivations belong in the first transformation CTE where all
  required fields are available. Exception: expensive derivations
  needed only for specific rows may remain in the final query.
- **Use CTEs for subqueries.** Encapsulate inline subqueries into
  CTEs. Including correlated scalar subqueries when they can be
  decorrelated via GROUP BY + LEFT JOIN.
- **Aggregation should be a separate step.** Any `GROUP BY` belongs
  in its own dedicated CTE; the WHERE clause and the GROUP BY must
  not coexist in the same step.
- **No derivations in WHERE.** Pre-compute Boolean fields
  (`is_valid`, `has_value`, etc.) in the source CTE; the WHERE
  clause should reference those Boolean columns.
- **Pre-processed joins.** No inline `CAST`, transformation, or
  literal in JOIN conditions. Materialize the transformed key in a
  CTE first.
- **UNION ALL must be "pure".** Only concatenate already-prepared,
  schema-aligned datasets. No casts, filters, expressions, or new
  columns inside the UNION ALL block.
- **Source-tagging metadata column.** Every UNION ALL branch must
  add a literal identifying the originating source (e.g.,
  `'A_SOURCE' AS source_ind`, `'CRE' AS source`).
- **PK dedup-check columns.** When the target table's primary key
  is known, add `ROW_NUMBER() OVER (PARTITION BY <pk>)` and
  `COUNT(*) OVER (PARTITION BY <pk>)` so downstream DQ checks can
  verify uniqueness.
- **Per-source filtering and consistency.** Each data source gets
  its own dedicated initial CTE. Filters for multiple sources must
  not be combined in a single WHERE clause.
- **Replace FULL OUTER JOIN + COALESCE with three CTEs.**
  `LEFT JOIN/IS NULL` (each direction) + `INNER JOIN`, then
  `UNION ALL`. Each branch adds the source-tagging column.
- **Avoid DISTINCT unless justified.** Use `ROW_NUMBER()` /
  `RANK()` or address root cause. If DISTINCT is unavoidable, add
  a comment referencing the rule.
- **Separate DQ queries.** Data Quality checks live in dedicated
  CTEs (named `dq_<purpose>`), separate from the integration flow.

---

## 8. Comment Header

**Objective:** Include a detailed comment header summarizing the
changes for traceability.

**Template:**
```sql
/*
Migration Details:
- Original SQL File: <original_file_name.sql>
- Target SQL File:  <target_file_name.sql>
- Summary of Changes:
  1. Replaced old schemas with `automatically_inferred_qualifier`.
  2. Applied SCD2 filtering logic for point-in-time analysis.
  3. Removed obsolete CTEs (`extract_dates`, `create_timeline`, etc.).
  4. Excluded unnecessary metadata columns.
  5. Optimized joins and removed unused `LEFT JOIN`s.
  6. Replaced `SELECT *` with explicit column selection.

Validation Checklist:
- [X] Schema replacement completed.
- [X] SCD2 filtering applied.
- [X] Obsolete logic removed.
- [X] Metadata columns excluded.
- [X] Explicit column selection used.
*/
```

The script auto-generates this header. Each summary bullet and
checklist line is included only when the corresponding transform
actually fired (so a file that didn't need schema replacement won't
report it as applied).

**Validation Checklist:**
- [ ] Header is present at the top of every transformed query
- [ ] Original and target file names are recorded
- [ ] Summary of changes reflects what actually changed
- [ ] Validation checklist entries match the applied transforms

---

## Implementation status

Each rule below is tagged with its current implementation tier in
this pipeline:

| Rule | Detection | Auto-fix |
|---|---|---|
| 1. Schema Replacement | ✓ | ✓ (opt-in via `--legacy-schemas`) |
| 2. SCD2 Filtering Logic | ✓ (legacy patterns flagged) | guidance only |
| 3. Removal of Obsolete Logic | ✓ | ✓ (CTE drop by name) |
| 4. Exclusion of Unnecessary Metadata | ✓ | guidance only |
| 5. Removal of Unused Joins | ✓ | guidance only |
| 6. Explicit Column Selection | ✓ (`SELECT_STAR`) | guidance only |
| 7. Modular Query Design | partial — projection-pushdown done | extends to cross-table tier (planned) |
| 8. Comment Header | n/a | ✓ |

Higher-risk transforms (`FULL OUTER → UNION` rewrite, scalar-subquery
decorrelation, aggregation-step splitting) are detection-only at this
stage; auto-fix is incremental work for later.
