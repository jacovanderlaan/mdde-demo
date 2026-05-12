# Genie instructions

Apply the following SQL migration and optimisation rules to the query below. Produce a single rewritten query plus a short summary of what changed.

1. **SCD2 Filtering** — every source table CTE must apply point-in-time filtering:
   ```sql
   WHERE CAST('{process_date}' AS DATE) >= _valid_from
     AND CAST('{process_date}' AS DATE) <  COALESCE(_valid_to, CAST('9999-12-31' AS DATE))
   ```
   Use the variable above; do **not** use `BETWEEN`. Replace any legacy `{reporting_date}` references with `{process_date}`.
2. **Remove Obsolete Logic** — delete CTEs named `extract_dates`, `create_timeline`, `finalize_timeline` and update downstream references. Replace any `snapshot_date` filtering with the SCD2 predicate above.
3. **Exclude Metadata Columns** — never project `snapshot_date`, `insert_dts`, `update_dts`, `current_flag`, `delete_flag`, `delta_flag`, `create_timestamp`, `start_dts`, `end_dts`, `file_delivery_entity`, `delivery_set`, `file_reporting_date`, `period_version`, `file_reporting_period`, `xsd_version`, `redelivery_number` in CTE outputs or the final SELECT. Drop any WHERE predicate that references these columns; if the WHERE becomes empty, remove it. Retain `_valid_from` / `_valid_to` ONLY in the WHERE clause of initial source CTEs.
4. **Optimize Joins** — remove `LEFT JOIN`s that contribute no columns to the final result. Pre-process any join-key transformations in source CTEs (no inline `CAST`, function call, or literal inside JOIN `ON` clauses).
5. **Explicit Column Selection** — replace every `SELECT *` with an explicit column list. Only project the columns the downstream consumer needs.
6. **Modular Query Design** — structure the rewrite as logical CTEs in three tiers:
   - **Preparation CTEs** named `<source_table>_filtered` (filter + SCD2 predicate per source).
   - **Transformation CTEs** named for their purpose (`transformed_data`, `combined_data`, `aggregated_data`, `ranked_customers`, etc.). Push column derivations to the earliest CTE where all required fields are available. `GROUP BY` belongs in its own dedicated CTE.
   - **Final SELECT** — selects pre-prepared fields only; no derivations, no filtering, no joins.
7. **UNION ALL Purity** — when combining datasets, do all transformations in per-source CTEs first. The UNION ALL block must only concatenate prepared, schema-aligned datasets (no casts, no filters, no expressions inside the UNION). Each branch must add a string-literal column identifying the originating source (e.g., `'A_SOURCE' AS source_ind`).
8. **Avoid DISTINCT** — use explicit deduplication via `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <tie>)` instead of `DISTINCT`. If `DISTINCT` is unavoidable, add a comment justifying why.
9. **Table Qualifier** — every base table reference in the rewrite must use the qualifier `schema_identifier_ssf_snapshot` in place of its original catalog/schema (e.g., `catalog.schema.t` -> `schema_identifier_ssf_snapshot.t`).
10. **Replace FULL OUTER JOIN + COALESCE** — restructure as three CTEs: `unique_rows_from_<a>` (LEFT JOIN + IS NULL), `unique_rows_from_<b>` (reverse direction), `matching_rows` (INNER JOIN), then `UNION ALL` the three. Each branch adds a source-tagging column.
11. **Comment Header** — prepend a `/* Migration Details ... */` block to the rewrite, listing the rules applied and an `[X]` validation checklist.

---
Original query follows.

