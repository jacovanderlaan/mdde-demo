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
6. **Layered CTE Structure** — every query should fan out into single-concern CTEs. Each CTE answers one question:
   - **Source layer** — `<table>_prepared` or `<table>_filtered`. Owns bare columns, renames, single-source value transforms (UPPER, TRIM, arithmetic), and single-source WHERE filters. One CTE per source table referenced.
   - **Joined layer** — `<entity>_joined`. Owns JOINs and multi-source derivations (column expressions that depend on more than one source). NO WHERE clause. NO aggregation.
   - **Filtered layer** — `<entity>_filtered`. Owns cross-source WHERE predicates (filters whose operands come from multiple source CTEs). Emitted ONLY when such predicates exist. NO joins, NO derivations, NO aggregation.
   - **Aggregated layer** — `<entity>_aggregated`. Owns `GROUP BY`, aggregate functions (`SUM`, `MAX`, `COUNT`, ...), and `HAVING`. Reads from the filtered or joined CTE. NO JOINs of its own, NO derivations, NO formatting.
   - **Final SELECT** (no CTE — top level). Owns `CAST`, `COALESCE`, `NULLIF`, `CASE` with defaults, constants/literals, `ORDER BY`, `LIMIT`, and window functions (`ROW_NUMBER`, `LAG`, `SUM OVER PARTITION BY`). Reads from the aggregated/filtered/joined CTE. NO joins of its own, NO WHERE, NO GROUP BY.
7. **UNION ALL Purity** — when combining datasets, lift each branch into its own CTE first. The top-level body must be a pure `SELECT * FROM <branch_cte_a> UNION ALL SELECT * FROM <branch_cte_b> ...` with no casts, no filters, no expressions. Each branch must add a string-literal tag column identifying the source (e.g., `'A_SOURCE' AS source_ind`), and each branch's CTE body must itself be fully layered using the rules above.
8. **Predicate Subquery Lift** — `WHERE x IN (SELECT ...)` and `WHERE EXISTS (SELECT ...)` must have their inner SELECT lifted into a dedicated CTE; the predicate becomes `WHERE x IN (SELECT col FROM <cte>)` or `WHERE EXISTS (SELECT 1 FROM <cte> WHERE <cte>.x = outer.x)`. For correlated cases, promote the correlation column as a projection in the lifted CTE; non-correlation predicates stay inside the CTE.
9. **EXCEPT / INTERSECT Lift** — top-level `EXCEPT` / `INTERSECT` operands that aren't already bare `SELECT * FROM <cte>` references must be lifted into their own CTEs. The resulting body should be a pure `SELECT * FROM <a> EXCEPT SELECT * FROM <b>`.
10. **Replace DISTINCT with explicit dedup** — `SELECT DISTINCT` hides data-quality problems. Rewrite as two CTEs:
   ```sql
   WITH <entity>_ranked AS (
     SELECT <projection_list>,
            ROW_NUMBER() OVER (
              PARTITION BY <all projection columns>
              ORDER BY (SELECT NULL)
            ) AS rn
     FROM <original-FROM-WHERE-GROUP-BY>
   ), <entity>_deduped AS (
     SELECT <projection_list> FROM <entity>_ranked WHERE rn = 1
   )
   SELECT * FROM <entity>_deduped
   ```
   The `_ranked` CTE makes the duplication visible and inspectable; the `_deduped` CTE is a separate filtering step. Use a meaningful ORDER BY in the window when there's a deterministic tie-break key (e.g., latest scan_date wins); otherwise `(SELECT NULL)` signals an order-independent dedup.
11. **Table Qualifier** — every base table reference in the rewrite must use the qualifier `schema_identifier_ssf_snapshot` in place of its original catalog/schema (e.g., `catalog.schema.t` -> `schema_identifier_ssf_snapshot.t`).
12. **Replace FULL OUTER JOIN + COALESCE** — restructure as three CTEs: `unique_rows_from_<a>` (LEFT JOIN + IS NULL), `unique_rows_from_<b>` (reverse direction), `matching_rows` (INNER JOIN), then `UNION ALL` the three. Each branch adds a source-tagging column.
13. **Comment Header** — prepend a `/* Migration Details ... */` block to the rewrite, listing the rules applied and an `[X]` validation checklist.

---
Original query follows.

