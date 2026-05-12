# sql_process — design decisions log

This file records every interactive decision made while extending
`sql_process`. Each entry has:

- **Question** — what option-set was put to the user
- **Decision** — the option chosen
- **Why it matters** — what behaviour the alternatives would have
  produced, and why this one was preferred
- **Reflected in code at** — where the decision is enforced

Read this file when extending the script, or when an output looks
"wrong" but was in fact deliberate. The alternatives we rejected
are listed too, so a future contributor can re-litigate with the
full context.

---

## Layer / concern model

Each CTE in the rewritten output has exactly one job. The layered
pipeline maps SQL phases to CTE names. A query goes through (some
or all of) the following layers, top to bottom:

| Layer | CTE name pattern | Single concern |
|---|---|---|
| Source | `<table>_prepared` / `<table>_filtered` | SELECT columns + renames + single-table filters |
| Joined | `<entity>_joined` | JOIN + multi-source derivations |
| Filtered (planned) | `<entity>_filtered` | Cross-source WHERE predicates |
| Aggregated | `<entity>_aggregated` | GROUP BY + aggregates only |
| Unioned (implicit) | `<entity>_<n>` branch CTEs | UNION ALL of fully-prepared branches |
| Final SELECT | (no CTE — top-level) | CAST + COALESCE + defaults + constants |

**Implemented today:**
- Source pushdown (with strict renames-only rule, plus passthrough-CTE
  in-place rewrite).
- Joined CTE for JOIN + multi-source derivations.
- Aggregation CTE.
- UNION-branch lifting.
- Final SELECT for casting / defaulting / constants.

**Planned (not yet implemented):**
- Single-source value transforms (UPPER, TRIM, arithmetic over one
  table) folded back INTO the source CTE.
- Cross-source filtering CTE `<entity>_filtered` between joined and
  aggregated.
- Recursive layering inside UNION-branch CTEs (each branch gets the
  full pipeline applied internally).

### Why split into so many CTEs?

Each layer answers exactly one question:
- "What does this table expose?" → source CTE
- "How do these tables relate?" → joined CTE
- "What slice are we operating on?" → filtered CTE
- "What's the aggregate signature?" → aggregated CTE
- "How is the result formatted?" → final SELECT

A reader can stop at the layer they care about. Lineage tools can
attribute every column to a single CTE. The customer's existing
DataForge convention (`_filtered`, `_prepared`) maps directly to
the source layer and inspired the rest of the pattern.

---

## Subquery → CTE lifting

### D1. Which subquery shapes to lift

- **Question:** which inline subquery patterns should be rewritten
  into named CTEs?
- **Decision:** lift derived tables in FROM/JOIN, scalar subqueries
  in SELECT projections, and UNION subqueries wrapped in a FROM
  clause. Leave WHERE-IN / EXISTS / comparison predicates inline.
- **Why it matters:**
  - **Derived tables in FROM/JOIN** are pure aliases — moving them
    to a CTE preserves semantics and improves readability.
  - **Scalar subqueries in SELECT** can be lifted only if the inner
    SELECT has a single projection. Otherwise the wrapper
    `(SELECT col FROM cte)` wouldn't compile back to a scalar
    value. (Conservative single-column-only rule enforced in code.)
  - **UNION wrapped in FROM** is structurally identical to a
    derived table — same lift logic.
  - **WHERE IN / EXISTS** subqueries are boolean predicates, not
    table-like values. Rewriting them as a join-with-DISTINCT
    changes row counts in pathological cases.
- **Rejected:** "lift everything we can find" — would break the
  predicate cases.
- **Reflected in code at:** `lift_subqueries_to_ctes()`, candidate
  collection loop. Predicates are routed to the `non_candidates`
  list and produce `SUBQUERY_NOT_LIFTED` findings.

### D2. CTE naming for lifted subqueries

- **Question:** what should the generated CTEs be named?
- **Decision:** prefer the existing subquery alias when present;
  fall back to `_sub1`, `_sub2`, ... otherwise.
- **Why it matters:** if the analyst wrote `FROM (SELECT ...) AS
  recent_orders`, the alias already carries intent. Promoting it
  to a CTE name preserves that intent in the output. Numeric
  fallbacks are stable across re-runs and never collide with
  existing CTEs.
- **Rejected:**
  - "Always numeric" — loses the analyst's intent.
  - "Derive from the inner table" — heuristic, brittle when the
    subquery touches multiple tables.
- **Reflected in code at:** `_make_cte_name()`.

### D3. Safety policy for non-liftable subqueries

- **Question:** when a subquery can't safely be lifted, do we still
  try, do we skip silently, or do we skip and flag?
- **Decision:** skip and emit a `SUBQUERY_NOT_LIFTED` info finding.
- **Why it matters:** silent skip means the user can't tell what we
  did and didn't touch. Aggressive lift could produce wrong SQL.
  The middle ground is "always conservative, always observable."
- **Rejected:**
  - "Skip silently" — no signal to the user.
  - "Lift anyway with a comment" — can change semantics.
- **Reflected in code at:** `lift_subqueries_to_ctes()` `non_candidates`
  loop and the correlated/predicate-detection branches.

---

## Single-table projection pushdown

### D4. Which expressions count as pushable into a source CTE

- **Question:** what expression shapes are "single-table enough" to
  push into a `<table>_filtered` / `<table>_prepared` CTE?
- **Decision (2026-05-12, revised):** ONLY bare columns and pure
  renames (`col AS alias`). Anything that transforms the value —
  `CAST`, `CASE`, `COALESCE`, arithmetic, function calls,
  constants — stays in the outer SELECT, even when single-source.
  WHERE-predicate pushdown (a filter, not a value transform) is
  unchanged.
- **Why it matters:** the source CTE is a "what this table exposes
  (with target vocabulary)" layer. Casting and defaulting are
  formatting concerns — keeping them next to the JOINs/result lets
  a reader see the whole shape of the final output in one place
  instead of chasing values through three CTE bodies.
- **Earlier (2026-05) decision (superseded):** push all four shapes,
  including single-table CASE/CAST/COALESCE/arith. Reverted after
  customer-site feedback: casting and defaulting belong with the
  joins, not in the source layer.
- **Rejected alternatives:**
  - "Push lossless single-table functions like `UPPER`/`TRIM`/
    `SUBSTRING` but not casts/defaults" — requires maintaining a
    function-purity classifier and still leaves a gray area around
    `LPAD`, `TO_CHAR`, etc. Strict-renames-only is simpler and
    matches the customer's own pattern.
- **Reflected in code at:** `_is_pushable_projection()` (renames-only
  filter) and `_expression_uses_only()` (the single-table check
  used downstream for filter pushdown).

### D5. Naming the per-source CTE

- **Question:** what should the projection-pushdown CTEs be named?
- **Decision:** `<table>_proj` (e.g., `stg_customers_proj`).
- **Why it matters:** the suffix makes the intent obvious and the
  full table name avoids cryptic single-letter naming. A second
  pushdown for the same table in the same file would collide with
  the existing `stg_customers` source table only if we used `stg_*`
  prefix instead — the `_proj` suffix sidesteps that.
- **Rejected:**
  - `<alias>_proj` — cryptic out of context.
  - `stg_<table>` — collides with existing staging conventions.
- **Reflected in code at:** `push_projections_to_source_ctes()`,
  CTE-name generation block.

### D6. Non-pushable projections

- **Question:** how to handle projections that can't be pushed
  (aggregates, windows, cross-table expressions)?
- **Decision:** keep them in the outer SELECT, no findings.
- **Why it matters:** these are exactly what the outer SELECT
  *should* be doing — joining multiple tables together with
  cross-table work. Flagging them would be noise; failing the
  whole file is too strict.
- **Rejected:**
  - "Push everything we can + flag the rest" — extra noise.
  - "Skip the whole file if any projection is unpushable" — would
    rarely fire and disable a useful optimisation.
- **Reflected in code at:** `push_projections_to_source_ctes()`'s
  outer-projection walk leaves unpushable items in
  `new_outer_projections`.

### D7. WHERE clause pushdown

- **Question:** should single-table WHERE predicates also be
  pushed into the matching `_proj` CTE?
- **Decision:** yes — push single-table predicates alongside the
  projections.
- **Why it matters:** filter pushdown produces strictly smaller
  intermediate result sets at the join. Standard optimisation, no
  semantic risk.
- **Rejected:**
  - "Leave WHERE untouched" — would push half the work but leave
    the rest, weirdly asymmetric.
  - "Only push WHERE for tables that already get a _proj CTE" —
    same effect as the chosen approach since a table with no
    projections to push wouldn't gain a `_proj` CTE either way.
    (The code does build a CTE for filter-only sources; see D11.)
- **Reflected in code at:** `push_projections_to_source_ctes()`'s
  WHERE-walk routes each leaf predicate (split on AND) to the
  matching plan, or keeps it in the outer WHERE if cross-table.

### D8. Existing CTEs in the outer FROM

- **Question:** if the outer FROM references a CTE (not a base
  table), should pushdown still wrap it in a `_proj` CTE?
- **Decision:** skip — only push to base tables, leave existing
  CTEs untouched.
- **Why it matters:** existing CTEs (`shipped_orders`,
  `customer_totals`) often already do per-source work. Wrapping
  them in another `_proj` CTE creates noise without value.
  Skipping them respects the analyst's existing structure.
- **Rejected:**
  - "Push for everything, including CTEs" — produces
    `customer_totals_proj` on top of `customer_totals`, noisy.
  - "Push only when the existing CTE doesn't already do
    projections" — heuristic and hard to predict.
- **Reflected in code at:**
  `push_projections_to_source_ctes()`'s `collect_table()` helper
  routes CTE-named tables to `other_aliases` (excluded from
  pushdown plans) rather than `sources`.

### D9. Identity CTEs

- **Question:** should we create a `_proj` CTE for a source that
  has nothing to push (no renames, no derivations, no
  single-table filters)?
- **Decision:** skip — never create identity wrappers.
- **Why it matters:** wrapping a source in
  `SELECT * FROM raw_orders` adds no information and clutters the
  output. Pushdown should only fire when it actually changes
  something.
- **Rejected:**
  - "Always create _proj for every source" — verbose, identity
    CTEs.
  - "Create _proj only when at least 2 columns are picked" —
    arbitrary threshold.
- **Reflected in code at:**
  `push_projections_to_source_ctes()`'s bail-out check
  `if not any(p.projections or p.predicates for p in plans.values())`.

---

## Genie prompt emitter

### D10. Where the prompt is derived from

- **Question:** should the Genie prompt describe the original
  query or the optimised output?
- **Decision:** derive from the **original** parsed AST.
- **Why it matters:** Genie should see the analyst's *intent*, not
  the script's optimisation steps. If Genie can produce the same
  result from the unoptimised description, the optimisation
  becomes verifiable by external comparison.
- **Rejected:** "from the optimised SQL" — would conflate intent
  and implementation.
- **Reflected in code at:** `emit_genie_prompt()` consumes
  `pf.parsed` (the AST captured before `apply_auto_fixes`).

### D11. Conditional sections in the prompt

- **Question:** when a query has no UNIONs / no derivations / no
  LIMIT / etc., should the prompt still emit "Unions: none" type
  sections?
- **Decision:** sections appear ONLY when they have content.
- **Why it matters:** Genie is an LLM. Filler instructions eat
  tokens and dilute attention. A prompt with nine
  "Section: none" lines and one real instruction is worse than a
  prompt with just the real instruction.
- **Rejected:** "always emit every section for consistency."
- **Reflected in code at:** `emit_genie_prompt()`'s eight
  conditional sections (goal line always; sources, joins,
  filters, group-by, return columns, sort, limit only when
  present).

### D12. Identifier quoting in the prompt

- **Question:** keep `qualify()`'s identifier quoting, or strip?
- **Decision:** strip — render unquoted for readability.
- **Why it matters:** `"customer_totals" AS "t"` is harder to read
  than `customer_totals AS t`. Genie understands either, but
  prose is for humans first.
- **Reflected in code at:** `_unquoted_sql()` helper, used by
  every section emitter that calls `.sql()`.

### D13. What the prompt deliberately excludes

- **Decision** (no explicit question, decided in the doc):
  - No metadata headers
  - No annotation tags (`@pii`, `@pk`, etc.) — those live in
    the entity YAML
  - No schema dumps — Genie has its own INFORMATION_SCHEMA
  - No commentary about why the query was written
  - No optimisation hints
- **Why it matters:** keeps the prompt actionable and short.
  Annotations belong in the entity YAML where they're queryable
  programmatically. Schema info is a separate concern.

---

## Output folder layout

### D14. Per-query folders vs per-artefact-type folders

- **Question:** keep the original `optimized/`, `mapping/`,
  `annotations/`, `genie/` top-level folders, or restructure to
  one folder per query?
- **Decision:** per-query folders.
- **Why it matters:** every artefact for one input SQL file lives
  together — drop a folder into a code review, a ticket, or a
  shared drive and the reader has everything they need (rewritten
  SQL, mapping, Genie prompt, findings) side by side. The
  per-artefact-type layout required scrolling across four
  separate folders to assemble one query's full picture.
- **Reflected in code at:** `process_folder()` writes
  `<output>/<rel.parent>/<stem>/<artefact>` instead of
  `<output>/<artefact>/<rel.parent>/<stem>.<ext>`.

### D15. Per-query folder name

- **Question:** SQL filename, `@mdde-entity`, or hybrid?
- **Decision:** SQL filename stem.
- **Why it matters:** filename is the canonical input identifier.
  It's stable, predictable, mirrors the input layout, and avoids
  collisions when two files happen to declare the same entity
  name (which is a copy-paste bug we'd otherwise paper over).
- **Rejected:**
  - "@mdde-entity if present, else filename" — two files
    declaring the same entity collide.
  - "Both, hyphenated" — verbose folder names.
- **Reflected in code at:** `process_folder()`'s
  `query_dir = output_dir / rel.parent / rel.stem`.

### D16. Per-query findings file

- **Question:** add a `findings.md` per query, a plain-text
  `log.txt`, or skip and keep findings only in the run-level
  `report.md`?
- **Decision:** `findings.md` per query — markdown, scoped subset
  of `report.md`.
- **Why it matters:** the per-query folder should be
  self-contained. A reader looking at one folder shouldn't need
  to open the run-level report to know what's flagged. Same
  format (markdown table) makes it copy-pasteable into tickets.
- **Rejected:**
  - "log.txt" — closer to a build log, less useful for review.
  - "Skip" — defeats the self-contained-folder principle.
- **Reflected in code at:** `emit_findings_md(pf, findings)`,
  called once per file in the orchestrator.

### D17. Recursive-mode subfolder handling

- **Question:** in `--recursive` mode, preserve input subfolders
  above the per-query folder, or flatten with path-in-name?
- **Decision:** preserve subfolders.
- **Why it matters:** mirroring the input tree makes the output
  navigable in the same mental model as the source. Flattened
  names with `__` separators are easier to script over but
  harder to read.
- **Reflected in code at:** `process_folder()`'s
  `rel.parent / rel.stem` path construction.

---

## Movement CSV (customer-specific mapping format)

### D18. How configurable values are passed

- **Question:** CLI flags, optional config file, or both?
- **Decision:** CLI flags with sensible defaults
  (`--target-model SSF`, `--source-model SSF_SOURCE`,
  `--dependency-type strict`).
- **Why it matters:** CLI flags are visible in the run command —
  trivial to grep CI logs for what values were used. A config
  file would hide the defaults behind a second file the reader
  has to also open.
- **Rejected:**
  - "Config file only" — hidden defaults.
  - "Both" — extra moving parts for marginal gain at this stage.
- **Reflected in code at:** `MovementConfig` dataclass + the
  three `parser.add_argument` calls in `main()`.

### D19. `target_table_name` derivation

- **Question:** when the filename has no hyphen, what's the
  `target_table_name`?
- **Decision:** the full filename stem.
- **Why it matters:** the hyphen-stripping rule is a customer-site
  convention for a specific naming pattern
  (`customer-revenue.sql` → table `customer`). For files that
  don't follow that pattern, the full stem is the obvious
  identifier. Falling back to `@mdde-entity` would break for
  unannotated files.
- **Rejected:**
  - "Use @mdde-entity if present, else stem" — inconsistent.
  - "Always require @mdde-entity" — breaks unannotated files.
- **Reflected in code at:** `_target_table_name()`.

### D20. Join-only source rows

- **Question:** how to record dependencies on tables that are
  joined but contribute no projection columns?
- **Decision:** emit one row per join-only source with empty
  `target_column_name` and `source_column_name`,
  `derived_indicator = false`.
- **Why it matters:** the dependency is real (the table is
  required to run the query) but the column-level mapping is
  empty. The empty-string row records the dependency without
  inventing fictional column data.
- **Rejected:**
  - "Emit a row per JOIN-condition column" — would duplicate join
    keys across many rows.
  - "Skip join-only tables" — loses the dependency, the customer
    wouldn't know which sources the query reads.
- **Reflected in code at:** `emit_movement_csv_rows()`'s
  `sources_with_projections` tracking + the trailing loop over
  `pf.source_tables`.

### D21. `derived_indicator` rule

- **Question:** what counts as "derived"?
- **Decision:** True if the lineage classifier returns anything
  other than `direct` or `rename`. False for direct column refs
  and pure renames.
- **Why it matters:** the existing lineage classifier already
  distinguishes `direct` / `rename` / `expression` / `aggregate`
  / `constant`. Re-using it keeps the rule consistent with the
  BFM mapping output and avoids a second source of truth.
- **Rejected:**
  - "Use the @derived annotation only" — strict but misses
    derivations the analyst forgot to annotate.
  - "Tag OR auto-detected" — most permissive, but the auto-detect
    alone already catches everything the annotation would; the
    OR adds no real signal.
- **Reflected in code at:** `_is_derived_lineage()`.

### D22. Where movement.csv is written

- **Question:** per-query, run-level rollup, or both?
- **Decision:** both — `<output>/<query>/movement.csv` per query
  AND `<output>/movement.csv` rolling up all rows.
- **Why it matters:** per-query slice matches the rest of the
  per-query folder layout (D14). Run-level rollup matches the
  customer's expected single-file import format. Cost is
  near-zero (rendering is fast); having both is strictly
  additive.
- **Reflected in code at:** `emit_movement_csv()` (per-query) and
  `emit_movement_csv_rollup()` (run-level), both called from
  `process_folder()`.

### D23. Unknown source column handling

- **Question:** what to write in `source_column_name` when the
  source is unknown (e.g., un-expanded `*`, unresolved
  unqualified ref)?
- **Decision:** empty string.
- **Why it matters:** matches the join-only row convention
  (D20). CSV parsers handle empty cells natively. A magic
  `<unknown>` string would introduce a value the customer's
  import tool would have to special-case.
- **Reflected in code at:** `emit_movement_csv_rows()`'s
  empty-source branch (when `lin.source_columns` is empty).

### D24. CSV quoting style

- **Question:** quote-minimal (only when needed) or quote-all?
- **Decision:** quote-all — every cell is double-quoted.
- **Why it matters:** the customer's import tool expects this
  shape. Minimal quoting works for most CSV consumers but the
  customer's specifically requires fully-quoted columns.
- **Reflected in code at:** `_rows_to_csv()` uses
  `csv.QUOTE_ALL`.

### D25. `movement_expression` cleanup

- **Decision** (made during implementation, no explicit question):
  strip MDDE annotation block-comments and identifier quoting
  before writing the SQL fragment into the CSV cell.
- **Why it matters:**
  - Annotation comments (`/* @pk @business_key */`) are noise in
    a mapping CSV — they belong in the SQL file, not the export.
  - Identifier quoting from `qualify()`
    (`"c"."customer_id" AS "customer_id"`) is unreadable in a
    spreadsheet cell.
  - Stripping both produces a clean, paste-able fragment.
- **Reflected in code at:** `_clean_movement_expression()`.

---

## Customer rule pack consolidation (15 rule pages → 8 numbered rules + sub-rules)

### D26. Implementation aggression for the customer rule pack

- **Question:** how aggressive should the implementation pass be — detection-only with selective auto-fix, all rules with auto-fix where tractable, or auto-fix everything including the hard cases?
- **Decision:** detection-first. Auto-fix only for the low-risk shapes (schema replacement, obsolete-CTE removal, legacy date variable, comment header, CTE rename). Higher-risk transforms (FULL OUTER → UNION rewrite, scalar-subquery decorrelation, aggregation-step splitting) stay flagged-only.
- **Why it matters:**
  - 15 rule pages adds ~16 new detections at once. Shipping them all as findings is low risk and immediately useful (the report flags every gap).
  - Auto-fix is incremental — each rewrite can land in its own PR once the detection has stabilised.
  - Hard rewrites (FULL OUTER, decorrelation) have edge cases that aren't tractable without a cost model and good test coverage.
- **Rejected:**
  - "All auto-fix where tractable" — bigger first PR, harder to review.
  - "Auto-fix everything including hard ones" — risk of producing wrong SQL on edge cases.
- **Reflected in code at:** `_optimizer.py` (16 new detection functions); `sql_process.py`'s `apply_schema_replacement`, `apply_legacy_date_variable_replacement`, `remove_obsolete_ctes`.

### D27. CTE naming convention

- **Question:** keep the existing `<table>_proj` naming, rename to the customer's `<source>_filtered` convention, or make it configurable?
- **Decision:** rename to match customer convention. `<table>_filtered` when a WHERE predicate is pushed, `<table>_prepared` when only projections are pushed (no filter).
- **Why it matters:**
  - Customer's "Naming of Initial CTEs for Filtering Source Tables" page is explicit: `<source_table>_filtered`.
  - `_proj` was internal jargon nobody else recognises.
  - `_prepared` fallback handles the "projection-only, no filter" case the customer doesn't have a name for.
- **Reflected in code at:** `push_projections_to_source_ctes()` CTE-name generation block.

### D28. Customer rule-pack documentation location

- **Question:** where should the consolidated 8-rule pack live — new file, merged into RULES.md, or both?
- **Decision:** new `CUSTOMER_RULES.md` mirroring the master page format. Existing `RULES.md` stays as internal per-detection reference. `DECISIONS.md` gets the per-decision rationale.
- **Why it matters:**
  - The customer's master page has a specific format (8 numbered rules, validation checklists, workflow) that's recognised by their team. Mirroring it makes the doc immediately reviewable.
  - Our internal `RULES.md` is per-detection (rule type, severity, Genie mapping). Different audience, different structure.
- **Reflected in code at:** new file `CUSTOMER_RULES.md`.

### D29. Date variable default

- **Question:** default to `{process_date}` (customer master pack), `{reporting_date}` (older pages), or require CLI?
- **Decision:** default to `{process_date}`. Legacy `{reporting_date}` is auto-rewritten via `apply_legacy_date_variable_replacement`.
- **Why it matters:**
  - The customer's master "Restructured and AI Optimized Rule Set" is canonical; rule 2.2 explicitly normalises to `{process_date}`.
  - Earlier rule pages use `{reporting_date}` because they predate the master pack. Auto-rewrite preserves backwards compat.
- **Reflected in code at:** `CustomerRuleConfig.date_variable` default; `apply_legacy_date_variable_replacement()`.

### D30. Schema replacement default

- **Question:** on by default with the customer's `bodm/csz/hz/cz` blacklist, or off by default opt-in via CLI?
- **Decision:** off by default. `--legacy-schemas` must be passed explicitly to activate.
- **Why it matters:**
  - The blacklist is the customer's specific data lake naming. Other users running the script shouldn't get those replacements unrequested.
  - Off-by-default keeps the script generic; opt-in keeps the customer flow ergonomic.
- **Reflected in code at:** `CustomerRuleConfig.legacy_schemas` defaults to empty list; rule fires only when non-empty.

### D31. Comment header emission policy

- **Question:** always emit, emit when ANY transform fired, or opt-in?
- **Decision:** emit only when at least one transform actually fired. Skip on no-op runs.
- **Why it matters:**
  - A header that reports "no changes made" is noise.
  - Already-compliant files get a clean output without spurious "Migration Details" banners.
  - Emission is driven by `TransformLog` — each transform sets a flag, and the header is generated only when the log has anything in it.
- **Reflected in code at:** `emit_comment_header()` returns empty string when no flags set; orchestrator's prepend is conditional on the header being non-empty.

### D32. Per-rule auto-fix surface

- **Decision** (implicit from D26):
  - **Auto-fix** (5 rules): Schema Replacement (rule 1); Legacy Date Variable (rule 2.2); Obsolete CTE Removal (rule 3); `WHERE 1=1` removal (pre-existing); single-source projection pushdown (pre-existing, now renamed).
  - **Detection-only** (everything else): BETWEEN-for-SCD2 (rule 2); Metadata Column Exposed (rule 4); Unused LEFT JOIN (rule 5); SELECT * (rule 6); PK dedup missing; DISTINCT without justification; UNION missing source tag; FULL OUTER with COALESCE; inline CAST/literal in JOIN; derivation in WHERE; inline transform in UNION; combined source filters; GROUP BY not isolated; non-descriptive CTE name.
- **Why it matters:**
  - The auto-fix list covers the rewrites that are mechanical (find → replace by name pattern) or already implemented.
  - Everything detection-only is either a structural transform that needs design care or a stylistic finding the analyst should review.
- **Reflected in code at:** the wiring in `apply_auto_fixes` (which transforms run) and the rule list in `_optimizer.get_all_check_types()`.

### D33. `analyze_sql` parameter-bug fix

- **Decision** (made during implementation, no explicit question): the existing call `lite_optimizer.analyze_sql(pf.raw_sql, pf.path.name)` was passing the filename as the `include_determinism` boolean. Fixed to pass `include_determinism=True` explicitly and added the new `config=` keyword.
- **Why it matters:** `pf.path.name` is truthy for any non-empty string, so determinism checks always ran — but the call was semantically broken and would fail loudly the moment the signature changed. Fixed in this PR before adding the config parameter.
- **Reflected in code at:** `run_quality_checks()`.

---

## Configurable YAML + per-customer overrides

### D34. Config file location and naming

- **Question:** where should the config YAML live?
- **Decision:** ``sql_process.config.yaml`` next to ``sql_process.py``. Auto-discovered on every run; ``--config <path>`` overrides; ``--config none`` disables loading.
- **Why it matters:** Bundling the default config with the script keeps "what does this run actually do" answerable from one place. Customers can clone, edit one YAML, and re-run. Putting the config next to input data was a tempting alternative but means two layouts to remember.
- **Rejected:**
  - "Auto-discover at ``<input_dir>/sql_process.config.yaml``" — clearer for multi-tenant but less obvious where defaults live.
  - "Script + input-dir cascade" — three precedence layers is too much surface for a tool that runs in seconds.
- **Reflected in code at:** ``sql_process.config.yaml`` (the file), ``load_config()``.

### D35. Movement.csv defaults switched to Converter / SSF

- **Question:** the customer asked for different defaults than what we'd shipped (``SSF`` / ``SSF_SOURCE``).
- **Decision:** ``target_model_name`` defaults to ``Converter``; ``source_model_name`` defaults to ``SSF``. Override via YAML or ``--target-model`` / ``--source-model``.
- **Why it matters:** Aligns with the customer site's vocabulary. The previous values had been guesses based on early conversations.
- **Reflected in code at:** ``MovementConfig`` dataclass defaults; ``sql_process.config.yaml``.

### D36. ``source_version`` column behaviour

- **Question:** the customer wants a ``source_version`` column in movement.csv populated from the third hyphen-part of the filename. What if the filename has fewer than 3 parts?
- **Decision:** Emit a warning (``MISSING_SOURCE_VERSION`` info finding) + leave the cell empty.
- **Why it matters:**
  - Empty cell matches the existing convention for join-only rows (D20). CSV consumers handle empty natively.
  - Warning surfaces the gap so analysts can rename the file if needed. No silent failure.
- **Rejected:**
  - "Use the full stem when no hyphens" — surprising values in a downstream version column.
  - "Empty without warning" — invisible failure mode.
- **Reflected in code at:** ``_source_version()``; ``MISSING_SOURCE_VERSION`` finding emitted in ``run_quality_checks()``.

### D37. Source qualifier handling in movement.csv

- **Question:** when a source table has a schema qualifier (``schema.table``), where does the schema name go in the CSV row?
- **Decision:** Strip catalog/schema from ``source_table_name`` (bare table name only). The schema name goes into ``source_model_name`` in UPPERCASE. Falls back to the config default when no schema is present.
- **Why it matters:** Keeps ``source_table_name`` consistent regardless of how the analyst wrote the FROM clause. Per-row ``source_model_name`` preserves multi-schema fidelity (each row says exactly which model the column came from).
- **Rejected:**
  - "Single per-file source_model_name" — lossy when the query reads from multiple schemas.
  - "Keep the qualified name in source_table_name" — inconsistent with what the customer's import tool expects.
- **Reflected in code at:** ``_build_source_info_map()``, ``SourceInfo`` dataclass, ``emit_movement_csv_rows()``.

### D38. LEFT JOIN → ``dependency_type=loose``

- **Decision:** When a source is reached via ``LEFT JOIN``, every movement.csv row for that source uses ``dependency_type=loose`` instead of the config default (typically ``strict``).
- **Why it matters:** A LEFT JOIN signals that the source is optional — its absence doesn't fail the query, so the dependency is genuinely looser. The customer's downstream tool treats ``loose`` differently from ``strict``.
- **Reflected in code at:** ``SourceInfo.left_join`` populated by ``_build_source_info_map()``; ``_dep()`` helper in ``emit_movement_csv_rows()``.

### D39. Table-level mapping granularity

- **Question:** the customer wants a mode that emits one row per source table (no column-level detail).
- **Decision:** Add a ``granularity`` field to ``MovementConfig`` with two values: ``column`` (default; existing behaviour) and ``table``. In table mode, one row per unique source table; ``target_column_name`` and ``source_column_name`` are empty; ``movement_expression`` is empty.
- **Why it matters:** Table-level mapping is a separate customer ask — they want the dependency graph without the column detail for cases where column lineage isn't tractable or relevant.
- **Reflected in code at:** ``MovementConfig.granularity``; the ``if config.granularity == "table"`` branch in ``emit_movement_csv_rows()``.

### D40. Optional mapping outputs

- **Question:** customers asked that ``mapping.bfm.yaml``, ``mapping.cte.yaml``, and ``annotation.entity.yaml`` become opt-in.
- **Decision:** Default to OFF for all three. Enable via the ``outputs`` block in YAML (``bfm_mapping: true``, ``cte_mapping: true``, ``annotation_entity: true``).
- **Why it matters:** These three artefacts duplicate information already in ``movement.csv`` or the optimised SQL. Most customers don't need them. Keeping them off by default produces tighter output folders.
- **Reflected in code at:** ``OutputConfig`` dataclass; conditional ``write_yaml`` calls in ``process_folder``.

### D41. Comma-to-UNION-ALL pre-parse

- **Question:** the customer site writes UNION ALL between SELECTs as a bare comma at the start of a SELECT line. sqlglot can't parse that. What shape do we support and how strict is the pattern match?
- **Decision:** A configurable ``optimize.union_separator`` (default ``","``). The pre-parse step matches commas that are alone on their line BETWEEN two SELECT statements, conservatively. Commas inside a SELECT list (between projections) are left alone.
- **Why it matters:** Surgical regex avoids false positives on real SELECT lists. The customer's convention is the only triggering shape we support today; the config knob keeps the door open for other separators if a different site uses a different one.
- **Reflected in code at:** ``_pre_parse_union_separator()`` called from ``parse_file()``.

### D42. Qualifier-rewrite transform

- **Question:** the customer wants every table reference in the optimised SQL prefixed by a single qualifier (default ``schema_identifier_ssf_snapshot``). What gets rewritten exactly?
- **Decision:** Both catalog and schema parts are replaced. ``catalog.schema.table`` → ``<qualifier>.table``. ``schema.table`` → ``<qualifier>.table``. **Bare** table names are NOT rewritten (the rule only fires when there's already a qualifier). CTE names defined in the same query are skipped.
- **Why it matters:**
  - Replacing both catalog and schema matches what an SSF migration target expects.
  - Leaving bare tables alone keeps test fixtures (which often use unqualified table names) untouched.
  - Skipping CTEs avoids rewriting query-local names as if they were base tables.
- **Rejected:**
  - "Schema only, leave catalog" — would leave inconsistent qualifications in the output.
  - "Add qualifier to bare tables too" — too aggressive; would mangle test fixtures.
- **Reflected in code at:** ``apply_table_qualifier()``.

### D43. Metadata-column stripping (rule 4 / 13 auto-fix)

- **Question:** the customer wants metadata columns excluded from SELECT projections AND from WHERE predicates. What about when stripping leaves the WHERE empty?
- **Decision:** Drop the WHERE clause entirely. Same auto-fix marks ``METADATA_COLUMN_EXPOSED`` findings as fixed. Movement.csv is filtered too (excluded outputs don't appear as rows) so the CSV reflects the optimised query.
- **Why it matters:**
  - Dropping the WHERE is the semantic intent: the predicate was about filtering on metadata; remove the metadata, remove the filter.
  - The previous ``WHERE 1=1`` placeholder option conflicts with our existing ``WHERE_1_EQUALS_1`` auto-fix.
  - Movement.csv staying in sync with the optimised SQL is the customer's primary expectation — both artefacts ship together.
- **Rejected:**
  - "Keep ``WHERE 1=1``" — bypass-then-undo with the existing rule.
  - "Skip strip when WHERE would be empty" — leaves the metadata predicate intact, defeating the rule.
- **Reflected in code at:** ``strip_metadata_columns()``; ``metadata_blacklist`` parameter on ``emit_movement_csv_rows()``.

### D44. Genie prompt rewritten as instruction block

- **Question:** the previous ``genie.md`` was a description of the query the user might paste *as* a query. The customer wants instructions to paste *above* their original query.
- **Decision:** Rewrite ``emit_genie_prompt`` to produce a numbered list of optimisation rules drawn from the customer's pack, scoped to what's relevant for this run's configuration. Ends with ``"Original query follows."``.
- **Why it matters:**
  - Matches how the customer actually uses Genie: paste-rules-above-query, not paste-query-only.
  - Scoping the rules by config keeps each prompt minimal (no schema-replacement instruction in the prompt when ``legacy_schemas`` is empty).
- **Reflected in code at:** rewritten ``emit_genie_prompt()``.

### D45. File include/exclude patterns

- **Decision:** Two new config keys, ``files.include`` and ``files.exclude``, both lists of pathlib-glob strings relative to the input dir. ``include`` non-empty restricts the set; ``exclude`` is applied after include.
- **Why it matters:** Customer test runs often target a subset of their corpus (one folder, one naming pattern). Adding includes/excludes avoids the workaround of copying SQL files to a scratch input dir.
- **Reflected in code at:** ``FileConfig`` dataclass; ``_select_input_files()`` helper in ``process_folder()``.

### D46. Aggregation CTE (2026-05-12)

- **Question:** Customer site has top-level SELECTs that mix aggregates (`SUM`, `MAX`, `COUNT`) with casts (`CAST(SUM(x) AS DECIMAL)`) and defaulting (`CASE WHEN ... ELSE 'unknown' END`). How should we split that?
- **Decision:** When the outer SELECT has top-level aggregates AND any formatting projection (CAST/CASE/COALESCE/literal/function call), lift JOINs + aggregates + GROUP BY into `<entity>_aggregated`. Outer SELECT then applies casting/defaulting only.
- **Why it matters:** keeping aggregation in its own CTE matches the customer's mental model: "compute the rollup once, then format". The two-step structure makes the SQL self-documenting and surfaces the GROUP BY keys at the layer that needs them.
- **Carve-outs that DON'T trigger extraction:**
  - Aggregates inside subqueries (`(SELECT MAX(x) FROM t)`) — separate SELECT scope.
  - Aggregates inside `OVER (...)` clauses (windowed) — semantically window functions, not group aggregates.
  - Top-level UNION ALL — handled separately.
- **Reflected in code at:** `extract_aggregation_cte()` + `_expression_has_aggregate()` (scope-aware check).

### D47. UNION-branch lifting (2026-05-12)

- **Question:** Customer SQL chains 2+ UNION ALL branches; readers can't tell where one branch ends. How do we make it scannable?
- **Decision:** Each top-level UNION ALL branch becomes its own CTE; top-level body is reduced to `SELECT * FROM cte_a UNION ALL SELECT * FROM cte_b ...`. Branch CTE names are inferred from a `'X' AS <tag>` string-literal projection inside the branch (snake-cased), with `<entity>_<n>` fallback.
- **Scope limits:**
  - UNION ALL only (UNION-distinct, INTERSECT, EXCEPT left alone).
  - Skipped when every branch is already a bare `SELECT * FROM <name>`.
  - Each branch is currently lifted as-is — recursive per-branch layering is planned, not implemented.
- **Why it matters:** the customer's UNION-of-valuations files (loan_loss + carrying_amount + ...) become readable: each branch is a self-contained CTE with a meaningful name; the UNION body documents the composition without burying it under 200 lines of projection logic.
- **Reflected in code at:** `extract_union_branches_to_ctes()` + `_walk_union_branches()` + `_infer_branch_name_from_literal()`.

### D48. Joined CTE (`<entity>_joined`) (2026-05-12)

- **Question:** Customer wants JOIN logic separated from aggregation, formatting, and source-table prep. Where does the JOIN live?
- **Decision:** Whenever the outer SELECT has at least one JOIN, lift the FROM + JOINs + WHERE into `<entity>_joined`. Single-source value transforms (UPPER, TRIM, arithmetic) that aren't casts/defaults/constants are also folded in. Outer SELECT then reads from a single-table FROM.
- **Why it matters:** the joined CTE answers "how do these tables relate" in one place. The downstream agg CTE (when present) just GROUP BYs over a single-table input — no JOIN logic mixed in. Matches the customer's stated rule: "each CTE has one concern."
- **Aggregate-CTE interaction:** when both transforms fire, the agg CTE's FROM is the joined CTE — agg CTE has no JOINs of its own.
- **Carve-outs:**
  - Top-level UNION — handled separately.
  - No JOINs in the outer SELECT — no joined CTE needed.
- **Planned extension:** single-source derivations (UPPER, TRIM, arithmetic over one source) should fold INTO the source CTE rather than the joined CTE; today they live in the joined CTE. Multi-source derivations stay joined.
- **Reflected in code at:** `extract_joined_cte()` + `_is_non_cast_derivation()`.

### D49. Passthrough-CTE rewrite (2026-05-12)

- **Question:** Customer's hand-rolled CTEs are sometimes thin wrappers: `WITH x AS (SELECT * FROM real_table)`. Should pushdown go INTO `x`'s body or add a sibling CTE?
- **Decision:** When a CTE body is exactly `SELECT * FROM <real_table> [WHERE ...]` (no JOIN/GROUP/UNION/etc.), mutate the body in place — replace the `*` with the renames the outer SELECT actually uses, AND-merge new WHERE predicates into the existing one. CTEs with anything more complex stay untouched.
- **Why it matters:** preserves the user's CTE name (which carries intent) and avoids the awkward `x` + `x_prepared` double-CTE for the same source.
- **Reflected in code at:** `_passthrough_cte_target()` (shape detector) + the passthrough branch in `push_projections_to_source_ctes()`.
