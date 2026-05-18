-- @mdde-entity: metadata_in_subqueries
-- @mdde-layer: business
-- @mdde-stereotype: fact
-- @mdde-description: Customer-site regression fixture. Metadata columns
-- (Delivery_Set, XSD_Version, Period_Version, etc.) are filtered on at EVERY
-- level: outer WHERE, inline-subquery WHERE, inline-subquery projection list,
-- and JOIN ON keys. The strip pass must clean ALL of them, AND the source
-- pushdown's "expose referenced columns" walk must NOT re-introduce them by
-- noticing references inside other CTE bodies (different scope).
-- File_Delivery_Entity is intentionally KEPT (not in the blacklist) — it
-- identifies the source feed and is useful for lineage.

/*
Migration Details:
- sql_process Version: 1319747f (2026-05-18)
- Original SQL File: metadata_in_subqueries.sql
- Target SQL File:  optimized.sql
- Summary of Changes:
  - Lifted inline subqueries into named CTEs.
  - Pushed single-table projections and filters into per-source `_filtered` / `_prepared` CTEs.
  - Lifted JOINs and multi-source derivations into a dedicated `_joined` CTE; outer SELECT reads from a single-table FROM.
  - Excluded metadata columns from outputs and WHERE (`Delivery_Set`, `File_Delivery_Entity`, `File_Reporting_Date`, `File_Reporting_Period`, `Period_Version`, `Redelivery_Number`, `XSD_Version`).
  - Rewrote table qualifiers (catalog/schema) to the target qualifier.

Validation Checklist:
- [X] Subqueries encapsulated as CTEs.
- [X] Modular CTE structure applied.
- [X] JOIN isolated into joined CTE.
- [X] Metadata columns excluded.
- [X] Table qualifiers normalised.
*/

-- Source filter: single-table SELECT + WHERE for one source
WITH prp_filtered AS (
  SELECT
    Product AS financing_product_id,
    SourceSystemIdentifier AS financing_product_ssid,
    RepaymentMethod AS repayment_method,
    Product
  FROM schema_identifier_ssf_snapshot.prp
  WHERE
    NOT RepaymentMethod IS NULL
), fp AS (
  SELECT
    fp.Product,
    fp.PortfolioBalance
  FROM schema_identifier_ssf_snapshot.fp AS fp
)
-- Source prep: single-table SELECT + renames + single-source value transforms
, fp_prepared AS (
  SELECT
    PortfolioBalance AS portfolio_balance,
    Product
  FROM fp
)
-- Joined: JOINs + multi-source derivations only (no WHERE, no aggregation)
, prp_joined AS (
  SELECT
    financing_product_id,
    financing_product_ssid,
    repayment_method,
    portfolio_balance
  FROM prp_filtered AS prp
  LEFT JOIN fp_prepared AS fp
    ON fp.Product = prp.Product
)
/* @mdde-entity: metadata_in_subqueries */
/* @mdde-layer: business */
/* @mdde-stereotype: fact */
/* @mdde-description: Customer-site regression fixture. Metadata columns */
/* (Delivery_Set, XSD_Version, Period_Version, etc.) are filtered on at EVERY */
/* level: outer WHERE, inline-subquery WHERE, inline-subquery projection list, */
/* and JOIN ON keys. The strip pass must clean ALL of them, AND the source */
/* pushdown's "expose referenced columns" walk must NOT re-introduce them by */
/* noticing references inside other CTE bodies (different scope). */
/* File_Delivery_Entity is intentionally KEPT (not in the blacklist) — it */
/* identifies the source feed and is useful for lineage. */
SELECT
  financing_product_id,
  financing_product_ssid,
  repayment_method,
  portfolio_balance
FROM prp_joined