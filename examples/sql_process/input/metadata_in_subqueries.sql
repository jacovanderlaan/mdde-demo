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

SELECT
    prp.Product AS financing_product_id,
    prp.SourceSystemIdentifier AS financing_product_ssid,
    prp.RepaymentMethod AS repayment_method,
    fp.PortfolioBalance AS portfolio_balance,
    prp.File_Delivery_Entity AS FileDeliveryEntity,
    prp.XSD_Version AS XSDVersion,
    prp.Period_Version AS PeriodVersion,
    prp.Delivery_Set AS DeliverySet,
    prp.Redelivery_Number AS RedeliveryNumber,
    prp.File_Reporting_Date AS FileReportingDate,
    prp.File_Reporting_Period AS FileReportingPeriod
FROM raw.prp AS prp
LEFT JOIN (
    SELECT
        fp.Product,
        fp.PortfolioBalance,
        fp.Period_Version,
        fp.Delivery_Set,
        fp.File_Reporting_Date
    FROM raw.fp AS fp
    WHERE fp.File_Delivery_Entity = 'GRIPReporter'
      AND fp.Delivery_Set = '8F'
      AND fp.Period_Version = 'V1'
      AND fp.XSD_Version = '2025Q4V2'
      AND fp.File_Reporting_Date = '2026-03-31'
      AND fp.File_Reporting_Period = 'EOM'
      AND fp.Redelivery_Number = '1'
) AS fp
  ON fp.Product = prp.Product
  AND fp.Delivery_Set = prp.Delivery_Set
  AND fp.Period_Version = prp.Period_Version
WHERE prp.File_Delivery_Entity = 'GRIPReporter'
  AND prp.Delivery_Set = '8F'
  AND prp.Period_Version = 'V1'
  AND prp.XSD_Version = '2025Q4V2'
  AND prp.File_Reporting_Date = '2026-03-31'
  AND prp.File_Reporting_Period = 'EOM'
  AND prp.Redelivery_Number = '1'
  AND NOT prp.RepaymentMethod IS NULL
