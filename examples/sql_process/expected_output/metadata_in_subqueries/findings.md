# Findings — `metadata_in_subqueries.sql`

**Parse:** ok
**Qualify:** ok

**Total:** 14 (error=0, warning=10, info=4)  
**Auto-fixed:** 10

| Location | Rule | Severity | Auto-fixed | Message |
|---|---|---|---|---|
| <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2026-03-31' |
| <file> | HARDCODED_DATE | info | no | Hardcoded date literal: '2026-03-31' |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'File_Delivery_Entity' exposed in output (as 'FileDeliveryEntity') |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'XSD_Version' exposed in output (as 'XSDVersion') |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'Period_Version' exposed in output (as 'PeriodVersion') |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'Delivery_Set' exposed in output (as 'DeliverySet') |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'Redelivery_Number' exposed in output (as 'RedeliveryNumber') |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'File_Reporting_Date' exposed in output (as 'FileReportingDate') |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'File_Reporting_Period' exposed in output (as 'FileReportingPeriod') |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'Period_Version' exposed in output |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'Delivery_Set' exposed in output |
| <file> | METADATA_COLUMN_EXPOSED | warning | yes | Metadata column 'File_Reporting_Date' exposed in output |
| <file> | DERIVATION_IN_WHERE | info | no | IS [NOT] NULL on raw column 'repaymentmethod' inside WHERE |
| <file> | MISSING_SOURCE_VERSION | info | no | Filename 'metadata_in_subqueries.sql' has fewer than 3 hyphen-separated parts; movement.csv source_version will be empty |
