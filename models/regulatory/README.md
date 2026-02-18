# Regulatory Models

This folder contains MDDE models based on public regulatory frameworks. These models demonstrate how MDDE can capture complex regulatory data requirements in a structured, metadata-driven format.

## ECB Regulatory Models

### AnaCredit (Analytical Credit Datasets)

**Location:** `ecb/anacredit/`

AnaCredit is a dataset containing detailed information on individual bank loans in the euro area, collected by the ECB under Regulation (EU) 2016/867.

**Key Entities:**
- `counterparty` - Institutional units that are parties to credit instruments
- `contract` - Legally binding agreements between parties
- `instrument` - Individual credit instruments (loans, credit lines, etc.)
- `protection_received` - Collateral and guarantees securing instruments
- `accounting_data` - IFRS/GAAP accounting information
- `financial_data` - Balance and flow information

**References:**
- [ECB AnaCredit Portal](https://www.ecb.europa.eu/stats/money_credit_banking/anacredit/html/index.en.html)
- [AnaCredit Reporting Manual Part II](https://www.ecb.europa.eu/stats/money_credit_banking/anacredit/html/index.en.html)
- [Regulation (EU) 2016/867](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX%3A32016R0867)

### RRE (Residential Real Estate)

**Location:** `ecb/rre/`

The RRE model extends AnaCredit with additional data requirements specific to residential real estate lending. It supports monitoring of systemic risks in the residential mortgage market.

**Key Entities:**
- `counterparty` - Borrowers (primarily households)
- `instrument` - Mortgage instruments with LTV, income data
- `immovable_property` - Real estate collateral details
- `household` - Household-level borrower information
- `natural_person` - Individual borrower characteristics

**RRE-Specific Attributes:**
- Loan-to-Value (LTV) ratios
- Household income at inception
- Buy-to-let indicators
- Property type and valuation method
- Employment status

**References:**
- [DNB Residential Real Estate Dashboard](https://www.dnb.nl/en/statistics/dashboards/residential-real-estate/)
- [ECB Macroprudential Bulletin](https://www.ecb.europa.eu/pub/financial-stability/macroprudential-bulletin/html/index.en.html)

## Model Structure

Each model follows the MDDE three-layer architecture:

```
model/
├── model.yaml              # Model metadata and description
├── entities/
│   └── {entity_name}/
│       └── entity.logical.yaml   # Entity definition
└── domains/
    └── domains.yaml        # Domain/enumeration definitions
```

## Regulatory Stereotypes

These models use MDDE's regulatory stereotypes to classify entities:

| Stereotype | Description | BCBS 239 Domain |
|------------|-------------|-----------------|
| `reg_critical_data` | Critical data elements requiring governance | Governance |
| `reg_risk_aggregation` | Data used for risk aggregation | Aggregation |
| `reg_exposure` | Exposure-related data | Reporting |
| `reg_risk_report` | Risk reporting outputs | Reporting |

## BCBS 239 Compliance

Both models include BCBS 239 (Basel Committee on Banking Supervision) metadata:
- `bcbs239_domain` - Maps entities to BCBS 239 domains (governance, aggregation, reporting, supervision)
- `is_critical_data_element` - Identifies critical data elements
- `data_quality_tier` - Data quality classification

## Using These Models

These models can be processed by the MDDE framework to generate:
- DDL for various platforms (Snowflake, Databricks, BigQuery)
- dbt models with regulatory validation
- Data lineage documentation
- ERD diagrams
- Data quality rules

## Note

These are public regulatory frameworks. The YAML representations here demonstrate MDDE's modeling capabilities. For production use with the full MDDE framework (generators, analyzers, VS Code extension), please contact for licensing information.
