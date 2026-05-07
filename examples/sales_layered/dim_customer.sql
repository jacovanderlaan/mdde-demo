-- Layer: business
-- Stereotype: dim_scd2
-- Description: Customer dimension with SCD Type 2 history tracking

CREATE OR REPLACE VIEW dim_customer AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id, _valid_from) AS customer_sk,
    customer_id,
    full_name,
    email,
    phone,
    onboarding_date,
    customer_tenure_segment,
    source_system,
    _valid_from,
    _valid_to,
    _is_current
FROM (
    SELECT
        customer_id,
        full_name,
        email,
        phone,
        onboarding_date,
        customer_tenure_segment,
        source_system,
        _ingested_at AS _valid_from,
        LEAD(_ingested_at) OVER (PARTITION BY customer_id ORDER BY _ingested_at) AS _valid_to,
        CASE
            WHEN LEAD(_ingested_at) OVER (PARTITION BY customer_id ORDER BY _ingested_at) IS NULL
            THEN TRUE ELSE FALSE
        END AS _is_current
    FROM int_customer_master
);
