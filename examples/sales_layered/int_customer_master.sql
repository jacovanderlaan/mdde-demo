-- Layer: integration
-- Stereotype: int_master
-- Description: Conformed customer master — joins staging with reference data,
--              applies business rules, prepares for dimensional modeling

CREATE OR REPLACE VIEW int_customer_master AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.email,
    c.phone,
    c.created_at AS onboarding_date,
    CASE
        WHEN c.created_at >= CURRENT_DATE - INTERVAL '90' DAY THEN 'new'
        WHEN c.created_at >= CURRENT_DATE - INTERVAL '365' DAY THEN 'active'
        ELSE 'tenured'
    END AS customer_tenure_segment,
    c.source_system,
    c._ingested_at
FROM stg_customers c;
