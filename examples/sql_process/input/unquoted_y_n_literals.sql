-- @mdde-entity: unquoted_y_n_literals
-- @mdde-layer: business
-- @mdde-stereotype: dim
-- @mdde-description: Customer SQL writes Y / N as bare tokens (interpreted by
-- the SQL author as string literals). sqlglot parses them as columns and the
-- rewrite breaks. The `optimize.unquoted_literals: ['Y', 'N']` pre-parse
-- substitution wraps them in single quotes before parsing.

SELECT
    customer_id,
    email,
    CASE WHEN email IS NOT NULL THEN Y ELSE N END AS has_email,
    CASE WHEN country = 'NL' THEN Y WHEN country = 'BE' THEN Y ELSE N END AS is_benelux
FROM raw.customer
