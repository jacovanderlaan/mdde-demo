-- Sample metadata for dbt generation demo
-- Run this to populate the MDDE Lite schema with sample data

-- Source layer entities
INSERT INTO entity (entity_id, name, description, layer, stereotype) VALUES
    ('src_customers', 'raw_customers', 'Raw customer data from source system', 'source', 'src_external'),
    ('src_orders', 'raw_orders', 'Raw order data from source system', 'source', 'src_external'),
    ('src_products', 'raw_products', 'Raw product catalog', 'source', 'src_external');

-- Staging layer entities
INSERT INTO entity (entity_id, name, description, layer, stereotype) VALUES
    ('stg_customers', 'stg_customers', 'Cleaned and standardized customer data', 'staging', 'stg_cleaned'),
    ('stg_orders', 'stg_orders', 'Cleaned order data with data quality fixes', 'staging', 'stg_cleaned');

-- Business layer entities (dimensional model)
INSERT INTO entity (entity_id, name, description, layer, stereotype) VALUES
    ('dim_customer', 'dim_customer', 'Customer dimension with SCD Type 2', 'business', 'dim_scd2'),
    ('dim_product', 'dim_product', 'Product dimension', 'business', 'dim_dimension'),
    ('fct_orders', 'fct_orders', 'Order facts at line item grain', 'business', 'dim_fact');

-- Source attributes
INSERT INTO attribute (attribute_id, entity_id, name, data_type, ordinal_position, is_primary_key, description) VALUES
    ('src_cust_id', 'src_customers', 'customer_id', 'INTEGER', 1, TRUE, 'Customer identifier'),
    ('src_cust_name', 'src_customers', 'name', 'VARCHAR(100)', 2, FALSE, 'Customer full name'),
    ('src_cust_email', 'src_customers', 'email', 'VARCHAR(255)', 3, FALSE, 'Customer email address'),
    ('src_cust_region', 'src_customers', 'region', 'VARCHAR(50)', 4, FALSE, 'Geographic region'),
    ('src_cust_created', 'src_customers', 'created_at', 'TIMESTAMP', 5, FALSE, 'Account creation timestamp');

INSERT INTO attribute (attribute_id, entity_id, name, data_type, ordinal_position, is_primary_key, description) VALUES
    ('src_ord_id', 'src_orders', 'order_id', 'INTEGER', 1, TRUE, 'Order identifier'),
    ('src_ord_cust', 'src_orders', 'customer_id', 'INTEGER', 2, FALSE, 'Customer FK'),
    ('src_ord_prod', 'src_orders', 'product_id', 'INTEGER', 3, FALSE, 'Product FK'),
    ('src_ord_qty', 'src_orders', 'quantity', 'INTEGER', 4, FALSE, 'Order quantity'),
    ('src_ord_amt', 'src_orders', 'amount', 'DECIMAL(10,2)', 5, FALSE, 'Order amount'),
    ('src_ord_date', 'src_orders', 'order_date', 'DATE', 6, FALSE, 'Order date');

-- Staging attributes (with transformations)
INSERT INTO attribute (attribute_id, entity_id, name, data_type, ordinal_position, is_primary_key, description) VALUES
    ('stg_cust_id', 'stg_customers', 'customer_id', 'INTEGER', 1, TRUE, 'Customer identifier'),
    ('stg_cust_name', 'stg_customers', 'customer_name', 'VARCHAR(100)', 2, FALSE, 'Cleaned customer name'),
    ('stg_cust_email', 'stg_customers', 'email_address', 'VARCHAR(255)', 3, FALSE, 'Validated email'),
    ('stg_cust_region', 'stg_customers', 'region_code', 'VARCHAR(10)', 4, FALSE, 'Standardized region code'),
    ('stg_load_ts', 'stg_customers', '_loaded_at', 'TIMESTAMP', 5, FALSE, 'ETL load timestamp');

-- Dimension attributes (SCD2)
INSERT INTO attribute (attribute_id, entity_id, name, data_type, ordinal_position, is_primary_key, description) VALUES
    ('dim_cust_sk', 'dim_customer', 'customer_sk', 'INTEGER', 1, TRUE, 'Surrogate key'),
    ('dim_cust_id', 'dim_customer', 'customer_id', 'INTEGER', 2, FALSE, 'Natural key'),
    ('dim_cust_name', 'dim_customer', 'customer_name', 'VARCHAR(100)', 3, FALSE, 'Customer name'),
    ('dim_cust_email', 'dim_customer', 'email_address', 'VARCHAR(255)', 4, FALSE, 'Email address'),
    ('dim_cust_region', 'dim_customer', 'region_code', 'VARCHAR(10)', 5, FALSE, 'Region code'),
    ('dim_valid_from', 'dim_customer', 'valid_from', 'TIMESTAMP', 6, FALSE, 'SCD2 valid from'),
    ('dim_valid_to', 'dim_customer', 'valid_to', 'TIMESTAMP', 7, FALSE, 'SCD2 valid to'),
    ('dim_is_current', 'dim_customer', 'is_current', 'BOOLEAN', 8, FALSE, 'Current record flag');

-- Attribute mappings (column-level lineage)
-- Source -> Staging mappings
INSERT INTO attribute_mapping (mapping_id, target_entity_id, target_attribute_id, source_entity_id, source_attribute_id, mapping_type, transformation) VALUES
    ('map_stg_1', 'stg_customers', 'stg_cust_id', 'src_customers', 'src_cust_id', 'direct', NULL),
    ('map_stg_2', 'stg_customers', 'stg_cust_name', 'src_customers', 'src_cust_name', 'derived', 'TRIM(UPPER(name))'),
    ('map_stg_3', 'stg_customers', 'stg_cust_email', 'src_customers', 'src_cust_email', 'derived', 'LOWER(TRIM(email))'),
    ('map_stg_4', 'stg_customers', 'stg_cust_region', 'src_customers', 'src_cust_region', 'derived', 'UPPER(SUBSTRING(region, 1, 10))'),
    ('map_stg_5', 'stg_customers', 'stg_load_ts', NULL, NULL, 'constant', 'CURRENT_TIMESTAMP');

-- Staging -> Dimension mappings
INSERT INTO attribute_mapping (mapping_id, target_entity_id, target_attribute_id, source_entity_id, source_attribute_id, mapping_type, transformation) VALUES
    ('map_dim_1', 'dim_customer', 'dim_cust_sk', NULL, NULL, 'derived', 'ROW_NUMBER() OVER (ORDER BY customer_id)'),
    ('map_dim_2', 'dim_customer', 'dim_cust_id', 'stg_customers', 'stg_cust_id', 'direct', NULL),
    ('map_dim_3', 'dim_customer', 'dim_cust_name', 'stg_customers', 'stg_cust_name', 'direct', NULL),
    ('map_dim_4', 'dim_customer', 'dim_cust_email', 'stg_customers', 'stg_cust_email', 'direct', NULL),
    ('map_dim_5', 'dim_customer', 'dim_cust_region', 'stg_customers', 'stg_cust_region', 'direct', NULL),
    ('map_dim_6', 'dim_customer', 'dim_valid_from', NULL, NULL, 'constant', 'CURRENT_TIMESTAMP'),
    ('map_dim_7', 'dim_customer', 'dim_valid_to', NULL, NULL, 'constant', '''9999-12-31 23:59:59''::TIMESTAMP'),
    ('map_dim_8', 'dim_customer', 'dim_is_current', NULL, NULL, 'constant', 'TRUE');
