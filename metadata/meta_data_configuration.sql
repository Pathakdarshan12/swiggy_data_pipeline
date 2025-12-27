INSERT INTO COMMON.FILE_FORMAT_MASTER VALUES  (1, 'CSV', ',', '"', NULL, 'Y', 'NONE', '\n', 'UTF-8', CURRENT_TIMESTAMP);

INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(101, 'ORDERS_BATCH', 'SNOWFLAKE', 1, '@SWIGGY.BRONZE.CSV_STG/order/', 'orders_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(102, 'ORDERS_STREAM', 'KAFKA_STREAM', 1, 'SWIGGY.BRONZE.STREAM_ORDERS_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO COMMON.DATA_FIELD_MASTER(FIELD_ID, FIELD_NAME, DATA_TYPE, LENGTH, PRECISION, SCALE, NULLABLE_FLAG, BUSINESS_DESCRIPTION, DATA_DOMAIN, PII_FLAG, CREATED_AT) VALUES
(1, 'ORDER_ID', 'VARCHAR', 50, NULL, NULL, 'N', 'Unique order identifier', 'ORDER', 'N', CURRENT_TIMESTAMP),
(2, 'CUSTOMER_ID', 'VARCHAR', 50, NULL, NULL, 'N', 'Customer foreign key', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(3, 'RESTAURANT_ID', 'INTEGER', NULL, NULL, NULL, 'N', 'Restaurant foreign key', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(4, 'ORDER_DATE', 'DATE', NULL, NULL, NULL, 'N', 'Order placement date', 'ORDER', 'N', CURRENT_TIMESTAMP),
(5, 'TOTAL_AMOUNT', 'NUMBER', NULL, 10, 2, 'N', 'Order total amount', 'ORDER', 'N', CURRENT_TIMESTAMP),
(6, 'STATUS', 'VARCHAR', 20, NULL, NULL, 'N', 'Order status', 'ORDER', 'N', CURRENT_TIMESTAMP),
(7, 'PAYMENT_METHOD', 'VARCHAR', 50, NULL, NULL, 'Y', 'Payment method used', 'ORDER', 'N', CURRENT_TIMESTAMP),
(8, 'MENU_ID', 'INTEGER', NULL, NULL, NULL, 'N', 'MENU ID', 'MENU', 'N', CURRENT_TIMESTAMP);

INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1001, 101, 'order_id', 1, 1, TRUE, NULL, 'IDENTITY START 1 INCREMENT 1 NOORDER', NULL, 'Y', CURRENT_TIMESTAMP),
(1002, 101, 'customer_id', 2, 2, FALSE, 'TRIM(customer_id)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1003, 101, 'restaurant_id', 3, 3, FALSE, 'CAST(restaurant_id AS INT)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1004, 101, 'order_date', 4, 4, FALSE, 'TO_DATE(order_date)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1005, 101, 'total_amount', 5, 5, FALSE, 'CAST(total_amount AS NUMBER)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1006, 101, 'status', 6, 6, FALSE, 'UPPER(TRIM(status))', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1007, 101, 'payment_method', 7, 7, FALSE, 'TRIM(payment_method)', NULL, NULL, 'Y', CURRENT_TIMESTAMP);

INSERT INTO COMMON.FILE_COLUMN_MAPPING
(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1001, 102, 'order_id', 1, 1, TRUE, TRUE,'UUID_STRING()', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1002, 102, 'customer_id', 2, 2, FALSE, FALSE, 'TRIM(customer_id)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1003, 102, 'restaurant_id', 3, 3, FALSE, FALSE, 'CAST(restaurant_id AS INT)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1004, 102, 'order_date', 4, 4, FALSE, FALSE, 'TO_DATE(order_date)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1005, 102, 'total_amount', 5, 5, FALSE, FALSE, 'CAST(total_amount AS NUMBER)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1006, 102, 'status', 6, 6, FALSE, FALSE, 'UPPER(TRIM(status))', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1007, 102, 'payment_method', 7, 7, FALSE, FALSE, 'TRIM(payment_method)', NULL, NULL, 'Y', CURRENT_TIMESTAMP);

INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(501, 101, 'SWIGGY', 'BRONZE','ORDER_BRZ', 'APPEND', 'ORDER_ID',NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(502, 102, 'SWIGGY', 'BRONZE', 'ORDER_BRZ', 'APPEND', 'ORDER_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- MENU --
-- FILE_FORMAT_MASTER (already exists, reusing format_id = 1 for CSV)

-- SOURCE_FILE_CONFIG for Menu data
INSERT INTO COMMON.SOURCE_FILE_CONFIG
(SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT)
VALUES
(103, 'MENU_BATCH', 'SNOWFLAKE', 1, '@SWIGGY.BRONZE.CSV_STG/menu/', 'menu_*.csv', 'BATCH', '0 3 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(104, 'MENU_STREAM', 'KAFKA_STREAM', 1, 'SWIGGY.BRONZE.STREAM_MENU_CHANGES', NULL, 'STREAM', '0 3 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- DATA_FIELD_MASTER for Menu fields
INSERT INTO COMMON.DATA_FIELD_MASTER
(FIELD_ID, FIELD_NAME, DATA_TYPE, LENGTH, PRECISION, SCALE, NULLABLE_FLAG, BUSINESS_DESCRIPTION, DATA_DOMAIN, PII_FLAG, CREATED_AT)
VALUES
(8, 'MENU_ID', 'INTEGER', NULL, NULL, NULL, 'N', 'Unique menu item identifier', 'MENU', 'N', CURRENT_TIMESTAMP),
(9, 'RESTAURANT_ID', 'INTEGER', NULL, NULL, NULL, 'N', 'Restaurant foreign key', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(10, 'ITEM_NAME', 'VARCHAR', NULL, NULL, NULL, 'Y', 'Menu item name', 'MENU', 'N', CURRENT_TIMESTAMP),
(11, 'DESCRIPTION', 'VARCHAR', NULL, NULL, NULL, 'Y', 'Menu item description', 'MENU', 'N', CURRENT_TIMESTAMP),
(12, 'PRICE', 'NUMBER', NULL, 10, 2, 'Y', 'Menu item price', 'MENU', 'N', CURRENT_TIMESTAMP),
(13, 'CATEGORY', 'VARCHAR', NULL, NULL, NULL, 'Y', 'Menu item category', 'MENU', 'N', CURRENT_TIMESTAMP),
(14, 'AVAILABILITY', 'VARCHAR', NULL, NULL, NULL, 'Y', 'Item availability status', 'MENU', 'N', CURRENT_TIMESTAMP),
(15, 'ITEM_TYPE', 'VARCHAR', NULL, NULL, NULL, 'Y', 'Item type (veg/non-veg)', 'MENU', 'N', CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for MENU_BATCH (Source 103)
INSERT INTO COMMON.FILE_COLUMN_MAPPING
(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT)
VALUES
(1008, 103, 'menu_id', 8, 1, TRUE, NULL, 'IDENTITY START 1 INCREMENT 1 NOORDER', NULL, 'Y', CURRENT_TIMESTAMP),
(1009, 103, 'restaurant_id', 9, 2, FALSE, 'CAST(restaurant_id AS INT)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1010, 103, 'item_name', 10, 3, FALSE, 'TRIM(item_name)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1011, 103, 'description', 11, 4, FALSE, 'TRIM(description)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1012, 103, 'price', 12, 5, FALSE, 'CAST(price AS NUMBER(10,2))', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1013, 103, 'category', 13, 6, FALSE, 'UPPER(TRIM(category))', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1014, 103, 'availability', 14, 7, FALSE, 'UPPER(TRIM(availability))', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1015, 103, 'item_type', 15, 8, FALSE, 'UPPER(TRIM(item_type))', NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for MENU_STREAM (Source 104)
INSERT INTO COMMON.FILE_COLUMN_MAPPING
(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT)
VALUES
(1016, 104, 'menu_id', 8, 1, TRUE, 'UUID_STRING()', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1017, 104, 'restaurant_id', 9, 2, FALSE, 'CAST(restaurant_id AS INT)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1018, 104, 'item_name', 10, 3, FALSE, 'TRIM(item_name)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1019, 104, 'description', 11, 4, FALSE, 'TRIM(description)', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1020, 104, 'price', 12, 5, FALSE, 'CAST(price AS NUMBER(10,2))', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1021, 104, 'category', 13, 6, FALSE, 'UPPER(TRIM(category))', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1022, 104, 'availability', 14, 7, FALSE, 'UPPER(TRIM(availability))', NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1023, 104, 'item_type', 15, 8, FALSE, 'UPPER(TRIM(item_type))', NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- TARGET_TABLE_MAPPING for MENU_BATCH
INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT)
VALUES
(503, 103, 'SWIGGY', 'BRONZE', 'MENU_BRZ', 'APPEND', 'MENU_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- TARGET_TABLE_MAPPING for MENU_STREAM
INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, TARGET_DATABASE, TARGET_SCHEMA, TARGET_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT)
VALUES
(504, 104, 'SWIGGY', 'BRONZE', 'MENU_BRZ', 'APPEND', 'MENU_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- METADATA_KEYS_VIEW
-- Purpose: Centralized view for all key types used in data ingestion and transformation
-- Provides: Business keys, Audit keys, Merge keys, Partition keys, and Clustering keys
-- =======================================================================================================================================
CREATE OR REPLACE VIEW COMMON.METADATA_KEYS_VIEW AS
SELECT
    -- Source & Target Identification
    t.source_id,
    s.source_name,
    t.target_id,
    t.target_database,
    t.target_schema,
    t.target_table,
    CONCAT(t.target_database, '.', t.target_schema, '.', t.target_table) AS full_target_table_name,

    -- Business Keys (Primary Keys)
    t.primary_key_fields AS business_keys_string,
    SPLIT(t.primary_key_fields, ',') AS business_keys_array,

    -- Business Keys with Field Details
    LISTAGG(
        CASE
            WHEN fcm.is_primary_key = TRUE
            THEN dfm.field_name
        END,
        ', '
    ) WITHIN GROUP (ORDER BY fcm.column_position) AS business_keys_list,

    -- Business Keys with Data Types (for MERGE conditions)
    LISTAGG(
        CASE
            WHEN fcm.is_primary_key = TRUE
            THEN dfm.field_name || '::' || dfm.data_type
        END,
        ', '
    ) WITHIN GROUP (ORDER BY fcm.column_position) AS business_keys_typed,

    -- Merge Keys
    t.merge_key_fields AS merge_keys_string,
    SPLIT(t.merge_key_fields, ',') AS merge_keys_array,

    -- Audit Columns Configuration
    t.enable_audit_columns,
    CASE
        WHEN t.enable_audit_columns = 'Y'
        THEN 'INGEST_RUN_ID, CREATED_AT, UPDATED_AT'
        ELSE ''
    END AS audit_columns_list,

    -- Audit Column Data Types
    CASE
        WHEN t.enable_audit_columns = 'Y'
        THEN 'INGEST_RUN_ID::NUMBER, CREATED_AT::TIMESTAMP, UPDATED_AT::TIMESTAMP'
        ELSE ''
    END AS audit_columns_typed,

    -- Raw Columns Configuration
    t.enable_raw_columns,

    -- Raw Columns List (non-PK fields with _RAW suffix)
    LISTAGG(
        CASE
            WHEN fcm.is_primary_key = FALSE
            THEN dfm.field_name || '_RAW'
        END,
        ', '
    ) WITHIN GROUP (ORDER BY fcm.column_position) AS raw_columns_list,

    -- Partition Keys
    t.partition_columns AS partition_keys_string,
    SPLIT(t.partition_columns, ',') AS partition_keys_array,

    -- Clustering Keys
    t.clustering_columns AS clustering_keys_string,
    SPLIT(t.clustering_columns, ',') AS clustering_keys_array,

    -- Load Configuration
    t.load_type,
    s.ingestion_type,

    -- All Target Columns (Business + Raw + Audit)
    LISTAGG(dfm.field_name, ', ')
        WITHIN GROUP (ORDER BY fcm.column_position)
    || CASE
        WHEN t.enable_raw_columns = 'Y'
        THEN ', ' || LISTAGG(
            CASE
                WHEN fcm.is_primary_key = FALSE
                THEN dfm.field_name || '_RAW'
            END,
            ', '
        ) WITHIN GROUP (ORDER BY fcm.column_position)
        ELSE ''
    END
    || CASE
        WHEN t.enable_audit_columns = 'Y'
        THEN ', INGEST_RUN_ID, CREATED_AT, UPDATED_AT'
        ELSE ''
    END AS all_target_columns,

    -- Source Column Count
    COUNT(DISTINCT fcm.mapping_id) AS total_source_columns,

    -- Primary Key Count
    SUM(CASE WHEN fcm.is_primary_key = TRUE THEN 1 ELSE 0 END) AS pk_column_count,

    -- Active Flags
    t.active_flag AS target_active_flag,
    s.active_flag AS source_active_flag

FROM COMMON.TARGET_TABLE_MAPPING t
JOIN COMMON.SOURCE_FILE_CONFIG s
    ON t.source_id = s.source_id
JOIN COMMON.FILE_COLUMN_MAPPING fcm
    ON s.source_id = fcm.source_id
JOIN COMMON.DATA_FIELD_MASTER dfm
    ON fcm.field_id = dfm.field_id

WHERE t.active_flag = 'Y'
  AND s.active_flag = 'Y'
  AND fcm.active_flag = 'Y'

GROUP BY
    t.source_id,
    s.source_name,
    t.target_id,
    t.target_database,
    t.target_schema,
    t.target_table,
    t.primary_key_fields,
    t.merge_key_fields,
    t.enable_audit_columns,
    t.enable_raw_columns,
    t.partition_columns,
    t.clustering_columns,
    t.load_type,
    s.ingestion_type,
    t.active_flag,
    s.active_flag;


SELECT * FROM METADATA_KEYS_VIEW; WHERE SOURCE_ID IN (103,104);