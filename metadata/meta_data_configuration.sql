-- =======================================================================================================================================
-- FILE FORMAT CONFIGURATION
-- =======================================================================================================================================
INSERT INTO COMMON.FILE_FORMAT_MASTER VALUES  (1, 'CSV', ',', '"', NULL, 'Y', 'NONE', '\n', 'UTF-8', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- DATA FIELD MASTER CONFIGURATION
-- USE- Define all the columns here which will be used in pipeline reuse same column for compatiblity between pipelines
-- =======================================================================================================================================
INSERT INTO COMMON.DATA_FIELD_MASTER(FIELD_ID, FIELD_NAME, DATA_TYPE, LENGTH, PRECISION, SCALE, NULLABLE_FLAG, BUSINESS_DESCRIPTION, DATA_DOMAIN, PII_FLAG, CREATED_AT) VALUES
(1, 'ORDER_ID', 'VARCHAR', 50, NULL, NULL, 'N', 'Unique order identifier', 'ORDER', 'N', CURRENT_TIMESTAMP),
(2, 'CUSTOMER_ID', 'VARCHAR', 50, NULL, NULL, 'N', 'Customer foreign key', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(3, 'RESTAURANT_ID', 'INTEGER', NULL, NULL, NULL, 'N', 'Restaurant foreign key', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(4, 'ORDER_DATE', 'DATE', NULL, NULL, NULL, 'N', 'Order placement date', 'ORDER', 'N', CURRENT_TIMESTAMP),
(5, 'TOTAL_AMOUNT', 'NUMBER', NULL, 10, 2, 'N', 'Order total amount', 'ORDER', 'N', CURRENT_TIMESTAMP),
(6, 'ORDER_STATUS', 'VARCHAR', 20, NULL, NULL, 'N', 'Order status', 'ORDER', 'N', CURRENT_TIMESTAMP),
(7, 'PAYMENT_METHOD', 'VARCHAR', 50, NULL, NULL, 'Y', 'Payment method used', 'ORDER', 'N', CURRENT_TIMESTAMP),
(8, 'MENU_ID', 'INTEGER', NULL, NULL, NULL, 'N', 'MENU ID', 'MENU', 'N', CURRENT_TIMESTAMP),
(9, 'DELIVERY_ID', 'VARCHAR',50, NULL, NULL, 'N', 'Delivery ID', 'Delivery', 'N', CURRENT_TIMESTAMP ),
(10, 'DELIVERY_AGENT_ID', 'INTEGER',NULL, NULL, NULL, 'N', 'Delivery AGENT ID', 'Delivery', 'N', CURRENT_TIMESTAMP ),
(11, 'DELIVERY_STATUS', 'VARCHAR',50, NULL, NULL, 'N', 'Delivery STATUS', 'Delivery', 'N', CURRENT_TIMESTAMP ),
(12, 'ESTIMATED_TIME', 'VARCHAR',50, NULL, NULL, 'N', 'Delivery ESTIMATED TIME', 'Delivery', 'N', CURRENT_TIMESTAMP ),
(13, 'CUSTOMER_ADDRESS_ID', 'INTEGER',NULL, NULL, NULL, 'N', 'CUSTOMER ADDRESS ID', 'Customer', 'N', CURRENT_TIMESTAMP ),
(14, 'DELIVERY_DATE', 'DATE',NULL, NULL, NULL, 'N', 'DELIVERY_DATE', 'Customer', 'N', CURRENT_TIMESTAMP ),
(15, 'ORDER_ITEM_ID', 'VARCHAR', 50, NULL, NULL, 'N', 'Unique order item identifier', 'ORDER', 'N', CURRENT_TIMESTAMP),
(16, 'QUANTITY', 'NUMBER', NULL, 10, 2, 'N', 'Item quantity ordered', 'ORDER', 'N', CURRENT_TIMESTAMP),
(17, 'PRICE', 'NUMBER', NULL, 10, 2, 'N', 'Unit price of item', 'ORDER', 'N', CURRENT_TIMESTAMP),
(18, 'SUBTOTAL', 'NUMBER', NULL, 10, 2, 'N', 'Line item subtotal', 'ORDER', 'N', CURRENT_TIMESTAMP),
(19, 'ORDER_TIMESTAMP', 'DATE', NULL, NULL, NULL, 'N', 'Order item timestamp', 'ORDER', 'N', CURRENT_TIMESTAMP),
(21, 'FSSAI_REGISTRATION_NO', 'INTEGER', NULL, NULL, NULL, 'N', 'FSSAI registration number', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(22, 'RESTAURANT_NAME', 'VARCHAR', 200, NULL, NULL, 'N', 'Restaurant name', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(23, 'CUISINE_TYPE', 'VARCHAR', 100, NULL, NULL, 'Y', 'Type of cuisine offered', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(24, 'PRICING_FOR_TWO', 'INTEGER', NULL, 10, 2, 'Y', 'Average pricing for two people', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(25, 'RESTAURANT_PHONE', 'VARCHAR', 20, NULL, NULL, 'Y', 'Restaurant contact phone', 'RESTAURANT', 'Y', CURRENT_TIMESTAMP),
(26, 'OPERATING_HOURS', 'VARCHAR', 100, NULL, NULL, 'Y', 'Restaurant operating hours', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(27, 'LOCATION_ID', 'INTEGER', NULL, NULL, NULL, 'Y', 'Location foreign key', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(28, 'ACTIVE_FLAG', 'VARCHAR', 1, NULL, NULL, 'Y', 'Restaurant active status flag', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(29, 'OPEN_STATUS', 'VARCHAR', 20, NULL, NULL, 'Y', 'Current open/closed status', 'RESTAURANT', 'N', CURRENT_TIMESTAMP),
(30, 'LOCALITY', 'VARCHAR', 100, NULL, NULL, 'Y', 'Restaurant locality/area', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(31, 'RESTAURANT_ADDRESS', 'VARCHAR', 500, NULL, NULL, 'Y', 'Full restaurant address', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(32, 'LATITUDE', 'NUMBER', NULL, 9, 6, 'Y', 'Geographic latitude', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(33, 'LONGITUDE', 'NUMBER', NULL, 9, 6, 'Y', 'Geographic longitude', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(34, 'CUSTOMER_ID', 'INTEGER', NULL, NULL, NULL, 'N', 'Unique customer bronze identifier', 'CUSTOMER', 'N', CURRENT_TIMESTAMP),
(35, 'CUSTOMER_NAME', 'VARCHAR', 100, NULL, NULL, 'Y', 'Customer full name', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(36, 'MOBILE', 'VARCHAR', 20, NULL, NULL, 'Y', 'Customer mobile number', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(37, 'EMAIL', 'VARCHAR', 50, NULL, NULL, 'Y', 'Customer email address', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(38, 'LOGIN_BY_USING', 'VARCHAR', 20, NULL, NULL, 'Y', 'Login method used', 'CUSTOMER', 'N', CURRENT_TIMESTAMP),
(39, 'GENDER', 'VARCHAR', 10, NULL, NULL, 'Y', 'Customer gender', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(40, 'DOB', 'DATE', NULL, NULL, NULL, 'Y', 'Customer date of birth', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(41, 'ANNIVERSARY', 'DATE', NULL, NULL, NULL, 'Y', 'Customer anniversary date', 'CUSTOMER', 'N', CURRENT_TIMESTAMP),
(42, 'PREFERENCES', 'VARIANT', NULL, NULL, NULL, 'Y', 'Customer preferences JSON', 'CUSTOMER', 'N', CURRENT_TIMESTAMP),
(43, 'CUSTOMER_ADDRESS_BRZ_ID', 'INTEGER', NULL, NULL, NULL, 'N', 'Unique customer address bronze identifier', 'CUSTOMER', 'N', CURRENT_TIMESTAMP),
(44, 'FLAT_NO', 'INTEGER', NULL, NULL, NULL, 'Y', 'Flat/Apartment number', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(45, 'HOUSE_NO', 'INTEGER', NULL, NULL, NULL, 'Y', 'House number', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(46, 'FLOOR_NO', 'INTEGER', NULL, NULL, NULL, 'Y', 'Floor number', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(47, 'BUILDING', 'VARCHAR', 100, NULL, NULL, 'Y', 'Building name', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(48, 'LANDMARK', 'VARCHAR', 100, NULL, NULL, 'Y', 'Nearby landmark', 'CUSTOMER', 'Y', CURRENT_TIMESTAMP),
(49, 'LOCALITY', 'VARCHAR', 100, NULL, NULL, 'Y', 'Locality/Area', 'LOCATION', 'Y', CURRENT_TIMESTAMP),
(50, 'CITY', 'VARCHAR', 100, NULL, NULL, 'Y', 'City name', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(51, 'STATE', 'VARCHAR', 100, NULL, NULL, 'Y', 'State name', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(52, 'ZIPCODE', 'INTEGER', NULL, NULL, NULL, 'Y', 'Postal/ZIP code', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(53, 'COORDINATES', 'VARCHAR', 100, NULL, NULL, 'Y', 'Geographic coordinates', 'LOCATION', 'N', CURRENT_TIMESTAMP),
(54, 'PRIMARYFLAG', 'VARCHAR', 10, NULL, NULL, 'Y', 'Primary address flag', 'CUSTOMER', 'N', CURRENT_TIMESTAMP),
(55, 'ADDRESSTYPE', 'VARCHAR', 50, NULL, NULL, 'Y', 'Address type (HOME/WORK/OTHER)', 'CUSTOMER', 'N', CURRENT_TIMESTAMP),
(56, 'ITEM_NAME', 'VARCHAR', 200, NULL, NULL, 'N', 'Menu item name', 'MENU', 'N', CURRENT_TIMESTAMP),
(57, 'DESCRIPTION', 'VARCHAR', 500, NULL, NULL, 'Y', 'Menu item description', 'MENU', 'N', CURRENT_TIMESTAMP),
(58, 'PRICE', 'NUMBER', NULL, 10, 2, 'N', 'Menu item price', 'MENU', 'N', CURRENT_TIMESTAMP),
(59, 'CATEGORY', 'VARCHAR', 100, NULL, NULL, 'Y', 'Menu item category', 'MENU', 'N', CURRENT_TIMESTAMP),
(60, 'AVAILABILITY', 'VARCHAR', 20, NULL, NULL, 'Y', 'Item availability status', 'MENU', 'N', CURRENT_TIMESTAMP),
(61, 'ITEM_TYPE', 'VARCHAR', 50, NULL, NULL, 'Y', 'Item type (VEG/NON-VEG/VEGAN)', 'MENU', 'N', CURRENT_TIMESTAMP),
(62, 'DELIVERY_AGENT_NAME', 'VARCHAR', 100, NULL, NULL, 'N', 'Delivery agent full name', 'DELIVERY', 'Y', CURRENT_TIMESTAMP),
(63, 'PHONE', 'INTEGER', NULL, NULL, NULL, 'Y', 'Delivery agent phone number', 'DELIVERY', 'Y', CURRENT_TIMESTAMP),
(64, 'VEHICLE_TYPE', 'VARCHAR', 50, NULL, NULL, 'Y', 'Type of delivery vehicle', 'DELIVERY', 'N', CURRENT_TIMESTAMP),
(65, 'IS_ACTIVE', 'VARCHAR', 10, NULL, NULL, 'Y', 'Agent active status flag', 'DELIVERY', 'N', CURRENT_TIMESTAMP),
(66, 'GENDER', 'VARCHAR', 10, NULL, NULL, 'Y', 'Delivery agent gender', 'DELIVERY', 'Y', CURRENT_TIMESTAMP),
(67, 'RATING', 'NUMBER', NULL, 2, 1, 'Y', 'Agent performance rating', 'DELIVERY', 'N', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- ORDER PIPELINE
-- =======================================================================================================================================
INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(101, 'ORDER_BATCH', 'SNOWFLAKE', 1, '@DATAVELOCITY.BRONZE.CSV_STG/order/', 'order_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(102, 'ORDER_STREAM', 'KAFKA_STREAM', 1, 'DATAVELOCITY.BRONZE.STREAM_ORDERS_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1001, 101, 'order_id'      , 1, 1, TRUE,  FALSE, NULL                          , 'UUID_STRING()' , NULL, 'Y', CURRENT_TIMESTAMP),
(1002, 101, 'customer_id'   , 2, 2, FALSE, FALSE, 'TRIM(customer_id)'           , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1003, 101, 'restaurant_id' , 3, 3, FALSE, FALSE, 'CAST(restaurant_id AS INT)'  , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1004, 101, 'order_date'    , 4, 4, FALSE, FALSE, 'TO_DATE(order_date)'         , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1005, 101, 'total_amount'  , 5, 5, FALSE, FALSE, 'CAST(total_amount AS NUMBER)', NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1006, 101, 'order_status'  , 6, 6, FALSE, FALSE, 'UPPER(TRIM(order_status))'   , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1007, 101, 'payment_method', 7, 7, FALSE, FALSE, 'TRIM(payment_method)'        , NULL            , NULL, 'Y', CURRENT_TIMESTAMP);

INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1008, 102, 'order_id'      , 1, 1, TRUE,  FALSE , NULL                         , 'UUID_STRING()', NULL, 'Y', CURRENT_TIMESTAMP),
(1009, 102, 'customer_id'   , 2, 2, FALSE, FALSE, 'TRIM(customer_id)'           , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1010, 102, 'restaurant_id' , 3, 3, FALSE, FALSE, 'CAST(restaurant_id AS INT)'  , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1011, 102, 'order_date'    , 4, 4, FALSE, FALSE, 'TO_DATE(order_date)'         , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1012, 102, 'total_amount'  , 5, 5, FALSE, FALSE, 'CAST(total_amount AS NUMBER)', NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1013, 102, 'order_status', 6, 6, FALSE, FALSE, 'UPPER(TRIM(order_status))'     , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1014, 102, 'payment_method', 7, 7, FALSE, FALSE, 'TRIM(payment_method)'        , NULL           , NULL, 'Y', CURRENT_TIMESTAMP);

INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, BRONZE_TABLE, LOAD_ERROR_TABLE, SILVER_TABLE, GOLD_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(501, 101, 'BRONZE.ORDER_BRZ', 'BRONZE.ORDER_LOAD_ERROR', 'SILVER.ORDER_SLV', 'GOLD.FACT_ORDER', 'MERGE' , 'ORDER_ID',NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP),
(502, 102, 'BRONZE.ORDER_BRZ', 'BRONZE.ORDER_LOAD_ERROR', 'SILVER.ORDER_SLV', 'GOLD.FACT_ORDER', 'MERGE' , 'ORDER_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- DELIVERY PIPELINE
-- =======================================================================================================================================
INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(103, 'DELIVERY_BATCH', 'SNOWFLAKE', 1, '@DATAVELOCITY.BRONZE.CSV_STG/delivery/', 'delivery_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(104, 'DELIVERY_STREAM', 'KAFKA_STREAM', 1, 'DATAVELOCITY.BRONZE.STREAM_DELIVERY_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1015, 103, 'delivery_id'        ,  9, 1, TRUE,  FALSE, NULL                              , 'UUID_STRING()' , NULL, 'Y', CURRENT_TIMESTAMP),
(1016, 103, 'order_id'           ,  1, 2, FALSE, FALSE, 'TRIM(order_id)'                  , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1017, 103, 'delivery_agent_id'  , 10, 3, FALSE, FALSE, 'CAST(delivery_agent_id AS INT)'  , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1018, 103, 'delivery_status'    , 11, 4, FALSE, FALSE, 'TRIM(delivery_status)'           , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1019, 103, 'ESTIMATED_TIME'     , 12, 5, FALSE, FALSE, 'TRIM(ESTIMATED_TIME)'            , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1020, 103, 'CUSTOMER_ADDRESS_ID', 13, 6, FALSE, FALSE, 'CAST(CUSTOMER_ADDRESS_ID AS INT)', NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1021, 103, 'DELIVERY_DATE'      , 14, 7, FALSE, FALSE, 'TO_DATE(DELIVERY_DATE)'          , NULL            , NULL, 'Y', CURRENT_TIMESTAMP);

INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1022, 104, 'delivery_id'        ,  9, 1, TRUE,  FALSE, NULL                              , 'UUID_STRING()' , NULL, 'Y', CURRENT_TIMESTAMP),
(1023, 104, 'order_id'           ,  1, 2, FALSE, FALSE, 'TRIM(order_id)'                  , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1024, 104, 'delivery_agent_id'  , 10, 3, FALSE, FALSE, 'CAST(delivery_agent_id AS INT)'  , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1025, 104, 'delivery_status'    , 11, 4, FALSE, FALSE, 'TRIM(delivery_status)'           , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1026, 104, 'ESTIMATED_TIME'     , 12, 5, FALSE, FALSE, 'TRIM(ESTIMATED_TIME)'            , NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1027, 104, 'CUSTOMER_ADDRESS_ID', 13, 6, FALSE, FALSE, 'CAST(CUSTOMER_ADDRESS_ID AS INT)', NULL            , NULL, 'Y', CURRENT_TIMESTAMP),
(1028, 104, 'DELIVERY_DATE'      , 14, 7, FALSE, FALSE, 'TO_DATE(DELIVERY_DATE)'          , NULL            , NULL, 'Y', CURRENT_TIMESTAMP);

INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, BRONZE_TABLE, LOAD_ERROR_TABLE, SILVER_TABLE, GOLD_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(503, 103, 'BRONZE.DELIVERY_BRZ', 'BRONZE.DELIVERY_LOAD_ERROR', 'SILVER.DELIVERY_SLV', 'GOLD.FACT_DELIVERY', 'MERGE' , 'DELIVERY_ID',NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP),
(504, 104, 'BRONZE.DELIVERY_BRZ', 'BRONZE.DELIVERY_LOAD_ERROR', 'SILVER.DELIVERY_SLV', 'GOLD.FACT_DELIVERY', 'MERGE' , 'DELIVERY_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- ORDER_ITEM PIPELINE
-- =======================================================================================================================================
INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(105, 'ORDER_ITEM_BATCH', 'SNOWFLAKE', 1, '@DATAVELOCITY.BRONZE.CSV_STG/order_item/', 'order_item_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(106, 'ORDER_ITEM_STREAM', 'KAFKA_STREAM', 1, 'DATAVELOCITY.BRONZE.STREAM_ORDER_ITEM_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for ORDER_ITEM_BATCH (SOURCE_ID: 105)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1029, 105, 'order_item_id'   , 15, 1, TRUE,  FALSE, NULL                              , 'UUID_STRING()', NULL, 'Y', CURRENT_TIMESTAMP),
(1030, 105, 'order_id'        ,  1, 2, FALSE, FALSE, 'TRIM(order_id)'                  , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1031, 105, 'menu_id'         ,  8, 3, FALSE, FALSE, 'CAST(menu_id AS INT)'            , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1032, 105, 'quantity'        , 16, 4, FALSE, FALSE, 'CAST(quantity AS NUMBER(10,2))'  , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1033, 105, 'price'           , 17, 5, FALSE, FALSE, 'CAST(price AS NUMBER(10,2))'     , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1034, 105, 'subtotal'        , 18, 6, FALSE, FALSE, 'CAST(subtotal AS NUMBER(10,2))'  , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1035, 105, 'order_timestamp' , 19, 7, FALSE, FALSE, 'TO_DATE(order_timestamp)'        , NULL           , NULL, 'Y', CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for ORDER_ITEM_STREAM (SOURCE_ID: 106)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1036, 106, 'order_item_id'   , 15, 1, TRUE,  FALSE, NULL                              , 'UUID_STRING()', NULL, 'Y', CURRENT_TIMESTAMP),
(1037, 106, 'order_id'        ,  1, 2, FALSE, FALSE, 'TRIM(order_id)'                  , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1038, 106, 'menu_id'         ,  8, 3, FALSE, FALSE, 'CAST(menu_id AS INT)'            , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1039, 106, 'quantity'        , 16, 4, FALSE, FALSE, 'CAST(quantity AS NUMBER(10,2))'  , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1040, 106, 'price'           , 17, 5, FALSE, FALSE, 'CAST(price AS NUMBER(10,2))'     , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1041, 106, 'subtotal'        , 18, 6, FALSE, FALSE, 'CAST(subtotal AS NUMBER(10,2))'  , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1042, 106, 'order_timestamp' , 19, 7, FALSE, FALSE, 'TO_DATE(order_timestamp)'        , NULL           , NULL, 'Y', CURRENT_TIMESTAMP);

-- TARGET_TABLE_MAPPING for ORDER_ITEM
INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, BRONZE_TABLE, LOAD_ERROR_TABLE, SILVER_TABLE, GOLD_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(505, 105, 'BRONZE.ORDER_ITEM_BRZ', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'SILVER.ORDER_ITEM_SLV', 'GOLD.FACT_ORDER_ITEM', 'MERGE', 'ORDER_ITEM_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP),
(506, 106, 'BRONZE.ORDER_ITEM_BRZ', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'SILVER.ORDER_ITEM_SLV', 'GOLD.FACT_ORDER_ITEM', 'MERGE', 'ORDER_ITEM_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- RESTAURANT PIPELINE
-- =======================================================================================================================================
INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(107, 'RESTAURANT_BATCH', 'SNOWFLAKE', 1, '@DATAVELOCITY.BRONZE.CSV_STG/restaurant/', 'restaurant_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(108, 'RESTAURANT_STREAM', 'KAFKA_STREAM', 1, 'DATAVELOCITY.BRONZE.STREAM_RESTAURANT_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for RESTAURANT_BATCH (SOURCE_ID: 107)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1043, 107, 'restaurant_id'          , 3,  1, TRUE,  TRUE , NULL                                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1044, 107, 'fssai_registration_no'  , 21,  2, FALSE, FALSE, 'CAST(fssai_registration_no AS INT)'     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1045, 107, 'restaurant_name'        , 22,  3, FALSE, FALSE, 'TRIM(restaurant_name)'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1046, 107, 'cuisine_type'           , 23,  4, FALSE, FALSE, 'TRIM(cuisine_type)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1047, 107, 'pricing_for_two'        , 24,  5, FALSE, FALSE, 'CAST(pricing_for_two AS INT)'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1048, 107, 'restaurant_phone'       , 25,  6, FALSE, FALSE, 'TRIM(restaurant_phone)'                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1049, 107, 'operating_hours'        , 26,  7, FALSE, FALSE, 'TRIM(operating_hours)'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1050, 107, 'location_id'            , 27,  8, FALSE, FALSE, 'CAST(location_id AS INT)'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1051, 107, 'active_flag'            , 28,  9, FALSE, FALSE, 'UPPER(TRIM(active_flag))'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1052, 107, 'open_status'            , 29, 10, FALSE, FALSE, 'UPPER(TRIM(open_status))'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1053, 107, 'locality'               , 30, 11, FALSE, FALSE, 'TRIM(locality)'                         , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1054, 107, 'restaurant_address'     , 31, 12, FALSE, FALSE, 'TRIM(restaurant_address)'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1055, 107, 'latitude'               , 32, 13, FALSE, FALSE, 'CAST(latitude AS NUMBER(9,6))'          , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1056, 107, 'longitude'              , 33, 14, FALSE, FALSE, 'CAST(longitude AS NUMBER(9,6))'         , NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for RESTAURANT_STREAM (SOURCE_ID: 108)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1057, 108, 'restaurant_id'          , 3,  1, TRUE,  TRUE , NULL                                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1058, 108, 'fssai_registration_no'  , 21,  2, FALSE, FALSE, 'CAST(fssai_registration_no AS INT)'     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1059, 108, 'restaurant_name'        , 22,  3, FALSE, FALSE, 'TRIM(restaurant_name)'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1060, 108, 'cuisine_type'           , 23,  4, FALSE, FALSE, 'TRIM(cuisine_type)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1061, 108, 'pricing_for_two'        , 24,  5, FALSE, FALSE, 'CAST(pricing_for_two AS INT)'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1062, 108, 'restaurant_phone'       , 25,  6, FALSE, FALSE, 'TRIM(restaurant_phone)'                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1063, 108, 'operating_hours'        , 26,  7, FALSE, FALSE, 'TRIM(operating_hours)'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1064, 108, 'location_id'            , 27,  8, FALSE, FALSE, 'CAST(location_id AS INT)'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1065, 108, 'active_flag'            , 28,  9, FALSE, FALSE, 'UPPER(TRIM(active_flag))'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1066, 108, 'open_status'            , 29, 10, FALSE, FALSE, 'UPPER(TRIM(open_status))'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1067, 108, 'locality'               , 30, 11, FALSE, FALSE, 'TRIM(locality)'                         , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1068, 108, 'restaurant_address'     , 31, 12, FALSE, FALSE, 'TRIM(restaurant_address)'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1069, 108, 'latitude'               , 32, 13, FALSE, FALSE, 'CAST(latitude AS NUMBER(9,6))'          , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1070, 108, 'longitude'              , 33, 14, FALSE, FALSE, 'CAST(longitude AS NUMBER(9,6))'         , NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- TARGET_TABLE_MAPPING for RESTAURANT
INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, BRONZE_TABLE, LOAD_ERROR_TABLE, SILVER_TABLE, GOLD_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(507, 107, 'BRONZE.RESTAURANT_BRZ', 'BRONZE.RESTAURANT_LOAD_ERROR', 'SILVER.RESTAURANT_SLV', 'GOLD.DIM_RESTAURANT', 'MERGE', 'RESTAURANT_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP),
(508, 108, 'BRONZE.RESTAURANT_BRZ', 'BRONZE.RESTAURANT_LOAD_ERROR', 'SILVER.RESTAURANT_SLV', 'GOLD.DIM_RESTAURANT', 'MERGE', 'RESTAURANT_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- CUSTOMER PIPELINE
-- =======================================================================================================================================
INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(109, 'CUSTOMER_BATCH', 'SNOWFLAKE', 1, '@DATAVELOCITY.BRONZE.CSV_STG/customer/', 'customer_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(110, 'CUSTOMER_STREAM', 'KAFKA_STREAM', 1, 'DATAVELOCITY.BRONZE.STREAM_CUSTOMER_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for CUSTOMER_BATCH (SOURCE_ID: 109)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1071, 109, 'customer_id'  , 2, 1, TRUE,  TRUE ,  NULL                                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1072, 109, 'customer_name'    , 35, 2, FALSE, FALSE, 'TRIM(customer_name)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1073, 109, 'mobile'           , 36, 3, FALSE, FALSE, 'TRIM(mobile)'                            , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1074, 109, 'email'            , 37, 4, FALSE, FALSE, 'LOWER(TRIM(email))'                      , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1075, 109, 'login_by_using'   , 38, 5, FALSE, FALSE, 'TRIM(login_by_using)'                    , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1076, 109, 'gender'           , 39, 6, FALSE, FALSE, 'UPPER(TRIM(gender))'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1077, 109, 'dob'              , 40, 7, FALSE, FALSE, 'TO_DATE(dob)'                            , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1078, 109, 'anniversary'      , 41, 8, FALSE, FALSE, 'TO_DATE(anniversary)'                    , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1079, 109, 'preferences'      , 42, 9, FALSE, FALSE, 'PARSE_JSON(preferences)'                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for CUSTOMER_STREAM (SOURCE_ID: 110)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1080, 110, 'customer_id'  , 2, 1, TRUE,  TRUE , NULL                                      , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1081, 110, 'customer_name'    , 35, 2, FALSE, FALSE, 'TRIM(customer_name)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1082, 110, 'mobile'           , 36, 3, FALSE, FALSE, 'TRIM(mobile)'                            , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1083, 110, 'email'            , 37, 4, FALSE, FALSE, 'LOWER(TRIM(email))'                      , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1084, 110, 'login_by_using'   , 38, 5, FALSE, FALSE, 'TRIM(login_by_using)'                    , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1085, 110, 'gender'           , 39, 6, FALSE, FALSE, 'UPPER(TRIM(gender))'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1086, 110, 'dob'              , 40, 7, FALSE, FALSE, 'TO_DATE(dob)'                            , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1087, 110, 'anniversary'      , 41, 8, FALSE, FALSE, 'TO_DATE(anniversary)'                    , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1088, 110, 'preferences'      , 42, 9, FALSE, FALSE, 'PARSE_JSON(preferences)'                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- TARGET_TABLE_MAPPING for CUSTOMER
INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, BRONZE_TABLE, LOAD_ERROR_TABLE, SILVER_TABLE, GOLD_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(509, 109, 'BRONZE.CUSTOMER_BRZ', 'BRONZE.CUSTOMER_LOAD_ERROR', 'SILVER.CUSTOMER_SLV', 'GOLD.DIM_CUSTOMER', 'MERGE', 'CUSTOMER_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP),
(510, 110, 'BRONZE.CUSTOMER_BRZ', 'BRONZE.CUSTOMER_LOAD_ERROR', 'SILVER.CUSTOMER_SLV', 'GOLD.DIM_CUSTOMER', 'MERGE', 'CUSTOMER_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- CUSTOMER_ADDRESS PIPELINE
-- =======================================================================================================================================
INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(111, 'CUSTOMER_ADDRESS_BATCH', 'SNOWFLAKE', 1, '@DATAVELOCITY.BRONZE.CSV_STG/customer_address/', 'customer_address_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(112, 'CUSTOMER_ADDRESS_STREAM', 'KAFKA_STREAM', 1, 'DATAVELOCITY.BRONZE.STREAM_CUSTOMER_ADDRESS_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for CUSTOMER_ADDRESS_BATCH (SOURCE_ID: 111)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1089, 111, 'customer_address_id', 13,  1, TRUE,  TRUE , NULL                                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1090, 111, 'customer_id'            , 34,  2, FALSE, FALSE, 'CAST(customer_id AS INT)'           , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1091, 111, 'flat_no'                , 44,  3, FALSE, FALSE, 'CAST(flat_no AS INT)'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1092, 111, 'house_no'               , 45,  4, FALSE, FALSE, 'CAST(house_no AS INT)'              , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1093, 111, 'floor_no'               , 46,  5, FALSE, FALSE, 'CAST(floor_no AS INT)'              , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1094, 111, 'building'               , 47,  6, FALSE, FALSE, 'TRIM(building)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1095, 111, 'landmark'               , 48,  7, FALSE, FALSE, 'TRIM(landmark)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1096, 111, 'locality'               , 49,  8, FALSE, FALSE, 'TRIM(locality)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1097, 111, 'city'                   , 50,  9, FALSE, FALSE, 'UPPER(TRIM(city))'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1098, 111, 'state'                  , 51, 10, FALSE, FALSE, 'UPPER(TRIM(state))'                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1099, 111, 'zipcode'                , 52, 11, FALSE, FALSE, 'CAST(zipcode AS INT)'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1100, 111, 'coordinates'            , 53, 12, FALSE, FALSE, 'TRIM(coordinates)'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1101, 111, 'primaryflag'            , 54, 13, FALSE, FALSE, 'UPPER(TRIM(primaryflag))'           , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1102, 111, 'addresstype'            , 55, 14, FALSE, FALSE, 'UPPER(TRIM(addresstype))'           , NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for CUSTOMER_ADDRESS_STREAM (SOURCE_ID: 112)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1103, 112, 'customer_address_id', 13,  1, TRUE,  TRUE , NULL                                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1104, 112, 'customer_id'            , 34,  2, FALSE, FALSE, 'CAST(customer_id AS INT)'           , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1105, 112, 'flat_no'                , 44,  3, FALSE, FALSE, 'CAST(flat_no AS INT)'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1106, 112, 'house_no'               , 45,  4, FALSE, FALSE, 'CAST(house_no AS INT)'              , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1107, 112, 'floor_no'               , 46,  5, FALSE, FALSE, 'CAST(floor_no AS INT)'              , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1108, 112, 'building'               , 47,  6, FALSE, FALSE, 'TRIM(building)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1109, 112, 'landmark'               , 48,  7, FALSE, FALSE, 'TRIM(landmark)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1110, 112, 'locality'               , 49,  8, FALSE, FALSE, 'TRIM(locality)'                     , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1111, 112, 'city'                   , 50,  9, FALSE, FALSE, 'UPPER(TRIM(city))'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1112, 112, 'state'                  , 51, 10, FALSE, FALSE, 'UPPER(TRIM(state))'                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1113, 112, 'zipcode'                , 52, 11, FALSE, FALSE, 'CAST(zipcode AS INT)'               , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1114, 112, 'coordinates'            , 53, 12, FALSE, FALSE, 'TRIM(coordinates)'                  , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1115, 112, 'primaryflag'            , 54, 13, FALSE, FALSE, 'UPPER(TRIM(primaryflag))'           , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1116, 112, 'addresstype'            , 55, 14, FALSE, FALSE, 'UPPER(TRIM(addresstype))'           , NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- TARGET_TABLE_MAPPING for CUSTOMER_ADDRESS
INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, BRONZE_TABLE, LOAD_ERROR_TABLE, SILVER_TABLE, GOLD_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(511, 111, 'BRONZE.CUSTOMER_ADDRESS_BRZ', 'BRONZE.CUSTOMER_ADDRESS_LOAD_ERROR', 'SILVER.CUSTOMER_ADDRESS_SLV', 'GOLD.DIM_CUSTOMER_ADDRESS', 'MERGE', 'CUSTOMER_ADDRESS_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP),
(512, 112, 'BRONZE.CUSTOMER_ADDRESS_BRZ', 'BRONZE.CUSTOMER_ADDRESS_LOAD_ERROR', 'SILVER.CUSTOMER_ADDRESS_SLV', 'GOLD.DIM_CUSTOMER_ADDRESS', 'MERGE', 'CUSTOMER_ADDRESS_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- MENU PIPELINE
-- =======================================================================================================================================
INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(113, 'MENU_BATCH', 'SNOWFLAKE', 1, '@DATAVELOCITY.BRONZE.CSV_STG/menu/', 'menu_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(114, 'MENU_STREAM', 'KAFKA_STREAM', 1, 'DATAVELOCITY.BRONZE.STREAM_MENU_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for MENU_BATCH (SOURCE_ID: 113)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1117, 113, 'menu_id'       ,  8, 1, TRUE,  TRUE , NULL                                , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1118, 113, 'restaurant_id' ,  3, 2, FALSE, FALSE, 'CAST(restaurant_id AS INT)'        , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1119, 113, 'item_name'     , 56, 3, FALSE, FALSE, 'TRIM(item_name)'                   , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1120, 113, 'description'   , 57, 4, FALSE, FALSE, 'TRIM(description)'                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1121, 113, 'price'         , 58, 5, FALSE, FALSE, 'CAST(price AS NUMBER(10,2))'       , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1122, 113, 'category'      , 59, 6, FALSE, FALSE, 'UPPER(TRIM(category))'             , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1123, 113, 'availability'  , 60, 7, FALSE, FALSE, 'UPPER(TRIM(availability))'         , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1124, 113, 'item_type'     , 61, 8, FALSE, FALSE, 'UPPER(TRIM(item_type))'            , NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for MENU_STREAM (SOURCE_ID: 114)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1125, 114, 'menu_id'       ,  8, 1, TRUE,  TRUE , NULL                                , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1126, 114, 'restaurant_id' ,  3, 2, FALSE, FALSE, 'CAST(restaurant_id AS INT)'        , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1127, 114, 'item_name'     , 56, 3, FALSE, FALSE, 'TRIM(item_name)'                   , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1128, 114, 'description'   , 57, 4, FALSE, FALSE, 'TRIM(description)'                 , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1129, 114, 'price'         , 58, 5, FALSE, FALSE, 'CAST(price AS NUMBER(10,2))'       , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1130, 114, 'category'      , 59, 6, FALSE, FALSE, 'UPPER(TRIM(category))'             , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1131, 114, 'availability'  , 60, 7, FALSE, FALSE, 'UPPER(TRIM(availability))'         , NULL, NULL, 'Y', CURRENT_TIMESTAMP),
(1132, 114, 'item_type'     , 61, 8, FALSE, FALSE, 'UPPER(TRIM(item_type))'            , NULL, NULL, 'Y', CURRENT_TIMESTAMP);

-- TARGET_TABLE_MAPPING for MENU
INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, BRONZE_TABLE, LOAD_ERROR_TABLE, SILVER_TABLE, GOLD_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(513, 113, 'BRONZE.MENU_BRZ', 'BRONZE.MENU_LOAD_ERROR', 'SILVER.MENU_SLV', 'GOLD.DIM_MENU', 'MERGE', 'MENU_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP),
(514, 114, 'BRONZE.MENU_BRZ', 'BRONZE.MENU_LOAD_ERROR', 'SILVER.MENU_SLV', 'GOLD.DIM_MENU', 'MERGE', 'MENU_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- DELIVERY_AGENT PIPELINE
-- =======================================================================================================================================
INSERT INTO COMMON.SOURCE_FILE_CONFIG (SOURCE_ID, SOURCE_NAME, SOURCE_SYSTEM, FILE_FORMAT_ID, LANDING_PATH, FILE_PATTERN, INGESTION_TYPE, SCHEDULE_CRON, ACTIVE_FLAG, CREATED_AT, UPDATED_AT) VALUES
(115, 'DELIVERY_AGENT_BATCH', 'SNOWFLAKE', 1, '@DATAVELOCITY.BRONZE.CSV_STG/delivery_agent/', 'delivery_agent_*.csv', 'BATCH', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
(116, 'DELIVERY_AGENT_STREAM', 'KAFKA_STREAM', 1, 'DATAVELOCITY.BRONZE.STREAM_DELIVERY_AGENT_CHANGES', NULL, 'STREAM', '0 2 * * *', 'Y', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- =======================================================================================================================================
-- FILE_COLUMN_MAPPING for DELIVERY_AGENT_BATCH (SOURCE_ID: 115)
-- =======================================================================================================================================
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1133, 115, 'delivery_agent_id'  , 10, 1, TRUE,  FALSE, NULL                                , 'UUID_STRING()', NULL, 'Y', CURRENT_TIMESTAMP),
(1134, 115, 'delivery_agent_name', 62, 2, FALSE, FALSE, 'TRIM(delivery_agent_name)'         , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1135, 115, 'phone'              , 63, 3, FALSE, FALSE, 'CAST(phone AS INT)'                , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1136, 115, 'vehicle_type'       , 64, 4, FALSE, FALSE, 'UPPER(TRIM(vehicle_type))'         , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1137, 115, 'location_id'        , 27, 5, FALSE, FALSE, 'CAST(location_id AS INT)'          , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1138, 115, 'is_active'          , 65, 6, FALSE, FALSE, 'UPPER(TRIM(is_active))'            , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1139, 115, 'gender'             , 66, 7, FALSE, FALSE, 'UPPER(TRIM(gender))'               , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1140, 115, 'rating'             , 67, 8, FALSE, FALSE, 'CAST(rating AS NUMBER(2,1))'       , NULL           , NULL, 'Y', CURRENT_TIMESTAMP);

-- FILE_COLUMN_MAPPING for DELIVERY_AGENT_STREAM (SOURCE_ID: 116)
INSERT INTO COMMON.FILE_COLUMN_MAPPING(MAPPING_ID, SOURCE_ID, FILE_COLUMN_NAME, FIELD_ID, COLUMN_POSITION, IS_PRIMARY_KEY, AUTO_INCREMENT_FLAG, TRANSFORMATION_RULE, DEFAULT_VALUE, VALIDATION_RULE, ACTIVE_FLAG, CREATED_AT) VALUES
(1141, 116, 'delivery_agent_id'  , 10, 1, TRUE,  FALSE, NULL                                , 'UUID_STRING()', NULL, 'Y', CURRENT_TIMESTAMP),
(1142, 116, 'delivery_agent_name', 62, 2, FALSE, FALSE, 'TRIM(delivery_agent_name)'         , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1143, 116, 'phone'              , 63, 3, FALSE, FALSE, 'CAST(phone AS INT)'                , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1144, 116, 'vehicle_type'       , 64, 4, FALSE, FALSE, 'UPPER(TRIM(vehicle_type))'         , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1145, 116, 'location_id'        , 27, 5, FALSE, FALSE, 'CAST(location_id AS INT)'          , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1146, 116, 'is_active'          , 65, 6, FALSE, FALSE, 'UPPER(TRIM(is_active))'            , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1147, 116, 'gender'             , 66, 7, FALSE, FALSE, 'UPPER(TRIM(gender))'               , NULL           , NULL, 'Y', CURRENT_TIMESTAMP),
(1148, 116, 'rating'             , 67, 8, FALSE, FALSE, 'CAST(rating AS NUMBER(2,1))'       , NULL           , NULL, 'Y', CURRENT_TIMESTAMP);

-- TARGET_TABLE_MAPPING for DELIVERY_AGENT
INSERT INTO COMMON.TARGET_TABLE_MAPPING
(TARGET_ID, SOURCE_ID, BRONZE_TABLE, LOAD_ERROR_TABLE, SILVER_TABLE, GOLD_TABLE, LOAD_TYPE, PRIMARY_KEY_FIELDS, PARTITION_COLUMNS, CLUSTERING_COLUMNS, MERGE_KEY_FIELDS, ENABLE_RAW_COLUMNS, ENABLE_AUDIT_COLUMNS, ACTIVE_FLAG, CREATED_AT) VALUES
(515, 115, 'BRONZE.DELIVERY_AGENT_BRZ', 'BRONZE.DELIVERY_AGENT_LOAD_ERROR', 'SILVER.DELIVERY_AGENT_SLV', 'GOLD.DIM_DELIVERY_AGENT', 'MERGE', 'DELIVERY_AGENT_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP),
(516, 116, 'BRONZE.DELIVERY_AGENT_BRZ', 'BRONZE.DELIVERY_AGENT_LOAD_ERROR', 'SILVER.DELIVERY_AGENT_SLV', 'GOLD.DIM_DELIVERY_AGENT', 'MERGE', 'DELIVERY_AGENT_ID', NULL, NULL, NULL, 'Y', 'Y', 'Y', CURRENT_TIMESTAMP);

SELECT * 
FROM SOURCE_FILE_CONFIG SFC
JOIN FILE_FORMAT_MASTER FFM ON SFC.FILE_FORMAT_ID = FFM.FILE_FORMAT_ID
JOIN FILE_COLUMN_MAPPING FCM ON SFC.SOURCE_ID = FCM.SOURCE_ID
JOIN TARGET_TABLE_MAPPING TTM ON SFC.SOURCE_ID = TTM.SOURCE_ID;

