-- =====================================================
-- CONFIGURATION & METADATA TABLES
-- =====================================================

-- Configuration table for pipeline metadata
CREATE OR REPLACE TABLE COMMON.IMPORT_CONFIGURATION (
    CONFIGURATION_IMPORT_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_NAME VARCHAR(100) NOT NULL UNIQUE,
    SOURCE_TYPE VARCHAR(50) NOT NULL,  -- FILE, DATABASE, API, STREAM
    SOURCE_LOCATION VARCHAR(500),      -- S3 path, table name, API endpoint
    TARGET_SCHEMA VARCHAR(100),        -- BRONZE, SILVER, GOLD
    TARGET_TABLE VARCHAR(100),
    FILE_FORMAT VARCHAR(50),           -- CSV, JSON, PARQUET, etc.
    ENABLED BOOLEAN DEFAULT TRUE,
    LOAD_TYPE VARCHAR(20) DEFAULT 'FULL',  -- FULL, INCREMENTAL
    SCHEDULE_CRON VARCHAR(100),        -- Cron expression for scheduling
    STAGE_TO_BRONZE_PROC VARCHAR(200),
    BRONZE_TO_SILVER_PROC VARCHAR(200),
    SILVER_TO_GOLD_PROC VARCHAR(200),
    PRIORITY INTEGER DEFAULT 5,        -- 1=Highest, 10=Lowest
    DEPENDENCY_PIPELINE VARCHAR(100),  -- Pipeline that must complete first
    RETRY_COUNT INTEGER DEFAULT 3,
    TIMEOUT_MINUTES INTEGER DEFAULT 60,
    DESCRIPTION VARCHAR(1000),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100),
    UPDATED_AT TIMESTAMP_NTZ
)
COMMENT = 'Metadata configuration for all ETL pipelines';

-- Insert configuration for Location pipeline
INSERT INTO COMMON.IMPORT_CONFIGURATION ( PIPELINE_NAME, SOURCE_TYPE, SOURCE_LOCATION, TARGET_SCHEMA, TARGET_TABLE, FILE_FORMAT, STAGE_TO_BRONZE_PROC, BRONZE_TO_SILVER_PROC, SILVER_TO_GOLD_PROC, DESCRIPTION)
VALUES
('LOCATION_PIPELINE',
    'FILE', '@CSV_STAGE/location/',
    'BRONZE', 'LOCATION_BRZ', 'CSV',
    'BRONZE.SP_LOCATION_STAGE_TO_BRONZE',
    'SILVER.SP_LOCATION_BRONZE_TO_SILVER',
    'GOLD.SP_LOCATION_SILVER_TO_GOLD',
    'Location master data pipeline with SCD Type 2'),

('CUSTOMER_PIPELINE',
    'FILE',
    '@CSV_STAGE/customer/',
    'BRONZE', 'CUSTOMER_BRZ',
    'CSV',
    'BRONZE.SP_CUSTOMER_STAGE_TO_BRONZE',
    'SILVER.SP_CUSTOMER_BRONZE_TO_SILVER',
    'GOLD.SP_CUSTOMER_SILVER_TO_GOLD',
    'Customer master data pipeline with SCD Type 2'),

('CUSTOMER_ADDRESS_PIPELINE',
    'FILE', '@CSV_STAGE/customer_address/',
    'BRONZE', 'CUSTOMER_ADDRESS_BRZ', 'CSV',
    'BRONZE.SP_CUSTOMER_ADDRESS_STAGE_TO_BRONZE',
    'SILVER.SP_CUSTOMER_ADDRESS_BRONZE_TO_SILVER',
    'GOLD.SP_CUSTOMER_ADDRESS_SILVER_TO_GOLD',
    'Customer Address master data pipeline with SCD Type 2'),

('DELIVERY_AGENT_PIPELINE',
    'FILE', '@CSV_STAGE/delivery_agent/',
    'BRONZE', 'DELIVERY_AGENT_BRZ', 'CSV',
    'BRONZE.SP_DELIVERY_AGENT_STAGE_TO_BRONZE',
    'SILVER.SP_DELIVERY_AGENT_BRONZE_TO_SILVER',
    'GOLD.SP_DELIVERY_AGENT_SILVER_TO_GOLD',
    'Delivery Agent master data pipeline with SCD Type 2'),

('MENU_PIPELINE',
    'FILE', '@CSV_STAGE/menu/',
    'BRONZE', 'MENU_BRZ', 'CSV',
    'BRONZE.SP_MENU_STAGE_TO_BRONZE',
    'SILVER.SP_MENU_BRONZE_TO_SILVER',
    'GOLD.SP_MENU_SILVER_TO_GOLD',
    'Menu master data pipeline with SCD Type 2'),

('RESTAURANT_PIPELINE',
    'FILE', '@CSV_STAGE/restaurant/',
    'BRONZE', 'RESTAURANT_BRZ', 'CSV',
    'BRONZE.SP_RESTAURANT_STAGE_TO_BRONZE',
    'SILVER.SP_RESTAURANT_BRONZE_TO_SILVER',
    'GOLD.SP_RESTAURANT_SILVER_TO_GOLD',
    'Menu master data pipeline with SCD Type 2'),

('DELIVERY_PIPELINE',
    'FILE', '@CSV_STAGE/delivery/',
    'BRONZE', 'DELIVERY_BRZ', 'CSV',
    'BRONZE.SP_DELIVERY_STAGE_TO_BRONZE',
    'SILVER.SP_DELIVERY_BRONZE_TO_SILVER',
    'GOLD.SP_DELIVERY_SILVER_TO_GOLD',
    'Delivery master data pipeline with SCD Type 2'),

('ORDER_PIPELINE',
    'FILE', '@CSV_STAGE/order/',
    'BRONZE', 'ORDER_BRZ', 'CSV',
    'BRONZE.SP_ORDER_STAGE_TO_BRONZE',
    'SILVER.SP_ORDER_BRONZE_TO_SILVER',
    'GOLD.SP_ORDER_SILVER_TO_GOLD',
    'Order master data pipeline with SCD Type 2'),

('ORDER_ITEM_PIPELINE',
    'FILE', '@CSV_STAGE/order_item/',
    'BRONZE', 'ORDER_ITEM_BRZ', 'CSV',
    'BRONZE.SP_ORDER_ITEM_STAGE_TO_BRONZE',
    'SILVER.SP_ORDER_ITEM_BRONZE_TO_SILVER',
    'GOLD.SP_ORDER_ITEM_SILVER_TO_GOLD',
    'Order Item master data pipeline with SCD Type 2');
-- =====================================================
-- BATCH TABLE - STORES BATCH EXECUTION DATA
-- =====================================================

CREATE OR REPLACE TABLE COMMON.BATCH (
    BATCH_ID VARCHAR(36) PRIMARY KEY,              -- Generated UUID for each run
    PIPELINE_NAME VARCHAR(100) NOT NULL,
    LAST_BATCH_EXECUTED_AT TIMESTAMP_NTZ,

    -- Stage Execution Tracking
    STAGE_TO_BRONZE_STATUS VARCHAR(20),            -- SUCCESS, FAILED, SKIPPED
    BRONZE_INSERT_RECORDS INTEGER DEFAULT 0,

    -- Bronze to Silver Tracking
    BRONZE_TO_SILVER_STATUS VARCHAR(20),
    SILVER_INSERTED INTEGER DEFAULT 0,
    SILVER_UPDATED INTEGER DEFAULT 0,

    -- Silver to Gold Tracking
    SILVER_TO_GOLD_STATUS VARCHAR(20),
    GOLD_INSERTED INTEGER DEFAULT 0,
    GOLD_UPDATED INTEGER DEFAULT 0,
    GOLD_DELETED INTEGER DEFAULT 0,

    BATCH_LOG VARIANT
)
COMMENT = 'Master tracking table storing all execution details for each batch run';

-- =====================================================
-- LOAD_ERROR TABLE - STORES BATCH EXECUTION DATA
-- =====================================================
CREATE TABLE IF NOT EXISTS COMMON.LOAD_ERROR (
    ERROR_ID        STRING DEFAULT UUID_STRING(),
    SOURCE_TABLE    STRING,
    RECORD_ID       INT,
    BATCH_ID        STRING,
    ERROR_TYPE      STRING,
    ERROR_MESSAGE   STRING,
    ERROR_TS        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
-- =====================================================
-- IMPORT MASTER PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE COMMON.SP_IMPORT_MASTER(
    PIPELINE_NAME_PARAM VARCHAR,
    STAGE_PATH_PARAM VARCHAR
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_batch_id STRING;
    v_stage_to_bronze_proc STRING;
    v_bronze_to_silver_proc STRING;
    v_silver_to_gold_proc STRING;
    v_pipeline_enabled BOOLEAN;

    -- Stage Execution Tracking
    v_stb_start_time TIMESTAMP_NTZ;
    v_stb_end_time TIMESTAMP_NTZ;
    v_stb_status VARCHAR(20);
    v_b_inserted_records INTEGER DEFAULT 0;
    v_stb_duration_sec NUMBER(10,2);
    v_stb_error VARCHAR(1000);
    v_stb_result VARIANT;

    -- Bronze to Silver Tracking
    v_bts_start_time TIMESTAMP_NTZ;
    v_bts_end_time TIMESTAMP_NTZ;
    v_bts_status VARCHAR(20);
    v_s_inserted_records INTEGER DEFAULT 0;
    v_s_updated_records INTEGER DEFAULT 0;
    v_bts_duration_sec NUMBER(10,2);
    v_bts_error VARCHAR(1000);
    v_bts_result VARIANT;

    -- Silver to Gold Tracking
    v_stg_start_time TIMESTAMP_NTZ;
    v_stg_end_time TIMESTAMP_NTZ;
    v_stg_status VARCHAR(20);
    v_g_inserted_records INTEGER DEFAULT 0;
    v_g_updated_records INTEGER DEFAULT 0;
    v_g_deleted_records INTEGER DEFAULT 0;
    v_stg_duration_sec NUMBER(10,2);
    v_stg_error VARCHAR(1000);
    v_stg_result VARIANT;

    v_batch_start_time TIMESTAMP_NTZ;
    v_batch_end_time TIMESTAMP_NTZ;
    v_last_batch_executed_at TIMESTAMP_NTZ;

BEGIN
    ------------------------------------------------------------------
    -- STEP 1: INITIALIZE
    ------------------------------------------------------------------
    v_batch_id := UUID_STRING();
    v_batch_start_time := CURRENT_TIMESTAMP();
    v_last_batch_executed_at := CURRENT_TIMESTAMP();

    SELECT
        ENABLED,
        STAGE_TO_BRONZE_PROC,
        BRONZE_TO_SILVER_PROC,
        SILVER_TO_GOLD_PROC
    INTO
        v_pipeline_enabled,
        v_stage_to_bronze_proc,
        v_bronze_to_silver_proc,
        v_silver_to_gold_proc
    FROM COMMON.IMPORT_CONFIGURATION
    WHERE PIPELINE_NAME = :PIPELINE_NAME_PARAM;

    -- INITIALIZE BATCH TABLE
    INSERT INTO COMMON.BATCH(BATCH_ID, PIPELINE_NAME)
    VALUES (:v_batch_id, :PIPELINE_NAME_PARAM);

    ------------------------------------------------------------------
    -- STEP 2: STAGE → BRONZE
    ------------------------------------------------------------------
    BEGIN
        v_stb_start_time := CURRENT_TIMESTAMP();

        -- Call stored procedure dynamically
        LET res RESULTSET := (EXECUTE IMMEDIATE 'CALL ' || :v_stage_to_bronze_proc || '(?, ?)' USING (v_batch_id, STAGE_PATH_PARAM));
        LET cur CURSOR FOR res;
        OPEN cur;
        FETCH cur INTO v_stb_result;
        CLOSE cur;

        -- Extract values from the result array
        v_stb_status := v_stb_result[0]::VARCHAR;
        v_b_inserted_records := v_stb_result[1]::INTEGER;
        v_stb_end_time := CURRENT_TIMESTAMP();
        v_stb_duration_sec := DATEDIFF(SECOND, v_stb_start_time, v_stb_end_time);

    EXCEPTION
        WHEN OTHER THEN
            v_stb_status := 'FAILED';
            v_stb_error := SQLERRM;
            v_stb_end_time := CURRENT_TIMESTAMP();
            v_stb_duration_sec := DATEDIFF(SECOND, v_stb_start_time, v_stb_end_time);
    END;

    ------------------------------------------------------------------
    -- STEP 3: BRONZE → SILVER
    ------------------------------------------------------------------
    BEGIN
        v_bts_start_time := CURRENT_TIMESTAMP();

        -- Call stored procedure dynamically
        LET res RESULTSET := (EXECUTE IMMEDIATE 'CALL ' || :v_bronze_to_silver_proc || '(?)' USING (v_batch_id));
        LET cur CURSOR FOR res;
        OPEN cur;
        FETCH cur INTO v_bts_result;
        CLOSE cur;

        v_bts_status := v_bts_result[0]::VARCHAR;
        v_s_inserted_records := v_bts_result[1]::INTEGER;
        v_s_updated_records := v_bts_result[2]::INTEGER;
        v_bts_end_time := CURRENT_TIMESTAMP();
        v_bts_duration_sec := DATEDIFF(SECOND, v_bts_start_time, v_bts_end_time);

    EXCEPTION
        WHEN OTHER THEN
            v_bts_status := 'FAILED';
            v_bts_error := SQLERRM;
            v_bts_end_time := CURRENT_TIMESTAMP();
            v_bts_duration_sec := DATEDIFF(SECOND, v_bts_start_time, v_bts_end_time);
    END;

    ------------------------------------------------------------------
    -- STEP 4: SILVER → GOLD
    ------------------------------------------------------------------
    BEGIN
        v_stg_start_time := CURRENT_TIMESTAMP();

        -- Call stored procedure dynamically
        LET res RESULTSET := (EXECUTE IMMEDIATE 'CALL ' || :v_silver_to_gold_proc || '(?)' USING (v_batch_id));
        LET cur CURSOR FOR res;
        OPEN cur;
        FETCH cur INTO v_stg_result;
        CLOSE cur;

        v_stg_status := v_stg_result[0]::VARCHAR;
        v_g_inserted_records := v_stg_result[1]::INTEGER;
        v_g_updated_records := v_stg_result[2]::INTEGER;
        v_g_deleted_records := v_stg_result[3]::INTEGER;
        v_stg_end_time := CURRENT_TIMESTAMP();
        v_stg_duration_sec := DATEDIFF(SECOND, v_stg_start_time, v_stg_end_time);

    EXCEPTION
        WHEN OTHER THEN
            v_stg_status := 'FAILED';
            v_stg_error := SQLERRM;
            v_stg_end_time := CURRENT_TIMESTAMP();
            v_stg_duration_sec := DATEDIFF(SECOND, v_stg_start_time, v_stg_end_time);
    END;

    ------------------------------------------------------------------
    -- STEP 5: FINALIZE
    ------------------------------------------------------------------
    v_batch_end_time := CURRENT_TIMESTAMP();

    -- UPDATE BATCH TABLE with all execution details
    UPDATE COMMON.BATCH
    SET
        PIPELINE_NAME = :PIPELINE_NAME_PARAM,
        LAST_BATCH_EXECUTED_AT = :v_last_batch_executed_at,

        -- Stage to Bronze
        STAGE_TO_BRONZE_STATUS = :v_stb_status,
        BRONZE_INSERT_RECORDS = :v_b_inserted_records,

        -- Bronze to Silver
        BRONZE_TO_SILVER_STATUS = :v_bts_status,
        SILVER_INSERTED = :v_s_inserted_records,
        SILVER_UPDATED = :v_s_updated_records,

        -- Silver to Gold
        SILVER_TO_GOLD_STATUS = :v_stg_status,
        GOLD_INSERTED = :v_g_inserted_records,
        GOLD_UPDATED = :v_g_updated_records,
        GOLD_DELETED = :v_g_deleted_records,

        -- Batch Log with all timing and error details
        BATCH_LOG = OBJECT_CONSTRUCT(
            'batch_start_time', :v_batch_start_time,
            'batch_end_time', :v_batch_end_time,
            'total_duration_sec', DATEDIFF(SECOND, :v_batch_start_time, :v_batch_end_time),
            'stage_path', :STAGE_PATH_PARAM,

            'stage_to_bronze', OBJECT_CONSTRUCT(
                'status', :v_stb_status,
                'start_time', :v_stb_start_time,
                'end_time', :v_stb_end_time,
                'duration_sec', :v_stb_duration_sec,
                'records_inserted', :v_b_inserted_records,
                'error', :v_stb_error,
                'procedure_name', :v_stage_to_bronze_proc
            ),

            'bronze_to_silver', OBJECT_CONSTRUCT(
                'status', :v_bts_status,
                'start_time', :v_bts_start_time,
                'end_time', :v_bts_end_time,
                'duration_sec', :v_bts_duration_sec,
                'records_inserted', :v_s_inserted_records,
                'records_updated', :v_s_updated_records,
                'error', :v_bts_error,
                'procedure_name', :v_bronze_to_silver_proc
            ),

            'silver_to_gold', OBJECT_CONSTRUCT(
                'status', :v_stg_status,
                'start_time', :v_stg_start_time,
                'end_time', :v_stg_end_time,
                'duration_sec', :v_stg_duration_sec,
                'records_inserted', :v_g_inserted_records,
                'records_updated', :v_g_updated_records,
                'records_deleted', :v_g_deleted_records,
                'error', :v_stg_error,
                'procedure_name', :v_silver_to_gold_proc
            )
        )
    WHERE BATCH_ID = :v_batch_id;

    RETURN 'SUCCESS | BATCH_ID=' || :v_batch_id ||
           ' | STB=' || :v_stb_status ||
           ' | BTS=' || :v_bts_status ||
           ' | STG=' || :v_stg_status;

EXCEPTION
    WHEN OTHER THEN
        -- Log critical failure
        BEGIN
            UPDATE COMMON.BATCH
            SET
                BATCH_LOG = OBJECT_CONSTRUCT(
                    'critical_error', SQLERRM,
                    'error_timestamp', CURRENT_TIMESTAMP(),
                    'batch_start_time', :v_batch_start_time,
                    'stage_to_bronze_status', :v_stb_status,
                    'bronze_to_silver_status', :v_bts_status,
                    'silver_to_gold_status', :v_stg_status
                )
            WHERE BATCH_ID = :v_batch_id;
        EXCEPTION
            WHEN OTHER THEN
                NULL;
        END;

        RETURN 'CRITICAL FAILURE | BATCH_ID=' || :v_batch_id || ' | ' || SQLERRM;
END;
$$;

-- Execute the pipeline
CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_ADDRESS_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/customer_address/customer_address_01-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('CUSTOMER_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/customer/customer_01-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('DELIVERY_AGENT_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/delivery_agent/delivery_agent_01-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('LOCATION_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/location/location_01-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('MENU_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/menu/menu_03-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('RESTAURANT_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/restaurant/restaurant_01-01-2025.csv');
CALL COMMON.SP_IMPORT_MASTER('DELIVERY_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/delivery/day-01-delivery.csv');
CALL COMMON.SP_IMPORT_MASTER('ORDER_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order/orders-initial.csv');
CALL COMMON.SP_IMPORT_MASTER('ORDER_ITEM_PIPELINE', '@"SWIGGY"."BRONZE"."CSV_STG"/order_item/order-item-initial-v2.csv');