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
    BATCH_STATUS VARCHAR(20) DEFAULT 'INITIATED',

    -- Stage Execution Tracking
    STAGE_TO_BRONZE_STATUS VARCHAR(20),            -- SUCCESS, FAILED, SKIPPED
    STAGE_TO_BRONZE_ROWS INTEGER DEFAULT 0,
    STAGE_TO_BRONZE_START_TIME TIMESTAMP_NTZ,
    STAGE_TO_BRONZE_END_TIME TIMESTAMP_NTZ,
    STAGE_TO_BRONZE_DURATION_SEC NUMBER(10,2),
    STAGE_TO_BRONZE_ERROR VARCHAR(5000),

    -- Bronze to Silver Tracking
    BRONZE_TO_SILVER_STATUS VARCHAR(20),
    BRONZE_TO_SILVER_ROWS_INSERTED INTEGER DEFAULT 0,
    BRONZE_TO_SILVER_ROWS_UPDATED INTEGER DEFAULT 0,
    BRONZE_TO_SILVER_START_TIME TIMESTAMP_NTZ,
    BRONZE_TO_SILVER_END_TIME TIMESTAMP_NTZ,
    BRONZE_TO_SILVER_DURATION_SEC NUMBER(10,2),
    BRONZE_TO_SILVER_ERROR VARCHAR(5000),

    -- Silver to Gold Tracking
    SILVER_TO_GOLD_STATUS VARCHAR(20),
    SILVER_TO_GOLD_ROWS_INSERTED INTEGER DEFAULT 0,
    SILVER_TO_GOLD_ROWS_EXPIRED INTEGER DEFAULT 0,
    SILVER_TO_GOLD_START_TIME TIMESTAMP_NTZ,
    SILVER_TO_GOLD_END_TIME TIMESTAMP_NTZ,
    SILVER_TO_GOLD_DURATION_SEC NUMBER(10,2),
    SILVER_TO_GOLD_ERROR VARCHAR(5000),

    -- Overall Batch Tracking
    TOTAL_STAGES INTEGER DEFAULT 3,
    COMPLETED_STAGES INTEGER DEFAULT 0,
    FAILED_STAGES INTEGER DEFAULT 0,
    OVERALL_START_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    OVERALL_END_TIME TIMESTAMP_NTZ,
    OVERALL_DURATION_SEC NUMBER(10,2),

    -- Metadata
    INITIATED_BY VARCHAR(100) DEFAULT CURRENT_USER(),
    RETRY_ATTEMPT INTEGER DEFAULT 0
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
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
BEGIN
    ------------------------------------------------------------------
    -- STEP 1: INITIALIZE
    ------------------------------------------------------------------
    v_batch_id := UUID_STRING();
    v_start_time := CURRENT_TIMESTAMP();

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

    IF (NOT v_pipeline_enabled) THEN
        RETURN 'PIPELINE DISABLED: ' || :PIPELINE_NAME_PARAM;
    END IF;

    INSERT INTO COMMON.BATCH (
        BATCH_ID,
        PIPELINE_NAME,
        BATCH_STATUS,
        OVERALL_START_TIME,
        INITIATED_BY
    )
    VALUES (
        :v_batch_id,
        :PIPELINE_NAME_PARAM,
        'RUNNING',
        :v_start_time,
        CURRENT_USER()
    );

    ------------------------------------------------------------------
    -- STEP 2: STAGE → BRONZE
    ------------------------------------------------------------------
    BEGIN
        UPDATE COMMON.BATCH
        SET STAGE_TO_BRONZE_START_TIME = CURRENT_TIMESTAMP()
        WHERE BATCH_ID = :v_batch_id;

        EXECUTE IMMEDIATE
            'CALL ' || v_stage_to_bronze_proc || '(?, ?)'
        USING
            (v_batch_id, STAGE_PATH_PARAM);

        UPDATE COMMON.BATCH
        SET
            STAGE_TO_BRONZE_STATUS = 'SUCCESS',
            STAGE_TO_BRONZE_END_TIME = CURRENT_TIMESTAMP(),
            STAGE_TO_BRONZE_DURATION_SEC =
                DATEDIFF(SECOND, STAGE_TO_BRONZE_START_TIME, CURRENT_TIMESTAMP())
        WHERE BATCH_ID = :v_batch_id;

    EXCEPTION
        WHEN OTHER THEN
            UPDATE COMMON.BATCH
            SET
                STAGE_TO_BRONZE_STATUS = 'FAILED',
                STAGE_TO_BRONZE_END_TIME = CURRENT_TIMESTAMP(),
                STAGE_TO_BRONZE_ERROR = 'STAGE TO BRONZE FAILED'
            WHERE BATCH_ID = :v_batch_id;

            RETURN 'FAILED AT STAGE → BRONZE | BATCH_ID=' || v_batch_id || ' | ERROR=' || SQLERRM;
    END;

    ------------------------------------------------------------------
    -- STEP 3: BRONZE → SILVER
    ------------------------------------------------------------------
    BEGIN
        UPDATE COMMON.BATCH
        SET BRONZE_TO_SILVER_START_TIME = CURRENT_TIMESTAMP()
        WHERE BATCH_ID = :v_batch_id;

        EXECUTE IMMEDIATE
            'CALL ' || v_bronze_to_silver_proc || '(?)'
        USING
            (v_batch_id);

        UPDATE COMMON.BATCH
        SET
            BRONZE_TO_SILVER_STATUS = 'SUCCESS',
            BRONZE_TO_SILVER_END_TIME = CURRENT_TIMESTAMP(),
            BRONZE_TO_SILVER_DURATION_SEC =
                DATEDIFF(SECOND, BRONZE_TO_SILVER_START_TIME, CURRENT_TIMESTAMP())
        WHERE BATCH_ID = :v_batch_id;

    EXCEPTION
        WHEN OTHER THEN
            UPDATE COMMON.BATCH
            SET
                BRONZE_TO_SILVER_STATUS = 'FAILED',
                BRONZE_TO_SILVER_END_TIME = CURRENT_TIMESTAMP(),
                BRONZE_TO_SILVER_ERROR = 'BRONZE TO SILVER FAILED'
            WHERE BATCH_ID = :v_batch_id;

            RETURN 'FAILED AT BRONZE → SILVER | BATCH_ID=' || :v_batch_id || ' | ERROR=' || SQLERRM;
    END;

    ------------------------------------------------------------------
    -- STEP 4: SILVER → GOLD
    ------------------------------------------------------------------
    BEGIN
        UPDATE COMMON.BATCH
        SET SILVER_TO_GOLD_START_TIME = CURRENT_TIMESTAMP()
        WHERE BATCH_ID = :v_batch_id;

        EXECUTE IMMEDIATE
            'CALL ' || v_silver_to_gold_proc || '(?)'
        USING
            (v_batch_id);

        UPDATE COMMON.BATCH
        SET
            SILVER_TO_GOLD_STATUS = 'SUCCESS',
            SILVER_TO_GOLD_END_TIME = CURRENT_TIMESTAMP(),
            SILVER_TO_GOLD_DURATION_SEC =
                DATEDIFF(SECOND, SILVER_TO_GOLD_START_TIME, CURRENT_TIMESTAMP())
        WHERE BATCH_ID = :v_batch_id;

    EXCEPTION
        WHEN OTHER THEN
            UPDATE COMMON.BATCH
            SET
                SILVER_TO_GOLD_STATUS = 'FAILED',
                SILVER_TO_GOLD_END_TIME = CURRENT_TIMESTAMP(),
                SILVER_TO_GOLD_ERROR = 'SILVER TO GOLD FAILED'
            WHERE BATCH_ID = :v_batch_id;

            RETURN 'FAILED AT SILVER → GOLD | BATCH_ID=' || :v_batch_id || ' | ERROR=' || SQLERRM;;
    END;

    ------------------------------------------------------------------
    -- STEP 5: FINALIZE
    ------------------------------------------------------------------
    v_end_time := CURRENT_TIMESTAMP();

    UPDATE COMMON.BATCH
    SET
        BATCH_STATUS = 'COMPLETED',
        OVERALL_END_TIME = :v_end_time,
        OVERALL_DURATION_SEC =
            DATEDIFF(SECOND, :v_start_time, :v_end_time)
    WHERE BATCH_ID = :v_batch_id;

    RETURN
        'SUCCESS | BATCH_ID=' || :v_batch_id ||
        ' | DURATION=' ||
        DATEDIFF(SECOND, :v_start_time, :v_end_time) || 's';

EXCEPTION
    WHEN OTHER THEN
        UPDATE COMMON.BATCH
        SET
            BATCH_STATUS = 'FAILED',
            OVERALL_END_TIME = CURRENT_TIMESTAMP()
        WHERE BATCH_ID = :v_batch_id;

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