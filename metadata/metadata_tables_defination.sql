-- =======================================================================================================================================
-- ============================================== CONFIGURATION & METADATA TABLES ========================================================
-- =======================================================================================================================================
-- FILE_FORMAT_MASTER
-- Purpose: Defines reusable file format specifications
-- Answers: How to parse it?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.FILE_FORMAT_MASTER (
    file_format_id      INTEGER PRIMARY KEY,
    format_type         VARCHAR(20),        -- CSV | JSON | PARQUET | XML
    delimiter           VARCHAR(5),         -- , | | | \t | NULL
    quote_char          VARCHAR(1),         -- " | ' | NULL
    escape_char         VARCHAR(1),
    header_present      CHAR(1),            -- Y/N
    compression_type    VARCHAR(10),        -- GZIP | BZIP2 | NONE
    record_separator    VARCHAR(5),         -- \n | \r\n
    encoding            VARCHAR(20),        -- UTF-8 | ISO-8859-1
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- =======================================================================================================================================
-- SOURCE_FILE_CONFIG
-- Purpose: Defines file sources and their ingestion rules
-- Answers: What arrives and where?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.SOURCE_FILE_CONFIG (
    source_id           INTEGER PRIMARY KEY,
    source_name         VARCHAR(100) UNIQUE,
    source_system       VARCHAR(50),        -- SAP | SALESFORCE | API
    file_format_id      INTEGER,            -- FK → FILE_FORMAT_MASTER
    landing_path        VARCHAR(500),       -- s3://bucket/raw/orders/, INSERT CDC_STREAM NAME IN CASE OF STREAM EX- BRONZE.STREAM_ORDERS_CHANGES:
    file_pattern        VARCHAR(100),       -- orders_*.csv | orders_{YYYY}{MM}{DD}.csv
    ingestion_type      VARCHAR(20),        -- BATCH | STREAM | INCREMENTAL
    schedule_cron       VARCHAR(50),        -- 0 2 * * * (daily 2 AM)
    active_flag         CHAR(1) DEFAULT 'Y',
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (file_format_id) REFERENCES COMMON.FILE_FORMAT_MASTER(file_format_id)
);
-- ======================================================================================================================================
-- DATA_FIELD_MASTER
-- Purpose: Canonical definition of business fields (logical schema)
-- Answers: How it is structured?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.DATA_FIELD_MASTER (
    field_id                INTEGER PRIMARY KEY,
    field_name              VARCHAR(100) UNIQUE,
    data_type               VARCHAR(20),        -- VARCHAR | INTEGER | DATE | TIMESTAMP | NUMBER
    length                  INTEGER,
    precision               INTEGER,
    scale                   INTEGER,
    nullable_flag           CHAR(1) DEFAULT 'Y',
    business_description    VARCHAR(500),
    data_domain             VARCHAR(50),        -- CUSTOMER | ORDER | PRODUCT
    pii_flag                CHAR(1) DEFAULT 'N',
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- =======================================================================================================================================
-- FILE_COLUMN_MAPPING
-- Purpose: Maps physical file columns to logical business fields
-- Answers: How to transform source → target?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.FILE_COLUMN_MAPPING (
    mapping_id              INTEGER PRIMARY KEY,
    source_id               INTEGER,
    file_column_name        VARCHAR(100),
    field_id                INTEGER,
    column_position         INTEGER,
    is_primary_key          BOOLEAN DEFAULT FALSE,
    auto_increment_flag     BOOLEAN DEFAULT FALSE,  -- NEW: Tracks if PK is auto-generated
    transformation_rule     VARCHAR(500),
    default_value           VARCHAR(100),           -- NEW: Use UUID_STRING() for non-auto PKs
    validation_rule         VARCHAR(500),
    active_flag             CHAR(1) DEFAULT 'Y',
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES COMMON.SOURCE_FILE_CONFIG(source_id),
    FOREIGN KEY (field_id) REFERENCES COMMON.DATA_FIELD_MASTER(field_id)
);
-- =======================================================================================================================================
-- TARGET_TABLE_MAPPING
-- Purpose: Defines where parsed data lands
-- Answers: Where it lands?
-- ---------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE COMMON.TARGET_TABLE_MAPPING (
    target_id               INTEGER PRIMARY KEY,
    source_id               INTEGER,            -- FK → SOURCE_FILE_CONFIG
    bronze_table            VARCHAR(50),
    load_error_table        VARCHAR(50),
    silver_table            VARCHAR(50),
    gold_table              VARCHAR(50),
    load_type               VARCHAR(20),        -- APPEND | MERGE | SCD2
    primary_key_fields      VARCHAR(200),       -- Comma-separated: ORDER_ID,ORDER_DATE
    partition_columns       VARCHAR(200),       -- order_date,region
    clustering_columns      VARCHAR(200),
    merge_key_fields        VARCHAR(200),       -- For MERGE operations
    enable_raw_columns      CHAR(1) DEFAULT 'Y', -- Store *_RAW columns
    enable_audit_columns    CHAR(1) DEFAULT 'Y', -- Add INGEST_RUN_ID, CREATED_AT, etc.
    active_flag             CHAR(1) DEFAULT 'Y',
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES COMMON.SOURCE_FILE_CONFIG(source_id)
);
-- =====================================================
-- BATCH TABLE - STORES BATCH EXECUTION DATA
-- =====================================================

CREATE OR REPLACE TABLE COMMON.BATCH (
    BATCH_ID VARCHAR PRIMARY KEY,
    PIPELINE_NAME VARCHAR(100),
    LAST_BATCH_EXECUTED_AT TIMESTAMP_NTZ,

    -- Stage Execution Tracking
    STAGE_TO_BRONZE_STATUS VARCHAR(20),
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
-- INGEST RUN TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.INGEST_RUN (
    INGEST_RUN_ID INTEGER PRIMARY KEY,
    PIPELINE_NAME VARCHAR(50),
    SOURCE_TABLE VARCHAR(50),
    LOAD_ERROR_TABLE VARCHAR(50),
    RUN_STATUS VARCHAR(50),
    SOURCE_ROW_COUNT INTEGER,
    VALID_ROW_COUNT INTEGER,
    INVALID_ROW_COUNT INTEGER,
    EXECUTION_DURATION_SEC INTEGER,
    ERROR_MESSAGE VARCHAR(5000),
    EXECUTED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
    EXECUTED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);
-- =====================================================
-- =====================================================
-- PIPELINE_EXECUTION_RESULT
-- =====================================================
CREATE OR REPLACE TABLE COMMON.PIPELINE_EXECUTION_RESULT (
    PIPELINE_RUN_ID STRING DEFAULT UUID_STRING(),
    TASK_NAME STRING,
    PIPELINE_NAME STRING,
    BATCH_START_TIME TIMESTAMP_TZ,
    BATCH_END_TIME TIMESTAMP_TZ,
    TOTAL_DURATION_SEC NUMBER,
    STAGE_PATH STRING,
    SOURCE_TYPE STRING,
    RESULT VARIANT,
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);