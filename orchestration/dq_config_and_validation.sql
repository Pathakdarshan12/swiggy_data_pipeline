-- =====================================================
-- STEP 1: DQ CONFIG TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.DQ_CONFIG (
    DQ_CONFIG_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    VALIDATE_TABLE VARCHAR(200) NOT NULL,
    PRIMARY_KEY_COLUMN STRING NOT NULL,
    ERROR_TABLE VARCHAR(200) NOT NULL,
    VALIDATION_NAME VARCHAR(200) UNIQUE NOT NULL,
    VALIDATION_TYPE VARCHAR(50) NOT NULL,
    VALIDATE_COLUMN VARCHAR(200),
    VALIDATION_QUERY VARCHAR(5000),
    VALIDATION_ERROR_MSG VARCHAR(1000),
    STATUS BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);

-- Create DQ Statistics Table
CREATE OR REPLACE TABLE COMMON.DQ_RUN_STATISTICS (
    DQ_RUN_STAT_ID NUMBER IDENTITY(1,1) PRIMARY KEY,
    INGEST_RUN_ID NUMBER NOT NULL,
    VALIDATE_TABLE VARCHAR(500) NOT NULL,
    STAGE_TABLE VARCHAR(500) NOT NULL,
    ERROR_TABLE VARCHAR(500) NOT NULL,

    -- Overall Statistics
    TOTAL_ROWS_PROCESSED NUMBER DEFAULT 0,
    TOTAL_VALID_ROWS NUMBER DEFAULT 0,
    TOTAL_INVALID_ROWS NUMBER DEFAULT 0,
    TOTAL_ERROR_COUNT NUMBER DEFAULT 0,
    VALIDATION_CHECKS_EXECUTED NUMBER DEFAULT 0,

    -- Percentage Metrics
    VALID_ROWS_PERCENTAGE NUMBER(5,2) DEFAULT 0.00,
    INVALID_ROWS_PERCENTAGE NUMBER(5,2) DEFAULT 0.00,
    ERROR_RATE_PERCENTAGE NUMBER(5,2) DEFAULT 0.00,

    -- Validation Type Breakdown (as VARIANT for flexibility)
    VALIDATION_TYPE_STATS VARIANT,

    -- Execution Details
    EXECUTION_STATUS VARCHAR(50),
    EXECUTION_ERROR_MESSAGE VARCHAR(5000),
    EXECUTION_START_TIME TIMESTAMP_NTZ,
    EXECUTION_END_TIME TIMESTAMP_NTZ,
    EXECUTION_DURATION_SECONDS NUMBER(10,2),

    -- Audit Columns
    CREATED_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);

-- =====================================================
-- STEP 2: INSERT DQ RULES FOR CUSTOMER
-- =====================================================

INSERT INTO COMMON.DQ_CONFIG (VALIDATE_TABLE, PRIMARY_KEY_COLUMN, ERROR_TABLE, VALIDATION_NAME, VALIDATION_TYPE, VALIDATE_COLUMN, VALIDATION_QUERY, VALIDATION_ERROR_MSG, STATUS) VALUES

-- ============= MANDATORY CHECKS =============
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_MAN_ORDER_ID', 'MANDATORY_CHECK', 'ORDER_ID', 'ORDER_ID IS NOT NULL AND TRIM(ORDER_ID) != ''''', 'ORDER_ID is mandatory', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_MAN_CUSTOMER_ID', 'MANDATORY_CHECK', 'CUSTOMER_ID', 'CUSTOMER_ID IS NOT NULL AND TRIM(CUSTOMER_ID) != ''''', 'CUSTOMER_ID is mandatory', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_MAN_RESTAURANT_ID', 'MANDATORY_CHECK', 'RESTAURANT_ID', 'RESTAURANT_ID IS NOT NULL', 'RESTAURANT_ID is mandatory', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_MAN_ORDER_DATE', 'MANDATORY_CHECK', 'ORDER_DATE', 'ORDER_DATE IS NOT NULL', 'ORDER_DATE is mandatory', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_MAN_TOTAL_AMOUNT', 'MANDATORY_CHECK', 'TOTAL_AMOUNT', 'TOTAL_AMOUNT IS NOT NULL', 'TOTAL_AMOUNT is mandatory', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_MAN_ORDER_STATUS', 'MANDATORY_CHECK', 'ORDER_STATUS', 'ORDER_STATUS IS NOT NULL AND TRIM(ORDER_STATUS) != ''''', 'ORDER_STATUS is mandatory', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_MAN_PAYMENT_METHOD', 'MANDATORY_CHECK', 'PAYMENT_METHOD', 'PAYMENT_METHOD IS NOT NULL AND TRIM(PAYMENT_METHOD) != ''''', 'PAYMENT_METHOD is mandatory', TRUE),

-- ============= VALUE CHECKS =============
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_VAL_RESTAURANT_ID', 'VALUE_CHECK', 'RESTAURANT_ID', 'TRY_CAST(RESTAURANT_ID AS INTEGER) IS NOT NULL', 'RESTAURANT_ID is not a valid integer', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_VAL_TOTAL_AMOUNT', 'VALUE_CHECK', 'TOTAL_AMOUNT', 'TRY_CAST(TOTAL_AMOUNT AS NUMBER(10,2)) IS NOT NULL', 'TOTAL_AMOUNT is not a valid number', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_VAL_ORDER_STATUS', 'VALUE_CHECK', 'ORDER_STATUS', 'UPPER(ORDER_STATUS) IN (''OUT_FOR_DELIVERY'', ''COMPLETED'', ''CANCELLED'', ''PREPARING'', ''CONFIRMED'', ''DELIVERED'', ''PLACED'', ''READY'', ''PENDING'')', 'ORDER_STATUS is not valid', TRUE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_VAL_PAYMENT_METHOD', 'VALUE_CHECK', 'PAYMENT_METHOD', 'UPPER(PAYMENT_METHOD) IN (''WALLET'' ,''CASH'' ,''UPI'' ,''DEBIT CARD'' ,''CREDIT CARD'')', 'PAYMENT_METHOD is not valid', FALSE),

-- ============= LOOKUP CHECKS =============
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_OI_LKP_CUSTOMER_ID', 'LOOKUP_CHECK', 'CUSTOMER_ID', 'CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM GOLD.FACT_ORDER WHERE CUSTOMER_ID IS NOT NULL)', 'CUSTOMER_ID does not exist in reference table', FALSE),
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_OI_LKP_RESTAURANT_ID', 'LOOKUP_CHECK', 'RESTAURANT_ID', 'RESTAURANT_ID IN (SELECT RESTAURANT_ID FROM GOLD.FACT_ORDER WHERE RESTAURANT_ID IS NOT NULL)', 'RESTAURANT_ID does not exist in reference table', FALSE),

-- ============= DUPLICATE CHECKS =============
('BRONZE.ORDER_BRZ', 'ORDER_ID', 'BRONZE.ORDER_LOAD_ERROR', 'DQ_O_DUP_ORDER_ID', 'DUPLICATE_ALLOW_ONE_CHECK', 'ORDER_ID', NULL, 'Duplicate ORDER_ID found', TRUE);

-- =====================================================
-- DQ RULES FOR DELIVERY_BRZ
-- =====================================================

INSERT INTO COMMON.DQ_CONFIG (VALIDATE_TABLE, PRIMARY_KEY_COLUMN, ERROR_TABLE, VALIDATION_NAME, VALIDATION_TYPE, VALIDATE_COLUMN, VALIDATION_QUERY, VALIDATION_ERROR_MSG, STATUS) VALUES

-- ============= MANDATORY CHECKS =============
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_MAN_DELIVERY_ID', 'MANDATORY_CHECK', 'DELIVERY_ID', 'DELIVERY_ID IS NOT NULL AND TRIM(DELIVERY_ID) != ''''', 'DELIVERY_ID is mandatory', TRUE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_MAN_ORDER_ID', 'MANDATORY_CHECK', 'ORDER_ID', 'ORDER_ID IS NOT NULL AND TRIM(ORDER_ID) != ''''', 'ORDER_ID is mandatory', TRUE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_MAN_DELIVERY_AGENT_ID', 'MANDATORY_CHECK', 'DELIVERY_AGENT_ID', 'DELIVERY_AGENT_ID IS NOT NULL', 'DELIVERY_AGENT_ID is mandatory', TRUE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_MAN_DELIVERY_STATUS', 'MANDATORY_CHECK', 'DELIVERY_STATUS', 'DELIVERY_STATUS IS NOT NULL AND TRIM(DELIVERY_STATUS) != ''''', 'DELIVERY_STATUS is mandatory', TRUE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_MAN_CUSTOMER_ADDRESS_ID', 'MANDATORY_CHECK', 'CUSTOMER_ADDRESS_ID', 'CUSTOMER_ADDRESS_ID IS NOT NULL', 'CUSTOMER_ADDRESS_ID is mandatory', TRUE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_MAN_DELIVERY_DATE', 'MANDATORY_CHECK', 'DELIVERY_DATE', 'DELIVERY_DATE IS NOT NULL', 'DELIVERY_DATE is mandatory', TRUE),

-- ============= VALUE CHECKS =============
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_VAL_DELIVERY_AGENT_ID', 'VALUE_CHECK', 'DELIVERY_AGENT_ID', 'TRY_CAST(DELIVERY_AGENT_ID AS INTEGER) IS NOT NULL', 'DELIVERY_AGENT_ID is not a valid integer', TRUE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_VAL_CUSTOMER_ADDRESS_ID', 'VALUE_CHECK', 'CUSTOMER_ADDRESS_ID', 'TRY_CAST(CUSTOMER_ADDRESS_ID AS INTEGER) IS NOT NULL', 'CUSTOMER_ADDRESS_ID is not a valid integer', TRUE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_VAL_DELIVERY_STATUS', 'VALUE_CHECK', 'DELIVERY_STATUS', 'UPPER(DELIVERY_STATUS) IN (''ASSIGNED'', ''PICKED_UP'', ''IN_TRANSIT'', ''DELIVERED'', ''FAILED'', ''CANCELLED'', ''PENDING'')', 'DELIVERY_STATUS is not valid', TRUE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_VAL_DELIVERY_DATE', 'VALUE_CHECK', 'DELIVERY_DATE', 'TRY_CAST(DELIVERY_DATE AS TIMESTAMP_TZ) IS NOT NULL', 'DELIVERY_DATE is not a valid timestamp', TRUE),

-- ============= LOOKUP CHECKS =============
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_LKP_ORDER_ID', 'LOOKUP_CHECK', 'ORDER_ID', 'ORDER_ID IN (SELECT ORDER_ID FROM BRONZE.ORDER_BRZ WHERE ORDER_ID IS NOT NULL)', 'ORDER_ID does not exist in reference table', FALSE),
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_LKP_DELIVERY_AGENT_ID', 'LOOKUP_CHECK', 'DELIVERY_AGENT_ID', 'DELIVERY_AGENT_ID IN (SELECT DELIVERY_AGENT_ID FROM GOLD.DIM_DELIVERY_AGENT WHERE DELIVERY_AGENT_ID IS NOT NULL)', 'DELIVERY_AGENT_ID does not exist in reference table', FALSE),

-- ============= DUPLICATE CHECKS =============
('BRONZE.DELIVERY_BRZ', 'DELIVERY_ID', 'BRONZE.DELIVERY_LOAD_ERROR', 'DQ_D_DUP_DELIVERY_ID', 'DUPLICATE_ALLOW_ONE_CHECK', 'DELIVERY_ID', NULL, 'Duplicate DELIVERY_ID found', TRUE);

-- =====================================================
-- DQ RULES FOR ORDER_ITEM_BRZ
-- =====================================================

INSERT INTO COMMON.DQ_CONFIG (VALIDATE_TABLE, PRIMARY_KEY_COLUMN, ERROR_TABLE, VALIDATION_NAME, VALIDATION_TYPE, VALIDATE_COLUMN, VALIDATION_QUERY, VALIDATION_ERROR_MSG, STATUS) VALUES

-- ============= MANDATORY CHECKS =============
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_MAN_ORDER_ITEM_ID', 'MANDATORY_CHECK', 'ORDER_ITEM_ID', 'ORDER_ITEM_ID IS NOT NULL AND TRIM(ORDER_ITEM_ID) != ''''', 'ORDER_ITEM_ID is mandatory', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_MAN_ORDER_ID', 'MANDATORY_CHECK', 'ORDER_ID', 'ORDER_ID IS NOT NULL AND TRIM(ORDER_ID) != ''''', 'ORDER_ID is mandatory', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_MAN_MENU_ID', 'MANDATORY_CHECK', 'MENU_ID', 'MENU_ID IS NOT NULL', 'MENU_ID is mandatory', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_MAN_QUANTITY', 'MANDATORY_CHECK', 'QUANTITY', 'QUANTITY IS NOT NULL', 'QUANTITY is mandatory', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_MAN_PRICE', 'MANDATORY_CHECK', 'PRICE', 'PRICE IS NOT NULL', 'PRICE is mandatory', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_MAN_SUBTOTAL', 'MANDATORY_CHECK', 'SUBTOTAL', 'SUBTOTAL IS NOT NULL', 'SUBTOTAL is mandatory', TRUE),

-- ============= VALUE CHECKS =============
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_VAL_MENU_ID', 'VALUE_CHECK', 'MENU_ID', 'TRY_CAST(MENU_ID AS INTEGER) IS NOT NULL', 'MENU_ID is not a valid integer', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_VAL_QUANTITY', 'VALUE_CHECK', 'QUANTITY', 'TRY_CAST(QUANTITY AS NUMBER(10,2)) IS NOT NULL AND QUANTITY > 0', 'QUANTITY is not a valid positive number', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_VAL_PRICE', 'VALUE_CHECK', 'PRICE', 'TRY_CAST(PRICE AS NUMBER(10,2)) IS NOT NULL AND PRICE >= 0', 'PRICE is not a valid non-negative number', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_VAL_SUBTOTAL', 'VALUE_CHECK', 'SUBTOTAL', 'TRY_CAST(SUBTOTAL AS NUMBER(10,2)) IS NOT NULL AND SUBTOTAL >= 0', 'SUBTOTAL is not a valid non-negative number', TRUE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_VAL_ORDER_TIMESTAMP', 'VALUE_CHECK', 'ORDER_TIMESTAMP', 'TRY_CAST(ORDER_TIMESTAMP AS DATE) IS NOT NULL', 'ORDER_TIMESTAMP is not a valid date', TRUE),

-- ============= BUSINESS LOGIC CHECKS =============
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_BUS_SUBTOTAL_CALC', 'VALUE_CHECK', 'SUBTOTAL', 'ABS(SUBTOTAL - (QUANTITY * PRICE)) < 0.01', 'SUBTOTAL does not match QUANTITY * PRICE', TRUE),

-- ============= LOOKUP CHECKS =============
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_LKP_ORDER_ID', 'LOOKUP_CHECK', 'ORDER_ID', 'ORDER_ID IN (SELECT ORDER_ID FROM BRONZE.ORDER_BRZ WHERE ORDER_ID IS NOT NULL)', 'ORDER_ID does not exist in reference table', FALSE),
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_LKP_MENU_ID', 'LOOKUP_CHECK', 'MENU_ID', 'MENU_ID IN (SELECT MENU_ID FROM GOLD.DIM_MENU WHERE MENU_ID IS NOT NULL)', 'MENU_ID does not exist in reference table', FALSE),

-- ============= DUPLICATE CHECKS =============
('BRONZE.ORDER_ITEM_BRZ', 'ORDER_ITEM_ID', 'BRONZE.ORDER_ITEM_LOAD_ERROR', 'DQ_OI_DUP_ORDER_ITEM_ID', 'DUPLICATE_ALLOW_ONE_CHECK', 'ORDER_ITEM_ID', NULL, 'Duplicate ORDER_ITEM_ID found', TRUE);

-- =====================================================
-- DATA QUALITY VALIDATION PROCEDURE
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_EXECUTE_DATA_QUALITY_VALIDATION(
    P_STAGE_TABLE VARCHAR,
    P_VALIDATE_TABLE VARCHAR,
    P_ERROR_TABLE VARCHAR,
    P_INGEST_RUN_ID NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_sql STRING;
    v_rs RESULTSET;
    v_pk_rs RESULTSET;
    v_dq_config_id INTEGER;
    v_primary_key_column VARCHAR;
    v_validation_name VARCHAR;
    v_validation_type VARCHAR;
    v_validate_column VARCHAR;
    v_validation_query VARCHAR;
    v_validation_error_msg VARCHAR;
    v_check_count INTEGER DEFAULT 0;

    -- Statistics variables
    v_start_time TIMESTAMP_NTZ;
    v_end_time TIMESTAMP_NTZ;
    v_stats_result RESULTSET;
    v_total_rows NUMBER DEFAULT 0;
    v_valid_rows NUMBER DEFAULT 0;
    v_invalid_rows NUMBER DEFAULT 0;
    v_total_errors NUMBER DEFAULT 0;
    v_mandatory_check_count NUMBER DEFAULT 0;
    v_value_check_count NUMBER DEFAULT 0;
    v_lookup_check_count NUMBER DEFAULT 0;
    v_duplicate_check_count NUMBER DEFAULT 0;
    v_mandatory_errors NUMBER DEFAULT 0;
    v_value_errors NUMBER DEFAULT 0;
    v_lookup_errors NUMBER DEFAULT 0;
    v_duplicate_errors NUMBER DEFAULT 0;
    v_valid_pct NUMBER(5,2) DEFAULT 0.00;
    v_invalid_pct NUMBER(5,2) DEFAULT 0.00;
    v_error_rate_pct NUMBER(5,2) DEFAULT 0.00;
    v_duration_seconds NUMBER(10,2) DEFAULT 0.00;
    v_error_message VARCHAR;

BEGIN
    v_start_time := CURRENT_TIMESTAMP();

    -- Fetch primary key column
    v_sql := '
        SELECT PRIMARY_KEY_COLUMN
        FROM COMMON.DQ_CONFIG
        WHERE VALIDATE_TABLE = ?
          AND ERROR_TABLE = ?
          AND STATUS = TRUE
        LIMIT 1
    ';

    v_pk_rs := (EXECUTE IMMEDIATE :v_sql USING (P_VALIDATE_TABLE, P_ERROR_TABLE));

    FOR pk_record IN v_pk_rs DO
        v_primary_key_column := pk_record.PRIMARY_KEY_COLUMN;
    END FOR;

    IF (v_primary_key_column IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', 'Primary key column not configured for table: ' || P_VALIDATE_TABLE,
            'ROWS_VALIDATED', '0'
        );
    END IF;

    -- Fetch validation rules
    v_sql := '
        SELECT
            DQ_CONFIG_ID,
            VALIDATION_NAME,
            VALIDATION_TYPE,
            VALIDATE_COLUMN,
            VALIDATION_QUERY,
            VALIDATION_ERROR_MSG
        FROM COMMON.DQ_CONFIG
        WHERE VALIDATE_TABLE = ?
          AND ERROR_TABLE = ?
          AND STATUS = TRUE
        ORDER BY
            CASE VALIDATION_TYPE
                WHEN ''MANDATORY_CHECK'' THEN 1
                WHEN ''VALUE_CHECK'' THEN 2
                WHEN ''LOOKUP_CHECK'' THEN 3
                WHEN ''DUPLICATE_ALLOW_ONE_CHECK'' THEN 4
                ELSE 5
            END,
            DQ_CONFIG_ID
    ';

    v_rs := (EXECUTE IMMEDIATE :v_sql USING (P_VALIDATE_TABLE, P_ERROR_TABLE));

    FOR record IN v_rs DO
        v_dq_config_id := record.DQ_CONFIG_ID;
        v_validation_name := record.VALIDATION_NAME;
        v_validation_type := record.VALIDATION_TYPE;
        v_validate_column := record.VALIDATE_COLUMN;
        v_validation_query := record.VALIDATION_QUERY;
        v_validation_error_msg := record.VALIDATION_ERROR_MSG;

        v_check_count := v_check_count + 1;

        CASE v_validation_type
            -- ============= MANDATORY CHECK =============
            WHEN 'MANDATORY_CHECK' THEN
                v_mandatory_check_count := v_mandatory_check_count + 1;

                -- Single MERGE operation combining UPDATE and INSERT
                v_sql := '
                    MERGE INTO ' || P_ERROR_TABLE || ' e
                    USING (
                        SELECT
                            ' || v_primary_key_column || ' as pk,
                            ''' || v_validate_column || ''' as validate_col,
                            ''' || v_validation_type || ''' as val_type,
                            ''' || v_validation_error_msg || ''' as error_msg
                        FROM ' || P_STAGE_TABLE || '
                        WHERE IS_VALID = TRUE
                          AND NOT (' || v_validation_query || ')
                    ) s
                    ON e.ERROR_ID = s.pk
                       AND e.INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
                       AND e.VALIDATE_COLUMN = s.validate_col
                       AND e.VALIDATION_TYPE = s.val_type
                    WHEN NOT MATCHED THEN
                        INSERT (ERROR_ID, VALIDATE_COLUMN, VALIDATION_TYPE, VALIDATION_ERROR_MSG, INGEST_RUN_ID)
                        VALUES (s.pk, s.validate_col, s.val_type, s.error_msg, ' || P_INGEST_RUN_ID || ')
                ';
                EXECUTE IMMEDIATE v_sql;

                -- Update IS_VALID flag after inserting errors
                v_sql := '
                    UPDATE ' || P_STAGE_TABLE || '
                    SET IS_VALID = FALSE
                    WHERE IS_VALID = TRUE
                      AND NOT (' || v_validation_query || ')
                ';
                EXECUTE IMMEDIATE v_sql;

            -- ============= VALUE CHECK =============
            WHEN 'VALUE_CHECK' THEN
                v_value_check_count := v_value_check_count + 1;

                v_sql := '
                    MERGE INTO ' || P_ERROR_TABLE || ' e
                    USING (
                        SELECT
                            ' || v_primary_key_column || ' as pk,
                            ''' || v_validate_column || ''' as validate_col,
                            ''' || v_validation_type || ''' as val_type,
                            ''' || v_validation_error_msg || ' - Value: '' || COALESCE(TO_VARCHAR(' || v_validate_column || '), ''NULL'') as error_msg
                        FROM ' || P_STAGE_TABLE || '
                        WHERE IS_VALID = TRUE
                          AND ' || v_validate_column || ' IS NOT NULL
                          AND NOT (' || v_validation_query || ')
                    ) s
                    ON e.ERROR_ID = s.pk
                       AND e.INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
                       AND e.VALIDATE_COLUMN = s.validate_col
                       AND e.VALIDATION_TYPE = s.val_type
                    WHEN NOT MATCHED THEN
                        INSERT (ERROR_ID, VALIDATE_COLUMN, VALIDATION_TYPE, VALIDATION_ERROR_MSG, INGEST_RUN_ID)
                        VALUES (s.pk, s.validate_col, s.val_type, s.error_msg, ' || P_INGEST_RUN_ID || ')
                ';
                EXECUTE IMMEDIATE v_sql;

                v_sql := '
                    UPDATE ' || P_STAGE_TABLE || '
                    SET IS_VALID = FALSE
                    WHERE IS_VALID = TRUE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND NOT (' || v_validation_query || ')
                ';
                EXECUTE IMMEDIATE v_sql;

            -- ============= LOOKUP CHECK =============
            WHEN 'LOOKUP_CHECK' THEN
                v_lookup_check_count := v_lookup_check_count + 1;

                v_sql := '
                    MERGE INTO ' || P_ERROR_TABLE || ' e
                    USING (
                        SELECT
                            ' || v_primary_key_column || ' as pk,
                            ''' || v_validate_column || ''' as validate_col,
                            ''' || v_validation_type || ''' as val_type,
                            ''' || v_validation_error_msg || ' - Value: '' || COALESCE(TO_VARCHAR(' || v_validate_column || '), ''NULL'') as error_msg
                        FROM ' || P_STAGE_TABLE || '
                        WHERE IS_VALID = TRUE
                          AND ' || v_validate_column || ' IS NOT NULL
                          AND NOT (' || v_validation_query || ')
                    ) s
                    ON e.ERROR_ID = s.pk
                       AND e.INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
                       AND e.VALIDATE_COLUMN = s.validate_col
                       AND e.VALIDATION_TYPE = s.val_type
                    WHEN NOT MATCHED THEN
                        INSERT (ERROR_ID, VALIDATE_COLUMN, VALIDATION_TYPE, VALIDATION_ERROR_MSG, INGEST_RUN_ID)
                        VALUES (s.pk, s.validate_col, s.val_type, s.error_msg, ' || P_INGEST_RUN_ID || ')
                ';
                EXECUTE IMMEDIATE v_sql;

                v_sql := '
                    UPDATE ' || P_STAGE_TABLE || '
                    SET IS_VALID = FALSE
                    WHERE IS_VALID = TRUE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND NOT (' || v_validation_query || ')
                ';
                EXECUTE IMMEDIATE v_sql;

            -- ============= DUPLICATE_ALLOW_ONE CHECK =============
            WHEN 'DUPLICATE_ALLOW_ONE_CHECK' THEN
                v_duplicate_check_count := v_duplicate_check_count + 1;

                -- Optimized duplicate check using window functions with CTE
                v_sql := '
                    MERGE INTO ' || P_ERROR_TABLE || ' e
                    USING (
                        WITH dup_analysis AS (
                            SELECT
                                ' || v_primary_key_column || ' as pk_col,
                                ' || v_validate_column || ' as val_col,
                                ROW_NUMBER() OVER (PARTITION BY ' || v_validate_column || ' ORDER BY ' || v_primary_key_column || ') as rn,
                                COUNT(*) OVER (PARTITION BY ' || v_validate_column || ') as dup_cnt
                            FROM ' || P_STAGE_TABLE || '
                            WHERE IS_VALID = TRUE
                              AND ' || v_validate_column || ' IS NOT NULL
                        )
                        SELECT
                            pk_col as pk,
                            ''' || v_validate_column || ''' as validate_col,
                            ''' || v_validation_type || ''' as val_type,
                            ''' || v_validation_error_msg || ' - Value: '' || COALESCE(TO_VARCHAR(val_col), ''NULL'') as error_msg
                        FROM dup_analysis
                        WHERE rn > 1 AND dup_cnt > 1
                    ) s
                    ON e.ERROR_ID = s.pk
                       AND e.INGEST_RUN_ID = ' || P_INGEST_RUN_ID || '
                       AND e.VALIDATE_COLUMN = s.validate_col
                       AND e.VALIDATION_TYPE = s.val_type
                    WHEN NOT MATCHED THEN
                        INSERT (ERROR_ID, VALIDATE_COLUMN, VALIDATION_TYPE, VALIDATION_ERROR_MSG, INGEST_RUN_ID)
                        VALUES (s.pk, s.validate_col, s.val_type, s.error_msg, ' || P_INGEST_RUN_ID || ')
                ';
                EXECUTE IMMEDIATE v_sql;

                v_sql := '
                    UPDATE ' || P_STAGE_TABLE || '
                    SET IS_VALID = FALSE
                    WHERE IS_VALID = TRUE
                      AND ' || v_validate_column || ' IS NOT NULL
                      AND ' || v_primary_key_column || ' IN (
                          WITH dup_analysis AS (
                              SELECT
                                  ' || v_primary_key_column || ' as pk_col,
                                  ROW_NUMBER() OVER (PARTITION BY ' || v_validate_column || ' ORDER BY ' || v_primary_key_column || ') as rn,
                                  COUNT(*) OVER (PARTITION BY ' || v_validate_column || ') as dup_cnt
                              FROM ' || P_STAGE_TABLE || '
                              WHERE ' || v_validate_column || ' IS NOT NULL
                          )
                          SELECT pk_col
                          FROM dup_analysis
                          WHERE rn > 1 AND dup_cnt > 1
                      )
                ';
                EXECUTE IMMEDIATE v_sql;

            ELSE
                RETURN OBJECT_CONSTRUCT(
                    'STATUS', 'FAILED',
                    'ERROR', 'Unknown validation type: ' || v_validation_type,
                    'ROWS_VALIDATED', v_check_count::VARCHAR
                );
        END CASE;
    END FOR;

    -- ============= COLLECT STATISTICS (Single Query) =============
    v_sql := '
        SELECT
            (SELECT COUNT(*) FROM ' || P_STAGE_TABLE || ') as total_rows,
            (SELECT COUNT(*) FROM ' || P_STAGE_TABLE || ' WHERE IS_VALID = TRUE) as valid_rows,
            (SELECT COUNT(*) FROM ' || P_STAGE_TABLE || ' WHERE IS_VALID = FALSE) as invalid_rows,
            (SELECT COUNT(*) FROM ' || P_ERROR_TABLE || ' WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || ') as total_errors,
            (SELECT COUNT(*) FROM ' || P_ERROR_TABLE || ' WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || ' AND VALIDATION_TYPE = ''MANDATORY_CHECK'') as mandatory_errors,
            (SELECT COUNT(*) FROM ' || P_ERROR_TABLE || ' WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || ' AND VALIDATION_TYPE = ''VALUE_CHECK'') as value_errors,
            (SELECT COUNT(*) FROM ' || P_ERROR_TABLE || ' WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || ' AND VALIDATION_TYPE = ''LOOKUP_CHECK'') as lookup_errors,
            (SELECT COUNT(*) FROM ' || P_ERROR_TABLE || ' WHERE INGEST_RUN_ID = ' || P_INGEST_RUN_ID || ' AND VALIDATION_TYPE = ''DUPLICATE_ALLOW_ONE_CHECK'') as duplicate_errors
    ';

    v_stats_result := (EXECUTE IMMEDIATE :v_sql);
    FOR rec IN v_stats_result DO
        v_total_rows := rec.TOTAL_ROWS;
        v_valid_rows := rec.VALID_ROWS;
        v_invalid_rows := rec.INVALID_ROWS;
        v_total_errors := rec.TOTAL_ERRORS;
        v_mandatory_errors := rec.MANDATORY_ERRORS;
        v_value_errors := rec.VALUE_ERRORS;
        v_lookup_errors := rec.LOOKUP_ERRORS;
        v_duplicate_errors := rec.DUPLICATE_ERRORS;
    END FOR;

    v_end_time := CURRENT_TIMESTAMP();
    v_duration_seconds := DATEDIFF(SECOND, v_start_time, v_end_time);

    -- Calculate percentages
    IF (v_total_rows > 0) THEN
        v_valid_pct := ROUND((v_valid_rows / v_total_rows) * 100, 2);
        v_invalid_pct := ROUND((v_invalid_rows / v_total_rows) * 100, 2);
        v_error_rate_pct := ROUND((v_total_errors / v_total_rows) * 100, 2);
    END IF;

    -- Insert statistics
    INSERT INTO COMMON.DQ_RUN_STATISTICS (
        INGEST_RUN_ID,
        VALIDATE_TABLE,
        STAGE_TABLE,
        ERROR_TABLE,
        TOTAL_ROWS_PROCESSED,
        TOTAL_VALID_ROWS,
        TOTAL_INVALID_ROWS,
        TOTAL_ERROR_COUNT,
        VALIDATION_CHECKS_EXECUTED,
        VALID_ROWS_PERCENTAGE,
        INVALID_ROWS_PERCENTAGE,
        ERROR_RATE_PERCENTAGE,
        VALIDATION_TYPE_STATS,
        EXECUTION_STATUS,
        EXECUTION_ERROR_MESSAGE,
        EXECUTION_START_TIME,
        EXECUTION_END_TIME,
        EXECUTION_DURATION_SECONDS
    )
    SELECT
        :P_INGEST_RUN_ID,
        :P_VALIDATE_TABLE,
        :P_STAGE_TABLE,
        :P_ERROR_TABLE,
        :v_total_rows,
        :v_valid_rows,
        :v_invalid_rows,
        :v_total_errors,
        :v_check_count,
        :v_valid_pct,
        :v_invalid_pct,
        :v_error_rate_pct,
        OBJECT_CONSTRUCT(
            'MANDATORY_CHECK', OBJECT_CONSTRUCT(
                'checks_executed', :v_mandatory_check_count,
                'errors_found', :v_mandatory_errors
            ),
            'VALUE_CHECK', OBJECT_CONSTRUCT(
                'checks_executed', :v_value_check_count,
                'errors_found', :v_value_errors
            ),
            'LOOKUP_CHECK', OBJECT_CONSTRUCT(
                'checks_executed', :v_lookup_check_count,
                'errors_found', :v_lookup_errors
            ),
            'DUPLICATE_ALLOW_ONE_CHECK', OBJECT_CONSTRUCT(
                'checks_executed', :v_duplicate_check_count,
                'errors_found', :v_duplicate_errors
            )
        ),
        'SUCCESS',
        'NONE',
        :v_start_time,
        :v_end_time,
        :v_duration_seconds;

    RETURN OBJECT_CONSTRUCT(
        'STATUS', 'SUCCESS',
        'ERROR', 'NONE',
        'ROWS_VALIDATED', v_check_count::VARCHAR,
        'MESSAGE', 'validation checks executed',
        'TOTAL_ROWS', v_total_rows,
        'VALID_ROWS', v_valid_rows,
        'INVALID_ROWS', v_invalid_rows,
        'TOTAL_ERRORS', v_total_errors
    );

EXCEPTION
    WHEN OTHER THEN
        v_end_time := CURRENT_TIMESTAMP();
        v_duration_seconds := DATEDIFF(SECOND, v_start_time, v_end_time);
        V_ERROR_MESSAGE := SQLERRM;

        INSERT INTO COMMON.DQ_RUN_STATISTICS (
            INGEST_RUN_ID,
            VALIDATE_TABLE,
            STAGE_TABLE,
            ERROR_TABLE,
            VALIDATION_CHECKS_EXECUTED,
            EXECUTION_STATUS,
            EXECUTION_ERROR_MESSAGE,
            EXECUTION_START_TIME,
            EXECUTION_END_TIME,
            EXECUTION_DURATION_SECONDS
        )
        VALUES (
            :P_INGEST_RUN_ID,
            :P_VALIDATE_TABLE,
            :P_STAGE_TABLE,
            :P_ERROR_TABLE,
            :v_check_count,
            'FAILED',
            :V_ERROR_MESSAGE,
            :v_start_time,
            :v_end_time,
            :v_duration_seconds
        );

        RETURN OBJECT_CONSTRUCT(
            'STATUS', 'FAILED',
            'ERROR', SQLERRM,
            'ROWS_VALIDATED', v_check_count::VARCHAR,
            'MESSAGE', 'validation checks executed'
        );
END;
$$;
-- =====================================================
-- DQ CHECK VALIDATION
-- =====================================================
            -- WHEN 'VALUE_RANGE' THEN
            --     -- Value range check using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors,
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;

            -- WHEN 'PATTERN' THEN
            --     -- Pattern validation using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors,
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE ' || v_validation_column || ' IS NOT NULL
            --                AND NOT (' || v_validation_query || ')
            --                AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;

            -- WHEN 'DUPLICATE' THEN
            --     -- Duplicate check using validation_column
            --     v_sql := 'UPDATE ' || p_stage_table || ' stg
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors,
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE is_valid = TRUE
            --                AND ' || v_validation_column || ' IN (
            --                    SELECT ' || v_validation_column || '
            --                    FROM ' || p_stage_table || '
            --                    WHERE ' || v_validation_column || ' IS NOT NULL
            --                    GROUP BY ' || v_validation_column || '
            --                    HAVING COUNT(*) > 1
            --                )';
            --     EXECUTE IMMEDIATE v_sql;

            -- WHEN 'COMPLETENESS' THEN
            --     -- Completeness check using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors,
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || COALESCE(v_validation_column, 'Multiple') || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;

            -- WHEN 'CONSISTENCY' THEN
            --     -- Consistency check using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors,
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;

            -- WHEN 'FRESHNESS' THEN
            --     -- Freshness check using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors,
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''threshold_days'', ''' || v_threshold_value || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;

            -- WHEN 'VOLUME' THEN
            --     -- Volume check - table level validation
            --     v_validation_query := 'SELECT CASE WHEN ' || v_validation_query || ' THEN 1 ELSE 0 END FROM ' || p_stage_table || ' LIMIT 1';
            --     EXECUTE IMMEDIATE v_validation_query INTO v_count;

            --     IF v_count = 0 THEN
            --         -- Log volume check failure (not row-level)
            --         INSERT INTO common.load_error (
            --             ingest_run_id, source_table, error_type, error_column,
            --             error_description, error_severity
            --         ) VALUES (
            --             p_ingest_run_id, p_bronze_table, v_validation_type, 'TABLE_LEVEL',
            --             v_validation_error_msg || ' (Threshold: ' || v_threshold_value || ')', v_severity
            --         );
            --     END IF;

            -- WHEN 'STATISTICAL' THEN
            --     -- Statistical outlier detection using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors,
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || v_validation_column || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || ''',
            --                          ''threshold'', ''' || v_threshold_value || ''',
            --                          ''value'', TO_VARCHAR(' || v_validation_column || ')
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;

            -- WHEN 'CUSTOM_QUERY' THEN
            --     -- Custom business rule using validation_query
            --     v_sql := 'UPDATE ' || p_stage_table || '
            --              SET is_valid = FALSE,
            --                  dq_errors = ARRAY_APPEND(dq_errors,
            --                      OBJECT_CONSTRUCT(
            --                          ''check_id'', ' || v_dq_config_id || ',
            --                          ''check_name'', ''' || v_validation_name || ''',
            --                          ''type'', ''' || v_validation_type || ''',
            --                          ''column'', ''' || COALESCE(v_validation_column, 'Multiple') || ''',
            --                          ''message'', ''' || v_validation_error_msg || ''',
            --                          ''severity'', ''' || v_severity || '''
            --                      ))
            --              WHERE NOT (' || v_validation_query || ') AND is_valid = TRUE';
            --     EXECUTE IMMEDIATE v_sql;