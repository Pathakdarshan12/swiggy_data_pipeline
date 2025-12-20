-- ====================================================================================================
-- CUSTOMER
-- ====================================================================================================
-- CHANGE CONTEXT
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE RESTAURANT_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_BRZ (
    CUSTOMER_BRZ_ID INTEGER PRIMARY KEY AUTOINCREMENT,                    -- PRIMARY KEY AS TEXT
    NAME STRING,                          -- NAME AS TEXT
    MOBILE STRING WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                        -- MOBILE NUMBER AS TEXT
    EMAIL STRING WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),                         -- EMAIL AS TEXT
    LOGIN_BY_USING STRING,                  -- LOGIN METHOD AS TEXT
    GENDER STRING WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                        -- GENDER AS TEXT
    DOB STRING WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                           -- DATE OF BIRTH AS TEXT
    ANNIVERSARY STRING,                   -- ANNIVERSARY AS TEXT
    PREFERENCES STRING,                   -- PREFERENCES AS TEXT
    BATCH_ID STRING(36),
    CREATED_AT STRING                   -- CREATED DATE AS TEXT
)
COMMENT = 'THIS IS THE CUSTOMER STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE TEXT DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMER_SLV (
    CUSTOMER_SLV_ID INTEGER AUTOINCREMENT PRIMARY KEY,                -- AUTO-INCREMENTED PRIMARY KEY
    NAME STRING(100),                                   -- CUSTOMER NAME
    MOBILE STRING(15)  WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- MOBILE NUMBER, ACCOMMODATING INTERNATIONAL FORMAT
    EMAIL STRING(100) WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),                                           -- EMAIL
    LOGIN_BY_USING STRING(50),                                   -- METHOD OF LOGIN (E.G., SOCIAL, GOOGLE, ETC.)
    GENDER STRING(10)  WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- GENDER
    DOB DATE WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                                    -- DATE OF BIRTH IN DATE FORMAT
    ANNIVERSARY DATE,                                            -- ANNIVERSARY IN DATE FORMAT
    PREFERENCES STRING,                                          -- CUSTOMER PREFERENCES
    BATCH_ID STRING(36),
    CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP,           -- RECORD CREATION TIMESTAMP
    UPDATED_AT TIMESTAMP_TZ(9)                                    -- RECORD MODIFICATION TIMESTAMP, ALLOWS NULL IF NOT MODIFIED
)
COMMENT = 'CUSTOMER ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_DIM
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_CUSTOMER (
    CUSTOMER_ID INTEGER PRIMARY KEY AUTOINCREMENT,                                 -- NATURAL KEY FOR THE CUSTOMER
    NAME STRING(100) NOT NULL,                                   -- CUSTOMER NAME
    MOBILE STRING(15) WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- MOBILE NUMBER
    EMAIL STRING(100) WITH TAG (COMMON.PII_POLICY_TAG = 'EMAIL'),                                           -- EMAIL
    LOGIN_BY_USING STRING(50),                                   -- METHOD OF LOGIN
    GENDER STRING(10) WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                           -- GENDER
    DOB DATE WITH TAG (COMMON.PII_POLICY_TAG = 'PII'),                                                    -- DATE OF BIRTH
    ANNIVERSARY DATE,                                            -- ANNIVERSARY
    PREFERENCES STRING,                                          -- PREFERENCES
    STATUS VARCHAR(10) DEFAULT 'ACTIVE',                     -- Record status (ACTIVE/INACTIVE)
    EFF_START_DT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),-- Effective start date
    EFF_END_DT TIMESTAMP_TZ(9) DEFAULT '9999-12-31 23:59:59'::TIMESTAMP_TZ, -- Effective end date
    BATCH_ID VARCHAR(36) NOT NULL,                           -- Batch identifier
    CREATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(), -- Record creation timestamp
    UPDATED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP() -- Record update timestamp
)
COMMENT = 'CUSTOMER DIMENSION TABLE WITH SCD TYPE 2 HANDLING FOR HISTORICAL TRACKING.';

-- =====================================================
-- PROCEDURE 1: STAGE TO BRONZE
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_CUSTOMER_STAGE_TO_BRONZE(
    P_BATCH_ID STRING,
    P_FILE_PATH STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
BEGIN
    -- CREATE TEMP TABLE
    CREATE OR REPLACE TEMPORARY TABLE TEMP_CUSTOMER_LOAD(
        NAME STRING,
        MOBILE STRING,
        EMAIL STRING,
        LOGIN_BY_USING STRING,
        GENDER STRING,
        DOB STRING,
        ANNIVERSARY STRING,
        PREFERENCES STRING
    );

    -- Copy data from stage to temp table
    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_CUSTOMER_LOAD (
            NAME, MOBILE, EMAIL, LOGIN_BY_USING, GENDER, DOB, ANNIVERSARY, PREFERENCES
        )
        FROM (
            SELECT
                $1::STRING AS NAME,
                $2::STRING AS MOBILE,
                $3::STRING AS EMAIL,
                $4::STRING AS LOGIN_BY_USING,
                $5::STRING AS GENDER,
                $6::STRING AS DOB,
                $7::STRING AS ANNIVERSARY,
                $8::STRING AS PREFERENCES
            FROM ' || P_FILE_PATH || '
        )
        FILE_FORMAT = (FORMAT_NAME = ''BRONZE.CSV_FILE_FORMAT'')
        ON_ERROR = ABORT_STATEMENT
    ';

    -- Insert into bronze table
    INSERT INTO BRONZE.CUSTOMER_BRZ (
        NAME, MOBILE, EMAIL, LOGIN_BY_USING, GENDER, DOB,
        ANNIVERSARY, PREFERENCES, BATCH_ID, CREATED_AT
    )
    SELECT
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES,
        :P_BATCH_ID,
        CURRENT_TIMESTAMP()
    FROM TEMP_CUSTOMER_LOAD;

    -- Get row count
    SELECT COUNT(*) INTO :v_rows_inserted FROM TEMP_CUSTOMER_LOAD;

    -- Drop temp table
    DROP TABLE IF EXISTS TEMP_CUSTOMER_LOAD;

    RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            v_rows_inserted
        );

EXCEPTION
    WHEN OTHER THEN
        RETURN ARRAY_CONSTRUCT(
            'FAILED',
            v_rows_inserted
        );
END;
$$;

-- =====================================================
-- PROCEDURE 2: BRONZE TO SILVER
-- =====================================================
CREATE OR REPLACE PROCEDURE SILVER.SP_CUSTOMER_BRONZE_TO_SILVER(
    P_BATCH_ID STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
BEGIN
    -- Merge data from bronze to silver (no SCD2 at silver layer)
    MERGE INTO SILVER.CUSTOMER_SLV AS TGT
    USING (
        SELECT
            CUSTOMER_BRZ_ID,
            NAME,
            MOBILE,
            EMAIL,
            LOGIN_BY_USING,
            GENDER,
            TRY_CAST(DOB AS DATE) AS DOB,
            TRY_CAST(ANNIVERSARY AS DATE) AS ANNIVERSARY,
            PREFERENCES,
            BATCH_ID,
            CREATED_AT,
            CURRENT_TIMESTAMP() AS UPDATED_AT
        FROM BRONZE.CUSTOMER_BRZ
        WHERE BATCH_ID = :P_BATCH_ID
    ) AS SRC
    ON TGT.CUSTOMER_SLV_ID = SRC.CUSTOMER_BRZ_ID

    WHEN MATCHED THEN
        UPDATE SET
            TGT.NAME = SRC.NAME,
            TGT.MOBILE = SRC.MOBILE,
            TGT.EMAIL = SRC.EMAIL,
            TGT.LOGIN_BY_USING = SRC.LOGIN_BY_USING,
            TGT.GENDER = SRC.GENDER,
            TGT.DOB = SRC.DOB,
            TGT.ANNIVERSARY = SRC.ANNIVERSARY,
            TGT.PREFERENCES = SRC.PREFERENCES,
            TGT.BATCH_ID = SRC.BATCH_ID,
            TGT.UPDATED_AT = SRC.UPDATED_AT

    WHEN NOT MATCHED THEN
        INSERT (
            NAME, MOBILE, EMAIL, LOGIN_BY_USING, GENDER, DOB,
            ANNIVERSARY, PREFERENCES, BATCH_ID, CREATED_AT, UPDATED_AT
        )
        VALUES (
            SRC.NAME, SRC.MOBILE, SRC.EMAIL, SRC.LOGIN_BY_USING, SRC.GENDER,
            SRC.DOB, SRC.ANNIVERSARY, SRC.PREFERENCES, SRC.BATCH_ID,
            SRC.CREATED_AT, SRC.UPDATED_AT
        );

    -- Get merge statistics
    V_ROWS_INSERTED := (SELECT COUNT(*) FROM SILVER.CUSTOMER_SLV WHERE BATCH_ID = :P_BATCH_ID);

    RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            v_rows_inserted,
            v_rows_updated
        );

EXCEPTION
    WHEN OTHER THEN
        RETURN ARRAY_CONSTRUCT(
            'FAILED',
            v_rows_inserted,
            v_rows_updated
        );
END;
$$;

-- =====================================================
-- PROCEDURE 3: SILVER TO GOLD (WITH SCD2)
-- =====================================================
CREATE OR REPLACE PROCEDURE GOLD.SP_CUSTOMER_SILVER_TO_GOLD(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_CURRENT_TIMESTAMP TIMESTAMP_TZ;
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
    V_ROWS_DELETED INTEGER DEFAULT 0;
BEGIN
    V_CURRENT_TIMESTAMP := CURRENT_TIMESTAMP();

    -- Step 1: Expire records that have changed (set EFF_END_DT and STATUS = 'INACTIVE')
    UPDATE GOLD.DIM_CUSTOMER AS TGT
    SET
        EFF_END_DT = :V_CURRENT_TIMESTAMP,
        STATUS = 'INACTIVE',
        UPDATED_AT = :V_CURRENT_TIMESTAMP
    FROM (
        SELECT
            SRC.CUSTOMER_SLV_ID,
            SRC.NAME,
            SRC.MOBILE,
            SRC.EMAIL,
            SRC.LOGIN_BY_USING,
            SRC.GENDER,
            SRC.DOB,
            SRC.ANNIVERSARY,
            SRC.PREFERENCES
        FROM SILVER.CUSTOMER_SLV SRC
        WHERE SRC.BATCH_ID = :P_BATCH_ID
    ) AS SRC
    WHERE TGT.CUSTOMER_ID = SRC.CUSTOMER_SLV_ID
        AND TGT.STATUS = 'ACTIVE'
        AND (
            COALESCE(TGT.NAME, '') != COALESCE(SRC.NAME, '')
            OR COALESCE(TGT.MOBILE, '') != COALESCE(SRC.MOBILE, '')
            OR COALESCE(TGT.EMAIL, '') != COALESCE(SRC.EMAIL, '')
            OR COALESCE(TGT.LOGIN_BY_USING, '') != COALESCE(SRC.LOGIN_BY_USING, '')
            OR COALESCE(TGT.GENDER, '') != COALESCE(SRC.GENDER, '')
            OR COALESCE(TGT.DOB, '1900-01-01'::DATE) != COALESCE(SRC.DOB, '1900-01-01'::DATE)
            OR COALESCE(TGT.ANNIVERSARY, '1900-01-01'::DATE) != COALESCE(SRC.ANNIVERSARY, '1900-01-01'::DATE)
            OR COALESCE(TGT.PREFERENCES, '') != COALESCE(SRC.PREFERENCES, '')
        );

    -- Get count of expired rows
    V_ROWS_UPDATED := SQLROWCOUNT;

    -- Step 2: Insert new records (both brand new and changed records)
    INSERT INTO GOLD.DIM_CUSTOMER (
        CUSTOMER_ID, NAME, MOBILE, EMAIL, LOGIN_BY_USING, GENDER, DOB,
        ANNIVERSARY, PREFERENCES, STATUS, EFF_START_DT, EFF_END_DT,
        BATCH_ID, CREATED_AT, UPDATED_AT
    )
    SELECT
        SRC.CUSTOMER_SLV_ID AS CUSTOMER_ID,
        SRC.NAME,
        SRC.MOBILE,
        SRC.EMAIL,
        SRC.LOGIN_BY_USING,
        SRC.GENDER,
        SRC.DOB,
        SRC.ANNIVERSARY,
        SRC.PREFERENCES,
        'ACTIVE' AS STATUS,
        :V_CURRENT_TIMESTAMP AS EFF_START_DT,
        CAST('9999-12-31 23:59:59' AS TIMESTAMP_TZ(9)) AS EFF_END_DT,
        SRC.BATCH_ID,
        :V_CURRENT_TIMESTAMP AS CREATED_AT,
        :V_CURRENT_TIMESTAMP AS UPDATED_AT
    FROM SILVER.CUSTOMER_SLV SRC
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND NOT EXISTS (
            SELECT 1
            FROM GOLD.DIM_CUSTOMER TGT
            WHERE TGT.CUSTOMER_ID = SRC.CUSTOMER_SLV_ID
                AND TGT.STATUS = 'ACTIVE'
                AND COALESCE(TGT.NAME, '') = COALESCE(SRC.NAME, '')
                AND COALESCE(TGT.MOBILE, '') = COALESCE(SRC.MOBILE, '')
                AND COALESCE(TGT.EMAIL, '') = COALESCE(SRC.EMAIL, '')
                AND COALESCE(TGT.LOGIN_BY_USING, '') = COALESCE(SRC.LOGIN_BY_USING, '')
                AND COALESCE(TGT.GENDER, '') = COALESCE(SRC.GENDER, '')
                AND COALESCE(TGT.DOB, '1900-01-01'::DATE) = COALESCE(SRC.DOB, '1900-01-01'::DATE)
                AND COALESCE(TGT.ANNIVERSARY, '1900-01-01'::DATE) = COALESCE(SRC.ANNIVERSARY, '1900-01-01'::DATE)
                AND COALESCE(TGT.PREFERENCES, '') = COALESCE(SRC.PREFERENCES, '')
        );

    -- Get count of inserted rows
    V_ROWS_INSERTED := SQLROWCOUNT;

    RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            v_rows_inserted,
            v_rows_updated,
            v_rows_deleted
        );

EXCEPTION
    WHEN OTHER THEN
        RETURN ARRAY_CONSTRUCT(
            'FAILED',
            v_rows_inserted,
            v_rows_updated,
            v_rows_deleted
        );
END;
$$;

-- =====================================================
-- EXAMPLE USAGE
-- =====================================================
-- Query to see history of a specific customer
-- SELECT * FROM GOLD.DIM_CUSTOMER
-- WHERE CUSTOMER_ID = 123
-- ORDER BY EFF_START_DT;

-- -- Query to see customers with email changes
-- SELECT
--     CUSTOMER_ID,
--     NAME,
--     EMAIL,
--     MOBILE,
--     STATUS,
--     EFF_START_DT,
--     EFF_END_DT
-- FROM GOLD.DIM_CUSTOMER
-- WHERE CUSTOMER_ID IN (
--     SELECT CUSTOMER_ID
--     FROM GOLD.DIM_CUSTOMER
--     GROUP BY CUSTOMER_ID
--     HAVING COUNT(*) > 1
-- )
-- ORDER BY CUSTOMER_ID, EFF_START_DT;

-- -- Query to find customers whose preferences changed
-- SELECT
--     CUSTOMER_ID,
--     NAME,
--     PREFERENCES,
--     EFF_START_DT,
--     EFF_END_DT,
--     STATUS
-- FROM GOLD.DIM_CUSTOMER
-- WHERE STATUS = 'INACTIVE'
--     AND EFF_END_DT != '9999-12-31 23:59:59'::TIMESTAMP_TZ
-- ORDER BY EFF_END_DT DESC;

-- -- Query to get current customer count
-- SELECT COUNT(*) AS ACTIVE_CUSTOMERS
-- FROM GOLD.DIM_CUSTOMER
-- WHERE STATUS = 'ACTIVE';

-- -- Query to audit customer changes by date
-- SELECT
--     DATE(EFF_START_DT) AS CHANGE_DATE,
--     COUNT(*) AS NUMBER_OF_CHANGES
-- FROM GOLD.DIM_CUSTOMER
-- WHERE STATUS = 'INACTIVE'
-- GROUP BY DATE(EFF_START_DT)
-- ORDER BY CHANGE_DATE DESC;