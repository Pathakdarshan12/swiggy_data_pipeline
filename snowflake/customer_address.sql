-- ====================================================================================================
-- CUSTOMER_ADDRESS
-- ====================================================================================================
-- CHANGE CONSTRING(100)
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;
-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_ADDRESS
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMER_ADDRESS_BRZ (
    ADDRESS_BRZ_ID STRING(100),                    -- PRIMARY KEY AS STRING(100)
    CUSTOMER_ID STRING(100) COMMENT 'CUSTOMER FK (SOURCE DATA)',                   -- FOREIGN KEY REFERENCE AS STRING(100) (NO CONSTRAINT IN SNOWFLAKE)
    FLAT_NO STRING(100),                       -- FLAT NUMBER AS STRING(100)
    HOUSE_NO STRING(100),                      -- HOUSE NUMBER AS STRING(100)
    FLOOR_NO STRING(100),                        -- FLOOR_NO_NO AS STRING(100)
    BUILDING STRING(100),                     -- BUILDING NAME AS STRING(100)
    LANDMARK STRING(100),                     -- LANDMARK AS STRING(100)
    LOCALITY STRING(100),                     -- LOCALITY AS STRING(100)
    CITY STRING(100),                          -- CITY AS STRING(100)
    STATE STRING(100),                         -- STATE AS STRING(100)
    ZIPCODE STRING(100),                       -- ZIPCODE AS STRING(100)
    COORDINATES STRING(100),                  -- COORDINATES AS STRING(100)
    PRIMARYFLAG STRING(100),                  -- PRIMARY FLAG AS STRING(100)
    ADDRESSTYPE STRING(100),                  -- ADDRESS TYPE AS STRING(100)
    CREATED_AT TIMESTAMP_TZ,                  -- CREATED DATE AS STRING(100)
    UPDATED_AT TIMESTAMP_TZ,                 -- MODIFIED DATE AS STRING(100)
    BATCH_ID STRING(36)
    )
COMMENT = 'THIS IS THE CUSTOMER ADDRESS STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE STRING(100) DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';

-- ----------------------------------------------------------------------------------------------------
-- CREATE CUSTOMER_ADDRESS_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.CUSTOMER_ADDRESS_SLV (
    CUSTOMER_ADDRESS_SK INTEGER PRIMARY KEY AUTOINCREMENT,                -- AUTO-INCREMENTED PRIMARY KEY
    ADDRESS_SLV_ID INTEGER COMMENT 'PRIMARY KEY (SOURCE DATA)',                 -- PRIMARY KEY AS STRING(100)
    CUSTOMER_ID_FK INTEGER COMMENT 'CUSTOMER FK (SOURCE DATA)',                -- FOREIGN KEY REFERENCE AS STRING(100) (NO CONSTRAINT IN SNOWFLAKE)
    FLAT_NO STRING(100),
    HOUSE_NO STRING(100),
    FLOOR_NO STRING(100),
    BUILDING STRING(100),
    LANDMARK STRING(100),
    LOCALITY STRING(100),
    CITY STRING(100),
    STATE STRING(100),
    ZIPCODE STRING(100),
    COORDINATES STRING(100),
    PRIMARY_FLAG STRING(100),
    ADDRESS_TYPE STRING(100),
    CREATED_AT TIMESTAMP_TZ,         -- CREATED DATE AS TIMESTAMP WITH TIME ZONE
    UPDATED_AT TIMESTAMP_TZ,        -- MODIFIED DATE AS TIMESTAMP WITH TIME ZONE
    BATCH_ID STRING(36)
)
COMMENT = 'CUSTOMER ADDRESS ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DIM_CUSTOMER_ADDRESS
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_CUSTOMER_ADDRESS (
    CUSTOMER_ADDRESS_HK INTEGER PRIMARY KEY COMMENT 'CUSTOMER ADDRESS HK (EDW)',        -- SURROGATE KEY (HASH KEY)
    ADDRESS_ID INTEGER COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',                                -- ORIGINAL PRIMARY KEY
    CUSTOMER_ID_FK STRING(100) COMMENT 'CUSTOMER FK (SOURCE SYSTEM)',                            -- SURROGATE KEY FROM CUSTOMER DIMENSION (FOREIGN KEY)
    FLAT_NO STRING(100),                                -- FLAT NUMBER
    HOUSE_NO STRING(100),                               -- HOUSE NUMBER
    FLOOR_NO STRING(100),                                  -- FLOOR_NO_NO
    BUILDING STRING(100),                               -- BUILDING NAME
    LANDMARK STRING(100),                               -- LANDMARK
    LOCALITY STRING(100),                               -- LOCALITY
    CITY STRING(100),                                   -- CITY
    STATE STRING(100),                                  -- STATE
    ZIPCODE STRING(100),                                -- ZIPCODE
    COORDINATES STRING(100),                            -- GEO-COORDINATES
    PRIMARY_FLAG STRING(100),                           -- WHETHER IT'S THE PRIMARY ADDRESS
    ADDRESS_TYPE STRING(100),                           -- TYPE OF ADDRESS (E.G., HOME, OFFICE)
    -- SCD2 COLUMNS
    EFF_START_DATE TIMESTAMP_TZ,                                 -- EFFECTIVE START DATE
    EFF_END_DATE TIMESTAMP_TZ,                                   -- EFFECTIVE END DATE (NULL IF ACTIVE)
    STATUS BOOLEAN,                                      -- FLAG TO INDICATE THE CURRENT RECORD
    BATCH_ID STRING(36),
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ
);


-- =====================================================
-- PROCEDURE 1: STAGE TO BRONZE
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_CUSTOMER_ADDRESS_STAGE_TO_BRONZE(
    P_BATCH_ID STRING,
    P_FILE_PATH STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_LOADED INTEGER DEFAULT 0;
BEGIN
    -- CREATE TEMP TABLE
    CREATE OR REPLACE TEMPORARY TABLE TEMP_CUSTOMER_ADDRESS_LOAD(
        ADDRESS_BRZ_ID STRING(100),
        CUSTOMER_ID STRING(100),
        FLAT_NO STRING(100),
        HOUSE_NO STRING(100),
        FLOOR_NO STRING(100),
        BUILDING STRING(100),
        LANDMARK STRING(100),
        LOCALITY STRING(100),
        CITY STRING(100),
        STATE STRING(100),
        ZIPCODE STRING(100),
        COORDINATES STRING(100),
        PRIMARYFLAG STRING(100),
        ADDRESSTYPE STRING(100)
    );

    -- Copy data from stage to temp table
    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_CUSTOMER_ADDRESS_LOAD (
            ADDRESS_BRZ_ID, CUSTOMER_ID, FLAT_NO, HOUSE_NO, FLOOR_NO, BUILDING,
            LANDMARK, LOCALITY, CITY, STATE, ZIPCODE, COORDINATES, PRIMARYFLAG, ADDRESSTYPE
        )
        FROM (
            SELECT
                $1::STRING AS ADDRESS_BRZ_ID,
                $2::STRING AS CUSTOMER_ID,
                $3::STRING AS FLAT_NO,
                $4::STRING AS HOUSE_NO,
                $5::STRING AS FLOOR_NO,
                $6::STRING AS BUILDING,
                $7::STRING AS LANDMARK,
                $8::STRING AS LOCALITY,
                $9::STRING AS CITY,
                $10::STRING AS STATE,
                $11::STRING AS ZIPCODE,
                $12::STRING AS COORDINATES,
                $13::STRING AS PRIMARYFLAG,
                $14::STRING AS ADDRESSTYPE
            FROM ' || P_FILE_PATH || '
        )
        FILE_FORMAT = (FORMAT_NAME = ''BRONZE.CSV_FILE_FORMAT'')
        ON_ERROR = ABORT_STATEMENT
    ';

    -- Insert into bronze table
    INSERT INTO BRONZE.CUSTOMER_ADDRESS_BRZ (
        ADDRESS_BRZ_ID, CUSTOMER_ID, FLAT_NO, HOUSE_NO, FLOOR_NO, BUILDING,
        LANDMARK, LOCALITY, CITY, STATE, ZIPCODE, COORDINATES, PRIMARYFLAG,
        ADDRESSTYPE, CREATED_AT, UPDATED_AT, BATCH_ID
    )
    SELECT
        ADDRESS_BRZ_ID,
        CUSTOMER_ID,
        FLAT_NO,
        HOUSE_NO,
        FLOOR_NO,
        BUILDING,
        LANDMARK,
        LOCALITY,
        CITY,
        STATE,
        ZIPCODE,
        COORDINATES,
        PRIMARYFLAG,
        ADDRESSTYPE,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :P_BATCH_ID
    FROM TEMP_CUSTOMER_ADDRESS_LOAD;

    -- Get row count
    SELECT COUNT(*) INTO :V_ROWS_LOADED FROM TEMP_CUSTOMER_ADDRESS_LOAD;

    -- Drop temp table
    DROP TABLE IF EXISTS TEMP_CUSTOMER_ADDRESS_LOAD;

    RETURN 'Successfully loaded ' || :V_ROWS_LOADED || ' rows with batch_id: ' || :P_BATCH_ID;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;

-- =====================================================
-- PROCEDURE 2: BRONZE TO SILVER
-- =====================================================
CREATE OR REPLACE PROCEDURE SILVER.SP_CUSTOMER_ADDRESS_BRONZE_TO_SILVER(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_BATCH_DUP_CNT  INTEGER DEFAULT 0;
    V_STAGE_DUP_CNT  INTEGER DEFAULT 0;
    V_ROWS_MERGED    INTEGER DEFAULT 0;
BEGIN
    /* ===================================================
       1. LOG DUPLICATES WITHIN SAME BATCH
    =================================================== */
    INSERT INTO COMMON.LOAD_ERROR (
        SOURCE_TABLE,
        RECORD_ID,
        BATCH_ID,
        ERROR_TYPE,
        ERROR_MESSAGE
    )
    SELECT
        'BRONZE.CUSTOMER_ADDRESS_BRZ',
        TRY_CAST(ADDRESS_BRZ_ID AS INT),
        BATCH_ID,
        'BATCH_DUPLICATE',
        'Duplicate ADDRESS_BRZ_ID found in same batch'
    FROM BRONZE.CUSTOMER_ADDRESS_BRZ
    WHERE BATCH_ID = :P_BATCH_ID
    QUALIFY COUNT(*) OVER (
        PARTITION BY ADDRESS_BRZ_ID, BATCH_ID
    ) > 1;

    V_BATCH_DUP_CNT := SQLROWCOUNT;


    /* ===================================================
       2. LOG DUPLICATES ALREADY IN SILVER (STAGE DUP)
    =================================================== */
    INSERT INTO COMMON.LOAD_ERROR (
        SOURCE_TABLE,
        RECORD_ID,
        BATCH_ID,
        ERROR_TYPE,
        ERROR_MESSAGE
    )
    SELECT
        'SILVER.CUSTOMER_ADDRESS_SLV',
        TRY_CAST(B.ADDRESS_BRZ_ID AS INT),
        B.BATCH_ID,
        'STAGE_DUPLICATE',
        'ADDRESS already exists in SILVER table'
    FROM BRONZE.CUSTOMER_ADDRESS_BRZ B
    INNER JOIN SILVER.CUSTOMER_ADDRESS_SLV S
        ON TRY_CAST(B.ADDRESS_BRZ_ID AS INT) = S.ADDRESS_SLV_ID
    WHERE B.BATCH_ID = :P_BATCH_ID;

    V_STAGE_DUP_CNT := SQLROWCOUNT;


    /* ===================================================
       3. MERGE ONLY CLEAN RECORDS
          - NOT batch duplicates
          - NOT existing in silver
    =================================================== */
    MERGE INTO SILVER.CUSTOMER_ADDRESS_SLV AS TGT
    USING (
        SELECT *
        FROM (
            SELECT
                TRY_CAST(B.ADDRESS_BRZ_ID AS INT) AS ADDRESS_SLV_ID,
                TRY_CAST(B.CUSTOMER_ID AS INT) AS CUSTOMER_ID_FK,
                B.FLAT_NO,
                B.HOUSE_NO,
                B.FLOOR_NO,
                B.BUILDING,
                B.LANDMARK,
                B.LOCALITY,
                B.CITY,
                B.STATE,
                B.ZIPCODE,
                B.COORDINATES,
                B.PRIMARYFLAG AS PRIMARY_FLAG,
                B.ADDRESSTYPE AS ADDRESS_TYPE,
                B.CREATED_AT,
                B.UPDATED_AT,
                B.BATCH_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY B.ADDRESS_BRZ_ID
                    ORDER BY B.UPDATED_AT DESC
                ) AS RN
            FROM BRONZE.CUSTOMER_ADDRESS_BRZ B
            WHERE B.BATCH_ID = :P_BATCH_ID
              -- exclude stage duplicates
              AND NOT EXISTS (
                    SELECT 1
                    FROM SILVER.CUSTOMER_ADDRESS_SLV S
                    WHERE S.ADDRESS_SLV_ID = TRY_CAST(B.ADDRESS_BRZ_ID AS INT)
              )
        )
        WHERE RN = 1
    ) AS SRC
    ON TGT.ADDRESS_SLV_ID = SRC.ADDRESS_SLV_ID

    WHEN NOT MATCHED THEN
        INSERT (
            ADDRESS_SLV_ID, CUSTOMER_ID_FK, FLAT_NO, HOUSE_NO,
            FLOOR_NO, BUILDING, LANDMARK, LOCALITY, CITY, STATE,
            ZIPCODE, COORDINATES, PRIMARY_FLAG, ADDRESS_TYPE,
            CREATED_AT, UPDATED_AT, BATCH_ID
        )
        VALUES (
            SRC.ADDRESS_SLV_ID, SRC.CUSTOMER_ID_FK, SRC.FLAT_NO,
            SRC.HOUSE_NO, SRC.FLOOR_NO, SRC.BUILDING,
            SRC.LANDMARK, SRC.LOCALITY, SRC.CITY, SRC.STATE,
            SRC.ZIPCODE, SRC.COORDINATES, SRC.PRIMARY_FLAG,
            SRC.ADDRESS_TYPE, SRC.CREATED_AT, SRC.UPDATED_AT,
            SRC.BATCH_ID
        );

    V_ROWS_MERGED := SQLROWCOUNT;


    /* ===================================================
       4. RETURN STATUS
    =================================================== */
    RETURN
        'SUCCESS | BATCH_ID=' || P_BATCH_ID ||
        ' | ROWS_INSERTED=' || V_ROWS_MERGED ||
        ' | BATCH_DUPLICATES=' || V_BATCH_DUP_CNT ||
        ' | STAGE_DUPLICATES=' || V_STAGE_DUP_CNT;

EXCEPTION
    WHEN OTHER THEN
        RETURN
            'FAILURE | BATCH_ID=' || P_BATCH_ID ||
            ' | ERROR=' || SQLERRM;
END;
$$;


-- =====================================================
-- PROCEDURE 3: SILVER TO GOLD (WITH SCD2)
-- =====================================================
CREATE OR REPLACE PROCEDURE GOLD.SP_CUSTOMER_ADDRESS_SILVER_TO_GOLD(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_CURRENT_TIMESTAMP TIMESTAMP_TZ;
    V_ROWS_EXPIRED INTEGER DEFAULT 0;
    V_ROWS_INSERTED INTEGER DEFAULT 0;
BEGIN
    V_CURRENT_TIMESTAMP := CURRENT_TIMESTAMP();

    -- Step 1: Expire records that have changed (set EFF_END_DATE and STATUS = FALSE)
    UPDATE GOLD.DIM_CUSTOMER_ADDRESS AS TGT
    SET
        EFF_END_DATE = :V_CURRENT_TIMESTAMP,
        STATUS = FALSE
    FROM (
        SELECT
            SRC.ADDRESS_SLV_ID,
            SRC.CUSTOMER_ID_FK,
            SRC.FLAT_NO,
            SRC.HOUSE_NO,
            SRC.FLOOR_NO,
            SRC.BUILDING,
            SRC.LANDMARK,
            SRC.LOCALITY,
            SRC.CITY,
            SRC.STATE,
            SRC.ZIPCODE,
            SRC.COORDINATES,
            SRC.PRIMARY_FLAG,
            SRC.ADDRESS_TYPE
        FROM SILVER.CUSTOMER_ADDRESS_SLV SRC
        WHERE SRC.BATCH_ID = :P_BATCH_ID
    ) AS SRC
    WHERE TGT.ADDRESS_ID = SRC.ADDRESS_SLV_ID
        AND TGT.STATUS = TRUE
        AND (
            COALESCE(TGT.CUSTOMER_ID_FK, '') != COALESCE(SRC.CUSTOMER_ID_FK::STRING, '')
            OR COALESCE(TGT.FLAT_NO, '') != COALESCE(SRC.FLAT_NO, '')
            OR COALESCE(TGT.HOUSE_NO, '') != COALESCE(SRC.HOUSE_NO, '')
            OR COALESCE(TGT.FLOOR_NO, '') != COALESCE(SRC.FLOOR_NO, '')
            OR COALESCE(TGT.BUILDING, '') != COALESCE(SRC.BUILDING, '')
            OR COALESCE(TGT.LANDMARK, '') != COALESCE(SRC.LANDMARK, '')
            OR COALESCE(TGT.LOCALITY, '') != COALESCE(SRC.LOCALITY, '')
            OR COALESCE(TGT.CITY, '') != COALESCE(SRC.CITY, '')
            OR COALESCE(TGT.STATE, '') != COALESCE(SRC.STATE, '')
            OR COALESCE(TGT.ZIPCODE, '') != COALESCE(SRC.ZIPCODE, '')
            OR COALESCE(TGT.COORDINATES, '') != COALESCE(SRC.COORDINATES, '')
            OR COALESCE(TGT.PRIMARY_FLAG, '') != COALESCE(SRC.PRIMARY_FLAG, '')
            OR COALESCE(TGT.ADDRESS_TYPE, '') != COALESCE(SRC.ADDRESS_TYPE, '')
        );

    -- Get count of expired rows
    V_ROWS_EXPIRED := SQLROWCOUNT;

    -- Step 2: Insert new records (both brand new and changed records)
    INSERT INTO GOLD.DIM_CUSTOMER_ADDRESS (
        CUSTOMER_ADDRESS_HK, ADDRESS_ID, CUSTOMER_ID_FK, FLAT_NO, HOUSE_NO, FLOOR_NO,
        BUILDING, LANDMARK, LOCALITY, CITY, STATE, ZIPCODE, COORDINATES,
        PRIMARY_FLAG, ADDRESS_TYPE, EFF_START_DATE, EFF_END_DATE, STATUS, BATCH_ID
    )
    SELECT
        HASH(
            SRC.ADDRESS_SLV_ID,
            SRC.CUSTOMER_ID_FK,
            SRC.FLAT_NO,
            SRC.HOUSE_NO,
            SRC.FLOOR_NO,
            SRC.BUILDING,
            SRC.LANDMARK,
            SRC.LOCALITY,
            SRC.CITY,
            SRC.STATE,
            SRC.ZIPCODE,
            SRC.COORDINATES,
            SRC.PRIMARY_FLAG,
            SRC.ADDRESS_TYPE,
            :V_CURRENT_TIMESTAMP
        ) AS CUSTOMER_ADDRESS_HK,
        SRC.ADDRESS_SLV_ID AS ADDRESS_ID,
        SRC.CUSTOMER_ID_FK::STRING AS CUSTOMER_ID_FK,
        SRC.FLAT_NO,
        SRC.HOUSE_NO,
        SRC.FLOOR_NO,
        SRC.BUILDING,
        SRC.LANDMARK,
        SRC.LOCALITY,
        SRC.CITY,
        SRC.STATE,
        SRC.ZIPCODE,
        SRC.COORDINATES,
        SRC.PRIMARY_FLAG,
        SRC.ADDRESS_TYPE,
        :V_CURRENT_TIMESTAMP AS EFF_START_DATE,
        NULL AS EFF_END_DATE,
        TRUE AS STATUS,
        SRC.BATCH_ID
    FROM SILVER.CUSTOMER_ADDRESS_SLV SRC
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND NOT EXISTS (
            SELECT 1
            FROM GOLD.DIM_CUSTOMER_ADDRESS TGT
            WHERE TGT.ADDRESS_ID = SRC.ADDRESS_SLV_ID
                AND TGT.STATUS = TRUE
                AND COALESCE(TGT.CUSTOMER_ID_FK, '') = COALESCE(SRC.CUSTOMER_ID_FK::STRING, '')
                AND COALESCE(TGT.FLAT_NO, '') = COALESCE(SRC.FLAT_NO, '')
                AND COALESCE(TGT.HOUSE_NO, '') = COALESCE(SRC.HOUSE_NO, '')
                AND COALESCE(TGT.FLOOR_NO, '') = COALESCE(SRC.FLOOR_NO, '')
                AND COALESCE(TGT.BUILDING, '') = COALESCE(SRC.BUILDING, '')
                AND COALESCE(TGT.LANDMARK, '') = COALESCE(SRC.LANDMARK, '')
                AND COALESCE(TGT.LOCALITY, '') = COALESCE(SRC.LOCALITY, '')
                AND COALESCE(TGT.CITY, '') = COALESCE(SRC.CITY, '')
                AND COALESCE(TGT.STATE, '') = COALESCE(SRC.STATE, '')
                AND COALESCE(TGT.ZIPCODE, '') = COALESCE(SRC.ZIPCODE, '')
                AND COALESCE(TGT.COORDINATES, '') = COALESCE(SRC.COORDINATES, '')
                AND COALESCE(TGT.PRIMARY_FLAG, '') = COALESCE(SRC.PRIMARY_FLAG, '')
                AND COALESCE(TGT.ADDRESS_TYPE, '') = COALESCE(SRC.ADDRESS_TYPE, '')
        );

    -- Get count of inserted rows
    V_ROWS_INSERTED := SQLROWCOUNT;

    RETURN 'SCD2 processing completed successfully. Batch_id: ' || :P_BATCH_ID ||
           '. Rows expired: ' || :V_ROWS_EXPIRED ||
           '. New rows inserted: ' || :V_ROWS_INSERTED;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;
-- ====================================================================================================