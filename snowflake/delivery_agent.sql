-- ====================================================================================================
-- DELIVERY_AGENT
-- ====================================================================================================
-- CHANGE CONSTRING(100)
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_AGENT_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.DELIVERY_AGENT_BRZ (
    DELIVERY_AGENT_ID STRING(100) COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',         -- PRIMARY KEY AS STRING(100)
    NAME STRING(100),           -- NAME AS STRING(100), REQUIRED FIELD
    PHONE STRING(100),            -- PHONE AS STRING(100), UNIQUE CONSTRAINT INDICATED
    VEHICLETYPE STRING(100),             -- VEHICLE TYPE AS STRING(100)
    LOCATIONID STRING(100),              -- FOREIGN KEY REFERENCE AS STRING(100) (NO CONSTRAINT IN SNOWFLAKE)
    ACTIVE STRING(100),                  -- STATUS AS STRING(100)
    GENDER STRING(100),                  -- STATUS AS STRING(100)
    RATING STRING(100),                  -- RATING AS STRING(100)
    CREATED_AT STRING(100),             -- CREATED DATE AS STRING(100)
    UPDATED_AT STRING(100),            -- MODIFIED DATE AS STRING(100)
    BATCH_ID STRING(36)
)
COMMENT = 'THIS IS THE DELIVERY STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE STRING(100) DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_AGENT_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.DELIVERY_AGENT_SLV (
    DELIVERY_AGENT_ID INTEGER PRIMARY KEY,               -- DELIVERY AGENT ID AS INTEGER
    NAME STRING NOT NULL,                -- NAME AS STRING, REQUIRED FIELD
    PHONE STRING NOT NULL,                 -- PHONE AS STRING, UNIQUE CONSTRAINT
    VEHICLE_TYPE STRING NOT NULL,                 -- VEHICLE TYPE AS STRING
    LOCATION_ID_FK INT COMMENT 'LOCATION FK(SOURCE SYSTEM)',                     -- LOCATION ID AS INTEGER
    ACTIVE STRING,                       -- STATUS AS STRING
    GENDER STRING,                       -- GENDER AS STRING
    RATING INTEGER,                        -- RATING AS FLOAT
    CREATED_AT TIMESTAMP_NTZ,          -- CREATED DATE AS TIMESTAMP WITHOUT TIMEZONE
    UPDATED_AT TIMESTAMP_NTZ,         -- MODIFIED DATE AS TIMESTAMP WITHOUT TIMEZONE
    BATCH_ID STRING(36)
)
COMMENT = 'DELIVERY ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DELIVERY_AGENT_GLD
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_DELIVERY_AGENT (
    DELIVERY_AGENT_ID INTEGER PRIMARY KEY,               -- BUSINESS KEY
    NAME STRING NOT NULL,                   -- DELIVERY AGENT NAME
    PHONE STRING UNIQUE,                    -- PHONE INTEGER, UNIQUE
    VEHICLE_TYPE STRING,                    -- TYPE OF VEHICLE
    LOCATION_ID_FK INTEGER NOT NULL COMMENT 'LOCATION FK (SOURCE SYSTEM)',                     -- LOCATION ID
    ACTIVE STRING,                          -- CURRENT STATUS OF THE DELIVERY AGENT
    GENDER STRING,                          -- GENDER
    RATING INTEGER,                     -- RATING WITH ONE DECIMAL PRECISION
    EFF_START_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- EFFECTIVE START DATE
    EFF_END_DATE TIMESTAMP,                 -- EFFECTIVE END DATE (NULL FOR ACTIVE RECORD)
    STATUS BOOLEAN DEFAULT TRUE,
    BATCH_ID STRING(36)
)
COMMENT =  'DIM TABLE FOR DELIVERY AGENT ENTITY WITH SCD2 SUPPORT.';


-- =====================================================
-- PROCEDURE 1: STAGE TO BRONZE
-- =====================================================
CREATE OR REPLACE PROCEDURE BRONZE.SP_DELIVERY_AGENT_STAGE_TO_BRONZE(
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_DELIVERY_AGENT_LOAD(
        DELIVERY_AGENT_ID STRING(100),
        NAME STRING(100),
        PHONE STRING(100),
        VEHICLETYPE STRING(100),
        LOCATIONID STRING(100),
        ACTIVE STRING(100),
        GENDER STRING(100),
        RATING STRING(100)
    );

    -- Copy data from stage to temp table
    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_DELIVERY_AGENT_LOAD (
            DELIVERY_AGENT_ID, NAME, PHONE, VEHICLETYPE, LOCATIONID,
            ACTIVE, GENDER, RATING
        )
        FROM (
            SELECT
                $1::STRING AS DELIVERY_AGENT_ID,
                $2::STRING AS NAME,
                $3::STRING AS PHONE,
                $4::STRING AS VEHICLETYPE,
                $5::STRING AS LOCATIONID,
                $6::STRING AS ACTIVE,
                $7::STRING AS GENDER,
                $8::STRING AS RATING
            FROM ' || P_FILE_PATH || '
        )
        FILE_FORMAT = (FORMAT_NAME = ''BRONZE.CSV_FILE_FORMAT'')
        ON_ERROR = ABORT_STATEMENT
    ';

    -- Insert into bronze table
    INSERT INTO BRONZE.DELIVERY_AGENT_BRZ (
        DELIVERY_AGENT_ID, NAME, PHONE, VEHICLETYPE, LOCATIONID,
        ACTIVE, GENDER, RATING, CREATED_AT, UPDATED_AT, BATCH_ID
    )
    SELECT
        DELIVERY_AGENT_ID,
        NAME,
        PHONE,
        VEHICLETYPE,
        LOCATIONID,
        ACTIVE,
        GENDER,
        RATING,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :P_BATCH_ID
    FROM TEMP_DELIVERY_AGENT_LOAD;

    -- Get row count
    SELECT COUNT(*) INTO :v_rows_inserted FROM TEMP_DELIVERY_AGENT_LOAD;

    -- Drop temp table
    DROP TABLE IF EXISTS TEMP_DELIVERY_AGENT_LOAD;

    RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            v_rows_inserted
        );

EXCEPTION
    WHEN OTHER THEN
        RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            v_rows_inserted
        );
END;
$$;

-- =====================================================
-- PROCEDURE 2: BRONZE TO SILVER
-- =====================================================
CREATE OR REPLACE PROCEDURE SILVER.SP_DELIVERY_AGENT_BRONZE_TO_SILVER(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
BEGIN
    -- Merge data from bronze to silver (no SCD2 at silver layer)
    MERGE INTO SILVER.DELIVERY_AGENT_SLV AS TGT
    USING (
        SELECT
            TRY_CAST(DELIVERY_AGENT_ID AS INTEGER) AS DELIVERY_AGENT_ID,
            NAME,
            PHONE,
            VEHICLETYPE AS VEHICLE_TYPE,
            TRY_CAST(LOCATIONID AS INT) AS LOCATION_ID_FK,
            ACTIVE,
            GENDER,
            TRY_CAST(RATING AS INTEGER) AS RATING,
            TRY_CAST(CREATED_AT AS TIMESTAMP_NTZ) AS CREATED_AT,
            TRY_CAST(UPDATED_AT AS TIMESTAMP_NTZ) AS UPDATED_AT,
            BATCH_ID
        FROM BRONZE.DELIVERY_AGENT_BRZ
        WHERE BATCH_ID = :P_BATCH_ID
    ) AS SRC
    ON TGT.DELIVERY_AGENT_ID = SRC.DELIVERY_AGENT_ID

    WHEN MATCHED THEN
        UPDATE SET
            TGT.NAME = SRC.NAME,
            TGT.PHONE = SRC.PHONE,
            TGT.VEHICLE_TYPE = SRC.VEHICLE_TYPE,
            TGT.LOCATION_ID_FK = SRC.LOCATION_ID_FK,
            TGT.ACTIVE = SRC.ACTIVE,
            TGT.GENDER = SRC.GENDER,
            TGT.RATING = SRC.RATING,
            TGT.UPDATED_AT = SRC.UPDATED_AT,
            TGT.BATCH_ID = SRC.BATCH_ID

    WHEN NOT MATCHED THEN
        INSERT (
            DELIVERY_AGENT_ID, NAME, PHONE, VEHICLE_TYPE, LOCATION_ID_FK,
            ACTIVE, GENDER, RATING, CREATED_AT, UPDATED_AT, BATCH_ID
        )
        VALUES (
            SRC.DELIVERY_AGENT_ID, SRC.NAME, SRC.PHONE, SRC.VEHICLE_TYPE,
            SRC.LOCATION_ID_FK, SRC.ACTIVE, SRC.GENDER, SRC.RATING,
            SRC.CREATED_AT, SRC.UPDATED_AT, SRC.BATCH_ID
        );

    -- Get merge statistics
    V_ROWS_INSERTED := (SELECT COUNT(*) FROM SILVER.DELIVERY_AGENT_SLV WHERE BATCH_ID = :P_BATCH_ID);

    RETURN ARRAY_CONSTRUCT(
            'SUCCESSFUL',
            v_rows_inserted,
            v_rows_updated
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
-- PROCEDURE 3: SILVER TO GOLD (WITH SCD2)
-- =====================================================
CREATE OR REPLACE PROCEDURE GOLD.SP_DELIVERY_AGENT_SILVER_TO_GOLD(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_CURRENT_TIMESTAMP TIMESTAMP;
    V_ROWS_INSERTED INTEGER DEFAULT 0;
    V_ROWS_UPDATED INTEGER DEFAULT 0;
    V_ROWS_DELETED INTEGER DEFAULT 0;
BEGIN
    V_CURRENT_TIMESTAMP := CURRENT_TIMESTAMP();

    -- Step 1: Expire records that have changed (set EFF_END_DATE and STATUS = FALSE)
    UPDATE GOLD.DIM_DELIVERY_AGENT AS TGT
    SET
        EFF_END_DATE = :V_CURRENT_TIMESTAMP,
        STATUS = FALSE
    FROM (
        SELECT
            SRC.DELIVERY_AGENT_ID,
            SRC.NAME,
            SRC.PHONE,
            SRC.VEHICLE_TYPE,
            SRC.LOCATION_ID_FK,
            SRC.ACTIVE,
            SRC.GENDER,
            SRC.RATING
        FROM SILVER.DELIVERY_AGENT_SLV SRC
        WHERE SRC.BATCH_ID = :P_BATCH_ID
    ) AS SRC
    WHERE TGT.DELIVERY_AGENT_ID = SRC.DELIVERY_AGENT_ID
        AND TGT.STATUS = TRUE
        AND (
            COALESCE(TGT.NAME, '') != COALESCE(SRC.NAME, '')
            OR COALESCE(TGT.PHONE, '') != COALESCE(SRC.PHONE, '')
            OR COALESCE(TGT.VEHICLE_TYPE, '') != COALESCE(SRC.VEHICLE_TYPE, '')
            OR COALESCE(TGT.LOCATION_ID_FK, 0) != COALESCE(SRC.LOCATION_ID_FK, 0)
            OR COALESCE(TGT.ACTIVE, '') != COALESCE(SRC.ACTIVE, '')
            OR COALESCE(TGT.GENDER, '') != COALESCE(SRC.GENDER, '')
            OR COALESCE(TGT.RATING, 0) != COALESCE(SRC.RATING, 0)
        );

    -- Get count of expired rows
    v_rows_deleted := SQLROWCOUNT;

    -- Step 2: Insert new records (both brand new and changed records)
    INSERT INTO GOLD.DIM_DELIVERY_AGENT (
        DELIVERY_AGENT_ID, NAME, PHONE, VEHICLE_TYPE, LOCATION_ID_FK,
        ACTIVE, GENDER, RATING, EFF_START_DATE, EFF_END_DATE, STATUS, BATCH_ID
    )
    SELECT
        SRC.DELIVERY_AGENT_ID,
        SRC.NAME,
        SRC.PHONE,
        SRC.VEHICLE_TYPE,
        SRC.LOCATION_ID_FK,
        SRC.ACTIVE,
        SRC.GENDER,
        SRC.RATING,
        :V_CURRENT_TIMESTAMP AS EFF_START_DATE,
        NULL AS EFF_END_DATE,
        TRUE AS STATUS,
        SRC.BATCH_ID
    FROM SILVER.DELIVERY_AGENT_SLV SRC
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND NOT EXISTS (
            SELECT 1
            FROM GOLD.DIM_DELIVERY_AGENT TGT
            WHERE TGT.DELIVERY_AGENT_ID = SRC.DELIVERY_AGENT_ID
                AND TGT.STATUS = TRUE
                AND COALESCE(TGT.NAME, '') = COALESCE(SRC.NAME, '')
                AND COALESCE(TGT.PHONE, '') = COALESCE(SRC.PHONE, '')
                AND COALESCE(TGT.VEHICLE_TYPE, '') = COALESCE(SRC.VEHICLE_TYPE, '')
                AND COALESCE(TGT.LOCATION_ID_FK, 0) = COALESCE(SRC.LOCATION_ID_FK, 0)
                AND COALESCE(TGT.ACTIVE, '') = COALESCE(SRC.ACTIVE, '')
                AND COALESCE(TGT.GENDER, '') = COALESCE(SRC.GENDER, '')
                AND COALESCE(TGT.RATING, 0) = COALESCE(SRC.RATING, 0)
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
-- -- Query to see history of a specific delivery agent
-- SELECT * FROM GOLD.DIM_DELIVERY_AGENT
-- WHERE DELIVERY_AGENT_ID = 123
-- ORDER BY EFF_START_DATE;

-- -- Query to see agents whose details changed
-- SELECT
--     DELIVERY_AGENT_ID,
--     NAME,
--     PHONE,
--     VEHICLE_TYPE,
--     RATING,
--     EFF_START_DATE,
--     EFF_END_DATE,
--     STATUS
-- FROM GOLD.DIM_DELIVERY_AGENT
-- WHERE DELIVERY_AGENT_ID IN (
--     SELECT DELIVERY_AGENT_ID
--     FROM GOLD.DIM_DELIVERY_AGENT
--     GROUP BY DELIVERY_AGENT_ID
--     HAVING COUNT(*) > 1
-- )
-- ORDER BY DELIVERY_AGENT_ID, EFF_START_DATE;