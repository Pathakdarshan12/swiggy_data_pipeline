-- ====================================================================================================
-- Location
-- ====================================================================================================
-- CHANGE_CONTEXT
USE ROLE ACCOUNTADMIN;
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;
-- ====================================================================================================
-- CREATE LOCATION_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.LOCATION_BRZ (
    LOCATION_BRZ_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    CITY STRING,
    STATE STRING,
    ZIPCODE STRING,
    BATCH_ID VARCHAR(36),
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'THIS IS THE LOCATION STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE TEXT DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';;

-- ----------------------------------------------------------------------------------------------------
-- CREATING LOCATION_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.LOCATION_SLV (
    LOCATION_SLV_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    CITY STRING(100) ,
    STATE STRING(100) ,
    STATE_CODE STRING(2),
    IS_UNION_TERRITORY BOOLEAN DEFAULT FALSE,
    CAPITAL_CITY_FLAG BOOLEAN DEFAULT FALSE,
    CITY_TIER TEXT(6),
    ZIP_CODE STRING(10),
    STATUS STRING(10),
    BATCH_ID VARCHAR(36),
    CREATED_AT TIMESTAMP_NTZ(9),
    UPDATED_AT TIMESTAMP_NTZ(9)
)
COMMENT = 'LOCATION ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREAING DIM_LOCATION
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_LOCATION (
    LOCATION_ID NUMBER(38,0) PRIMARY KEY AUTOINCREMENT,      -- Surrogate Key
    CITY VARCHAR(100) NOT NULL,                              -- City name
    STATE VARCHAR(100) NOT NULL,                             -- State name
    STATE_CODE VARCHAR(2),                                   -- State code
    IS_UNION_TERRITORY BOOLEAN DEFAULT FALSE,                -- Union Territory flag
    CAPITAL_CITY_FLAG BOOLEAN DEFAULT FALSE,                 -- Capital city flag
    CITY_TIER VARCHAR(6),                                    -- City tier classification
    ZIP_CODE VARCHAR(10) NOT NULL,                           -- Zip code
    STATUS VARCHAR(10) DEFAULT 'ACTIVE',                     -- Record status (ACTIVE/INACTIVE)
    EFF_START_DT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),-- Effective start date
    EFF_END_DT TIMESTAMP_TZ(9) DEFAULT '9999-12-31 23:59:59'::TIMESTAMP_TZ, -- Effective end date
    BATCH_ID VARCHAR(36) NOT NULL,                           -- Batch identifier
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(), -- Record creation timestamp
    UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP() -- Record update timestamp
);

-- ----------------------------------------------------------------------------------------------------
-- STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.LOCATION_BRZ (
    LOCATION_BRZ_ID INTEGER AUTOINCREMENT,
    CITY STRING,
    STATE STRING,
    ZIPCODE STRING,
    INGEST_RUN_ID INTEGER,
    BATCH_ID VARCHAR(36),
    CREATED_AT TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE PROCEDURE SP_LOCATION_STAGE_TO_BRONZE(
    p_batch_id STRING,
    p_file_path STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_created_at TIMESTAMP_NTZ(9);
    v_rows_loaded INTEGER DEFAULT 0;
BEGIN
    -- Extract file name from path
    v_created_at := CURRENT_TIMESTAMP();

    -- Temp table
    CREATE OR REPLACE TEMPORARY TABLE temp_menu_load (
        CITY STRING,
        STATE STRING,
        ZIPCODE STRING
    );

    -- Load data from stage
    EXECUTE IMMEDIATE
    '
        COPY INTO temp_location_load (CITY, STATE, ZIPCODE)
        FROM (
            SELECT $1::STRING, $2::STRING, $3::STRING
            FROM ' || p_file_path || '
        )
        FILE_FORMAT = (FORMAT_NAME = ''BRONZE.CSV_FILE_FORMAT'')
        ON_ERROR = ABORT_STATEMENT
    ';

    -- Insert into bronze table
    INSERT INTO BRONZE.LOCATION_BRZ
        (CITY, STATE, ZIPCODE, BATCH_ID, CREATED_AT)
    SELECT
        CITY,
        STATE,
        ZIPCODE,
        :p_batch_id,
        :v_created_at
    FROM temp_location_load;

    v_rows_loaded := SQLROWCOUNT;

    DROP TABLE BRONZE.TEMP_LOCATION_LOAD;

    RETURN
        'Successfully loaded ' || v_rows_loaded ||
        ' rows with batch_id: ' || :p_batch_id;

EXCEPTION
    WHEN OTHER THEN
        RETURN
            'Error occurred: ' || SQLERRM;
END;
$$;

 CALL sp_location_stage_to_bronze(
    '123',
    '@"SWIGGY"."BRONZE"."CSV_STG"/location/location_01-01-2025.csv'
);

-- ----------------------------------------------------------------------------------------------------
-- BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SILVER.SP_LOCATION_BRONZE_TO_SILVER(BATCH_ID_PARAM VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_updated INTEGER DEFAULT 0;
BEGIN

    -- MERGE statement for CDC (Change Data Capture)
    MERGE INTO SILVER.LOCATION_SLV AS TGT
    USING (
        SELECT DISTINCT
            UPPER(TRIM(CITY)) AS CITY,
            UPPER(TRIM(STATE)) AS STATE,
            UPPER(TRIM(ZIPCODE)) AS ZIP_CODE,
            CASE
                WHEN UPPER(TRIM(STATE)) = 'MAHARASHTRA' THEN 'MH'
                WHEN UPPER(TRIM(STATE)) = 'DELHI' THEN 'DL'
                WHEN UPPER(TRIM(STATE)) = 'KARNATAKA' THEN 'KA'
                WHEN UPPER(TRIM(STATE)) = 'TAMIL NADU' THEN 'TN'
                WHEN UPPER(TRIM(STATE)) = 'UTTAR PRADESH' THEN 'UP'
                WHEN UPPER(TRIM(STATE)) = 'WEST BENGAL' THEN 'WB'
                WHEN UPPER(TRIM(STATE)) = 'RAJASTHAN' THEN 'RJ'
                WHEN UPPER(TRIM(STATE)) = 'GUJARAT' THEN 'GJ'
                ELSE LEFT(UPPER(TRIM(STATE)), 2)
            END AS STATE_CODE,
            CASE
                WHEN UPPER(TRIM(STATE)) IN ('DELHI', 'CHANDIGARH', 'PUDUCHERRY',
                    'ANDAMAN AND NICOBAR ISLANDS', 'LAKSHADWEEP', 'DADRA AND NAGAR HAVELI',
                    'DAMAN AND DIU', 'LADAKH', 'JAMMU AND KASHMIR')
                THEN TRUE
                ELSE FALSE
            END AS IS_UNION_TERRITORY,
            CASE
                WHEN UPPER(TRIM(CITY)) IN ('MUMBAI', 'DELHI', 'BANGALORE', 'HYDERABAD',
                    'CHENNAI', 'KOLKATA', 'PUNE', 'AHMEDABAD')
                THEN TRUE
                ELSE FALSE
            END AS CAPITAL_CITY_FLAG,
            CASE
                WHEN UPPER(TRIM(CITY)) IN ('MUMBAI', 'DELHI', 'BANGALORE', 'HYDERABAD',
                    'CHENNAI', 'KOLKATA', 'PUNE', 'AHMEDABAD')
                THEN 'TIER1'
                WHEN UPPER(TRIM(CITY)) IN ('NAGPUR', 'JAIPUR', 'LUCKNOW', 'CHANDIGARH',
                    'INDORE', 'COIMBATORE', 'KOCHI', 'VISAKHAPATNAM')
                THEN 'TIER2'
                ELSE 'TIER3'
            END AS CITY_TIER,
            'ACTIVE' AS STATUS,
            BATCH_ID AS BATCH_ID,
            CREATED_AT
        FROM BRONZE.LOCATION_BRZ
        WHERE BATCH_ID = :BATCH_ID_PARAM
    ) AS SRC
    ON TGT.CITY = SRC.CITY
       AND TGT.STATE = SRC.STATE
       AND TGT.ZIP_CODE = SRC.ZIP_CODE

    -- When record exists and data has changed, UPDATE
    WHEN MATCHED AND (
        COALESCE(TGT.STATE_CODE, '') != COALESCE(SRC.STATE_CODE, '') OR
        COALESCE(TGT.IS_UNION_TERRITORY, FALSE) != COALESCE(SRC.IS_UNION_TERRITORY, FALSE) OR
        COALESCE(TGT.CAPITAL_CITY_FLAG, FALSE) != COALESCE(SRC.CAPITAL_CITY_FLAG, FALSE) OR
        COALESCE(TGT.CITY_TIER, '') != COALESCE(SRC.CITY_TIER, '')
    ) THEN UPDATE SET
        TGT.STATE_CODE = SRC.STATE_CODE,
        TGT.IS_UNION_TERRITORY = SRC.IS_UNION_TERRITORY,
        TGT.CAPITAL_CITY_FLAG = SRC.CAPITAL_CITY_FLAG,
        TGT.CITY_TIER = SRC.CITY_TIER,
        TGT.BATCH_ID = SRC.BATCH_ID,
        TGT.UPDATED_AT = CURRENT_TIMESTAMP()

    -- When record doesn't exist, INSERT
    WHEN NOT MATCHED THEN INSERT (
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        STATUS,
        BATCH_ID,
        CREATED_AT,
        UPDATED_AT
    ) VALUES (
        SRC.CITY,
        SRC.STATE,
        SRC.STATE_CODE,
        SRC.IS_UNION_TERRITORY,
        SRC.CAPITAL_CITY_FLAG,
        SRC.CITY_TIER,
        SRC.ZIP_CODE,
        SRC.STATUS,
        SRC.BATCH_ID,
        SRC.CREATED_AT,
        CURRENT_TIMESTAMP()
    );

    -- Get row counts
    v_rows_inserted := (SELECT COUNT(*) FROM SILVER.LOCATION_SLV WHERE BATCH_ID = :BATCH_ID_PARAM AND CREATED_AT = UPDATED_AT);
    v_rows_updated := (SELECT COUNT(*) FROM SILVER.LOCATION_SLV WHERE BATCH_ID = :BATCH_ID_PARAM AND CREATED_AT != UPDATED_AT);

    RETURN 'SUCCESS: Inserted ' || v_rows_inserted || ' rows, Updated ' || v_rows_updated || ' rows';

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;




SELECT * FROM BRONZE.LOCATION_BRZ;
CALL SILVER.SP_LOCATION_BRONZE_TO_SILVER('fc09119f-bb39-4077-afb8-e1909a623917');
SELECT * FROM SILVER.LOCATION_SLV;
-- ----------------------------------------------------------------------------------------------------
-- SILVER TO GOLD
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_LOCATION_SILVER_TO_GOLD(BATCH_ID_PARAM VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_updated INTEGER DEFAULT 0;
    v_rows_deleted INTEGER DEFAULT 0;
BEGIN

    -- Step 1: Identify changed records and expire old versions
    -- Set EFF_END_DT for records that have changed
    UPDATE GOLD.DIM_LOCATION AS TGT
    SET
        TGT.EFF_END_DT = CURRENT_TIMESTAMP(),
        TGT.STATUS = 'INACTIVE',
        TGT.UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE TGT.STATUS = 'ACTIVE'
    AND EXISTS (
        SELECT 1
        FROM SILVER.LOCATION_SLV AS SRC
        WHERE SRC.BATCH_ID = :BATCH_ID_PARAM
        AND TGT.CITY = SRC.CITY
        AND TGT.STATE = SRC.STATE
        AND TGT.ZIP_CODE = SRC.ZIP_CODE
        AND (
            COALESCE(TGT.STATE_CODE, '') != COALESCE(SRC.STATE_CODE, '') OR
            COALESCE(TGT.IS_UNION_TERRITORY, FALSE) != COALESCE(SRC.IS_UNION_TERRITORY, FALSE) OR
            COALESCE(TGT.CAPITAL_CITY_FLAG, FALSE) != COALESCE(SRC.CAPITAL_CITY_FLAG, FALSE) OR
            COALESCE(TGT.CITY_TIER, '') != COALESCE(SRC.CITY_TIER, '')
        )
    );

    v_rows_deleted := SQLROWCOUNT;

    -- Step 2: Insert new versions of changed records AND new records
    INSERT INTO GOLD.DIM_LOCATION (
        CITY,
        STATE,
        STATE_CODE,
        IS_UNION_TERRITORY,
        CAPITAL_CITY_FLAG,
        CITY_TIER,
        ZIP_CODE,
        STATUS,
        EFF_START_DT,
        EFF_END_DT,
        BATCH_ID,
        CREATED_AT,
        UPDATED_AT
    )
    SELECT
        SRC.CITY,
        SRC.STATE,
        SRC.STATE_CODE,
        SRC.IS_UNION_TERRITORY,
        SRC.CAPITAL_CITY_FLAG,
        SRC.CITY_TIER,
        SRC.ZIP_CODE,
        'ACTIVE' AS STATUS,
        CURRENT_TIMESTAMP() AS EFF_START_DT,
        '9999-12-31 23:59:59'::TIMESTAMP_TZ AS EFF_END_DT,
        SRC.BATCH_ID,
        CURRENT_TIMESTAMP() AS CREATED_AT,
        CURRENT_TIMESTAMP() AS UPDATED_AT
    FROM SILVER.LOCATION_SLV AS SRC
    WHERE SRC.BATCH_ID = :BATCH_ID_PARAM
    AND NOT EXISTS (
        -- Exclude records that haven't changed
        SELECT 1
        FROM GOLD.DIM_LOCATION AS TGT
        WHERE TGT.CITY = SRC.CITY
        AND TGT.STATE = SRC.STATE
        AND TGT.ZIP_CODE = SRC.ZIP_CODE
        AND TGT.STATUS = 'ACTIVE'
        AND COALESCE(TGT.STATE_CODE, '') = COALESCE(SRC.STATE_CODE, '')
        AND COALESCE(TGT.IS_UNION_TERRITORY, FALSE) = COALESCE(SRC.IS_UNION_TERRITORY, FALSE)
        AND COALESCE(TGT.CAPITAL_CITY_FLAG, FALSE) = COALESCE(SRC.CAPITAL_CITY_FLAG, FALSE)
        AND COALESCE(TGT.CITY_TIER, '') = COALESCE(SRC.CITY_TIER, '')
    );

    v_rows_inserted := SQLROWCOUNT;

    RETURN 'SUCCESS: Expired ' || :v_rows_deleted || ' rows, Inserted ' || :v_rows_inserted || ' new versions';

EXCEPTION
    WHEN OTHER THEN
        RETURN 'ERROR: ' || SQLERRM;
END;
$$;
-- ====================================================================================================