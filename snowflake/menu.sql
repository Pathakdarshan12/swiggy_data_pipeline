-- ====================================================================================================
-- MENU
-- ====================================================================================================
-- CHANGE CONSTRING
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE MENU_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.MENU_BRZ (
    MENU_ID INTEGER PRIMARY KEY AUTOINCREMENT,                   -- PRIMARY KEY AS STRING
    RESTAURANT_ID STRING,             -- FOREIGN KEY REFERENCE AS STRING (NO CONSTRAINT IN SNOWFLAKE)
    ITEM_NAME STRING,                 -- ITEM NAME AS STRING
    DESCRIPTION STRING,              -- DESCRIPTION AS STRING
    PRICE STRING,                    -- PRICE AS STRING (NO DECIMAL CONSTRAINT)
    CATEGORY STRING,                 -- CATEGORY AS STRING
    AVAILABILITY STRING,             -- AVAILABILITY AS STRING
    ITEM_TYPE STRING,                 -- ITEM TYPE AS STRING
    -- AUDIT COLUMNS WITH APPROPRIATE DATA TYPES
    CREATED_AT STRING,              -- CREATED DATE AS STRING
    UPDATED_AT STRING,             -- MODIFIED DATE AS STRING
    BATCH_ID STRING(36)
)
COMMENT = 'THIS IS THE MENU STAGE/RAW TABLE WHERE DATA WILL BE COPIED FROM INTERNAL STAGE USING COPY COMMAND. THIS IS AS-IS DATA REPRESETATION FROM THE SOURCE LOCATION. ALL THE COLUMNS ARE STRING DATA TYPE EXCEPT THE AUDIT COLUMNS THAT ARE ADDED FOR TRACEABILITY.';

-- ----------------------------------------------------------------------------------------------------
-- CREATE MENU_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.MENU_SLV (
    MENU_ID INTEGER PRIMARY KEY AUTOINCREMENT ,             -- UNIQUE AND NON-NULL MENU_ID
    RESTAURANT_ID_FK INTEGER ,                      -- IDENTIFIER FOR THE RESTAURANT
    ITEM_NAME STRING NOT NULL,                        -- NAME OF THE MENU ITEM
    DESCRIPTION STRING NOT NULL,                     -- DESCRIPTION OF THE MENU ITEM
    PRICE DECIMAL(10, 2) NOT NULL,                   -- PRICE AS A NUMERIC VALUE WITH 2 DECIMAL PLACES
    CATEGORY STRING,                        -- FOOD CATEGORY (E.G., NORTH INDIAN)
    AVAILABILITY BOOLEAN,                   -- AVAILABILITY STATUS (TRUE/FALSE)
    ITEM_TYPE STRING,                        -- DIETARY CLASSIFICATION (E.G., VEGAN)
    CREATED_AT TIMESTAMP_NTZ,               -- DATE WHEN THE RECORD WAS CREATED
    UPDATED_AT TIMESTAMP_NTZ,              -- DATE WHEN THE RECORD WAS LAST MODIFIED
    BATCH_ID STRING(36)
)
COMMENT = 'MENU ENTITY UNDER CLEAN SCHEMA WITH APPROPRIATE DATA TYPE UNDER CLEAN SCHEMA LAYER, DATA IS POPULATED USING MERGE STATEMENT FROM THE STAGE LAYER LOCATION TABLE. THIS TABLE DOES NOT SUPPORT SCD2';

-- ----------------------------------------------------------------------------------------------------
-- CREATE DIM_MENU
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.DIM_MENU (
    MENU_DIM_HK NUMBER PRIMARY KEY COMMENT 'MENU DIM HK (EDW)',                         -- HASH KEY GENERATED FOR MENU DIM TABLE
    MENU_ID INT NOT NULL COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',                       -- UNIQUE AND NON-NULL MENU_ID
    RESTAURANT_ID_FK INT NOT NULL COMMENT 'RESTAURANT FK (SOURCE SYSTEM)',                          -- IDENTIFIER FOR THE RESTAURANT
    ITEM_NAME STRING,                            -- NAME OF THE MENU ITEM
    DESCRIPTION STRING,                         -- DESCRIPTION OF THE MENU ITEM
    PRICE DECIMAL(10, 2),                       -- PRICE AS A NUMERIC VALUE WITH 2 DECIMAL PLACES
    CATEGORY STRING,                            -- FOOD CATEGORY (E.G., NORTH INDIAN)
    AVAILABILITY BOOLEAN,                       -- AVAILABILITY STATUS (TRUE/FALSE)
    ITEM_TYPE STRING,                           -- DIETARY CLASSIFICATION (E.G., VEGAN)
    EFF_START_DATE TIMESTAMP_NTZ,               -- EFFECTIVE START DATE OF THE RECORD
    EFF_END_DATE TIMESTAMP_NTZ,                 -- EFFECTIVE END DATE OF THE RECORD
    STATUS BOOLEAN                         -- FLAG TO INDICATE IF THE RECORD IS CURRENT (TRUE/FALSE)
)
COMMENT = 'THIS TABLE STORES THE DIMENSION DATA FOR THE MENU ITEMS, TRACKING HISTORICAL CHANGES USING SCD TYPE 2. EACH MENU ITEM HAS AN EFFECTIVE START AND END DATE, WITH A FLAG INDICATING IF IT IS THE CURRENT RECORD OR HISTORICAL. THE HASH KEY (MENU_DIM_HK) IS GENERATED BASED ON MENU_ID AND RESTAURANT_ID.';

DESC TABLE BRONZE.MENU_BRZ;

-- ----------------------------------------------------------------------------------------------------
-- STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BRONZE.SP_MENU_STAGE_TO_BRONZE(p_batch_id STRING, p_file_path STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;

BEGIN
    -- CREATE TEMP TABLE
    CREATE OR REPLACE TEMPORARY TABLE TEMP_MENU_LOAD( RESTAURANT_ID STRING, ITEM_NAME STRING, DESCRIPTION STRING, PRICE STRING, CATEGORY STRING, AVAILABILITY STRING, ITEM_TYPE STRING);

    -- COPY STAGE FILE DATA INTO TEMP TABLE
    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_MENU_LOAD (RESTAURANT_ID, ITEM_NAME, DESCRIPTION, PRICE, CATEGORY,  AVAILABILITY, ITEM_TYPE)
        FROM (
            SELECT $1::STRING AS RESTAURANT_ID, $2::STRING AS ITEM_NAME, $3::STRING AS DESCRIPTION, $4::STRING AS PRICE, $5::STRING AS CATEGORY, $6::STRING AS AVAILABILITY, $7::STRING AS ITEM_TYPE
            FROM ' || p_file_path || '
        )
        FILE_FORMAT = (FORMAT_NAME = ''BRONZE.CSV_FILE_FORMAT'')
        ON_ERROR = ABORT_STATEMENT
    ';


    -- INSERT INTO BRONZE TABLE
    INSERT INTO BRONZE.MENU_BRZ(
            RESTAURANT_ID,
            ITEM_NAME,
            DESCRIPTION,
            PRICE,
            CATEGORY,
            AVAILABILITY,
            ITEM_TYPE,
            CREATED_AT,
            UPDATED_AT,
            BATCH_ID
    )
    SELECT
        RESTAURANT_ID,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :p_batch_id
    FROM TEMP_MENU_LOAD;

    v_rows_inserted := SQLROWCOUNT;
    DROP TABLE TEMP_MENU_LOAD;
    RETURN 'SUCCESS | Rows loaded: ' || :v_rows_inserted || ' | Batch ID: ' || :p_batch_id;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'FAILED | Batch ID: ' || p_batch_id || ' | Error Occured:' || SQLERRM ;
END;
$$;

CALL BRONZE.SP_MENU_STAGE_TO_BRONZE('123', '@"SWIGGY"."BRONZE"."CSV_STG"/menu/menu_03-01-2025.csv');
SELECT * FROM MENU_BRZ;
-- ----------------------------------------------------------------------------------------------------
-- BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SILVER.SP_MENU_BRONZE_TO_SILVER
(
    p_batch_id STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_updated INTEGER DEFAULT 0;
BEGIN

    MERGE INTO SILVER.MENU_SLV TARGET
    USING (
        SELECT
            TRY_CAST(RESTAURANT_ID AS INT)        AS RESTAURANT_ID_FK,
            TRIM(ITEM_NAME)                      AS ITEM_NAME,
            TRIM(DESCRIPTION)                    AS DESCRIPTION,
            TRY_CAST(PRICE AS DECIMAL(10,2))     AS PRICE,
            TRIM(CATEGORY)                       AS CATEGORY,
            CASE
                WHEN LOWER(AVAILABILITY) = 'true'  THEN TRUE
                WHEN LOWER(AVAILABILITY) = 'false' THEN FALSE
                ELSE NULL
            END                                  AS AVAILABILITY,
            TRIM(ITEM_TYPE)                       AS ITEM_TYPE,
            BATCH_ID
        FROM BRONZE.MENU_BRZ
        WHERE BATCH_ID = :p_batch_id
    ) SOURCE
    ON TARGET.RESTAURANT_ID_FK = SOURCE.RESTAURANT_ID_FK
   AND TARGET.ITEM_NAME = SOURCE.ITEM_NAME

    -- UPDATE WHEN DATA CHANGED
    WHEN MATCHED AND (
        COALESCE(TARGET.DESCRIPTION, '') <> COALESCE(SOURCE.DESCRIPTION, '') OR
        COALESCE(TARGET.PRICE, 0)        <> COALESCE(SOURCE.PRICE, 0) OR
        COALESCE(TARGET.CATEGORY, '')    <> COALESCE(SOURCE.CATEGORY, '') OR
        COALESCE(TARGET.AVAILABILITY, FALSE) <> COALESCE(SOURCE.AVAILABILITY, FALSE) OR
        COALESCE(TARGET.ITEM_TYPE, '')    <> COALESCE(SOURCE.ITEM_TYPE, '')
    )
    THEN UPDATE SET
        TARGET.DESCRIPTION  = SOURCE.DESCRIPTION,
        TARGET.PRICE        = SOURCE.PRICE,
        TARGET.CATEGORY     = SOURCE.CATEGORY,
        TARGET.AVAILABILITY = SOURCE.AVAILABILITY,
        TARGET.ITEM_TYPE     = SOURCE.ITEM_TYPE,
        TARGET.BATCH_ID     = SOURCE.BATCH_ID,
        TARGET.UPDATED_AT   = CURRENT_TIMESTAMP()

    -- INSERT NEW RECORD
    WHEN NOT MATCHED THEN
    INSERT (
        RESTAURANT_ID_FK,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE,
        BATCH_ID,
        CREATED_AT,
        UPDATED_AT
    )
    VALUES (
        SOURCE.RESTAURANT_ID_FK,
        SOURCE.ITEM_NAME,
        SOURCE.DESCRIPTION,
        SOURCE.PRICE,
        SOURCE.CATEGORY,
        SOURCE.AVAILABILITY,
        SOURCE.ITEM_TYPE,
        SOURCE.BATCH_ID,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );

    v_rows_inserted := (
        SELECT COUNT(*)
        FROM SILVER.MENU_SLV
        WHERE BATCH_ID = :p_batch_id
          AND CREATED_AT = UPDATED_AT
    );

    v_rows_updated := (
        SELECT COUNT(*)
        FROM SILVER.MENU_SLV
        WHERE BATCH_ID = :p_batch_id
          AND CREATED_AT <> UPDATED_AT
    );

    RETURN 'SUCCESS | Inserted=' || :v_rows_inserted || ', Updated=' || :v_rows_updated;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'FAILED | Batch=' || p_batch_id || ' | Error:' || SQLERRM;
END;
$$;


CALL SILVER.SP_MENU_BRONZE_TO_SILVER('123');

-- ----------------------------------------------------------------------------------------------------
-- SILVER TO GOLD
-- ----------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE GOLD.SP_MENU_SILVER_TO_GOLD(p_batch_id STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_rows_inserted INTEGER DEFAULT 0;
    v_rows_updated  INTEGER DEFAULT 0;
BEGIN

    -- STEP 1: CLOSE EXISTING ACTIVE RECORDS (SCD2 UPDATE)
    UPDATE GOLD.DIM_MENU TGT
    SET
        TGT.EFF_END_DATE = CURRENT_TIMESTAMP(),
        TGT.STATUS = FALSE
    FROM SILVER.MENU_SLV SRC
    WHERE
        TGT.MENU_ID = SRC.MENU_ID
        AND TGT.RESTAURANT_ID_FK = SRC.RESTAURANT_ID_FK
        AND TGT.STATUS = TRUE
        AND (
            COALESCE(TGT.ITEM_NAME, '')       <> COALESCE(SRC.ITEM_NAME, '') OR
            COALESCE(TGT.DESCRIPTION, '')     <> COALESCE(SRC.DESCRIPTION, '') OR
            COALESCE(TGT.PRICE, -1)            <> COALESCE(SRC.PRICE, -1) OR
            COALESCE(TGT.CATEGORY, '')         <> COALESCE(SRC.CATEGORY, '') OR
            COALESCE(TGT.AVAILABILITY, FALSE)  <> COALESCE(SRC.AVAILABILITY, FALSE) OR
            COALESCE(TGT.ITEM_TYPE, '')        <> COALESCE(SRC.ITEM_TYPE, '')
        );

    v_rows_updated := SQLROWCOUNT;

    -- STEP 2: INSERT NEW ACTIVE RECORDS
    INSERT INTO GOLD.DIM_MENU (
        MENU_DIM_HK,
        MENU_ID,
        RESTAURANT_ID_FK,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE,
        EFF_START_DATE,
        EFF_END_DATE,
        STATUS
    )
    SELECT
        HASH( SHA1_HEX( CONCAT(SRC.MENU_ID, SRC.RESTAURANT_ID_FK, SRC.ITEM_NAME, SRC.DESCRIPTION, SRC.PRICE, SRC.CATEGORY, SRC.AVAILABILITY, SRC.ITEM_TYPE))) AS MENU_DIM_HK,
        SRC.MENU_ID,
        SRC.RESTAURANT_ID_FK,
        SRC.ITEM_NAME,
        SRC.DESCRIPTION,
        SRC.PRICE,
        SRC.CATEGORY,
        SRC.AVAILABILITY,
        SRC.ITEM_TYPE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    FROM SILVER.MENU_SLV SRC
    LEFT JOIN GOLD.DIM_MENU TGT
        ON SRC.MENU_ID = TGT.MENU_ID
        AND SRC.RESTAURANT_ID_FK = TGT.RESTAURANT_ID_FK
        AND TGT.STATUS = TRUE
    WHERE
        TGT.MENU_ID IS NULL
        OR (
            COALESCE(TGT.ITEM_NAME, '')       <> COALESCE(SRC.ITEM_NAME, '') OR
            COALESCE(TGT.DESCRIPTION, '')     <> COALESCE(SRC.DESCRIPTION, '') OR
            COALESCE(TGT.PRICE, -1)            <> COALESCE(SRC.PRICE, -1) OR
            COALESCE(TGT.CATEGORY, '')         <> COALESCE(SRC.CATEGORY, '') OR
            COALESCE(TGT.AVAILABILITY, FALSE)  <> COALESCE(SRC.AVAILABILITY, FALSE) OR
            COALESCE(TGT.ITEM_TYPE, '')        <> COALESCE(SRC.ITEM_TYPE, '')
        );

    v_rows_inserted := SQLROWCOUNT;

    RETURN 'SUCCESS | Inserted: ' || v_rows_inserted || ' | Updated: ' || v_rows_updated;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'FAILED | ' || SQLERRM;
END;
$$;
-- ====================================================================================================