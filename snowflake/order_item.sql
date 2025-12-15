-- ====================================================================================================
-- ORDER_ITEM
-- ====================================================================================================
-- CHANGE_CONTEXT
USE ROLE ACCOUNTADMIN;
USE DATABASE SWIGGY;
USE SCHEMA SILVER;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_ITEM_BRZ (
    ORDER_ITEM_ID TEXT COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    ORDER_ID TEXT COMMENT 'ORDER FK(SOURCE SYSTEM)',
    MENU_ID TEXT COMMENT 'MENU FK(SOURCE SYSTEM)',
    QUANTITY TEXT,
    PRICE TEXT,
    SUBTOTAL TEXT,
    CREATED_AT TEXT,
    UPDATED_AT TEXT,
    BATCH_ID STRING(36)
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_ITEM_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDER_ITEM_SLV (
    ORDER_ITEM_SLV_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    ORDER_ITEM_ID INTEGER COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    ORDER_ID INTEGER COMMENT 'ORDER FK(SOURCE SYSTEM)',
    MENU_ID INTEGER COMMENT 'MENU FK(SOURCE SYSTEM)',
    QUANTITY INTEGER,
    PRICE NUMBER(10, 2),
    SUBTOTAL NUMBER(10, 2),
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(36)
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE FACT_ORDER_ITEM
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER_ITEM (
    ORDER_ITEM_KEY INTEGER PRIMARY KEY AUTOINCREMENT COMMENT 'SURROGATE KEY (EDW)',
    ORDER_ITEM_ID INTEGER UNIQUE COMMENT 'BUSINESS KEY (SOURCE SYSTEM)',
    ORDER_ID INTEGER COMMENT 'ORDER FK(SOURCE SYSTEM)',
    MENU_ID INTEGER COMMENT 'MENU FK(SOURCE SYSTEM)',
    QUANTITY INTEGER,
    PRICE NUMBER(10, 2),
    SUBTOTAL NUMBER(10, 2),
    BATCH_ID STRING(36),
    CREATED_AT TIMESTAMP_TZ,
    LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'FACT TABLE FOR ORDER ITEMS - APPEND ONLY';

-- =====================================================
-- ORDER_ITEM PROCEDURES (APPEND-ONLY)
-- =====================================================

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDER_ITEM STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BRONZE.SP_ORDER_ITEM_STAGE_TO_BRONZE(
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_ORDER_ITEM_LOAD(
        ORDER_ITEM_ID TEXT,
        ORDER_ID TEXT,
        MENU_ID TEXT,
        QUANTITY TEXT,
        PRICE TEXT,
        SUBTOTAL TEXT
    );

    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_ORDER_ITEM_LOAD (
            ORDER_ITEM_ID, ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL
        )
        FROM (
            SELECT
                $1::STRING AS ORDER_ITEM_ID,
                $2::STRING AS ORDER_ID,
                $3::STRING AS MENU_ID,
                $4::STRING AS QUANTITY,
                $5::STRING AS PRICE,
                $6::STRING AS SUBTOTAL
            FROM ' || P_FILE_PATH || '
        )
        FILE_FORMAT = (FORMAT_NAME = ''BRONZE.CSV_FILE_FORMAT'')
        ON_ERROR = ABORT_STATEMENT
    ';

    INSERT INTO BRONZE.ORDER_ITEM_BRZ (
        ORDER_ITEM_ID, ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL,
        CREATED_AT, UPDATED_AT, BATCH_ID
    )
    SELECT
        ORDER_ITEM_ID, ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :P_BATCH_ID
    FROM TEMP_ORDER_ITEM_LOAD;

    SELECT COUNT(*) INTO :V_ROWS_LOADED FROM TEMP_ORDER_ITEM_LOAD;
    DROP TABLE IF EXISTS TEMP_ORDER_ITEM_LOAD;

    RETURN 'Successfully loaded ' || :V_ROWS_LOADED || ' rows with batch_id: ' || :P_BATCH_ID;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDER_ITEM BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SILVER.SP_ORDER_ITEM_BRONZE_TO_SILVER(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_PROCESSED INTEGER DEFAULT 0;
BEGIN
    -- Simple INSERT for order items (append-only)
    INSERT INTO SILVER.ORDER_ITEM_SLV (
        ORDER_ITEM_ID, ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL,
        CREATED_AT, UPDATED_AT, BATCH_ID
    )
    SELECT
        TRY_CAST(ORDER_ITEM_ID AS INTEGER) AS ORDER_ITEM_ID,
        TRY_CAST(ORDER_ID AS INTEGER) AS ORDER_ID,
        TRY_CAST(MENU_ID AS INTEGER) AS MENU_ID,
        TRY_CAST(QUANTITY AS INTEGER) AS QUANTITY,
        TRY_CAST(PRICE AS NUMBER(10, 2)) AS PRICE,
        TRY_CAST(SUBTOTAL AS NUMBER(10, 2)) AS SUBTOTAL,
        TRY_CAST(CREATED_AT AS TIMESTAMP_TZ) AS CREATED_AT,
        TRY_CAST(UPDATED_AT AS TIMESTAMP_TZ) AS UPDATED_AT,
        BATCH_ID
    FROM BRONZE.ORDER_ITEM_BRZ
    WHERE BATCH_ID = :P_BATCH_ID
        AND NOT EXISTS (
            SELECT 1 FROM SILVER.ORDER_ITEM_SLV TGT
            WHERE TGT.ORDER_ITEM_ID = TRY_CAST(ORDER_ITEM_BRZ.ORDER_ITEM_ID AS INTEGER)
        );

    V_ROWS_PROCESSED := SQLROWCOUNT;

    RETURN 'Successfully inserted ' || :V_ROWS_PROCESSED ||
           ' order items for batch_id: ' || :P_BATCH_ID;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDER_ITEM SILVER TO GOLD (APPEND-ONLY)
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_ORDER_ITEM_SILVER_TO_GOLD(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_INSERTED INTEGER DEFAULT 0;
BEGIN
    -- Simple INSERT - order items are immutable
    INSERT INTO GOLD.FACT_ORDER_ITEM (
        ORDER_ITEM_ID, ORDER_ID, MENU_ID, QUANTITY, PRICE, SUBTOTAL,
        BATCH_ID, CREATED_AT
    )
    SELECT
        SRC.ORDER_ITEM_ID,
        SRC.ORDER_ID,
        SRC.MENU_ID,
        SRC.QUANTITY,
        SRC.PRICE,
        SRC.SUBTOTAL,
        SRC.BATCH_ID,
        SRC.CREATED_AT
    FROM SILVER.ORDER_ITEM_SLV SRC
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND NOT EXISTS (
            SELECT 1 FROM GOLD.FACT_ORDER_ITEM TGT
            WHERE TGT.ORDER_ITEM_ID = SRC.ORDER_ITEM_ID
        );

    V_ROWS_INSERTED := SQLROWCOUNT;

    RETURN 'Successfully inserted ' || :V_ROWS_INSERTED ||
           ' order items for batch_id: ' || :P_BATCH_ID;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;
-- ====================================================================================================