-- ====================================================================================================
-- ORDER
-- ====================================================================================================
-- CHANGE CONTEXT
USE DATABASE SWIGGY;
USE SCHEMA BRONZE;
USE WAREHOUSE ADHOC_WH;

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_BRZ
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDERS_BRZ (
    ORDER_ID TEXT COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    CUSTOMER_ID TEXT COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID TEXT COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE TEXT,
    TOTAL_AMOUNT TEXT,
    STATUS TEXT,
    PAYMENT_METHOD TEXT,
    CREATED_AT TEXT,
    UPDATED_AT TEXT,
    BATCH_ID STRING(36)
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE ORDER_SLV
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER.ORDERS_SLV (
    ORDERS_SLV_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    ORDER_ID INTEGER COMMENT 'PRIMARY KEY (SOURCE SYSTEM)',
    CUSTOMER_ID_FK INTEGER COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID_FK INTEGER COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE TIMESTAMP_TZ,
    TOTAL_AMOUNT NUMBER(10, 2),
    STATUS STRING(50),
    PAYMENT_METHOD STRING(50),
    CREATED_AT TIMESTAMP_TZ,
    UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(36)
);

-- ----------------------------------------------------------------------------------------------------
-- CREATE DIM_ORDER
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER (
    ORDER_KEY INTEGER PRIMARY KEY AUTOINCREMENT COMMENT 'SURROGATE KEY (EDW)',
    ORDER_ID INTEGER UNIQUE COMMENT 'BUSINESS KEY (SOURCE SYSTEM)',
    CUSTOMER_ID_FK INTEGER COMMENT 'CUSTOMER FK(SOURCE SYSTEM)',
    RESTAURANT_ID_FK INTEGER COMMENT 'RESTAURANT FK(SOURCE SYSTEM)',
    ORDER_DATE TIMESTAMP_TZ,
    TOTAL_AMOUNT NUMBER(10, 2),
    CURRENT_STATUS STRING(50),
    INITIAL_STATUS STRING(50),
    PAYMENT_METHOD STRING(50),
    STATUS_UPDATED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(36),
    CREATED_AT TIMESTAMP_TZ,
    LOADED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'FACT TABLE FOR ORDERS WITH STATUS TRACKING';

-- ----------------------------------------------------------------------------------------------------
-- STATUS HISTORY TABLES (OPTIONAL - FOR DETAILED AUDIT TRAIL)
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE GOLD.FACT_ORDER_STATUS_HISTORY (
    STATUS_HISTORY_KEY INTEGER PRIMARY KEY AUTOINCREMENT,
    ORDER_ID INTEGER,
    OLD_STATUS STRING(50),
    NEW_STATUS STRING(50),
    STATUS_CHANGED_AT TIMESTAMP_TZ,
    BATCH_ID STRING(36)
)
COMMENT = 'AUDIT TRAIL FOR ORDER STATUS CHANGES';

-- =====================================================
-- ORDERS PROCEDURES
-- =====================================================

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDERS STAGE TO BRONZE
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE BRONZE.SP_ORDER_STAGE_TO_BRONZE(
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
    CREATE OR REPLACE TEMPORARY TABLE TEMP_ORDERS_LOAD(
        ORDER_ID TEXT,
        CUSTOMER_ID TEXT,
        RESTAURANT_ID TEXT,
        ORDER_DATE TEXT,
        TOTAL_AMOUNT TEXT,
        STATUS TEXT,
        PAYMENT_METHOD TEXT
    );

    EXECUTE IMMEDIATE
    '
        COPY INTO TEMP_ORDERS_LOAD (
            ORDER_ID, CUSTOMER_ID, RESTAURANT_ID, ORDER_DATE,
            TOTAL_AMOUNT, STATUS, PAYMENT_METHOD
        )
        FROM (
            SELECT
                $1::STRING AS ORDER_ID,
                $2::STRING AS CUSTOMER_ID,
                $3::STRING AS RESTAURANT_ID,
                $4::STRING AS ORDER_DATE,
                $5::STRING AS TOTAL_AMOUNT,
                $6::STRING AS STATUS,
                $7::STRING AS PAYMENT_METHOD
            FROM ' || P_FILE_PATH || '
        )
        FILE_FORMAT = (FORMAT_NAME = ''BRONZE.CSV_FILE_FORMAT'')
        ON_ERROR = ABORT_STATEMENT
    ';

    INSERT INTO BRONZE.ORDERS_BRZ (
        ORDER_ID, CUSTOMER_ID, RESTAURANT_ID, ORDER_DATE, TOTAL_AMOUNT,
        STATUS, PAYMENT_METHOD, CREATED_AT, UPDATED_AT, BATCH_ID
    )
    SELECT
        ORDER_ID, CUSTOMER_ID, RESTAURANT_ID, ORDER_DATE, TOTAL_AMOUNT,
        STATUS, PAYMENT_METHOD,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :P_BATCH_ID
    FROM TEMP_ORDERS_LOAD;

    SELECT COUNT(*) INTO :V_ROWS_LOADED FROM TEMP_ORDERS_LOAD;
    DROP TABLE IF EXISTS TEMP_ORDERS_LOAD;

    RETURN 'Successfully loaded ' || :V_ROWS_LOADED || ' rows with batch_id: ' || :P_BATCH_ID;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDERS BRONZE TO SILVER
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SILVER.SP_ORDER_BRONZE_TO_SILVER(
    P_BATCH_ID STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    V_ROWS_PROCESSED INTEGER DEFAULT 0;
BEGIN
    MERGE INTO SILVER.ORDERS_SLV AS TGT
    USING (
        SELECT
            TRY_CAST(ORDER_ID AS INTEGER) AS ORDER_ID,
            TRY_CAST(CUSTOMER_ID AS INTEGER) AS CUSTOMER_ID_FK,
            TRY_CAST(RESTAURANT_ID AS INTEGER) AS RESTAURANT_ID_FK,
            TRY_CAST(ORDER_DATE AS TIMESTAMP_TZ) AS ORDER_DATE,
            TRY_CAST(TOTAL_AMOUNT AS NUMBER(10, 2)) AS TOTAL_AMOUNT,
            STATUS,
            PAYMENT_METHOD,
            TRY_CAST(CREATED_AT AS TIMESTAMP_TZ) AS CREATED_AT,
            TRY_CAST(UPDATED_AT AS TIMESTAMP_TZ) AS UPDATED_AT,
            BATCH_ID
        FROM BRONZE.ORDERS_BRZ
        WHERE BATCH_ID = :P_BATCH_ID
    ) AS SRC
    ON TGT.ORDER_ID = SRC.ORDER_ID

    WHEN MATCHED THEN
        UPDATE SET
            TGT.STATUS = SRC.STATUS,
            TGT.TOTAL_AMOUNT = SRC.TOTAL_AMOUNT,
            TGT.UPDATED_AT = SRC.UPDATED_AT,
            TGT.BATCH_ID = SRC.BATCH_ID

    WHEN NOT MATCHED THEN
        INSERT (
            ORDER_ID, CUSTOMER_ID_FK, RESTAURANT_ID_FK, ORDER_DATE,
            TOTAL_AMOUNT, STATUS, PAYMENT_METHOD, CREATED_AT, UPDATED_AT, BATCH_ID
        )
        VALUES (
            SRC.ORDER_ID, SRC.CUSTOMER_ID_FK, SRC.RESTAURANT_ID_FK, SRC.ORDER_DATE,
            SRC.TOTAL_AMOUNT, SRC.STATUS, SRC.PAYMENT_METHOD, SRC.CREATED_AT,
            SRC.UPDATED_AT, SRC.BATCH_ID
        );

    V_ROWS_PROCESSED := (SELECT COUNT(*) FROM SILVER.ORDERS_SLV WHERE BATCH_ID = :P_BATCH_ID);

    RETURN 'Successfully merged data. Batch_id: ' || :P_BATCH_ID ||
           '. Total rows processed: ' || :V_ROWS_PROCESSED;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;

-- ----------------------------------------------------------------------------------------------------
-- PROCEDURE: ORDERS SILVER TO GOLD (WITH STATUS TRACKING)
-- ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GOLD.SP_ORDER_SILVER_TO_GOLD(
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
BEGIN
    V_CURRENT_TIMESTAMP := CURRENT_TIMESTAMP();

    -- Step 1: Update existing orders if status changed
    UPDATE GOLD.FACT_ORDER AS TGT
    SET
        CURRENT_STATUS = SRC.STATUS,
        TOTAL_AMOUNT = SRC.TOTAL_AMOUNT,
        STATUS_UPDATED_AT = :V_CURRENT_TIMESTAMP
    FROM (
        SELECT
            ORDER_ID, STATUS, TOTAL_AMOUNT
        FROM SILVER.ORDERS_SLV
        WHERE BATCH_ID = :P_BATCH_ID
    ) AS SRC
    WHERE TGT.ORDER_ID = SRC.ORDER_ID
        AND TGT.CURRENT_STATUS != SRC.STATUS;

    V_ROWS_UPDATED := SQLROWCOUNT;

    -- Step 2: Log status changes to history table
    INSERT INTO GOLD.FACT_ORDER_STATUS_HISTORY (
        ORDER_ID, OLD_STATUS, NEW_STATUS, STATUS_CHANGED_AT, BATCH_ID
    )
    SELECT
        TGT.ORDER_ID,
        TGT.CURRENT_STATUS AS OLD_STATUS,
        SRC.STATUS AS NEW_STATUS,
        :V_CURRENT_TIMESTAMP AS STATUS_CHANGED_AT,
        :P_BATCH_ID
    FROM GOLD.FACT_ORDER TGT
    INNER JOIN SILVER.ORDERS_SLV SRC ON TGT.ORDER_ID = SRC.ORDER_ID
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND TGT.CURRENT_STATUS != SRC.STATUS;

    -- Step 3: Insert new orders
    INSERT INTO GOLD.FACT_ORDER (
        ORDER_ID, CUSTOMER_ID_FK, RESTAURANT_ID_FK, ORDER_DATE, TOTAL_AMOUNT,
        CURRENT_STATUS, INITIAL_STATUS, PAYMENT_METHOD, STATUS_UPDATED_AT,
        BATCH_ID, CREATED_AT
    )
    SELECT
        SRC.ORDER_ID,
        SRC.CUSTOMER_ID_FK,
        SRC.RESTAURANT_ID_FK,
        SRC.ORDER_DATE,
        SRC.TOTAL_AMOUNT,
        SRC.STATUS AS CURRENT_STATUS,
        SRC.STATUS AS INITIAL_STATUS,
        SRC.PAYMENT_METHOD,
        :V_CURRENT_TIMESTAMP AS STATUS_UPDATED_AT,
        SRC.BATCH_ID,
        SRC.CREATED_AT
    FROM SILVER.ORDERS_SLV SRC
    WHERE SRC.BATCH_ID = :P_BATCH_ID
        AND NOT EXISTS (
            SELECT 1 FROM GOLD.FACT_ORDER TGT
            WHERE TGT.ORDER_ID = SRC.ORDER_ID
        );

    V_ROWS_INSERTED := SQLROWCOUNT;

    RETURN 'Orders processed successfully. Batch_id: ' || :P_BATCH_ID ||
           '. New orders: ' || :V_ROWS_INSERTED ||
           '. Updated orders: ' || :V_ROWS_UPDATED;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error occurred: ' || SQLERRM;
END;
$$;
-- ====================================================================================================