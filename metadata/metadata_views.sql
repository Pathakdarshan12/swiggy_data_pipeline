-- =======================================================================================================================================
-- METADATA-DRIVEN UNIVERSAL ETL FRAMEWORK
-- Purpose: Create views that dynamically generate SQL for any entity based on metadata
-- =======================================================================================================================================
-- =======================================================================================================================================
-- VIEW 1: V_ENTITY_COLUMN_MAPPING
-- Purpose: Complete column mapping with all transformation details
-- =======================================================================================================================================
CREATE OR REPLACE VIEW COMMON.V_ENTITY_COLUMN_MAPPING AS
SELECT
    -- Source Information
    sfc.source_id,
    sfc.source_name,
    sfc.source_system,
    sfc.ingestion_type,
    sfc.landing_path,
    sfc.file_pattern,

    -- File Format Details
    ffm.format_type,
    ffm.delimiter,
    ffm.quote_char,
    ffm.header_present,

    -- Column Mapping
    fcm.mapping_id,
    fcm.file_column_name AS source_column_name,
    fcm.column_position,

    -- Field Details
    dfm.field_id,
    dfm.field_name AS target_column_name,
    dfm.data_type AS target_data_type,
    dfm.length,
    dfm.precision,
    dfm.scale,
    dfm.nullable_flag,
    dfm.pii_flag,

    -- Key Information
    fcm.is_primary_key,
    fcm.auto_increment_flag,
    fcm.default_value,

    -- Transformation Rules
    fcm.transformation_rule,
    fcm.validation_rule,

    -- Target Table Information
    (sfc.source_name) as temp_table,
    ttm.bronze_table,
    ttm.load_error_table,
    CONCAT('STG_', REPLACE(ttm.load_error_table,'LOAD_ERROR','DQ')) as stage_table,
    ttm.silver_table,
    ttm.gold_table,
    ttm.load_type,
    ttm.primary_key_fields,
    ttm.merge_key_fields,
    ttm.enable_raw_columns,
    ttm.enable_audit_columns,

    -- Active Flags
    fcm.active_flag AS mapping_active,
    sfc.active_flag AS source_active
FROM
    COMMON.SOURCE_FILE_CONFIG sfc
    JOIN COMMON.FILE_FORMAT_MASTER ffm ON sfc.file_format_id = ffm.file_format_id
    JOIN COMMON.FILE_COLUMN_MAPPING fcm ON sfc.source_id = fcm.source_id
    JOIN COMMON.DATA_FIELD_MASTER dfm ON fcm.field_id = dfm.field_id
    JOIN COMMON.TARGET_TABLE_MAPPING ttm ON sfc.source_id = ttm.source_id
WHERE
    fcm.active_flag = 'Y'
    AND sfc.active_flag = 'Y'
    AND ttm.active_flag = 'Y'
ORDER BY
    sfc.source_id,
    fcm.column_position;

-- =======================================================================================================================================
-- VIEW 2: V_BRONZE_COPY_SQL
-- Purpose: Generate COPY INTO statements for STAGE → BRONZE
-- =======================================================================================================================================
SELECT * FROM V_SILVER_MERGE_SQL;
CREATE OR REPLACE VIEW COMMON.V_BRONZE_COPY_SQL AS
WITH source_metadata AS (
    SELECT
        sfc.source_id,
        sfc.source_name,
        sfc.landing_path,
        sfc.ingestion_type,
        ttm.bronze_table,
        ttm.enable_raw_columns,
        ttm.enable_audit_columns,
        ffm.format_type,
        ffm.delimiter,
        ffm.quote_char,
        ffm.header_present
    FROM COMMON.SOURCE_FILE_CONFIG sfc
    JOIN COMMON.TARGET_TABLE_MAPPING ttm ON sfc.source_id = ttm.source_id
    LEFT JOIN COMMON.FILE_FORMAT_MASTER ffm ON sfc.file_format_id = ffm.file_format_id
    WHERE sfc.active_flag = 'Y'
),
column_data AS (
    SELECT
        fcm.source_id,
        fcm.file_column_name,
        fcm.column_position,
        fcm.is_primary_key,
        fcm.auto_increment_flag,
        fcm.default_value,
        fcm.transformation_rule,
        dfm.field_name,
        dfm.data_type
    FROM COMMON.FILE_COLUMN_MAPPING fcm
    JOIN COMMON.DATA_FIELD_MASTER dfm ON fcm.field_id = dfm.field_id
    WHERE fcm.active_flag = 'Y'
),
-- Add row numbers for non-PK columns (for BATCH)
column_data_numbered AS (
    SELECT
        cd.*,
        ROW_NUMBER() OVER (PARTITION BY cd.source_id ORDER BY cd.column_position) AS new_position
    FROM column_data cd
    WHERE cd.is_primary_key = FALSE
),
-- Build COPY SQL components for BATCH+CSV (EXCLUDE PRIMARY KEY)
batch_csv_copy AS (
    SELECT
        cdn.source_id,
        -- Target columns for COPY INTO (exclude primary key)
        LISTAGG(cdn.file_column_name, ', ')
            WITHIN GROUP (ORDER BY cdn.column_position) AS copy_target_columns,

        -- Source column positions ($1, $2, etc.)
        LISTAGG('$' || cdn.new_position || '::STRING AS ' || cdn.file_column_name, ', ')
            WITHIN GROUP (ORDER BY cdn.column_position) AS copy_source_columns
    FROM column_data_numbered cdn
    GROUP BY cdn.source_id
),
-- Build INSERT SQL components for STREAM (INCLUDE PRIMARY KEY)
stream_insert AS (
    SELECT
        cd.source_id,
        LISTAGG(cd.file_column_name, ', ')
            WITHIN GROUP (ORDER BY cd.column_position) AS stream_target_columns,
        LISTAGG('RECORD_CONTENT:' || cd.file_column_name || '::STRING', ', ')
            WITHIN GROUP (ORDER BY cd.column_position) AS stream_source_columns
    FROM column_data cd
    GROUP BY cd.source_id
),
-- Build temp table and bronze insert for BATCH (EXCLUDE PRIMARY KEY)
batch_bronze_parts AS (
    SELECT
        cd.source_id,
        -- Target columns (all fields including PK)
        LISTAGG(cd.field_name, ', ')
            WITHIN GROUP (ORDER BY cd.column_position) AS bronze_target_columns,

        -- Temp table columns (exclude primary key)
        LISTAGG(
            CASE WHEN cd.is_primary_key = FALSE
            THEN CONCAT(cd.field_name, ' VARCHAR')
            END,
            ', '
        ) WITHIN GROUP (ORDER BY cd.column_position) AS bronze_target_columns_typed,

        -- Transformation expressions (use default_value for PK, file_column_name for others)
        LISTAGG(
            CASE
                WHEN cd.is_primary_key = TRUE THEN COALESCE(cd.default_value, 'NULL')
                ELSE COALESCE(cd.transformation_rule, cd.file_column_name)
            END,
            ', '
        ) WITHIN GROUP (ORDER BY cd.column_position) AS bronze_transform_list,

        -- Raw columns (non-PK only)
        NULLIF(
            LISTAGG(
                CASE WHEN cd.is_primary_key = FALSE THEN cd.field_name || '_RAW' END,
                ', '
            ) WITHIN GROUP (ORDER BY cd.column_position),
            ''
        ) AS bronze_raw_columns,

        -- Raw source columns
        NULLIF(
            LISTAGG(
                CASE WHEN cd.is_primary_key = FALSE THEN cd.file_column_name END,
                ', '
            ) WITHIN GROUP (ORDER BY cd.column_position),
            ''
        ) AS bronze_raw_sources
    FROM column_data cd
    GROUP BY cd.source_id
),
-- Build temp table and bronze insert for STREAM (INCLUDE PRIMARY KEY)
stream_bronze_parts AS (
    SELECT
        cd.source_id,
        -- Target columns (all fields including PK)
        LISTAGG(cd.field_name, ', ')
            WITHIN GROUP (ORDER BY cd.column_position) AS bronze_target_columns,

        -- Temp table columns (include primary key)
        LISTAGG(CONCAT(cd.field_name, ' VARCHAR'), ', ')
            WITHIN GROUP (ORDER BY cd.column_position) AS bronze_target_columns_typed,

        -- Transformation expressions (use file_column_name for all including PK)
        LISTAGG(COALESCE(cd.transformation_rule, cd.file_column_name), ', ')
            WITHIN GROUP (ORDER BY cd.column_position) AS bronze_transform_list,

        -- Raw columns (non-PK only)
        NULLIF(
            LISTAGG(
                CASE WHEN cd.is_primary_key = FALSE THEN cd.field_name || '_RAW' END,
                ', '
            ) WITHIN GROUP (ORDER BY cd.column_position),
            ''
        ) AS bronze_raw_columns,

        -- Raw source columns
        NULLIF(
            LISTAGG(
                CASE WHEN cd.is_primary_key = FALSE THEN cd.file_column_name END,
                ', '
            ) WITHIN GROUP (ORDER BY cd.column_position),
            ''
        ) AS bronze_raw_sources
    FROM column_data cd
    GROUP BY cd.source_id
)
SELECT
    sm.source_id,
    sm.source_name,
    sm.landing_path,
    sm.bronze_table,
    sm.ingestion_type,
    sm.format_type,

    -- =====================================================================
    -- TEMP TABLE DDL
    -- =====================================================================
    'CREATE OR REPLACE TEMPORARY TABLE TEMP_' || sm.source_name || ' (' ||
    CASE
        WHEN UPPER(sm.ingestion_type) = 'BATCH' THEN bbp.bronze_target_columns_typed
        WHEN UPPER(sm.ingestion_type) = 'STREAM' THEN sbp.bronze_target_columns_typed
    END ||
    ');' AS temp_table_ddl,

    -- =====================================================================
    -- FILE COPY SQL (BATCH + CSV) (EXCLUDE PRIMARY KEY)
    -- =====================================================================
    CASE
        WHEN UPPER(sm.ingestion_type) = 'BATCH' AND UPPER(sm.format_type) = 'CSV'
        THEN 'COPY INTO TEMP_' || sm.source_name || ' (' ||
            bcs.copy_target_columns ||
            ') FROM (SELECT ' ||
            bcs.copy_source_columns ||
            ' FROM ''' || sm.landing_path || '{file_name}''' ||
            ') FILE_FORMAT = (TYPE = CSV ' ||
            'FIELD_DELIMITER = ''' || sm.delimiter || ''' ' ||
            IFF(sm.header_present = 'Y', 'SKIP_HEADER = 1 ', '') ||
            'FIELD_OPTIONALLY_ENCLOSED_BY = ''' || COALESCE(sm.quote_char, '"') || ''') ' ||
            'ON_ERROR = ABORT_STATEMENT;'
    END AS file_copy_sql,

    -- =====================================================================
    -- STREAM INSERT SQL (INTO TEMP TABLE, INCLUDE PRIMARY KEY)
    -- =====================================================================
    CASE
        WHEN UPPER(sm.ingestion_type) = 'STREAM'
        THEN 'INSERT INTO TEMP_' || sm.source_name || ' (' ||
            si.stream_target_columns ||
            ') SELECT ' ||
            si.stream_source_columns ||
            ' FROM ' || sm.landing_path ||
            ' WHERE METADATA$ACTION = ''INSERT'' ' ||
            'AND METADATA$ISUPDATE = FALSE;'
    END AS stream_insert_sql,

    -- =====================================================================
    -- BRONZE INSERT SQL
    -- =====================================================================
    'INSERT INTO ' || sm.bronze_table || ' (' ||
    CASE
        WHEN UPPER(sm.ingestion_type) = 'BATCH' THEN bbp.bronze_target_columns
        WHEN UPPER(sm.ingestion_type) = 'STREAM' THEN sbp.bronze_target_columns
    END ||

    -- Add raw columns if enabled
    CASE
        WHEN sm.enable_raw_columns = 'Y' AND
             CASE
                 WHEN UPPER(sm.ingestion_type) = 'BATCH' THEN bbp.bronze_raw_columns
                 WHEN UPPER(sm.ingestion_type) = 'STREAM' THEN sbp.bronze_raw_columns
             END IS NOT NULL
        THEN ', ' || CASE
                         WHEN UPPER(sm.ingestion_type) = 'BATCH' THEN bbp.bronze_raw_columns
                         WHEN UPPER(sm.ingestion_type) = 'STREAM' THEN sbp.bronze_raw_columns
                     END
        ELSE ''
    END ||

    -- Add audit columns if enabled
    CASE
        WHEN sm.enable_audit_columns = 'Y'
        THEN ', INGEST_RUN_ID, CREATED_AT, UPDATED_AT'
        ELSE ''
    END ||

    ') SELECT ' ||
    CASE
        WHEN UPPER(sm.ingestion_type) = 'BATCH' THEN bbp.bronze_transform_list
        WHEN UPPER(sm.ingestion_type) = 'STREAM' THEN sbp.bronze_transform_list
    END ||

    -- Add raw source columns if enabled
    CASE
        WHEN sm.enable_raw_columns = 'Y' AND
             CASE
                 WHEN UPPER(sm.ingestion_type) = 'BATCH' THEN bbp.bronze_raw_sources
                 WHEN UPPER(sm.ingestion_type) = 'STREAM' THEN sbp.bronze_raw_sources
             END IS NOT NULL
        THEN ', ' || CASE
                         WHEN UPPER(sm.ingestion_type) = 'BATCH' THEN bbp.bronze_raw_sources
                         WHEN UPPER(sm.ingestion_type) = 'STREAM' THEN sbp.bronze_raw_sources
                     END
        ELSE ''
    END ||

    -- Add audit values if enabled
    CASE
        WHEN sm.enable_audit_columns = 'Y'
        THEN ', {ingest_run_id}, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()'
        ELSE ''
    END ||

    ' FROM TEMP_' || sm.source_name || ';' AS bronze_insert_sql

FROM source_metadata sm
LEFT JOIN batch_csv_copy bcs ON sm.source_id = bcs.source_id
LEFT JOIN stream_insert si ON sm.source_id = si.source_id
LEFT JOIN batch_bronze_parts bbp ON sm.source_id = bbp.source_id
LEFT JOIN stream_bronze_parts sbp ON sm.source_id = sbp.source_id;

-- =======================================================================================================================================
-- VIEW 3: V_SILVER_MERGE_SQL
-- Purpose: Generate MERGE statements for BRONZE → SILVER
-- =======================================================================================================================================
CREATE OR REPLACE VIEW COMMON.V_GOLD_SCD2_SQL AS
WITH scd2_specs AS (
    SELECT
        source_id,
        source_name,
        silver_table,
        gold_table,
        load_type,
        primary_key_fields,

        -- Hash calculation for SOURCE table (with SRC. prefix)
        'SHA2_HEX(CONCAT_WS(''|'', ' || LISTAGG(
            'COALESCE(' || CASE
                WHEN target_data_type IN ('INTEGER', 'NUMBER', 'DATE', 'TIMESTAMP') THEN 'TO_VARCHAR(SRC.' || target_column_name || ')'
                ELSE 'SRC.' || target_column_name
            END || ', '''')',
            ', '
        ) WITHIN GROUP (ORDER BY column_position) || '))' AS src_hash_expression,

        -- Hash calculation for TARGET table (with TGT. prefix)
        'SHA2_HEX(CONCAT_WS(''|'', ' || LISTAGG(
            'COALESCE(' || CASE
                WHEN target_data_type IN ('INTEGER', 'NUMBER', 'DATE', 'TIMESTAMP') THEN 'TO_VARCHAR(TGT.' || target_column_name || ')'
                ELSE 'TGT.' || target_column_name
            END || ', '''')',
            ', '
        ) WITHIN GROUP (ORDER BY column_position) || '))' AS tgt_hash_expression,

        -- All columns for INSERT (with SRC. prefix for staging)
        LISTAGG('SRC.' || target_column_name, ', ') WITHIN GROUP (ORDER BY column_position) AS insert_columns,

        -- Columns without prefix for INSERT INTO clause
        LISTAGG(target_column_name, ', ') WITHIN GROUP (ORDER BY column_position) AS column_list,

        -- Primary key column (assuming first PK is surrogate key)
        MAX(CASE WHEN is_primary_key THEN target_column_name END) AS pk_column
    FROM
        COMMON.V_ENTITY_COLUMN_MAPPING
    GROUP BY
        source_id,
        source_name,
        silver_table,
        gold_table,
        load_type,
        primary_key_fields
)
SELECT
    source_id,
    source_name,
    silver_table,
    gold_table,

    -- SCD2 Staging Table SQL
    'CREATE OR REPLACE TEMPORARY TABLE TEMP_' || UPPER(source_name) || '_STAGING AS
SELECT
    SRC.*,
    ' || src_hash_expression || ' AS CURRENT_HASH,
    TGT.' || pk_column || ' AS EXISTING_ID,
    ' || tgt_hash_expression || ' AS EXISTING_HASH,

    CASE
        WHEN TGT.' || pk_column || ' IS NULL THEN ''INSERT''
        WHEN ' || src_hash_expression || ' != ' || tgt_hash_expression || ' THEN ''UPDATE''
        ELSE ''UNCHANGED''
    END AS SCD_ACTION

FROM ' || silver_table || ' SRC
LEFT JOIN ' || gold_table || ' TGT
    ON TGT.' || pk_column || ' = SRC.' || pk_column || '
WHERE SRC.BATCH_ID = :P_BATCH_ID;' AS staging_sql,

    -- SCD2 Expire SQL
    'UPDATE ' || gold_table || '
SET
    STATUS = ''INACTIVE'',
    EFF_END_DT = CURRENT_TIMESTAMP(),
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE ' || pk_column || ' IN (
    SELECT EXISTING_ID
    FROM TEMP_' || UPPER(source_name) || '_STAGING
    WHERE SCD_ACTION = ''UPDATE''
);' AS expire_sql,

    -- SCD2 Insert SQL
    'INSERT INTO ' || gold_table || ' (
    ' || column_list || ',
    STATUS,
    EFF_START_DT,
    EFF_END_DT,
    BATCH_ID,
    CREATED_AT,
    UPDATED_AT
)
SELECT
    ' || insert_columns || ',
    ''ACTIVE'',
    CURRENT_TIMESTAMP(),
    ''9999-12-31 23:59:59''::TIMESTAMP_TZ,
    SRC.BATCH_ID,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
FROM TEMP_' || UPPER(source_name) || '_STAGING SRC
WHERE SCD_ACTION IN (''INSERT'', ''UPDATE'');' AS insert_sql

FROM scd2_specs;
-- =======================================================================================================================================
-- VIEW 5: VW_INGESTION_PIPELINE_SQL
-- Purpose: Combine all STAGE → GOLD Dynamic Queries
-- =======================================================================================================================================
CREATE OR REPLACE VIEW COMMON.VW_INGESTION_PIPELINE_SQL AS
SELECT
    B.SOURCE_ID,
    B.SOURCE_NAME,
    B.LANDING_PATH,
    B.SOURCE_NAME as temp_table,
    B.BRONZE_TABLE,
    REPLACE(B.BRONZE_TABLE,'BRZ','LOAD_ERROR') as load_error_table,
    REPLACE(REPLACE(B.BRONZE_TABLE,'BRONZE.','STG_'), 'BRZ', 'DQ') as stage_table,
    G.SILVER_TABLE,
    G.GOLD_TABLE,
    B.INGESTION_TYPE,
    B.FORMAT_TYPE,
    B.TEMP_TABLE_DDL,
    B.FILE_COPY_SQL,
    B.STREAM_INSERT_SQL,
    B.BRONZE_INSERT_SQL,
    S.MERGE_SQL,
    S.APPEND_SQL,
    G.STAGING_SQL,
    G.EXPIRE_SQL,
    G.INSERT_SQL
FROM V_BRONZE_COPY_SQL B
JOIN V_SILVER_MERGE_SQL S
    ON B.SOURCE_ID = S.SOURCE_ID
JOIN V_GOLD_SCD2_SQL G
    ON B.SOURCE_ID = G.SOURCE_ID;
-- =======================================================================================================================================