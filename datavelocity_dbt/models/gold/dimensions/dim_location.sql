-- models/gold/dim_location.sql
{{
  config(
    materialized='view',
    tags=['reference', 'dimension', 'location']
  )
}}

-- This is a reference model for the existing DIM_LOCATION table
-- It does not create the table but documents its structure for use in other models

SELECT
    LOCATION_ID,
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
FROM {{ source('gold', 'dim_location') }}
WHERE STATUS = 'ACTIVE'
    AND EFF_END_DT = '9999-12-31 23:59:59'::TIMESTAMP_TZ