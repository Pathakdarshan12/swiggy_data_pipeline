-- models/gold/dim_restaurant.sql
{{
  config(
    materialized='view',
    tags=['reference', 'dimension', 'restaurant']
  )
}}

-- This is a reference model for the existing DIM_RESTAURANT table

SELECT
    RESTAURANT_ID,
    FSSAI_REGISTRATION_NO,
    RESTAURANT_NAME,
    CUISINE_TYPE,
    PRICING_FOR_TWO,
    RESTAURANT_PHONE,
    OPERATING_HOURS,
    LOCATION_ID,
    ACTIVE_FLAG,
    OPEN_STATUS,
    LOCALITY,
    RESTAURANT_ADDRESS,
    LATITUDE,
    LONGITUDE,
    BATCH_ID,
    STATUS,
    EFF_START_DT,
    EFF_END_DT,
    CREATED_AT,
    UPDATED_AT
FROM {{ source('gold', 'dim_restaurant') }}
WHERE STATUS = 'ACTIVE'
    AND EFF_END_DT = '9999-12-31 23:59:59'::TIMESTAMP_TZ