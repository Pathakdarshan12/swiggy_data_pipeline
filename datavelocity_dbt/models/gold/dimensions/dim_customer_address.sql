-- models/gold/dim_customer_address.sql
{{
  config(
    materialized='view',
    tags=['reference', 'dimension', 'customer_address']
  )
}}

-- This is a reference model for the existing DIM_CUSTOMER_ADDRESS table

SELECT
    CUSTOMER_ADDRESS_ID,
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
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    BATCH_ID,
    EFF_START_DATE,
    EFF_END_DATE,
    STATUS,
    CREATED_AT,
    UPDATED_AT
FROM {{ source('gold', 'dim_customer_address') }}
WHERE STATUS = 'ACTIVE'