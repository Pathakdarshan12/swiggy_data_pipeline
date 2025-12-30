-- models/gold/dim_customer.sql
{{
  config(
    materialized='view',
    tags=['reference', 'dimension', 'customer', 'pii']
  )
}}

-- This is a reference model for the existing DIM_CUSTOMER table

SELECT
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    STATUS,
    EFF_START_DT,
    EFF_END_DT,
    BATCH_ID,
    CREATED_AT,
    UPDATED_AT
FROM {{ source('gold', 'dim_customer') }}
WHERE STATUS = 'ACTIVE'
    AND EFF_END_DT = '9999-12-31 23:59:59'::TIMESTAMP_TZ