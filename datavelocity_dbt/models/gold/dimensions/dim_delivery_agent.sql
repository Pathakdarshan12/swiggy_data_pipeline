-- models/gold/dim_delivery_agent.sql
{{
  config(
    materialized='view',
    tags=['reference', 'dimension', 'delivery_agent']
  )
}}

-- This is a reference model for the existing DIM_DELIVERY_AGENT table

SELECT
    DELIVERY_AGENT_ID,
    DELIVERY_AGENT_NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID,
    IS_ACTIVE,
    GENDER,
    RATING,
    EFF_START_DATE,
    EFF_END_DATE,
    STATUS,
    BATCH_ID
FROM {{ source('gold', 'dim_delivery_agent') }}
WHERE STATUS = 'ACTIVE'