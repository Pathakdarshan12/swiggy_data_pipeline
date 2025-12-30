-- models/gold/fact_delivery.sql
{{
  config(
    materialized='view',
    tags=['reference', 'fact', 'delivery']
  )
}}

-- This is a reference model for the existing FACT_DELIVERY table

SELECT
    DELIVERY_ID,
    ORDER_ID,
    DELIVERY_AGENT_ID,
    CUSTOMER_ADDRESS_ID,
    CURRENT_STATUS,
    INITIAL_STATUS,
    ESTIMATED_TIME,
    DELIVERY_DATE,
    STATUS_UPDATED_AT,
    BATCH_ID,
    CREATED_AT,
    LOADED_AT
FROM {{ source('gold', 'fact_delivery') }}