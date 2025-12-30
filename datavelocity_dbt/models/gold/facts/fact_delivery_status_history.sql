-- models/gold/fact_delivery_status_history.sql
{{
  config(
    materialized='view',
    tags=['reference', 'fact', 'delivery_history']
  )
}}

-- This is a reference model for the existing FACT_DELIVERY_STATUS_HISTORY table

SELECT
    STATUS_HISTORY_KEY,
    DELIVERY_ID,
    OLD_STATUS,
    NEW_STATUS,
    STATUS_CHANGED_AT,
    BATCH_ID
FROM {{ source('gold', 'fact_delivery_status_history') }}