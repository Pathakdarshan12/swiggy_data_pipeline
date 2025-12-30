-- models/gold/fact_order.sql
{{
  config(
    materialized='view',
    tags=['reference', 'fact', 'order']
  )
}}

-- This is a reference model for the existing FACT_ORDER table

SELECT
    ORDER_ID,
    CUSTOMER_ID,
    RESTAURANT_ID,
    ORDER_DATE,
    TOTAL_AMOUNT,
    CURRENT_STATUS,
    INITIAL_STATUS,
    PAYMENT_METHOD,
    STATUS_UPDATED_AT,
    BATCH_ID,
    CREATED_AT,
    UPDATED_AT
FROM {{ source('gold', 'fact_order') }}