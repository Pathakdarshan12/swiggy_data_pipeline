-- models/gold/facts/fact_order_item.sql  (or models/gold/fact_order_item.sql)
{{
  config(
    materialized='view',
    tags=['reference', 'fact', 'order_item']
  )
}}

-- This is a reference model for the existing FACT_ORDER_ITEM table

SELECT
    ORDER_ITEM_ID,
    ORDER_ID,
    MENU_ID,
    QUANTITY,
    UNIT_PRICE,
    TOTAL_PRICE,
    SPECIAL_INSTRUCTIONS,
    BATCH_ID,
    CREATED_AT,
    UPDATED_AT
FROM {{ source('gold', 'fact_order_item') }}