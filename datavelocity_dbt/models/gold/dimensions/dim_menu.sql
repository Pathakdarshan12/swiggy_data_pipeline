-- models/gold/dim_menu.sql
{{
  config(
    materialized='view',
    tags=['reference', 'dimension', 'menu']
  )
}}

-- This is a reference model for the existing DIM_MENU table

SELECT
    MENU_ID,
    RESTAURANT_ID,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,
    EFF_START_DATE,
    EFF_END_DATE,
    STATUS,
    CREATED_AT,
    UPDATED_AT,
    BATCH_ID
FROM {{ source('gold', 'dim_menu') }}
WHERE STATUS = TRUE