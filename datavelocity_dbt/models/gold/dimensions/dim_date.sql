-- models/gold/dim_date.sql
{{
  config(
    materialized='view',
    tags=['reference', 'dimension', 'date']
  )
}}

-- This is a reference model for the existing DIM_DATE table

SELECT
    DIM_DATE_HK,
    CALENDAR_DATE,
    YEAR,
    QUARTER,
    MONTH,
    WEEK,
    DAY_OF_YEAR,
    DAY_OF_WEEK,
    DAY_OF_THE_MONTH,
    DAY_NAME
FROM {{ source('gold', 'dim_date') }}