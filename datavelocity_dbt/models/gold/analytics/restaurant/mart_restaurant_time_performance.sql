{{
    config(
        materialized='view',
        tags=['analytics', 'restaurant', 'daily'],
        description='Restaurant performance by time patterns'
    )
}}

SELECT
    r.RESTAURANT_ID,
    r.RESTAURANT_NAME,
    r.CUISINE_TYPE,

    -- Time Dimensions
    d.DAY_NAME,
    d.DAY_OF_WEEK,
    CASE
        WHEN d.DAY_OF_WEEK IN (6, 7) THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END AS DAY_TYPE,
    HOUR(o.ORDER_DATE) AS ORDER_HOUR,
    CASE
        WHEN HOUR(o.ORDER_DATE) BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 12 AND 16 THEN 'AFTERNOON'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 17 AND 21 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS TIME_OF_DAY,

    -- Performance Metrics
    COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS