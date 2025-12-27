{{
    config(
        materialized='view',
        tags=['analytics', 'customer', 'daily'],
        description='Customer ordering time patterns and preferences'
    )
}}

SELECT
    o.CUSTOMER_ID,
    c.NAME AS CUSTOMER_NAME,

    -- Day of Week Patterns
    d.DAY_NAME,
    d.DAY_OF_WEEK,
    COUNT(DISTINCT o.ORDER_ID) AS ORDERS_BY_DAY,
    SUM(o.TOTAL_AMOUNT) AS REVENUE_BY_DAY,

    -- Time of Day Patterns
    HOUR(o.ORDER_DATE) AS ORDER_HOUR,
    CASE
        WHEN HOUR(o.ORDER_DATE) BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 12 AND 16 THEN 'AFTERNOON'
        WHEN HOUR(o.ORDER_DATE) BETWEEN 17 AND 21 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS TIME_OF_DAY,

    -- Weekend vs Weekday
    CASE
        WHEN d.DAY_OF_WEEK IN (6, 7) THEN 'WEEKEND'
        ELSE 'WEEKDAY'
    END AS DAY_TYPE,

    CURRENT_TIMESTAMP() AS CREATED_AT
FROM {{ ref('fact_order') }} o
INNER JOIN {{ ref('dim_customer') }} c
    ON o.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN {{ ref('dim_date') }} d
    ON DATE(o.ORDER_DATE) = d.CALENDAR_DATE
WHERE c.STATUS = 'ACTIVE'
GROUP BY
    o.CUSTOMER_ID, c.NAME, d.DAY_NAME, d.DAY_OF_WEEK,
    ORDER_HOUR, TIME_OF_DAY, DAY_TYPE