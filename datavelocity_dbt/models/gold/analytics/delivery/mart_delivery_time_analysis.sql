{{
    config(
        materialized='view',
        tags=['analytics', 'delivery', 'daily'],
        description='Delivery time performance and variance analysis'
    )
}}

SELECT
    fd.DELIVERY_ID,
    fd.ORDER_ID,
    fd.DELIVERY_AGENT_ID,
    da.DELIVERY_AGENT_NAME,
    da.VEHICLE_TYPE,
    l.CITY,
    l.STATE,
    ca.LOCALITY AS DELIVERY_LOCALITY,

    -- Status Information
    fd.CURRENT_STATUS,
    fd.INITIAL_STATUS,
    fd.DELIVERY_DATE,
    fd.STATUS_UPDATED_AT,

    -- Time Calculations
    TRY_CAST(REGEXP_REPLACE(fd.ESTIMATED_TIME, '[^0-9]', '') AS INTEGER) AS ESTIMATED_TIME_MINS,
    DATEDIFF(MINUTE, fd.CREATED_AT, fd.STATUS_UPDATED_AT) AS ACTUAL_DELIVERY_TIME_MINS,
    DATEDIFF(MINUTE, fd.CREATED_AT, fd.STATUS_UPDATED_AT) -
        TRY_CAST(REGEXP_REPLACE(fd.ESTIMATED_TIME, '[^0-9]', '') AS INTEGER) AS TIME_VARIANCE_MINS,

    -- Performance Flags
    CASE
        WHEN DATEDIFF(MINUTE, fd.CREATED_AT, fd.STATUS_UPDATED_AT) <=
             TRY_CAST(REGEXP_REPLACE(fd.ESTIMATED_TIME, '[^0-9]', '') AS INTEGER) THEN 'ON_TIME'
        WHEN DATEDIFF(MINUTE, fd.CREATED_AT, fd.STATUS_UPDATED_AT) <=
             TRY_CAST(REGEXP_REPLACE(fd.ESTIMATED_TIME, '[^0-9]', '') AS INTEGER) + 10 THEN 'SLIGHTLY_DELAYED'
        ELSE 'DELAYED'
    END AS DELIVERY_TIMELINESS,

    -- Day and Time Context
    d.DAY_NAME,
    d.DAY_OF_WEEK,
    HOUR(fd.DELIVERY_DATE) AS DELIVERY_HOUR,
    CASE
        WHEN HOUR(fd.DELIVERY_DATE) BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN HOUR(fd.DELIVERY_DATE) BETWEEN 12 AND 16 THEN 'AFTERNOON'
        WHEN HOUR(fd.DELIVERY_DATE) BETWEEN 17 AND 21 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS TIME_OF_DAY,

    CURRENT_TIMESTAMP() AS CREATED_AT
FROM {{ ref('fact_delivery') }} fd
LEFT JOIN {{ ref('dim_delivery_agent') }} da
    ON fd.DELIVERY_AGENT_ID = da.DELIVERY_AGENT_ID
LEFT JOIN {{ ref('dim_customer_address') }} ca
    ON fd.CUSTOMER_ADDRESS_ID = ca.CUSTOMER_ADDRESS_ID
LEFT JOIN {{ ref('dim_location') }} l
    ON ca.CITY = l.CITY
LEFT JOIN {{ ref('dim_date') }} d
    ON DATE(fd.DELIVERY_DATE) = d.CALENDAR_DATE
WHERE fd.CURRENT_STATUS IN ('DELIVERED', 'FAILED')