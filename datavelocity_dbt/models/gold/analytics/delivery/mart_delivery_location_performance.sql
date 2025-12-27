{{
    config(
        materialized='view',
        tags=['analytics', 'delivery', 'daily'],
        description='Location-based delivery performance metrics'
    )
}}

SELECT
    l.CITY,
    l.STATE,
    l.CITY_TIER,
    ca.LOCALITY,

    -- Volume Metrics
    COUNT(DISTINCT fd.DELIVERY_ID) AS TOTAL_DELIVERIES,
    COUNT(DISTINCT fd.DELIVERY_AGENT_ID) AS UNIQUE_AGENTS,

    -- Success Metrics
    SUM(CASE WHEN fd.CURRENT_STATUS = 'DELIVERED' THEN 1 ELSE 0 END) AS SUCCESSFUL_DELIVERIES,
    DIV0(SUM(CASE WHEN fd.CURRENT_STATUS = 'DELIVERED' THEN 1 ELSE 0 END),
         COUNT(*)) * 100 AS SUCCESS_RATE,

    -- Time Metrics
    AVG(TRY_CAST(REGEXP_REPLACE(fd.ESTIMATED_TIME, '[^0-9]', '') AS INTEGER)) AS AVG_ESTIMATED_TIME,
    AVG(DATEDIFF(MINUTE, fd.CREATED_AT, fd.STATUS_UPDATED_AT)) AS AVG_ACTUAL_TIME,

    -- Peak Times
    MODE(HOUR(fd.DELIVERY_DATE)) AS PEAK_DELIVERY_HOUR,

    CURRENT_TIMESTAMP() AS CREATED_AT
FROM {{ ref('fact_delivery') }} fd
LEFT JOIN {{ ref('dim_customer_address') }} ca
    ON fd.CUSTOMER_ADDRESS_ID = ca.CUSTOMER_ADDRESS_ID
LEFT JOIN {{ ref('dim_location') }} l
    ON ca.CITY = l.CITY
GROUP BY l.CITY, l.STATE, l.CITY_TIER, ca.LOCALITY