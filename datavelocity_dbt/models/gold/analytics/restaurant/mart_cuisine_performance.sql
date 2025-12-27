{{
    config(
        materialized='view',
        tags=['analytics', 'restaurant', 'daily'],
        description='Cuisine type performance and market share analysis'
    )
}}

SELECT
    r.CUISINE_TYPE,
    l.CITY,
    l.STATE,
    l.CITY_TIER,

    -- Restaurant Count
    COUNT(DISTINCT r.RESTAURANT_ID) AS RESTAURANT_COUNT,

    -- Order Metrics
    COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
    SUM(o.TOTAL_AMOUNT) AS TOTAL_REVENUE,
    AVG(o.TOTAL_AMOUNT) AS AVG_ORDER_VALUE,

    -- Per Restaurant Metrics
    DIV0(COUNT(DISTINCT o.ORDER_ID),
         COUNT(DISTINCT r.RESTAURANT_ID)) AS AVG_ORDERS_PER_RESTAURANT,
    DIV0(SUM(o.TOTAL_AMOUNT),
         COUNT(DISTINCT r.RESTAURANT_ID)) AS AVG_REVENUE_PER_RESTAURANT,

    -- Customer Base
    COUNT(DISTINCT o.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,

    -- Pricing
    AVG(r.PRICING_FOR_TWO) AS AVG_PRICING_FOR_TWO,

    -- Market Share
    DIV0(SUM(o.TOTAL_AMOUNT),
         SUM(SUM(o.TOTAL_AMOUNT)) OVER (PARTITION BY l.CITY)) * 100 AS CITY_MARKET_SHARE_PCT,

    CURRENT_TIMESTAMP() AS CREATED_AT
FROM {{ ref('dim_restaurant') }} r
LEFT JOIN {{ ref('fact_order') }} o
    ON r.RESTAURANT_ID = o.RESTAURANT_ID
LEFT JOIN {{ ref('dim_location') }} l
    ON r.LOCATION_ID = l.LOCATION_ID
WHERE r.STATUS = 'ACTIVE'
GROUP BY r.CUISINE_TYPE, l.CITY, l.STATE, l.CITY_TIER