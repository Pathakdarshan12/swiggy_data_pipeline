{{
    config(
        materialized='view',
        tags=['analytics', 'customer', 'daily'],
        description='Customer restaurant and cuisine preferences'
    )
}}

SELECT
    c.CUSTOMER_ID,
    c.NAME AS CUSTOMER_NAME,
    r.RESTAURANT_ID,
    r.RESTAURANT_NAME,
    r.CUISINE_TYPE,
    l.CITY,
    l.STATE,

    -- Restaurant Ordering Metrics
    COUNT(DISTINCT o.ORDER_ID) AS ORDERS_FROM_RESTAURANT,
    SUM(o.TOTAL_AMOUNT) AS TOTAL_SPENT,
    AVG(o.TOTAL_AMOUNT) AS AVG_SPEND_PER_ORDER,
    MAX(o.ORDER_DATE) AS LAST_ORDER_DATE,

    -- Ranking
    ROW_NUMBER() OVER (
        PARTITION BY c.CUSTOMER_ID
        ORDER BY COUNT(DISTINCT o.ORDER_ID) DESC
    ) AS RESTAURANT_PREFERENCE_RANK,

    CURRENT_TIMESTAMP() AS CREATED_AT
FROM {{ ref('dim_customer') }} c
INNER JOIN {{ ref('fact_order') }} o
    ON c.CUSTOMER_ID = o.CUSTOMER_ID
INNER JOIN {{ ref('dim_restaurant') }} r
    ON o.RESTAURANT_ID = r.RESTAURANT_ID
LEFT JOIN {{ ref('dim_location') }} l
    ON r.LOCATION_ID = l.LOCATION_ID
WHERE c.STATUS = 'ACTIVE'
    AND r.STATUS = 'ACTIVE'
GROUP BY
    c.CUSTOMER_ID, c.NAME, r.RESTAURANT_ID, r.RESTAURANT_NAME,
    r.CUISINE_TYPE, l.CITY, l.STATE
QUALIFY RESTAURANT_PREFERENCE_RANK <= 10