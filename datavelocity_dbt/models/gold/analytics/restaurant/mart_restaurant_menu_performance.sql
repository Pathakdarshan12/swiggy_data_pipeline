{{
    config(
        materialized='view',
        tags=['analytics', 'restaurant', 'weekly'],
        description='Menu item performance and pricing analysis'
    )
}}

WITH menu_orders AS (
    SELECT
        m.MENU_ID,
        m.RESTAURANT_ID,
        m.ITEM_NAME,
        m.CATEGORY,
        m.PRICE,
        m.ITEM_TYPE,
        m.AVAILABILITY,
        COUNT(DISTINCT o.ORDER_ID) AS ESTIMATED_ORDERS,
        DIV0(SUM(o.TOTAL_AMOUNT),
             NULLIF(COUNT(DISTINCT o.ORDER_ID), 0)) AS AVG_ORDER_WITH_ITEM
    FROM {{ ref('dim_menu') }} m
    LEFT JOIN {{ ref('fact_order') }} o
        ON m.RESTAURANT_ID = o.RESTAURANT_ID
    WHERE m.STATUS = TRUE
    GROUP BY
        m.MENU_ID, m.RESTAURANT_ID, m.ITEM_NAME,
        m.CATEGORY, m.PRICE, m.ITEM_TYPE, m.AVAILABILITY
)

SELECT
    mo.MENU_ID,
    mo.RESTAURANT_ID,
    r.RESTAURANT_NAME,
    mo.ITEM_NAME,
    mo.CATEGORY,
    mo.PRICE,
    mo.ITEM_TYPE,
    mo.AVAILABILITY,
    mo.ESTIMATED_ORDERS,
    mo.AVG_ORDER_WITH_ITEM,

    -- Price Analysis
    AVG(mo.PRICE) OVER (
        PARTITION BY mo.RESTAURANT_ID, mo.CATEGORY
    ) AS AVG_CATEGORY_PRICE,

    DIV0(mo.PRICE,
         NULLIF(AVG(mo.PRICE) OVER (
             PARTITION BY mo.RESTAURANT_ID, mo.CATEGORY
         ), 0)) * 100 AS PRICE_INDEX,

    -- Performance Ranking
    ROW_NUMBER() OVER (
        PARTITION BY mo.RESTAURANT_ID
        ORDER BY mo.ESTIMATED_ORDERS DESC
    ) AS POPULARITY_RANK,

    ROW_NUMBER() OVER (
        PARTITION BY mo.RESTAURANT_ID
        ORDER BY (mo.PRICE * mo.ESTIMATED_ORDERS) DESC
    ) AS REVENUE_RANK,

    CURRENT_TIMESTAMP() AS CREATED_AT
FROM menu_orders mo
LEFT JOIN {{ ref('dim_restaurant') }} r
    ON mo.RESTAURANT_ID = r.RESTAURANT_ID