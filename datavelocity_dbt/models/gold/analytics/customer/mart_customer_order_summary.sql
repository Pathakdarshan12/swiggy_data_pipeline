{{
    config(
        materialized='view',
        tags=['analytics', 'customer', 'daily'],
        cluster_by=['CUSTOMER_SEGMENT', 'CUSTOMER_STATUS'],
        description='Comprehensive customer segmentation and lifetime value analysis'
    )
}}

WITH customer_orders AS (
    SELECT
        c.CUSTOMER_ID,
        c.NAME AS CUSTOMER_NAME,
        c.GENDER,
        c.DOB,
        DATEDIFF(YEAR, c.DOB, CURRENT_DATE()) AS CUSTOMER_AGE,
        c.LOGIN_BY_USING,
        c.PREFERENCES,
        o.ORDER_ID,
        o.ORDER_DATE,
        o.TOTAL_AMOUNT,
        o.PAYMENT_METHOD,
        o.CURRENT_STATUS
    FROM {{ ref('dim_customer') }} c
    LEFT JOIN {{ ref('fact_order') }} o
        ON c.CUSTOMER_ID = o.CUSTOMER_ID
    WHERE c.STATUS = 'ACTIVE'
),

customer_metrics AS (
    SELECT
        CUSTOMER_ID,
        CUSTOMER_NAME,
        GENDER,
        DOB,
        CUSTOMER_AGE,
        LOGIN_BY_USING,
        PREFERENCES,

        -- Order Metrics
        COUNT(DISTINCT ORDER_ID) AS TOTAL_ORDERS,
        SUM(TOTAL_AMOUNT) AS LIFETIME_VALUE,
        AVG(TOTAL_AMOUNT) AS AVG_ORDER_VALUE,
        MIN(ORDER_DATE) AS FIRST_ORDER_DATE,
        MAX(ORDER_DATE) AS LAST_ORDER_DATE,
        DATEDIFF(DAY, MIN(ORDER_DATE), MAX(ORDER_DATE)) AS CUSTOMER_TENURE_DAYS,
        DATEDIFF(DAY, MAX(ORDER_DATE), CURRENT_DATE()) AS DAYS_SINCE_LAST_ORDER,

        -- Frequency Metrics
        COUNT(DISTINCT DATE_TRUNC('MONTH', ORDER_DATE)) AS ACTIVE_MONTHS,
        DIV0(COUNT(DISTINCT ORDER_ID),
             NULLIF(COUNT(DISTINCT DATE_TRUNC('MONTH', ORDER_DATE)), 0)) AS ORDERS_PER_ACTIVE_MONTH,

        -- Payment Preferences
        MODE(PAYMENT_METHOD) AS PREFERRED_PAYMENT_METHOD,
        COUNT(DISTINCT PAYMENT_METHOD) AS PAYMENT_METHODS_USED,

        -- Status Analysis
        SUM(CASE WHEN CURRENT_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS COMPLETED_ORDERS,
        SUM(CASE WHEN CURRENT_STATUS = 'CANCELLED' THEN 1 ELSE 0 END) AS CANCELLED_ORDERS,
        DIV0(SUM(CASE WHEN CURRENT_STATUS = 'CANCELLED' THEN 1 ELSE 0 END),
             COUNT(*)) * 100 AS CANCELLATION_RATE
    FROM customer_orders
    GROUP BY
        CUSTOMER_ID, CUSTOMER_NAME, GENDER, DOB, CUSTOMER_AGE,
        LOGIN_BY_USING, PREFERENCES
)

SELECT
    *,

    -- Customer Segmentation
    CASE
        WHEN TOTAL_ORDERS >= 20 THEN 'VIP'
        WHEN TOTAL_ORDERS >= 10 THEN 'LOYAL'
        WHEN TOTAL_ORDERS >= 5 THEN 'REGULAR'
        WHEN TOTAL_ORDERS >= 2 THEN 'REPEAT'
        ELSE 'NEW'
    END AS CUSTOMER_SEGMENT,

    CASE
        WHEN DAYS_SINCE_LAST_ORDER <= 30 THEN 'ACTIVE'
        WHEN DAYS_SINCE_LAST_ORDER <= 90 THEN 'AT_RISK'
        ELSE 'CHURNED'
    END AS CUSTOMER_STATUS,

    CURRENT_TIMESTAMP() AS CREATED_AT
FROM customer_metrics