{{
    config(
        materialized='view',
        tags=['analytics', 'customer', 'weekly'],
        description='Customer cohort retention analysis'
    )
}}

WITH first_orders AS (
    SELECT
        CUSTOMER_ID,
        DATE_TRUNC('MONTH', MIN(ORDER_DATE)) AS COHORT_MONTH
    FROM {{ ref('fact_order') }}
    GROUP BY CUSTOMER_ID
),

cohort_data AS (
    SELECT
        f.COHORT_MONTH,
        DATE_TRUNC('MONTH', o.ORDER_DATE) AS ORDER_MONTH,
        DATEDIFF(MONTH, f.COHORT_MONTH, DATE_TRUNC('MONTH', o.ORDER_DATE)) AS MONTHS_SINCE_FIRST_ORDER,
        COUNT(DISTINCT o.CUSTOMER_ID) AS ACTIVE_CUSTOMERS,
        COUNT(DISTINCT o.ORDER_ID) AS TOTAL_ORDERS,
        SUM(o.TOTAL_AMOUNT) AS TOTAL_REVENUE
    FROM first_orders f
    INNER JOIN {{ ref('fact_order') }} o
        ON f.CUSTOMER_ID = o.CUSTOMER_ID
    GROUP BY
        f.COHORT_MONTH,
        ORDER_MONTH,
        MONTHS_SINCE_FIRST_ORDER
)

SELECT
    COHORT_MONTH,
    ORDER_MONTH,
    MONTHS_SINCE_FIRST_ORDER,
    ACTIVE_CUSTOMERS,
    TOTAL_ORDERS,
    TOTAL_REVENUE,
    ROUND(
        DIV0(ACTIVE_CUSTOMERS,
             FIRST_VALUE(ACTIVE_CUSTOMERS) OVER (
                 PARTITION BY COHORT_MONTH
                 ORDER BY MONTHS_SINCE_FIRST_ORDER
             )) * 100,
        2
    ) AS RETENTION_RATE,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM cohort_data