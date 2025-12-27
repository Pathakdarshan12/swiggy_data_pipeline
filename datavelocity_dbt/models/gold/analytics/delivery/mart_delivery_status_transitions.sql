{{
    config(
        materialized='view',
        tags=['analytics', 'delivery', 'daily'],
        description='Delivery status transition tracking and timing'
    )
}}

SELECT
    fdsh.DELIVERY_ID,
    fdsh.OLD_STATUS,
    fdsh.NEW_STATUS,
    fdsh.STATUS_CHANGED_AT,

    -- Time Between Status Changes
    LAG(fdsh.STATUS_CHANGED_AT) OVER (
        PARTITION BY fdsh.DELIVERY_ID
        ORDER BY fdsh.STATUS_CHANGED_AT
    ) AS PREVIOUS_STATUS_TIME,

    DATEDIFF(
        MINUTE,
        LAG(fdsh.STATUS_CHANGED_AT) OVER (
            PARTITION BY fdsh.DELIVERY_ID
            ORDER BY fdsh.STATUS_CHANGED_AT
        ),
        fdsh.STATUS_CHANGED_AT
    ) AS MINS_IN_PREVIOUS_STATUS,

    -- Sequence Number
    ROW_NUMBER() OVER (
        PARTITION BY fdsh.DELIVERY_ID
        ORDER BY fdsh.STATUS_CHANGED_AT
    ) AS STATUS_SEQUENCE,

    -- Agent and Location Context
    da.DELIVERY_AGENT_NAME,
    da.VEHICLE_TYPE,

    CURRENT_TIMESTAMP() AS CREATED_AT
FROM {{ ref('fact_delivery_status_history') }} fdsh
LEFT JOIN {{ ref('fact_delivery') }} fd
    ON fdsh.DELIVERY_ID = fd.DELIVERY_ID
LEFT JOIN {{ ref('dim_delivery_agent') }} da
    ON fd.DELIVERY_AGENT_ID = da.DELIVERY_AGENT_ID