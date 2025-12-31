-- =====================================================
-- ADVANCED MONITORING & ALERTING FRAMEWORK
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE DATAVELOCITY;
USE SCHEMA COMMON;

-- =====================================================
-- 1. SLA CONFIGURATION TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.SLA_CONFIG (
    SLA_CONFIG_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(100) NOT NULL,
    METRIC_TYPE VARCHAR(50) NOT NULL, -- LATENCY, THROUGHPUT, QUALITY, AVAILABILITY
    THRESHOLD_VALUE NUMBER(10,2) NOT NULL,
    THRESHOLD_OPERATOR VARCHAR(10) NOT NULL, -- >, <, >=, <=, =
    MEASUREMENT_UNIT VARCHAR(20), -- SECONDS, MINUTES, PERCENTAGE, ROWS
    SEVERITY VARCHAR(20) DEFAULT 'WARNING', -- INFO, WARNING, CRITICAL
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    NOTIFICATION_CHANNEL VARCHAR(100), -- EMAIL, SLACK, PAGERDUTY
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Sample SLA configurations
INSERT INTO COMMON.SLA_CONFIG (PIPELINE_NAME, METRIC_TYPE, THRESHOLD_VALUE, THRESHOLD_OPERATOR, MEASUREMENT_UNIT, SEVERITY) VALUES
('CUSTOMER_PIPELINE', 'LATENCY', 300, '>', 'SECONDS', 'WARNING'),
('CUSTOMER_PIPELINE', 'LATENCY', 600, '>', 'SECONDS', 'CRITICAL'),
('CUSTOMER_PIPELINE', 'QUALITY', 95, '<', 'PERCENTAGE', 'WARNING'),
('ORDER_PIPELINE_STREAM', 'LATENCY', 30, '>', 'SECONDS', 'CRITICAL'),
('ALL', 'AVAILABILITY', 99, '<', 'PERCENTAGE', 'CRITICAL');

-- =====================================================
-- 2. ALERTS TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.PIPELINE_ALERTS (
    ALERT_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(100) NOT NULL,
    ALERT_TYPE VARCHAR(50) NOT NULL, -- SLA_BREACH, FAILURE, DEGRADATION, ANOMALY
    SEVERITY VARCHAR(20) NOT NULL,
    METRIC_NAME VARCHAR(100),
    ACTUAL_VALUE NUMBER(10,2),
    THRESHOLD_VALUE NUMBER(10,2),
    ALERT_MESSAGE VARCHAR(2000),
    ALERT_STATUS VARCHAR(20) DEFAULT 'OPEN', -- OPEN, ACKNOWLEDGED, RESOLVED
    DETECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ACKNOWLEDGED_AT TIMESTAMP_NTZ,
    ACKNOWLEDGED_BY VARCHAR(100),
    RESOLVED_AT TIMESTAMP_NTZ,
    RESOLUTION_NOTES VARCHAR(2000)
);

-- =====================================================
-- 3. PERFORMANCE METRICS TABLE
-- =====================================================
CREATE OR REPLACE TABLE COMMON.PERFORMANCE_METRICS (
    METRIC_ID INTEGER AUTOINCREMENT PRIMARY KEY,
    PIPELINE_NAME VARCHAR(100) NOT NULL,
    METRIC_TYPE VARCHAR(50) NOT NULL,
    METRIC_VALUE NUMBER(10,2),
    MEASUREMENT_UNIT VARCHAR(20),
    BATCH_ID VARCHAR(50),
    INGEST_RUN_ID INTEGER,
    MEASURED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- 4. SLA MONITORING VIEW
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_SLA_DASHBOARD AS
WITH latest_runs AS (
    SELECT
        PIPELINE_NAME,
        AVG(EXECUTION_DURATION_SEC) AS avg_latency_sec,
        AVG(CASE
            WHEN SOURCE_ROW_COUNT > 0
            THEN (VALID_ROW_COUNT::FLOAT / SOURCE_ROW_COUNT * 100)
            ELSE 100
        END) AS avg_quality_pct,
        COUNT(*) AS total_runs,
        SUM(CASE WHEN RUN_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS success_runs,
        (success_runs::FLOAT / NULLIF(total_runs, 0) * 100) AS availability_pct,
        MAX(EXECUTED_AT) AS last_run_time
    FROM COMMON.INGEST_RUN
    WHERE EXECUTED_AT >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    GROUP BY PIPELINE_NAME
),
sla_checks AS (
    SELECT
        lr.PIPELINE_NAME,
        sc.METRIC_TYPE,
        sc.THRESHOLD_VALUE,
        sc.THRESHOLD_OPERATOR,
        sc.MEASUREMENT_UNIT,
        sc.SEVERITY,
        CASE sc.METRIC_TYPE
            WHEN 'LATENCY' THEN lr.avg_latency_sec
            WHEN 'QUALITY' THEN lr.avg_quality_pct
            WHEN 'AVAILABILITY' THEN lr.availability_pct
        END AS actual_value,
        CASE
            WHEN sc.METRIC_TYPE = 'LATENCY' AND sc.THRESHOLD_OPERATOR = '>'
                THEN lr.avg_latency_sec > sc.THRESHOLD_VALUE
            WHEN sc.METRIC_TYPE = 'QUALITY' AND sc.THRESHOLD_OPERATOR = '<'
                THEN lr.avg_quality_pct < sc.THRESHOLD_VALUE
            WHEN sc.METRIC_TYPE = 'AVAILABILITY' AND sc.THRESHOLD_OPERATOR = '<'
                THEN lr.availability_pct < sc.THRESHOLD_VALUE
            ELSE FALSE
        END AS is_breach
    FROM latest_runs lr
    INNER JOIN COMMON.SLA_CONFIG sc
        ON (lr.PIPELINE_NAME = sc.PIPELINE_NAME OR sc.PIPELINE_NAME = 'ALL')
        AND sc.IS_ACTIVE = TRUE
)
SELECT
    PIPELINE_NAME,
    METRIC_TYPE,
    actual_value AS ACTUAL_VALUE,
    THRESHOLD_VALUE,
    THRESHOLD_OPERATOR || ' ' || THRESHOLD_VALUE || ' ' || MEASUREMENT_UNIT AS SLA_THRESHOLD,
    CASE
        WHEN is_breach THEN '❌ BREACH'
        ELSE '✅ OK'
    END AS SLA_STATUS,
    SEVERITY,
    CASE
        WHEN is_breach THEN 'Exceeds SLA threshold'
        ELSE 'Within acceptable range'
    END AS STATUS_MESSAGE
FROM sla_checks
ORDER BY is_breach DESC, SEVERITY DESC, PIPELINE_NAME;

-- =====================================================
-- 5. PERFORMANCE DEGRADATION DETECTION
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_PERFORMANCE_DEGRADATION AS
WITH performance_baseline AS (
    SELECT
        PIPELINE_NAME,
        AVG(EXECUTION_DURATION_SEC) AS baseline_duration,
        STDDEV(EXECUTION_DURATION_SEC) AS duration_stddev,
        AVG(VALID_ROW_COUNT::FLOAT / NULLIF(SOURCE_ROW_COUNT, 0)) AS baseline_quality
    FROM COMMON.INGEST_RUN
    WHERE EXECUTED_AT >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
        AND EXECUTED_AT < DATEADD(DAY, -1, CURRENT_TIMESTAMP())
    GROUP BY PIPELINE_NAME
),
recent_performance AS (
    SELECT
        PIPELINE_NAME,
        AVG(EXECUTION_DURATION_SEC) AS recent_duration,
        AVG(VALID_ROW_COUNT::FLOAT / NULLIF(SOURCE_ROW_COUNT, 0)) AS recent_quality,
        COUNT(*) AS recent_run_count
    FROM COMMON.INGEST_RUN
    WHERE EXECUTED_AT >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    GROUP BY PIPELINE_NAME
)
SELECT
    rp.PIPELINE_NAME,
    pb.baseline_duration,
    rp.recent_duration,
    ROUND(((rp.recent_duration - pb.baseline_duration) / NULLIF(pb.baseline_duration, 0) * 100), 2) AS duration_change_pct,
    pb.baseline_quality * 100 AS baseline_quality_pct,
    rp.recent_quality * 100 AS recent_quality_pct,
    ROUND(((rp.recent_quality - pb.baseline_quality) * 100), 2) AS quality_change_pct,
    CASE
        WHEN rp.recent_duration > pb.baseline_duration + (2 * pb.duration_stddev)
            THEN '⚠️ DEGRADED'
        WHEN rp.recent_quality < pb.baseline_quality - 0.05
            THEN '⚠️ DEGRADED'
        ELSE '✅ NORMAL'
    END AS PERFORMANCE_STATUS,
    CASE
        WHEN rp.recent_duration > pb.baseline_duration + (2 * pb.duration_stddev)
            THEN 'Execution time increased by ' || ROUND(((rp.recent_duration - pb.baseline_duration) / NULLIF(pb.baseline_duration, 0) * 100), 0) || '%'
        WHEN rp.recent_quality < pb.baseline_quality - 0.05
            THEN 'Data quality dropped by ' || ROUND(((rp.recent_quality - pb.baseline_quality) * 100), 1) || ' percentage points'
        ELSE 'Performance within normal range'
    END AS ALERT_MESSAGE
FROM recent_performance rp
INNER JOIN performance_baseline pb ON rp.PIPELINE_NAME = pb.PIPELINE_NAME
WHERE rp.recent_run_count >= 3
ORDER BY PERFORMANCE_STATUS DESC, duration_change_pct DESC;

-- =====================================================
-- 6. AUTOMATED SLA MONITORING PROCEDURE
-- =====================================================
CREATE OR REPLACE PROCEDURE COMMON.SP_CHECK_SLA_COMPLIANCE()
RETURNS TABLE(alert_count INTEGER, critical_count INTEGER, warning_count INTEGER)
LANGUAGE SQL
AS
$$
DECLARE
    v_alert_count INTEGER DEFAULT 0;
    v_critical_count INTEGER DEFAULT 0;
    v_warning_count INTEGER DEFAULT 0;
    v_result_set RESULTSET;
BEGIN
    -- Check for SLA breaches
    INSERT INTO COMMON.PIPELINE_ALERTS (
        PIPELINE_NAME,
        ALERT_TYPE,
        SEVERITY,
        METRIC_NAME,
        ACTUAL_VALUE,
        THRESHOLD_VALUE,
        ALERT_MESSAGE
    )
    SELECT
        PIPELINE_NAME,
        'SLA_BREACH' AS ALERT_TYPE,
        SEVERITY,
        METRIC_TYPE AS METRIC_NAME,
        ACTUAL_VALUE,
        THRESHOLD_VALUE,
        STATUS_MESSAGE AS ALERT_MESSAGE
    FROM COMMON.VW_SLA_DASHBOARD
    WHERE SLA_STATUS = '❌ BREACH'
        AND NOT EXISTS (
            SELECT 1 FROM COMMON.PIPELINE_ALERTS pa
            WHERE pa.PIPELINE_NAME = VW_SLA_DASHBOARD.PIPELINE_NAME
                AND pa.METRIC_NAME = VW_SLA_DASHBOARD.METRIC_TYPE
                AND pa.ALERT_STATUS = 'OPEN'
                AND pa.DETECTED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
        );

    -- Check for performance degradation
    INSERT INTO COMMON.PIPELINE_ALERTS (
        PIPELINE_NAME,
        ALERT_TYPE,
        SEVERITY,
        METRIC_NAME,
        ACTUAL_VALUE,
        THRESHOLD_VALUE,
        ALERT_MESSAGE
    )
    SELECT
        PIPELINE_NAME,
        'DEGRADATION' AS ALERT_TYPE,
        'WARNING' AS SEVERITY,
        'PERFORMANCE' AS METRIC_NAME,
        recent_duration AS ACTUAL_VALUE,
        baseline_duration AS THRESHOLD_VALUE,
        ALERT_MESSAGE
    FROM COMMON.VW_PERFORMANCE_DEGRADATION
    WHERE PERFORMANCE_STATUS = '⚠️ DEGRADED'
        AND NOT EXISTS (
            SELECT 1 FROM COMMON.PIPELINE_ALERTS pa
            WHERE pa.PIPELINE_NAME = VW_PERFORMANCE_DEGRADATION.PIPELINE_NAME
                AND pa.ALERT_TYPE = 'DEGRADATION'
                AND pa.ALERT_STATUS = 'OPEN'
                AND pa.DETECTED_AT >= DATEADD(HOUR, -4, CURRENT_TIMESTAMP())
        );

    -- Count alerts
    SELECT
        COUNT(*) AS total_alerts,
        SUM(CASE WHEN SEVERITY = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_alerts,
        SUM(CASE WHEN SEVERITY = 'WARNING' THEN 1 ELSE 0 END) AS warning_alerts
    INTO v_alert_count, v_critical_count, v_warning_count
    FROM COMMON.PIPELINE_ALERTS
    WHERE ALERT_STATUS = 'OPEN';

    -- Return results
    v_result_set := (
        SELECT
            :v_alert_count AS alert_count,
            :v_critical_count AS critical_count,
            :v_warning_count AS warning_count
    );

    RETURN TABLE(v_result_set);
END;
$$;

-- =====================================================
-- 7. REAL-TIME MONITORING DASHBOARD
-- =====================================================
CREATE OR REPLACE VIEW COMMON.VW_MONITORING_DASHBOARD AS
SELECT
    'Pipeline Health' AS widget_name,
    'STATUS' AS widget_type,
    OBJECT_CONSTRUCT(
        'total_pipelines', COUNT(DISTINCT PIPELINE_NAME),
        'active_last_hour', COUNT(DISTINCT CASE
            WHEN EXECUTED_AT >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
            THEN PIPELINE_NAME END),
        'success_rate', ROUND(AVG(CASE WHEN RUN_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) * 100, 2),
        'failed_runs', SUM(CASE WHEN RUN_STATUS = 'FAILED' THEN 1 ELSE 0 END)
    ) AS widget_data
FROM COMMON.INGEST_RUN
WHERE EXECUTED_AT >= DATEADD(DAY, -1, CURRENT_TIMESTAMP())

UNION ALL

SELECT
    'SLA Compliance' AS widget_name,
    'GAUGE' AS widget_type,
    OBJECT_CONSTRUCT(
        'compliant', COUNT(CASE WHEN SLA_STATUS = '✅ OK' THEN 1 END),
        'breached', COUNT(CASE WHEN SLA_STATUS = '❌ BREACH' THEN 1 END),
        'compliance_rate', ROUND(
            COUNT(CASE WHEN SLA_STATUS = '✅ OK' THEN 1 END)::FLOAT /
            NULLIF(COUNT(*), 0) * 100, 2)
    ) AS widget_data
FROM COMMON.VW_SLA_DASHBOARD

UNION ALL

SELECT
    'Active Alerts' AS widget_name,
    'ALERT_LIST' AS widget_type,
    OBJECT_CONSTRUCT(
        'critical', COUNT(CASE WHEN SEVERITY = 'CRITICAL' THEN 1 END),
        'warning', COUNT(CASE WHEN SEVERITY = 'WARNING' THEN 1 END),
        'total', COUNT(*)
    ) AS widget_data
FROM COMMON.PIPELINE_ALERTS
WHERE ALERT_STATUS = 'OPEN'

UNION ALL

SELECT
    'Performance Trends' AS widget_name,
    'TIMESERIES' AS widget_type,
    OBJECT_CONSTRUCT(
        'avg_latency_sec', AVG(EXECUTION_DURATION_SEC),
        'avg_throughput', AVG(SOURCE_ROW_COUNT / NULLIF(EXECUTION_DURATION_SEC, 0)),
        'avg_quality_pct', AVG(VALID_ROW_COUNT::FLOAT / NULLIF(SOURCE_ROW_COUNT, 0) * 100)
    ) AS widget_data
FROM COMMON.INGEST_RUN
WHERE EXECUTED_AT >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP());

-- =====================================================
-- 8. CREATE SCHEDULED TASK FOR MONITORING
-- =====================================================
CREATE OR REPLACE TASK COMMON.TASK_SLA_MONITORING
    WAREHOUSE = ADHOC_WH
    SCHEDULE = '10 MINUTE'
AS
    CALL COMMON.SP_CHECK_SLA_COMPLIANCE();

-- Resume task
-- ALTER TASK COMMON.TASK_SLA_MONITORING RESUME;

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- View SLA dashboard
SELECT * FROM COMMON.VW_SLA_DASHBOARD;

-- Check for performance degradation
SELECT * FROM COMMON.VW_PERFORMANCE_DEGRADATION;

-- View active alerts
SELECT
    PIPELINE_NAME,
    ALERT_TYPE,
    SEVERITY,
    ALERT_MESSAGE,
    DETECTED_AT
FROM COMMON.PIPELINE_ALERTS
WHERE ALERT_STATUS = 'OPEN'
ORDER BY SEVERITY DESC, DETECTED_AT DESC;

-- View monitoring dashboard
SELECT * FROM COMMON.VW_MONITORING_DASHBOARD;

-- Test SLA monitoring procedure
CALL COMMON.SP_CHECK_SLA_COMPLIANCE();