import os
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Page configuration
st.set_page_config(
    page_title="DataVelocity Unified Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        margin-top: 2rem;
        margin-bottom: 1rem;
        color: #262730;
    }
    .alert-critical {
        background-color: #fee;
        border-left: 4px solid #dc3545;
        padding: 10px;
        margin: 5px 0;
    }
    .alert-warning {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 10px;
        margin: 5px 0;
    }
    .status-ok {
        color: #28a745;
        font-weight: bold;
    }
    .status-breach {
        color: #dc3545;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

load_dotenv()

# Snowflake connection
@st.cache_resource
def init_connection():
    """Initialize Snowflake connection"""
    return snowflake.connector.connect(
        user = os.getenv('SNOWFLAKE_USER'),
        password = os.getenv('SNOWFLAKE_PASSWORD'),
        account = os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
        database = os.getenv('SNOWFLAKE_DATABASE'),
        schema = os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

@st.cache_data(ttl=600)
def run_query(query):
    """Run query and return results as DataFrame"""
    conn = init_connection()
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Query error: {str(e)}")
        return pd.DataFrame()


# Sidebar Navigation
st.sidebar.title("🚀 DataVelocity Dashboard")
st.sidebar.markdown("---")

# Main category selection
category = st.sidebar.radio(
    "📁 Dashboard Category",
    ["🔧 Pipeline Monitoring", "📊 Business Analytics"]
)

st.sidebar.markdown("---")

# Sub-page selection based on category
if category == "🔧 Pipeline Monitoring":
    page = st.sidebar.radio(
        "Select Monitoring View",
        [
            "📊 Pipeline Overview",
            "⚡ SLA Compliance",
            "🚨 Active Alerts",
            "❌ Error Analysis",
            "🔄 Reconciliation",
            "📈 Performance Metrics"
        ]
    )
else:
    page = st.sidebar.radio(
        "Select Analytics View",
        [
            "📊 Executive Overview",
            "👥 Customer Analytics",
            "🍽️ Restaurant Performance",
            "🚗 Delivery Analytics",
            "📦 Order Analytics",
            "💰 Revenue Analytics"
        ]
    )

# Date filter in sidebar
st.sidebar.markdown("---")
st.sidebar.subheader("📅 Date Filter")
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(datetime.now() - timedelta(days=30), datetime.now()),
    max_value=datetime.now()
)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = date_range[0]
    end_date = datetime.now().date()

# Refresh button
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ============================================================================
# PIPELINE MONITORING SECTION
# ============================================================================

if category == "🔧 Pipeline Monitoring":

    # ========================================================================
    # MONITORING PAGE 1: PIPELINE OVERVIEW
    # ========================================================================
    if page == "📊 Pipeline Overview":
        st.markdown('<div class="main-header">🎯 Pipeline Health Overview</div>', unsafe_allow_html=True)

        try:
            # Fetch dashboard data
            query_dashboard = "SELECT * FROM COMMON.VW_MONITORING_DASHBOARD"
            dashboard_data = run_query(query_dashboard)

            if not dashboard_data.empty:
                # Parse JSON data
                health_row = dashboard_data[dashboard_data['WIDGET_NAME'] == 'Pipeline Health']
                sla_row = dashboard_data[dashboard_data['WIDGET_NAME'] == 'SLA Compliance']
                alert_row = dashboard_data[dashboard_data['WIDGET_NAME'] == 'Active Alerts']
                perf_row = dashboard_data[dashboard_data['WIDGET_NAME'] == 'Performance Trends']

                # Extract metrics
                import json

                health_data = json.loads(health_row['WIDGET_DATA'].values[0]) if not health_row.empty else {}
                sla_data = json.loads(sla_row['WIDGET_DATA'].values[0]) if not sla_row.empty else {}
                alert_data = json.loads(alert_row['WIDGET_DATA'].values[0]) if not alert_row.empty else {}
                perf_data = json.loads(perf_row['WIDGET_DATA'].values[0]) if not perf_row.empty else {}

                # Top metrics row
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.metric(
                        label="Total Pipelines",
                        value=health_data.get('total_pipelines', 0),
                        delta=f"{health_data.get('active_last_hour', 0)} active"
                    )

                with col2:
                    success_rate = health_data.get('success_rate', 0)
                    st.metric(
                        label="Success Rate",
                        value=f"{success_rate:.1f}%",
                        delta=f"{health_data.get('failed_runs', 0)} failed",
                        delta_color="inverse"
                    )

                with col3:
                    compliance_rate = sla_data.get('compliance_rate', 0)
                    st.metric(
                        label="SLA Compliance",
                        value=f"{compliance_rate:.1f}%",
                        delta=f"{sla_data.get('breached', 0)} breached",
                        delta_color="inverse"
                    )

                with col4:
                    total_alerts = alert_data.get('total', 0)
                    critical_alerts = alert_data.get('critical', 0)
                    st.metric(
                        label="Active Alerts",
                        value=total_alerts,
                        delta=f"{critical_alerts} critical",
                        delta_color="inverse"
                    )

                st.divider()

                # Performance summary
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("Performance Metrics")
                    perf_df = pd.DataFrame({
                        'Metric': ['Avg Latency', 'Avg Throughput', 'Avg Quality'],
                        'Value': [
                            f"{perf_data.get('avg_latency_sec', 0):.1f} sec",
                            f"{perf_data.get('avg_throughput', 0):.1f} rows/sec",
                            f"{perf_data.get('avg_quality_pct', 0):.1f}%"
                        ]
                    })
                    st.dataframe(perf_df, use_container_width=True, hide_index=True)

                with col2:
                    st.subheader("Alert Distribution")
                    alert_dist = pd.DataFrame({
                        'Severity': ['Critical', 'Warning'],
                        'Count': [alert_data.get('critical', 0), alert_data.get('warning', 0)]
                    })
                    fig = px.pie(alert_dist, values='Count', names='Severity',
                                 color='Severity',
                                 color_discrete_map={'Critical': '#dc3545', 'Warning': '#ffc107'})
                    st.plotly_chart(fig, use_container_width=True)

                # Recent pipeline runs
                st.subheader("Recent Pipeline Runs")
                query_runs = """
                SELECT 
                    INGEST_RUN_ID,
                    PIPELINE_NAME,
                    SOURCE_ROW_COUNT,
                    VALID_ROW_COUNT,
                    INVALID_ROW_COUNT,
                    EXECUTION_DURATION_SEC,
                    RUN_STATUS,
                    EXECUTED_AT
                FROM COMMON.INGEST_RUN
                WHERE EXECUTED_AT >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
                ORDER BY EXECUTED_AT DESC
                LIMIT 15
                """
                ingest_runs = run_query(query_runs)

                if not ingest_runs.empty:
                    ingest_runs['STATUS_ICON'] = ingest_runs['RUN_STATUS'].apply(
                        lambda x: '✅' if x == 'SUCCESS' else '❌'
                    )
                    ingest_runs['QUALITY_%'] = (
                            ingest_runs['VALID_ROW_COUNT'] / ingest_runs['SOURCE_ROW_COUNT'] * 100
                    ).round(2)

                    display_cols = ['STATUS_ICON', 'PIPELINE_NAME', 'SOURCE_ROW_COUNT',
                                    'VALID_ROW_COUNT', 'QUALITY_%', 'EXECUTION_DURATION_SEC', 'EXECUTED_AT']
                    st.dataframe(ingest_runs[display_cols], use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"Error loading dashboard data: {str(e)}")

    # ========================================================================
    # MONITORING PAGE 2: SLA COMPLIANCE
    # ========================================================================
    elif page == "⚡ SLA Compliance":
        st.markdown('<div class="main-header">⚡ SLA Compliance Dashboard</div>', unsafe_allow_html=True)

        try:
            query_sla = "SELECT * FROM COMMON.VW_SLA_DASHBOARD"
            sla_df = run_query(query_sla)

            if not sla_df.empty:
                # Summary metrics
                total_slas = len(sla_df)
                breached = len(sla_df[sla_df['SLA_STATUS'] == '❌ BREACH'])
                ok = len(sla_df[sla_df['SLA_STATUS'] == '✅ OK'])

                col1, col2, col3 = st.columns(3)
                col1.metric("Total SLAs", total_slas)
                col2.metric("Compliant", ok, delta=f"{(ok / total_slas * 100):.1f}%")
                col3.metric("Breached", breached, delta=f"{(breached / total_slas * 100):.1f}%", delta_color="inverse")

                st.divider()

                # SLA status by pipeline
                col1, col2 = st.columns([2, 1])

                with col1:
                    st.subheader("SLA Status by Pipeline")


                    def color_sla_status(val):
                        if '❌' in str(val):
                            return 'background-color: #fee; color: #dc3545; font-weight: bold'
                        elif '✅' in str(val):
                            return 'background-color: #d4edda; color: #28a745; font-weight: bold'
                        return ''


                    styled_df = sla_df.style.applymap(
                        color_sla_status,
                        subset=['SLA_STATUS']
                    )
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)

                with col2:
                    st.subheader("Breach by Severity")
                    breach_df = sla_df[sla_df['SLA_STATUS'] == '❌ BREACH']
                    if not breach_df.empty:
                        severity_counts = breach_df['SEVERITY'].value_counts()
                        fig = px.bar(
                            x=severity_counts.index,
                            y=severity_counts.values,
                            labels={'x': 'Severity', 'y': 'Count'},
                            color=severity_counts.index,
                            color_discrete_map={'CRITICAL': '#dc3545', 'WARNING': '#ffc107'}
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.success("No SLA breaches! 🎉")

                # Metric type analysis
                st.subheader("SLA Performance by Metric Type")
                metric_summary = sla_df.groupby(['METRIC_TYPE', 'SLA_STATUS']).size().reset_index(name='count')
                fig = px.bar(
                    metric_summary,
                    x='METRIC_TYPE',
                    y='count',
                    color='SLA_STATUS',
                    barmode='group',
                    color_discrete_map={'✅ OK': '#28a745', '❌ BREACH': '#dc3545'}
                )
                st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Error loading SLA data: {str(e)}")

    # ========================================================================
    # MONITORING PAGE 3: ACTIVE ALERTS
    # ========================================================================
    elif page == "🚨 Active Alerts":
        st.markdown('<div class="main-header">🚨 Active Alerts</div>', unsafe_allow_html=True)

        try:
            query_alerts = """
            SELECT 
                PIPELINE_NAME,
                ALERT_TYPE,
                SEVERITY,
                METRIC_NAME,
                ACTUAL_VALUE,
                THRESHOLD_VALUE,
                ALERT_MESSAGE,
                ALERT_STATUS,
                DETECTED_AT
            FROM COMMON.PIPELINE_ALERTS
            WHERE ALERT_STATUS = 'OPEN'
            ORDER BY SEVERITY DESC, DETECTED_AT DESC
            """
            alerts_df = run_query(query_alerts)

            if not alerts_df.empty:
                # Alert summary
                col1, col2, col3 = st.columns(3)

                critical_count = len(alerts_df[alerts_df['SEVERITY'] == 'CRITICAL'])
                warning_count = len(alerts_df[alerts_df['SEVERITY'] == 'WARNING'])

                col1.metric("🔴 Critical Alerts", critical_count)
                col2.metric("🟡 Warning Alerts", warning_count)
                col3.metric("📋 Total Open", len(alerts_df))

                st.divider()

                # Filter by severity
                severity_filter = st.multiselect(
                    "Filter by Severity",
                    options=['CRITICAL', 'WARNING', 'INFO'],
                    default=['CRITICAL', 'WARNING']
                )

                filtered_alerts = alerts_df[alerts_df['SEVERITY'].isin(severity_filter)]

                # Display alerts
                for idx, alert in filtered_alerts.iterrows():
                    severity_color = '#dc3545' if alert['SEVERITY'] == 'CRITICAL' else '#ffc107'
                    severity_icon = '🔴' if alert['SEVERITY'] == 'CRITICAL' else '🟡'

                    with st.container():
                        st.markdown(f"""
                        <div style="border-left: 4px solid {severity_color}; padding: 15px; margin: 10px 0; background-color: #f8f9fa; border-radius: 5px;">
                            <h4>{severity_icon} {alert['PIPELINE_NAME']} - {alert['ALERT_TYPE']}</h4>
                            <p><strong>Message:</strong> {alert['ALERT_MESSAGE']}</p>
                            <p><strong>Metric:</strong> {alert['METRIC_NAME']} | 
                               <strong>Actual:</strong> {alert['ACTUAL_VALUE']} | 
                               <strong>Threshold:</strong> {alert['THRESHOLD_VALUE']}</p>
                            <p><small>Detected at: {alert['DETECTED_AT']}</small></p>
                        </div>
                        """, unsafe_allow_html=True)

                        col1, col2, col3 = st.columns([1, 1, 3])
                        with col1:
                            if st.button(f"Acknowledge", key=f"ack_{idx}"):
                                st.success("Alert acknowledged")
                        with col2:
                            if st.button(f"Resolve", key=f"resolve_{idx}"):
                                st.success("Alert resolved")

                # Alert timeline
                st.subheader("Alert Timeline")
                alerts_df['DETECTED_AT'] = pd.to_datetime(alerts_df['DETECTED_AT'])
                fig = px.scatter(
                    alerts_df,
                    x='DETECTED_AT',
                    y='PIPELINE_NAME',
                    color='SEVERITY',
                    size='ACTUAL_VALUE',
                    hover_data=['ALERT_MESSAGE'],
                    color_discrete_map={'CRITICAL': '#dc3545', 'WARNING': '#ffc107'}
                )
                st.plotly_chart(fig, use_container_width=True)

            else:
                st.success("🎉 No active alerts! All systems operational.")

        except Exception as e:
            st.error(f"Error loading alerts: {str(e)}")

    # ========================================================================
    # MONITORING PAGE 4: ERROR ANALYSIS
    # ========================================================================
    elif page == "❌ Error Analysis":
        st.markdown('<div class="main-header">❌ Error Analysis & Dead Letter Queue</div>', unsafe_allow_html=True)

        try:
            query_errors = "SELECT * FROM COMMON.VW_ERROR_SUMMARY"
            error_df = run_query(query_errors)

            if not error_df.empty:
                # Error summary metrics
                col1, col2, col3, col4 = st.columns(4)

                total_errors = error_df['ERROR_COUNT'].sum()
                pending_errors = error_df['PENDING_COUNT'].sum()
                resolved_errors = error_df['RESOLVED_COUNT'].sum()
                abandoned_errors = error_df['ABANDONED_COUNT'].sum()

                col1.metric("Total Errors", total_errors)
                col2.metric("Pending", pending_errors)
                col3.metric("Resolved", resolved_errors)
                col4.metric("Abandoned", abandoned_errors, delta_color="inverse")

                st.divider()

                # Error breakdown
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("Errors by Pipeline")
                    pipeline_errors = error_df.groupby('PIPELINE_NAME')['ERROR_COUNT'].sum().reset_index()
                    fig = px.bar(
                        pipeline_errors,
                        x='PIPELINE_NAME',
                        y='ERROR_COUNT',
                        color='ERROR_COUNT',
                        color_continuous_scale='Reds'
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.subheader("Errors by Type")
                    type_errors = error_df.groupby('ERROR_TYPE')['ERROR_COUNT'].sum().reset_index()
                    fig = px.pie(
                        type_errors,
                        values='ERROR_COUNT',
                        names='ERROR_TYPE',
                        color_discrete_sequence=px.colors.sequential.Reds
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # Detailed error table
                st.subheader("Error Details")
                error_display = error_df.copy()
                error_display['Resolution_Rate'] = (
                        error_display['RESOLVED_COUNT'] / error_display['ERROR_COUNT'] * 100
                ).round(1)

                st.dataframe(error_display, use_container_width=True, hide_index=True)

            else:
                st.success("No errors in the dead letter queue!")

        except Exception as e:
            st.error(f"Error loading DLQ data: {str(e)}")

    # ========================================================================
    # MONITORING PAGE 5: RECONCILIATION
    # ========================================================================
    elif page == "🔄 Reconciliation":
        st.markdown('<div class="main-header">🔄 Data Reconciliation</div>', unsafe_allow_html=True)

        try:
            query_recon = """
            SELECT 
                PIPELINE_NAME,
                BATCH_ID,
                LAYER,
                SOURCE_COUNT,
                TARGET_COUNT,
                MATCHED_COUNT,
                MISSING_IN_TARGET,
                RECONCILIATION_STATUS,
                RECONCILED_AT
            FROM COMMON.RECONCILIATION_LOG
            ORDER BY RECONCILED_AT DESC
            LIMIT 50
            """
            recon_df = run_query(query_recon)

            if not recon_df.empty:
                # Reconciliation summary
                total_recons = len(recon_df)
                passed = len(recon_df[recon_df['RECONCILIATION_STATUS'] == 'PASS'])
                failed = len(recon_df[recon_df['RECONCILIATION_STATUS'] == 'FAIL'])
                warnings = len(recon_df[recon_df['RECONCILIATION_STATUS'] == 'WARNING'])

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Checks", total_recons)
                col2.metric("✅ Passed", passed, delta=f"{(passed / total_recons * 100):.1f}%")
                col3.metric("⚠️ Warnings", warnings)
                col4.metric("❌ Failed", failed, delta_color="inverse")

                st.divider()

                # Visualizations
                col1, col2 = st.columns(2)

                with col1:
                    st.subheader("Reconciliation by Pipeline")
                    pipeline_status = recon_df.groupby(['PIPELINE_NAME', 'RECONCILIATION_STATUS']).size().reset_index(
                        name='count')
                    fig = px.bar(
                        pipeline_status,
                        x='PIPELINE_NAME',
                        y='count',
                        color='RECONCILIATION_STATUS',
                        barmode='stack',
                        color_discrete_map={'PASS': '#28a745', 'WARNING': '#ffc107', 'FAIL': '#dc3545'}
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.subheader("Reconciliation by Layer")
                    layer_status = recon_df.groupby(['LAYER', 'RECONCILIATION_STATUS']).size().reset_index(name='count')
                    fig = px.bar(
                        layer_status,
                        x='LAYER',
                        y='count',
                        color='RECONCILIATION_STATUS',
                        barmode='group',
                        color_discrete_map={'PASS': '#28a745', 'WARNING': '#ffc107', 'FAIL': '#dc3545'}
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # Detailed table
                st.subheader("Recent Reconciliations")
                recon_df['MATCH_%'] = (
                        recon_df['MATCHED_COUNT'] / recon_df['SOURCE_COUNT'] * 100
                ).round(2)


                def color_recon_status(val):
                    if val == 'PASS':
                        return 'background-color: #d4edda; color: #28a745'
                    elif val == 'WARNING':
                        return 'background-color: #fff3cd; color: #856404'
                    elif val == 'FAIL':
                        return 'background-color: #fee; color: #dc3545'
                    return ''


                styled_recon = recon_df.style.applymap(
                    color_recon_status,
                    subset=['RECONCILIATION_STATUS']
                )
                st.dataframe(styled_recon, use_container_width=True, hide_index=True)

        except Exception as e:
            st.error(f"Error loading reconciliation data: {str(e)}")

    # ========================================================================
    # MONITORING PAGE 6: PERFORMANCE METRICS
    # ========================================================================
    elif page == "📈 Performance Metrics":
        st.markdown('<div class="main-header">📈 Performance Analysis</div>', unsafe_allow_html=True)

        try:
            query_perf_deg = "SELECT * FROM COMMON.VW_PERFORMANCE_DEGRADATION"
            perf_deg_df = run_query(query_perf_deg)

            query_runs = """
            SELECT 
                PIPELINE_NAME,
                EXECUTION_DURATION_SEC,
                SOURCE_ROW_COUNT,
                VALID_ROW_COUNT,
                EXECUTED_AT
            FROM COMMON.INGEST_RUN
            WHERE EXECUTED_AT >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
            ORDER BY EXECUTED_AT DESC
            """
            ingest_runs = run_query(query_runs)

            # Performance degradation summary
            if not perf_deg_df.empty:
                st.subheader("Performance Degradation Detection")

                degraded = len(perf_deg_df[perf_deg_df['PERFORMANCE_STATUS'] == '⚠️ DEGRADED'])
                normal = len(perf_deg_df[perf_deg_df['PERFORMANCE_STATUS'] == '✅ NORMAL'])

                col1, col2 = st.columns(2)
                col1.metric("⚠️ Degraded Pipelines", degraded, delta_color="inverse")
                col2.metric("✅ Normal Pipelines", normal)

                st.divider()

                # Performance comparison
                st.subheader("Baseline vs Recent Performance")

                for idx, row in perf_deg_df.iterrows():
                    with st.expander(f"{row['PERFORMANCE_STATUS']} {row['PIPELINE_NAME']}",
                                     expanded=(row['PERFORMANCE_STATUS'] == '⚠️ DEGRADED')):
                        col1, col2, col3 = st.columns(3)

                        with col1:
                            st.metric("Baseline Duration", f"{row['BASELINE_DURATION']:.1f}s")

                        with col2:
                            st.metric(
                                "Recent Duration",
                                f"{row['RECENT_DURATION']:.1f}s",
                                delta=f"{row['DURATION_CHANGE_PCT']:.1f}%",
                                delta_color="inverse"
                            )

                        with col3:
                            st.metric(
                                "Quality Change",
                                f"{row['RECENT_QUALITY_PCT']:.1f}%",
                                delta=f"{row['QUALITY_CHANGE_PCT']:.1f}%"
                            )

                        st.info(row['ALERT_MESSAGE'])

            # Pipeline execution trends
            if not ingest_runs.empty:
                st.subheader("Execution Duration Trends")

                ingest_runs['EXECUTED_AT'] = pd.to_datetime(ingest_runs['EXECUTED_AT'])

                fig = px.line(
                    ingest_runs,
                    x='EXECUTED_AT',
                    y='EXECUTION_DURATION_SEC',
                    color='PIPELINE_NAME',
                    markers=True,
                    title="Pipeline Execution Duration Over Time"
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

                # Throughput analysis
                st.subheader("Pipeline Throughput")

                ingest_runs['THROUGHPUT'] = ingest_runs['SOURCE_ROW_COUNT'] / ingest_runs['EXECUTION_DURATION_SEC']

                fig = px.bar(
                    ingest_runs.groupby('PIPELINE_NAME')['THROUGHPUT'].mean().reset_index(),
                    x='PIPELINE_NAME',
                    y='THROUGHPUT',
                    title="Average Throughput (rows/second)",
                    color='THROUGHPUT',
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig, use_container_width=True)

                # Quality trends
                st.subheader("Data Quality Trends")

                ingest_runs['QUALITY_%'] = (
                        ingest_runs['VALID_ROW_COUNT'] / ingest_runs['SOURCE_ROW_COUNT'] * 100
                ).round(2)

                fig = px.line(
                    ingest_runs,
                    x='EXECUTED_AT',
                    y='QUALITY_%',
                    color='PIPELINE_NAME',
                    markers=True,
                    title="Data Quality Over Time"
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

        except Exception as e:
            st.error(f"Error loading performance data: {str(e)}")

# ============================================================================
# BUSINESS ANALYTICS SECTION
# ============================================================================

else:  # Business Analytics

    # ========================================================================
    # ANALYTICS PAGE 1: EXECUTIVE OVERVIEW
    # ========================================================================
    if page == "📊 Executive Overview":
        st.markdown('<div class="main-header">🎯 Executive Overview Dashboard</div>', unsafe_allow_html=True)

        # Key Metrics
        col1, col2, col3, col4 = st.columns(4)

        query_orders = f"""
        SELECT COUNT(DISTINCT ORDER_ID) as TOTAL_ORDERS,
               SUM(TOTAL_AMOUNT) as TOTAL_REVENUE,
               AVG(TOTAL_AMOUNT) as AVG_ORDER_VALUE,
               COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS
        FROM ANALYTICS.MART_ORDER_SUMMARY
        WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
        """
        metrics = run_query(query_orders)

        if not metrics.empty:
            with col1:
                st.metric("Total Orders", f"{metrics['TOTAL_ORDERS'][0]:,}")
            with col2:
                st.metric("Total Revenue", f"₹{metrics['TOTAL_REVENUE'][0]:,.2f}")
            with col3:
                st.metric("Avg Order Value", f"₹{metrics['AVG_ORDER_VALUE'][0]:,.2f}")
            with col4:
                st.metric("Unique Customers", f"{metrics['UNIQUE_CUSTOMERS'][0]:,}")

        # Row 2: Charts
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📈 Daily Revenue Trend")
            query_daily = f"""
            SELECT DATE(ORDER_DATE) as ORDER_DATE,
                   COUNT(DISTINCT ORDER_ID) as ORDERS,
                   SUM(TOTAL_AMOUNT) as REVENUE
            FROM ANALYTICS.MART_ORDER_SUMMARY
            WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY DATE(ORDER_DATE)
            ORDER BY ORDER_DATE
            """
            daily_data = run_query(query_daily)
            if not daily_data.empty:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=daily_data['ORDER_DATE'],
                    y=daily_data['REVENUE'],
                    mode='lines+markers',
                    name='Revenue',
                    line=dict(color='#1f77b4', width=2)
                ))
                fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🕐 Orders by Time of Day")
            query_time = f"""
            SELECT TIME_OF_DAY,
                   COUNT(DISTINCT ORDER_ID) as ORDERS
            FROM ANALYTICS.MART_ORDER_TIME_ANALYSIS
            WHERE ORDER_DATE_KEY BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY TIME_OF_DAY
            ORDER BY 
                CASE TIME_OF_DAY
                    WHEN 'MORNING' THEN 1
                    WHEN 'AFTERNOON' THEN 2
                    WHEN 'EVENING' THEN 3
                    WHEN 'NIGHT' THEN 4
                END
            """
            time_data = run_query(query_time)
            if not time_data.empty:
                fig = px.bar(time_data, x='TIME_OF_DAY', y='ORDERS',
                             color='TIME_OF_DAY',
                             color_discrete_sequence=px.colors.qualitative.Set3)
                fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0), showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        # Row 3: More insights
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🏆 Top 5 Restaurants")
            query_top_rest = """
            SELECT RESTAURANT_NAME,
                   TOTAL_ORDERS,
                   TOTAL_REVENUE,
                   AVG_ORDER_VALUE
            FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE
            ORDER BY TOTAL_ORDERS DESC
            LIMIT 5
            """
            top_restaurants = run_query(query_top_rest)
            if not top_restaurants.empty:
                st.dataframe(
                    top_restaurants.style.format({
                        'TOTAL_ORDERS': '{:,.0f}',
                        'TOTAL_REVENUE': '₹{:,.2f}',
                        'AVG_ORDER_VALUE': '₹{:,.2f}'
                    }),
                    use_container_width=True,
                    hide_index=True
                )

        with col2:
            st.subheader("🍽️ Cuisine Performance")
            query_cuisine = """
            SELECT CUISINE_TYPE,
                   TOTAL_ORDERS,
                   TOTAL_REVENUE
            FROM ANALYTICS.MART_CUISINE_PERFORMANCE
            ORDER BY TOTAL_ORDERS DESC
            LIMIT 5
            """
            cuisine_data = run_query(query_cuisine)
            if not cuisine_data.empty:
                fig = px.pie(cuisine_data, values='TOTAL_ORDERS', names='CUISINE_TYPE',
                             title='Orders by Cuisine Type')
                fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)

    # ========================================================================
    # ANALYTICS PAGE 2: CUSTOMER ANALYTICS
    # ========================================================================
    elif page == "👥 Customer Analytics":
        st.markdown('<div class="main-header">👥 Customer Analytics Dashboard</div>', unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)

        query_customer_metrics = """
        SELECT 
            COUNT(DISTINCT CUSTOMER_ID) as TOTAL_CUSTOMERS,
            AVG(LIFETIME_VALUE) as AVG_LTV,
            AVG(TOTAL_ORDERS) as AVG_ORDERS_PER_CUSTOMER,
            SUM(CASE WHEN CUSTOMER_STATUS = 'ACTIVE' THEN 1 ELSE 0 END) as ACTIVE_CUSTOMERS
        FROM ANALYTICS.MART_CUSTOMER_ORDER_SUMMARY
        """
        cust_metrics = run_query(query_customer_metrics)

        if not cust_metrics.empty:
            with col1:
                st.metric("Total Customers", f"{cust_metrics['TOTAL_CUSTOMERS'][0]:,}")
            with col2:
                st.metric("Avg Lifetime Value", f"₹{cust_metrics['AVG_LTV'][0]:,.2f}")
            with col3:
                st.metric("Avg Orders/Customer", f"{cust_metrics['AVG_ORDERS_PER_CUSTOMER'][0]:.1f}")
            with col4:
                st.metric("Active Customers", f"{cust_metrics['ACTIVE_CUSTOMERS'][0]:,}")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🎯 Customer Segmentation")
            query_segments = """
            SELECT CUSTOMER_SEGMENT,
                   COUNT(DISTINCT CUSTOMER_ID) as CUSTOMERS,
                   AVG(LIFETIME_VALUE) as AVG_LTV
            FROM ANALYTICS.MART_CUSTOMER_ORDER_SUMMARY
            GROUP BY CUSTOMER_SEGMENT
            ORDER BY 
                CASE CUSTOMER_SEGMENT
                    WHEN 'VIP' THEN 1
                    WHEN 'LOYAL' THEN 2
                    WHEN 'REGULAR' THEN 3
                    WHEN 'REPEAT' THEN 4
                    WHEN 'NEW' THEN 5
                END
            """
            segments = run_query(query_segments)
            if not segments.empty:
                fig = px.bar(segments, x='CUSTOMER_SEGMENT', y='CUSTOMERS',
                             color='AVG_LTV',
                             color_continuous_scale='Blues')
                fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("📊 Customer Status Distribution")
            query_status = """
            SELECT CUSTOMER_STATUS,
                   COUNT(DISTINCT CUSTOMER_ID) as CUSTOMERS
            FROM ANALYTICS.MART_CUSTOMER_ORDER_SUMMARY
            GROUP BY CUSTOMER_STATUS
            """
            status_data = run_query(query_status)
            if not status_data.empty:
                fig = px.pie(status_data, values='CUSTOMERS', names='CUSTOMER_STATUS',
                             color_discrete_sequence=['#2ecc71', '#f39c12', '#e74c3c'])
                fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("📈 Customer Cohort Retention")
        query_cohort = """
        SELECT COHORT_MONTH,
        MONTHS_SINCE_FIRST_ORDER,
        AVG(RETENTION_RATE) as RETENTION_RATE  -- Aggregate to handle duplicates
        FROM ANALYTICS.MART_CUSTOMER_COHORT_ANALYSIS
        WHERE COHORT_MONTH >= DATEADD(MONTH, -6, CURRENT_DATE())
        GROUP BY COHORT_MONTH, MONTHS_SINCE_FIRST_ORDER
        ORDER BY COHORT_MONTH, MONTHS_SINCE_FIRST_ORDER
        """
        cohort_data = run_query(query_cohort)

        if not cohort_data.empty:
            pivot_cohort = cohort_data.pivot(
                index='COHORT_MONTH',
                columns='MONTHS_SINCE_FIRST_ORDER',
                values='RETENTION_RATE'
            )
            fig = px.imshow(pivot_cohort,
                            labels=dict(x="Months Since First Order", y="Cohort Month", color="Retention %"),
                            color_continuous_scale='RdYlGn',
                            aspect='auto')
            fig.update_layout(height=400, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

    # ========================================================================
    # Continue with remaining analytics pages...
    # ========================================================================
    elif page == "🍽️ Restaurant Performance":
        st.markdown('<div class="main-header">🍽️ Restaurant Performance Dashboard</div>', unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)

        query_rest_metrics = """
        SELECT 
            COUNT(DISTINCT RESTAURANT_ID) as TOTAL_RESTAURANTS,
            AVG(TOTAL_ORDERS) as AVG_ORDERS,
            AVG(AVG_ORDER_VALUE) as AVG_ORDER_VALUE,
            AVG(CANCELLATION_RATE) as AVG_CANCEL_RATE
        FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE
        """
        rest_metrics = run_query(query_rest_metrics)

        if not rest_metrics.empty:
            with col1:
                st.metric("Total Restaurants", f"{rest_metrics['TOTAL_RESTAURANTS'][0]:,}")
            with col2:
                st.metric("Avg Orders/Restaurant", f"{rest_metrics['AVG_ORDERS'][0]:.0f}")
            with col3:
                st.metric("Avg Order Value", f"₹{rest_metrics['AVG_ORDER_VALUE'][0]:,.2f}")
            with col4:
                st.metric("Avg Cancellation Rate", f"{rest_metrics['AVG_CANCEL_RATE'][0]:.1f}%")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🏆 Restaurant Tier Distribution")
            query_tiers = """
            SELECT RESTAURANT_TIER,
                   COUNT(DISTINCT RESTAURANT_ID) as RESTAURANTS,
                   AVG(TOTAL_REVENUE) as AVG_REVENUE
            FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE
            GROUP BY RESTAURANT_TIER
            """
            tiers = run_query(query_tiers)
            if not tiers.empty:
                fig = px.bar(tiers, x='RESTAURANT_TIER', y='RESTAURANTS',
                             color='AVG_REVENUE',
                             color_continuous_scale='Viridis')
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("📊 Top Cuisines by Revenue")
            query_cuisine_rev = """
            SELECT CUISINE_TYPE,
                   TOTAL_REVENUE
            FROM ANALYTICS.MART_CUISINE_PERFORMANCE
            ORDER BY TOTAL_REVENUE DESC
            LIMIT 8
            """
            cuisine_rev = run_query(query_cuisine_rev)
            if not cuisine_rev.empty:
                fig = px.bar(cuisine_rev, y='CUISINE_TYPE', x='TOTAL_REVENUE',
                             orientation='h',
                             color='TOTAL_REVENUE',
                             color_continuous_scale='Blues')
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("📋 Top Performing Restaurants")

        query_cities = "SELECT DISTINCT CITY FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE ORDER BY CITY"
        cities = run_query(query_cities)
        if not cities.empty:
            selected_city = st.selectbox("Filter by City", ["All"] + cities['CITY'].tolist())

            city_filter = "" if selected_city == "All" else f"WHERE CITY = '{selected_city}'"

            query_rest_table = f"""
            SELECT RESTAURANT_NAME,
                   CUISINE_TYPE,
                   CITY,
                   TOTAL_ORDERS,
                   TOTAL_REVENUE,
                   AVG_ORDER_VALUE,
                   CANCELLATION_RATE,
                   RESTAURANT_TIER
            FROM ANALYTICS.MART_RESTAURANT_PERFORMANCE
            {city_filter}
            ORDER BY TOTAL_REVENUE DESC
            LIMIT 20
            """
            rest_table = run_query(query_rest_table)
            if not rest_table.empty:
                st.dataframe(
                    rest_table.style.format({
                        'TOTAL_ORDERS': '{:,.0f}',
                        'TOTAL_REVENUE': '₹{:,.2f}',
                        'AVG_ORDER_VALUE': '₹{:,.2f}',
                        'CANCELLATION_RATE': '{:.1f}%'
                    }),
                    use_container_width=True,
                    hide_index=True
                )


    # ============================================================================
    # PAGE 4: DELIVERY ANALYTICS
    # ============================================================================

    elif page == "🚗 Delivery Analytics":
        st.markdown('<div class="main-header">🚗 Delivery Analytics Dashboard</div>', unsafe_allow_html=True)

        # Delivery Metrics
        col1, col2, col3, col4 = st.columns(4)

        query_delivery_metrics = f"""
        SELECT 
            COUNT(DISTINCT DELIVERY_ID) as TOTAL_DELIVERIES,
            AVG(CASE WHEN DELIVERY_TIMELINESS = 'ON_TIME' THEN 1 ELSE 0 END) * 100 as ON_TIME_PCT,
            AVG(ACTUAL_DELIVERY_TIME_MINS) as AVG_DELIVERY_TIME,
            AVG(TIME_VARIANCE_MINS) as AVG_VARIANCE
        FROM ANALYTICS.MART_DELIVERY_TIME_ANALYSIS
        WHERE DELIVERY_DATE BETWEEN '{start_date}' AND '{end_date}'
        """
        del_metrics = run_query(query_delivery_metrics)


        def safe_format(value, format_str, suffix="", default="N/A"):
            """Safely format a value that might be None"""
            if value is None:
                return default
            return format_str.format(value) + suffix


        with col1:
            st.metric("Total Deliveries", safe_format(del_metrics['TOTAL_DELIVERIES'][0], "{:,}"))

        with col2:
            st.metric("On-Time Delivery %", safe_format(del_metrics['ON_TIME_PCT'][0], "{:.1f}", "%"))

        with col3:
            st.metric("Avg Delivery Time", safe_format(del_metrics['AVG_DELIVERY_TIME'][0], "{:.0f}", " mins"))

        with col4:
            st.metric("Avg Time Variance", safe_format(del_metrics['AVG_VARIANCE'][0], "{:+.0f}", " mins"))

        # Row 2
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📊 Delivery Timeliness")
            query_timeliness = f"""
            SELECT DELIVERY_TIMELINESS,
                   COUNT(DISTINCT DELIVERY_ID) as DELIVERIES
            FROM ANALYTICS.MART_DELIVERY_TIME_ANALYSIS
            WHERE DELIVERY_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY DELIVERY_TIMELINESS
            """
            timeliness = run_query(query_timeliness)
            fig = px.pie(timeliness, values='DELIVERIES', names='DELIVERY_TIMELINESS',
                         color_discrete_sequence=['#2ecc71', '#f39c12', '#e74c3c'])
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🚴 Agent Performance Tiers")
            query_agent_tiers = """
            SELECT PERFORMANCE_TIER,
                   COUNT(DISTINCT DELIVERY_AGENT_ID) as AGENTS,
                   AVG(SUCCESS_RATE) as AVG_SUCCESS_RATE
            FROM ANALYTICS.MART_DELIVERY_AGENT_PERFORMANCE
            GROUP BY PERFORMANCE_TIER
            ORDER BY 
                CASE PERFORMANCE_TIER
                    WHEN 'EXCELLENT' THEN 1
                    WHEN 'GOOD' THEN 2
                    WHEN 'AVERAGE' THEN 3
                    WHEN 'NEEDS_IMPROVEMENT' THEN 4
                END
            """
            agent_tiers = run_query(query_agent_tiers)
            fig = px.bar(agent_tiers, x='PERFORMANCE_TIER', y='AGENTS',
                         color='AVG_SUCCESS_RATE',
                         color_continuous_scale='RdYlGn')
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

        # Row 3: Top Delivery Agents
        st.subheader("🏆 Top Delivery Agents")
        query_top_agents = """
        SELECT DELIVERY_AGENT_NAME,
               CITY,
               TOTAL_DELIVERIES,
               SUCCESSFUL_DELIVERIES,
               SUCCESS_RATE,
               AVG_ESTIMATED_TIME_MINS,
               PERFORMANCE_TIER
        FROM ANALYTICS.MART_DELIVERY_AGENT_PERFORMANCE
        ORDER BY SUCCESSFUL_DELIVERIES DESC
        LIMIT 15
        """
        top_agents = run_query(query_top_agents)

        if not top_agents.empty:
            # Fill NULLs with appropriate defaults
            top_agents = top_agents.fillna({
                'TOTAL_DELIVERIES': 0,
                'SUCCESSFUL_DELIVERIES': 0,
                'SUCCESS_RATE': 0,
                'AVG_ESTIMATED_TIME_MINS': 0
            })

            st.dataframe(
                top_agents.style.format({
                    'TOTAL_DELIVERIES': '{:,.0f}',
                    'SUCCESSFUL_DELIVERIES': '{:,.0f}',
                    'SUCCESS_RATE': '{:.1f}%',
                    'AVG_ESTIMATED_TIME_MINS': '{:.0f} mins'
                }),
                use_container_width=True,
                hide_index=True
            )

    # ============================================================================
    # PAGE 5: ORDER ANALYTICS
    # ============================================================================
    elif page == "📦 Order Analytics":
        st.markdown('<div class="main-header">📦 Order Analytics Dashboard</div>', unsafe_allow_html=True)

        # Order Metrics
        col1, col2, col3, col4 = st.columns(4)

        query_order_metrics = f"""
        SELECT 
            COUNT(DISTINCT ORDER_ID) as TOTAL_ORDERS,
            SUM(IS_COMPLETED) as COMPLETED_ORDERS,
            SUM(IS_CANCELLED) as CANCELLED_ORDERS,
            AVG(ORDER_PROCESSING_TIME_MINS) as AVG_PROCESS_TIME
        FROM ANALYTICS.MART_ORDER_SUMMARY
        WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
        """
        order_metrics = run_query(query_order_metrics)

        total_orders = order_metrics['TOTAL_ORDERS'][0]
        completed = order_metrics['COMPLETED_ORDERS'][0]
        cancelled = order_metrics['CANCELLED_ORDERS'][0]

        with col1:
            st.metric("Total Orders", f"{total_orders:,}")
        with col2:
            st.metric("Completed Orders", f"{completed:,}",
                      delta=f"{(completed / total_orders * 100):.1f}%")
        with col3:
            st.metric("Cancelled Orders", f"{cancelled:,}",
                      delta=f"{(cancelled / total_orders * 100):.1f}%", delta_color="inverse")
        with col4:
            st.metric("Avg Process Time", f"{order_metrics['AVG_PROCESS_TIME'][0]:.0f} mins")

        # Row 2
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📊 Order Value Distribution")
            query_value_dist = f"""
            SELECT ORDER_VALUE_SEGMENT,
                   COUNT(DISTINCT ORDER_ID) as ORDERS,
                   SUM(TOTAL_AMOUNT) as REVENUE
            FROM ANALYTICS.MART_ORDER_SUMMARY
            WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY ORDER_VALUE_SEGMENT
            ORDER BY 
                CASE ORDER_VALUE_SEGMENT
                    WHEN 'PREMIUM' THEN 1
                    WHEN 'HIGH_VALUE' THEN 2
                    WHEN 'MEDIUM_VALUE' THEN 3
                    WHEN 'LOW_VALUE' THEN 4
                END
            """
            value_dist = run_query(query_value_dist)
            fig = px.bar(value_dist, x='ORDER_VALUE_SEGMENT', y='ORDERS',
                         color='REVENUE',
                         color_continuous_scale='Greens')
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🕐 Peak Ordering Hours")
            query_peak_hours = f"""
            SELECT ORDER_HOUR,
                   COUNT(DISTINCT ORDER_ID) as ORDERS
            FROM ANALYTICS.MART_ORDER_TIME_ANALYSIS
            WHERE ORDER_DATE_KEY BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY ORDER_HOUR
            ORDER BY ORDER_HOUR
            """
            peak_hours = run_query(query_peak_hours)
            fig = px.line(peak_hours, x='ORDER_HOUR', y='ORDERS',
                          markers=True,
                          line_shape='spline')
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

        # Row 3
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📅 Weekday vs Weekend Orders")
            query_day_type = f"""
            SELECT DAY_TYPE,
                   COUNT(DISTINCT ORDER_ID) as ORDERS,
                   AVG(TOTAL_AMOUNT) as AVG_VALUE
            FROM ANALYTICS.MART_ORDER_SUMMARY
            WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY DAY_TYPE
            """
            day_type = run_query(query_day_type)
            fig = px.bar(day_type, x='DAY_TYPE', y='ORDERS',
                         color='AVG_VALUE',
                         text='ORDERS')
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🍽️ Meal Time Distribution")
            query_meal = f"""
            SELECT MEAL_TIME,
                   COUNT(DISTINCT ORDER_ID) as ORDERS
            FROM ANALYTICS.MART_ORDER_TIME_ANALYSIS
            WHERE ORDER_DATE_KEY BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY MEAL_TIME
            ORDER BY 
                CASE MEAL_TIME
                    WHEN 'BREAKFAST' THEN 1
                    WHEN 'LUNCH' THEN 2
                    WHEN 'SNACK' THEN 3
                    WHEN 'DINNER' THEN 4
                    WHEN 'LATE_NIGHT' THEN 5
                END
            """
            meal_data = run_query(query_meal)
            fig = px.pie(meal_data, values='ORDERS', names='MEAL_TIME',
                         hole=0.4)
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

    # ============================================================================
    # PAGE 6: REVENUE ANALYTICS
    # ============================================================================
    elif page == "💰 Revenue Analytics":
        st.markdown('<div class="main-header">💰 Revenue Analytics Dashboard</div>', unsafe_allow_html=True)

        # Revenue Metrics
        col1, col2, col3, col4 = st.columns(4)

        query_revenue = f"""
        SELECT 
            SUM(TOTAL_AMOUNT) as TOTAL_REVENUE,
            AVG(TOTAL_AMOUNT) as AVG_ORDER_VALUE,
            SUM(TOTAL_AMOUNT) / COUNT(DISTINCT CUSTOMER_ID) as REVENUE_PER_CUSTOMER,
            COUNT(DISTINCT ORDER_ID) as TOTAL_ORDERS
        FROM ANALYTICS.MART_ORDER_SUMMARY
        WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
        """
        rev_metrics = run_query(query_revenue)

        with col1:
            st.metric("Total Revenue", f"₹{rev_metrics['TOTAL_REVENUE'][0]:,.2f}")
        with col2:
            st.metric("Avg Order Value", f"₹{rev_metrics['AVG_ORDER_VALUE'][0]:,.2f}")
        with col3:
            st.metric("Revenue/Customer", f"₹{rev_metrics['REVENUE_PER_CUSTOMER'][0]:,.2f}")
        with col4:
            st.metric("Total Orders", f"{rev_metrics['TOTAL_ORDERS'][0]:,}")

        # Row 2
        st.subheader("📈 Revenue Trend Over Time")
        query_rev_trend = f"""
        SELECT DATE(ORDER_DATE) as DATE,
               SUM(TOTAL_AMOUNT) as DAILY_REVENUE,
               COUNT(DISTINCT ORDER_ID) as DAILY_ORDERS,
               AVG(TOTAL_AMOUNT) as AVG_ORDER_VALUE
        FROM ANALYTICS.MART_ORDER_SUMMARY
        WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY DATE(ORDER_DATE)
        ORDER BY DATE
        """
        rev_trend = run_query(query_rev_trend)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=rev_trend['DATE'],
            y=rev_trend['DAILY_REVENUE'],
            name='Daily Revenue',
            yaxis='y',
            line=dict(color='#1f77b4', width=3)
        ))
        fig.add_trace(go.Scatter(
            x=rev_trend['DATE'],
            y=rev_trend['AVG_ORDER_VALUE'],
            name='Avg Order Value',
            yaxis='y2',
            line=dict(color='#ff7f0e', width=2, dash='dash')
        ))
        fig.update_layout(
            height=400,
            yaxis=dict(title='Daily Revenue (₹)'),
            yaxis2=dict(title='Avg Order Value (₹)', overlaying='y', side='right'),
            hovermode='x unified',
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

        # Row 3
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("💳 Revenue by Payment Method")
            query_payment = f"""
            SELECT PAYMENT_CATEGORY,
                   SUM(TOTAL_AMOUNT) as REVENUE,
                   COUNT(DISTINCT ORDER_ID) as ORDERS
            FROM ANALYTICS.MART_ORDER_PAYMENT_ANALYSIS
            WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY PAYMENT_CATEGORY
            ORDER BY REVENUE DESC
            """
            payment_data = run_query(query_payment)
            fig = px.bar(payment_data, x='PAYMENT_CATEGORY', y='REVENUE',
                         color='ORDERS',
                         color_continuous_scale='Viridis',
                         text='REVENUE')
            fig.update_traces(texttemplate='₹%{text:,.0f}', textposition='outside')
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🏙️ Revenue by City Tier")
            query_city_tier = f"""
            SELECT CITY_TIER,
                   SUM(TOTAL_AMOUNT) as REVENUE,
                   COUNT(DISTINCT ORDER_ID) as ORDERS
            FROM ANALYTICS.MART_ORDER_SUMMARY
            WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY CITY_TIER
            ORDER BY REVENUE DESC
            """
            city_tier = run_query(query_city_tier)
            fig = px.pie(city_tier, values='REVENUE', names='CITY_TIER',
                         hole=0.4,
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

        # Row 4: Top Cities by Revenue
        st.subheader("🌆 Top 10 Cities by Revenue")
        query_top_cities = f"""
        SELECT CITY,
               SUM(TOTAL_AMOUNT) as TOTAL_REVENUE,
               COUNT(DISTINCT ORDER_ID) as TOTAL_ORDERS,
               AVG(TOTAL_AMOUNT) as AVG_ORDER_VALUE,
               COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS
        FROM ANALYTICS.MART_ORDER_SUMMARY
        WHERE ORDER_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY CITY
        ORDER BY TOTAL_REVENUE DESC
        LIMIT 10
        """
        top_cities = run_query(query_top_cities)
        st.dataframe(
            top_cities.style.format({
                'TOTAL_REVENUE': '₹{:,.2f}',
                'TOTAL_ORDERS': '{:,.0f}',
                'AVG_ORDER_VALUE': '₹{:,.2f}',
                'UNIQUE_CUSTOMERS': '{:,.0f}'
            }),
            use_container_width=True,
            hide_index=True
        )

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666; padding: 1rem;'>
    <p>🚀 DataVelocity Unified Dashboard | Pipeline Monitoring + Business Analytics</p>
    <p><small>Data refreshed every 10 minutes | Built with Streamlit & Snowflake</small></p>
</div>
""", unsafe_allow_html=True)