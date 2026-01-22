"""
Warwick SST Events Dashboard

A Streamlit dashboard for visualizing server-side tracking data from Athena.
Designed to be deployed on Streamlit Community Cloud (free tier).

Usage:
    Local: streamlit run app.py
    Cloud: Deploy via Streamlit Community Cloud connected to GitHub repo
"""

import streamlit as st
import pandas as pd
import boto3
import altair as alt
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
import subprocess

# Page configuration
st.set_page_config(
    page_title="Warwick SST Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for larger fonts
st.markdown("""
<style>
    /* Increase base font size */
    html, body, [class*="css"] {
        font-size: 20px;
    }
    /* Larger headers */
    h1 { font-size: 3rem !important; }
    h2 { font-size: 2.5rem !important; }
    h3 { font-size: 2rem !important; }
    h4 { font-size: 1.75rem !important; }
    /* Larger metrics */
    [data-testid="stMetricValue"] {
        font-size: 3rem !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 1.25rem !important;
    }
    /* Larger table text */
    .stDataFrame, .stDataFrame td, .stDataFrame th {
        font-size: 1.1rem !important;
    }
    /* Sidebar text */
    .css-1d391kg, .st-emotion-cache-1d391kg, [data-testid="stSidebar"] {
        font-size: 1.1rem !important;
    }
    /* Tab labels */
    .stTabs [data-baseweb="tab"] {
        font-size: 1.2rem !important;
    }
    /* Markdown text */
    .stMarkdown, .stMarkdown p {
        font-size: 1.1rem !important;
    }
    /* Selectbox and inputs */
    .stSelectbox, .stTextInput, .stDateInput {
        font-size: 1.1rem !important;
    }
    /* Info/warning boxes */
    .stAlert {
        font-size: 1.1rem !important;
    }
</style>
""", unsafe_allow_html=True)

# Version - automatically read from git at runtime
def get_version() -> str:
    """Get git commit hash. Works on Streamlit Cloud since repo is cloned with .git."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"

VERSION = get_version()

# Altair chart font configuration
CHART_FONT_SIZE = 17
CHART_TITLE_SIZE = 19
CHART_LABEL_SIZE = 16

# Constants
AWS_REGION = "ap-southeast-2"
ATHENA_DATABASE = "warwick_weave_sst_events"
ATHENA_TABLE = "events"
ATHENA_OUTPUT_LOCATION = "s3://warwick-com-au-athena-results/"
ATHENA_WORKGROUP = "primary"

# Helper to extract JSON fields from base64-encoded raw_payload
# Usage: JSON_FIELD("client_id") -> json_extract_scalar(json_parse(from_utf8(from_base64(raw_payload))), '$.client_id')
def json_field(field_name: str) -> str:
    """Generate Athena SQL to extract a field from base64-encoded JSON raw_payload."""
    return f"json_extract_scalar(json_parse(from_utf8(from_base64(raw_payload))), '$.{field_name}')"


@st.cache_resource
def get_athena_client():
    """Create Athena client using credentials from Streamlit secrets or environment."""
    try:
        # Try Streamlit secrets first (for Streamlit Cloud deployment)
        if "aws" in st.secrets:
            return boto3.client(
                "athena",
                region_name=AWS_REGION,
                aws_access_key_id=st.secrets["aws"]["access_key_id"],
                aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
            )
    except Exception:
        pass
    # Fall back to environment/profile credentials (for local development)
    return boto3.client("athena", region_name=AWS_REGION)


@st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour
def run_athena_query(query: str, timeout_seconds: int = 60) -> pd.DataFrame:
    """Execute an Athena query and return results as a DataFrame."""
    client = get_athena_client()

    # Start query execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        WorkGroup=ATHENA_WORKGROUP,
    )
    query_execution_id = response["QueryExecutionId"]

    # Wait for query to complete
    start_time = time.time()
    while True:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            error = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            raise Exception(f"Query {state}: {error}")

        if time.time() - start_time > timeout_seconds:
            client.stop_query_execution(QueryExecutionId=query_execution_id)
            raise Exception(f"Query timed out after {timeout_seconds} seconds")

        time.sleep(0.5)

    # Get results
    results = client.get_query_results(QueryExecutionId=query_execution_id)

    # Parse results into DataFrame
    columns = [col["Label"] for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in results["ResultSet"]["Rows"][1:]:  # Skip header row
        rows.append([field.get("VarCharValue", "") for field in row["Data"]])

    return pd.DataFrame(rows, columns=columns)


def main():
    st.title("📊 Warwick SST Events Dashboard")
    st.markdown("Real-time analytics from server-side tracking data")

    # Sidebar for date selection
    with st.sidebar:
        st.header("Filters")

        # Date range selector
        today = datetime.now().date()
        default_start = today - timedelta(days=7)

        date_range = st.date_input(
            "Date Range",
            value=(default_start, today),
            max_value=today,
            help="Select the date range for analysis"
        )

        if len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range[0]

        # Format dates for Athena query (UTC timestamps in S3)
        start_ts = f"{start_date.isoformat()}T00:00:00Z"
        end_ts = f"{end_date.isoformat()}T23:59:59Z"

        st.markdown("---")
        st.markdown(f"**Database:** `{ATHENA_DATABASE}`")
        st.markdown(f"**Region:** `{AWS_REGION}`")

        st.markdown("---")
        if st.button("🔄 Clear Cache", help="Force refresh all data from Athena"):
            st.cache_data.clear()
            st.rerun()

    # Main content area
    try:
        # Test connection with a simple query
        with st.spinner("Connecting to Athena..."):
            test_df = run_athena_query("SELECT 1 as test", timeout_seconds=30)

        # Layout with tabs
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Overview", "🔄 SST vs Direct", "🎯 Events", "🔍 Raw Data"])

        with tab1:
            st.subheader("Event Overview")

            # AU vs NZ Comparison (top section)
            st.markdown("#### AU vs NZ Comparison")
            query = f"""
            SELECT
                CASE
                    WHEN {json_field('page_location')} LIKE '%warwick.com.au%' THEN 'AU'
                    WHEN {json_field('page_location')} LIKE '%warwick.co.nz%' THEN 'NZ'
                    ELSE 'Other'
                END as region,
                COUNT(*) as events,
                COUNT(DISTINCT {json_field('ga_session_id')}) as sessions,
                COUNT(CASE WHEN event_name = 'page_view' THEN 1 END) as pageviews
            FROM {ATHENA_TABLE}
            WHERE timestamp >= '{start_ts}'
              AND timestamp <= '{end_ts}'
            GROUP BY 1
            ORDER BY events DESC
            """
            with st.spinner("Loading AU vs NZ comparison..."):
                df = run_athena_query(query)
                if not df.empty:
                    df["events"] = pd.to_numeric(df["events"])
                    df["sessions"] = pd.to_numeric(df["sessions"])
                    df["pageviews"] = pd.to_numeric(df["pageviews"])

                    col1, col2, col3 = st.columns(3)

                    # Metrics by region
                    au_row = df[df["region"] == "AU"]
                    nz_row = df[df["region"] == "NZ"]

                    au_sessions = int(au_row["sessions"].iloc[0]) if not au_row.empty else 0
                    nz_sessions = int(nz_row["sessions"].iloc[0]) if not nz_row.empty else 0
                    au_pageviews = int(au_row["pageviews"].iloc[0]) if not au_row.empty else 0
                    nz_pageviews = int(nz_row["pageviews"].iloc[0]) if not nz_row.empty else 0
                    au_events = int(au_row["events"].iloc[0]) if not au_row.empty else 0
                    nz_events = int(nz_row["events"].iloc[0]) if not nz_row.empty else 0

                    with col1:
                        st.metric("AU Sessions", f"{au_sessions:,}")
                        st.metric("NZ Sessions", f"{nz_sessions:,}")
                    with col2:
                        st.metric("AU Pageviews", f"{au_pageviews:,}")
                        st.metric("NZ Pageviews", f"{nz_pageviews:,}")
                    with col3:
                        st.metric("AU Events", f"{au_events:,}")
                        st.metric("NZ Events", f"{nz_events:,}")

                    # Bar chart comparison
                    chart_df = df[df["region"].isin(["AU", "NZ"])]
                    if not chart_df.empty:
                        chart = alt.Chart(chart_df).mark_bar().encode(
                            x=alt.X("region:N", title="Region"),
                            y=alt.Y("pageviews:Q", title="Pageviews"),
                            color=alt.Color("region:N", scale=alt.Scale(domain=["AU", "NZ"], range=["#1f77b4", "#ff7f0e"])),
                            tooltip=["region", "pageviews", "sessions", "events"]
                        ).properties(height=200).configure_axis(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        ).configure_legend(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        )
                        st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("No events found for selected date range")

            st.markdown("---")
            col1, col2 = st.columns(2)

            with col1:
                # Total events by type
                st.markdown("#### Events by Type")
                query = f"""
                SELECT
                    event_name,
                    COUNT(*) as count
                FROM {ATHENA_TABLE}
                WHERE timestamp >= '{start_ts}'
                  AND timestamp <= '{end_ts}'
                GROUP BY event_name
                ORDER BY count DESC
                LIMIT 20
                """
                with st.spinner("Loading event counts..."):
                    df = run_athena_query(query)
                    if not df.empty:
                        df["count"] = pd.to_numeric(df["count"])
                        chart = alt.Chart(df).mark_bar().encode(
                            x=alt.X("event_name:N", sort="-y", title="Event"),
                            y=alt.Y("count:Q", title="Count"),
                            tooltip=["event_name", "count"]
                        ).properties(height=300).configure_axis(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        )
                        st.altair_chart(chart, use_container_width=True)
                    else:
                        st.info("No events found for selected date range")

            with col2:
                # Daily traffic trends
                st.markdown("#### Traffic Trends (Daily)")
                query = f"""
                SELECT
                    DATE(from_iso8601_timestamp(timestamp)) as date,
                    CASE
                        WHEN {json_field('page_location')} LIKE '%warwick.com.au%' THEN 'AU'
                        WHEN {json_field('page_location')} LIKE '%warwick.co.nz%' THEN 'NZ'
                        ELSE 'Other'
                    END as region,
                    COUNT(*) as count
                FROM {ATHENA_TABLE}
                WHERE timestamp >= '{start_ts}'
                  AND timestamp <= '{end_ts}'
                GROUP BY 1, 2
                ORDER BY date
                """
                with st.spinner("Loading daily trends..."):
                    df = run_athena_query(query)
                    if not df.empty:
                        df["count"] = pd.to_numeric(df["count"])
                        df["date"] = pd.to_datetime(df["date"])
                        df = df[df["region"].isin(["AU", "NZ"])]
                        chart = alt.Chart(df).mark_line(point=True).encode(
                            x=alt.X("date:T",
                                    title="Date",
                                    axis=alt.Axis(format="%b %d", tickCount="day", ticks=True, tickSize=8)),
                            y=alt.Y("count:Q", title="Events", axis=alt.Axis(ticks=True, tickSize=8)),
                            color=alt.Color("region:N", scale=alt.Scale(domain=["AU", "NZ"], range=["#1f77b4", "#ff7f0e"])),
                            tooltip=[alt.Tooltip("date:T", format="%Y-%m-%d"), "region", "count"]
                        ).properties(height=300).configure_axis(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        ).configure_legend(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        )
                        st.altair_chart(chart, use_container_width=True)
                    else:
                        st.info("No events found for selected date range")

            st.markdown("---")
            col1, col2 = st.columns(2)

            with col1:
                # Top pages by domain
                st.markdown("#### Top Pages (AU)")
                query = f"""
                SELECT
                    regexp_extract({json_field('page_location')}, 'warwick\\.com\\.au(/[^?]*)', 1) as page_path,
                    COUNT(*) as pageviews
                FROM {ATHENA_TABLE}
                WHERE timestamp >= '{start_ts}'
                  AND timestamp <= '{end_ts}'
                  AND {json_field('page_location')} LIKE '%warwick.com.au%'
                  AND event_name = 'page_view'
                GROUP BY 1
                ORDER BY pageviews DESC
                LIMIT 10
                """
                with st.spinner("Loading top AU pages..."):
                    df = run_athena_query(query)
                    if not df.empty:
                        df["pageviews"] = pd.to_numeric(df["pageviews"])
                        df["page_path"] = df["page_path"].fillna("/")
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No AU pageviews found")

            with col2:
                st.markdown("#### Top Pages (NZ)")
                query = f"""
                SELECT
                    regexp_extract({json_field('page_location')}, 'warwick\\.co\\.nz(/[^?]*)', 1) as page_path,
                    COUNT(*) as pageviews
                FROM {ATHENA_TABLE}
                WHERE timestamp >= '{start_ts}'
                  AND timestamp <= '{end_ts}'
                  AND {json_field('page_location')} LIKE '%warwick.co.nz%'
                  AND event_name = 'page_view'
                GROUP BY 1
                ORDER BY pageviews DESC
                LIMIT 10
                """
                with st.spinner("Loading top NZ pages..."):
                    df = run_athena_query(query)
                    if not df.empty:
                        df["pageviews"] = pd.to_numeric(df["pageviews"])
                        df["page_path"] = df["page_path"].fillna("/")
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No NZ pageviews found")

            # Device breakdown
            st.markdown("---")
            st.markdown("#### Device Breakdown")
            query = f"""
            SELECT
                CASE
                    WHEN {json_field('page_location')} LIKE '%warwick.com.au%' THEN 'AU'
                    WHEN {json_field('page_location')} LIKE '%warwick.co.nz%' THEN 'NZ'
                    ELSE 'Other'
                END as region,
                COALESCE({json_field('device_category')}, 'unknown') as device,
                COUNT(DISTINCT {json_field('ga_session_id')}) as sessions
            FROM {ATHENA_TABLE}
            WHERE timestamp >= '{start_ts}'
              AND timestamp <= '{end_ts}'
            GROUP BY 1, 2
            ORDER BY sessions DESC
            """
            with st.spinner("Loading device breakdown..."):
                df = run_athena_query(query)
                if not df.empty:
                    df["sessions"] = pd.to_numeric(df["sessions"])
                    df = df[df["region"].isin(["AU", "NZ"])]
                    if not df.empty:
                        chart = alt.Chart(df).mark_bar().encode(
                            x=alt.X("region:N", title="Region"),
                            y=alt.Y("sessions:Q", title="Sessions"),
                            color=alt.Color("device:N", title="Device"),
                            tooltip=["region", "device", "sessions"]
                        ).properties(height=250).configure_axis(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        ).configure_legend(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        )
                        st.altair_chart(chart, use_container_width=True)
                    else:
                        st.info("No device data found")
                else:
                    st.info("No device data found")

            # Key metrics
            st.markdown("---")
            st.markdown("#### Key Metrics (Total)")

            query = f"""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT {json_field('client_id')}) as unique_clients,
                COUNT(DISTINCT {json_field('ga_session_id')}) as unique_sessions
            FROM {ATHENA_TABLE}
            WHERE timestamp >= '{start_ts}'
              AND timestamp <= '{end_ts}'
            """
            with st.spinner("Loading metrics..."):
                df = run_athena_query(query)
                if not df.empty:
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Total Events", f"{int(df['total_events'].iloc[0]):,}")
                    m2.metric("Unique Clients", f"{int(df['unique_clients'].iloc[0]):,}")
                    m3.metric("Unique Sessions", f"{int(df['unique_sessions'].iloc[0]):,}")

        with tab2:
            st.subheader("SST vs Direct Comparison")
            st.info("**Historical Snapshot:** Jan 10-14, 2026 (UTC-aligned, Warwick AU only)")

            # Executive Summary - Business Value
            st.markdown("#### 🎯 Executive Summary")

            # Key metrics row
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric("Dual-Property Lift", "+14.5%", help="Extra sessions captured by running both vs Direct alone")
            with m2:
                st.metric("Session Overlap", "71.6%", help="Sessions seen by both systems")
            with m3:
                st.metric("SST-Only", "12.7%", help="Ad-blocker bypass wins")
            with m4:
                st.metric("Direct-Only", "15.8%", help="Corporate firewalls blocking SST")

            st.success("""
            **SST is working and delivering measurable value.** Running both systems captures 1,672 sessions that would otherwise be invisible to Direct.
            """)

            # Three-column summary
            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown("""
                **✅ SST Captures (Direct Misses)**
                - **Ad-blocker users** — 81.7% desktop, 83% Chrome/Firefox/Edge
                - **China (GFW blocks Direct)** — 34.6% of SST-only from China
                - Scroll events (+9.8% more)
                - Conversion parity (99%+)
                """)

            with col2:
                st.markdown("""
                **⚠️ Direct Captures (SST Misses)**
                - **Corporate networks** — +8.3pp during business hours
                - Firewalls whitelist `google-analytics.com`
                - Block unknown domains like `sst.warwick.com.au`
                - ~5% invisible to both (Safari Private, Brave)
                """)

            with col3:
                st.markdown("""
                **📋 Recommendations**
                - Continue dual-property approach
                - No GTM proxy needed (low ROI)
                - SST value highest on weekends/holidays
                - Review quarterly for traffic shifts
                """)

            with st.expander("📐 Architecture & Data Sources"):
                arch_col1, arch_col2 = st.columns(2)
                with arch_col1:
                    st.markdown("""
                    **Tracking Flow:**
                    - **SST:** Browser → `sst.warwick.com.au` → GA4 `G-Y0RSKRWP87`
                    - **Direct:** Browser → `google-analytics.com` → GA4 `G-EP4KTC47K3`
                    - Both fire from GTM web container `GTM-P8LRDK2`
                    """)
                with arch_col2:
                    st.markdown("""
                    **Data Sources:**
                    - **Direct:** BigQuery `analytics_375839889`
                    - **SST:** Athena `warwick_weave_sst_events.events`
                    - **Period:** Jan 10-14, 2026 (5 days, UTC-aligned)
                    """)

            st.markdown("---")

            # Traffic Pattern Analysis
            st.markdown("#### 📅 Traffic Pattern: Weekday vs Holiday")
            st.markdown("*SST advantage varies by traffic type*")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Holiday & Weekend (Jan 1-4)**")
                holiday_data = pd.DataFrame({
                    "Date": ["Jan 1 (Thu)", "Jan 2 (Fri)", "Jan 3 (Sat)", "Jan 4 (Sun)"],
                    "Direct": [1402, 1659, 1346, 1260],
                    "SST": [1479, 1700, 1370, 1255],
                    "SST Advantage": ["+5.5%", "+2.5%", "+1.8%", "-0.4%"]
                })
                st.dataframe(holiday_data, use_container_width=True, hide_index=True)

            with col2:
                st.markdown("**Week 1 Weekdays (Jan 5-9)**")
                weekday_data = pd.DataFrame({
                    "Date": ["Jan 5 (Mon)", "Jan 6 (Tue)", "Jan 7 (Wed)", "Jan 8 (Thu)", "Jan 9 (Fri)"],
                    "Direct": [2068, 2106, 2246, 2086, 1954],
                    "SST": [2034, 2094, 2223, 2080, 1695],
                    "SST Advantage": ["-1.6%", "-0.6%", "-1.0%", "-0.3%", "-13.3%"]
                })
                st.dataframe(weekday_data, use_container_width=True, hide_index=True)

            col3, col4 = st.columns(2)
            with col3:
                st.markdown("**Weekend (Jan 10-11)**")
                weekend_data = pd.DataFrame({
                    "Date": ["Jan 10 (Sat)", "Jan 11 (Sun)"],
                    "Direct": [1248, 1168],
                    "SST": [1264, 1483],
                    "SST Advantage": ["+1.3%", "+27.0%"]
                })
                st.dataframe(weekend_data, use_container_width=True, hide_index=True)

            with col4:
                st.markdown("**Week 2 Weekdays (Jan 12-13)**")
                week2_data = pd.DataFrame({
                    "Date": ["Jan 12 (Mon)", "Jan 13 (Tue)"],
                    "Direct": [2962, 3226],
                    "SST": [3065, 3235],
                    "SST Advantage": ["+3.5%", "+0.3%"]
                })
                st.dataframe(week2_data, use_container_width=True, hide_index=True)

            st.info("""
            **Why the difference?**

            **Holidays (Jan 1-3):** Users browse from personal devices with ad-blockers → SST captures +2-5% more sessions

            **Weekdays:** B2B audience (architects, designers) on work devices with default browser settings → near parity or slight Direct advantage

            **Weekend (Jan 10-11):** Personal device usage returns → SST advantage spikes to **+27% on Sunday** (Jan 11)

            **Jan 9 anomaly (-13.3%):** Unusual Direct advantage on Friday - may indicate SST endpoint routing issue or traffic pattern anomaly. Worth investigating if pattern repeats.
            """)

            st.markdown("---")

            # Session-level breakdown
            st.markdown("#### Session Coverage")

            # Data from Jan 10-14 analysis
            total_sessions = 13199
            both_sessions = 9448
            direct_only = 2079
            sst_only = 1672

            col1, col2 = st.columns([1, 2])

            with col1:
                st.metric("Total Unique Sessions", f"{total_sessions:,}")
                st.metric("Captured by Both", f"{both_sessions:,}", help="71.6% overlap")
                st.metric("Direct-Only", f"{direct_only:,}", help="Corporate firewalls blocking SST domain")
                st.metric("SST-Only", f"{sst_only:,}", help="Ad-blocker bypass")

            with col2:
                # Stacked bar visualization
                session_data = pd.DataFrame({
                    "Category": ["Both", "Direct-Only", "SST-Only"],
                    "Sessions": [both_sessions, direct_only, sst_only],
                    "Percentage": ["71.6%", "15.8%", "12.7%"]
                })
                chart = alt.Chart(session_data).mark_bar().encode(
                    x=alt.X("Sessions:Q", title="Sessions"),
                    y=alt.Y("Category:N", sort=["Both", "Direct-Only", "SST-Only"], title=""),
                    color=alt.Color("Category:N",
                        scale=alt.Scale(
                            domain=["Both", "Direct-Only", "SST-Only"],
                            range=["#2ecc71", "#3498db", "#e74c3c"]
                        ),
                        legend=None
                    ),
                    tooltip=["Category", "Sessions", "Percentage"]
                ).properties(height=150).configure_axis(
                    labelFontSize=CHART_LABEL_SIZE,
                    titleFontSize=CHART_TITLE_SIZE
                )
                st.altair_chart(chart, use_container_width=True)

            st.markdown("---")
            st.markdown("#### Key Metrics")

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Session Overlap", "71.6%", help="Sessions captured by BOTH SST and Direct")
            with col2:
                st.metric("Dual-Property Lift", "+14.5%", help="Additional unique sessions captured by running both SST and Direct vs Direct alone")
            with col3:
                st.metric("SST-Only Sessions", "12.7%", help="Sessions captured ONLY by SST (ad-blocker bypass)")
            with col4:
                st.metric("Direct-Only Sessions", "15.8%", help="Sessions captured ONLY by Direct (SST domain blocked by corporate firewalls/proxies)")

            st.markdown("---")
            st.markdown("#### Why Run Both?")

            st.markdown("""
            | Scenario | SST Captures | Direct Captures |
            |----------|--------------|-----------------|
            | Ad-blocker blocks `google-analytics.com` | ✅ Yes | ❌ No |
            | Corporate firewall blocks `sst.warwick.com.au` | ❌ No | ✅ Yes |
            | Safari ITP (7-day cookie limit) | ✅ First-party cookies (longer) | ⚠️ Limited |
            | Normal browsing | ✅ Yes | ✅ Yes |

            **Key Insight:** SST and Direct have nearly equal blind spots (~13% each). SST captures +2.2% more events overall. Running both systems captures +14.5% more unique sessions than Direct alone.
            """)

            # Browser & Device Analysis
            st.markdown("---")
            st.markdown("#### 📊 Browser & Device Analysis")
            st.markdown("*Where does SST add the most value?*")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**By Device Category**")
                device_data = pd.DataFrame({
                    "Device": ["Desktop", "Mobile", "Tablet"],
                    "Direct": [8932, 3582, 166],
                    "SST": [9101, 3508, 163],
                    "Diff": ["+1.9%", "-2.1%", "-1.8%"]
                })
                st.dataframe(device_data, use_container_width=True, hide_index=True)
                st.caption("Desktop shows SST advantage because desktop browsers support ad-blocking extensions; mobile browsers generally don't.")

            with col2:
                st.markdown("**By Operating System**")
                os_data = pd.DataFrame({
                    "OS": ["Windows", "Macintosh", "iOS", "Android"],
                    "Direct": [6891, 1905, 2757, 937],
                    "SST": [6991, 1959, 2741, 948],
                    "Diff": ["+1.5%", "+2.8%", "-0.6%", "+1.2%"]
                })
                st.dataframe(os_data, use_container_width=True, hide_index=True)
                st.caption("Mac users show highest SST advantage (+2.8%) - tech-savvy demographic with higher ad-blocker adoption.")

            # Browser comparison
            st.markdown("**By Browser**")
            browser_data = pd.DataFrame({
                "Browser": ["Chrome", "Safari", "Edge", "Firefox", "Samsung Internet"],
                "Direct": [8382, 3178, 990, 104, 159],
                "SST": [8216, 3151, 971, 104, 157],
                "Diff": ["-2.0%", "-0.8%", "-1.9%", "0.0%", "-1.3%"],
                "Notes": [
                    "Slight Direct advantage",
                    "Near parity (ITP affects cookies, not sessions)",
                    "Slight Direct advantage",
                    "Perfect parity",
                    "Near parity"
                ]
            })
            st.dataframe(browser_data, use_container_width=True, hide_index=True)
            st.caption("Browser-level differences are minimal. The real SST advantage is in ad-blocker bypass (not visible in aggregate browser stats since blocked sessions don't appear in Direct at all).")

            with st.expander("⚠️ Technical Note: Device Classification & Transformation Layer"):
                st.markdown("""
                **SST Transformation Layer v3.4** - Verified 98%+ dimension match rate with BigQuery

                The transformation layer (`athena_transformation_layer.sql`) parses SST raw data to produce
                dimension values that match BigQuery exactly. This enables accurate session-level reconciliation.

                **Verified Match Rates (Jan 15-21, 2026):**
                | Dimension | Match Rate | Notes |
                |-----------|------------|-------|
                | device_category | 98.9% | desktop/mobile/tablet |
                | device_browser | 98.3% | Chrome/Safari/Edge/Firefox/Samsung Internet/Safari (in-app) |
                | device_operating_system | 97.4% | Windows/iOS/Macintosh/Android/Linux |
                | geo_country | 98.9% | ISO code → full name (AU → Australia) |

                *Remaining ~2% mismatches are due to session ID collisions (timestamp-based) and geo lookup differences between CloudFront and GA4.*

                ---

                **Why User-Agent Parsing (Not Client Hints)?**

                SST raw payloads include a `client_hints.mobile` field, but **Safari and Firefox do not support User-Agent Client Hints**.
                Only Chromium-based browsers (Chrome, Edge, Samsung Internet) send these headers.

                | Browser | Sends Client Hints? | `client_hints.mobile` value |
                |---------|---------------------|----------------------------|
                | Chrome/Edge | ✅ Yes | `true` or `false` |
                | Safari (iOS & Mac) | ❌ No | `NULL` |
                | Firefox | ❌ No | `NULL` |

                **Impact:** If you use `client_hints.mobile = 'true'` to filter mobile sessions,
                you'll miss all Safari mobile traffic (~40% of mobile sessions).

                **Correct approach:** Use the transformation layer views or parse `user_agent`:
                ```sql
                -- Use the transformation layer (recommended)
                SELECT device_category, device_browser, geo_country
                FROM warwick_weave_sst_events.sst_sessions
                WHERE site = 'AU';

                -- Or parse user_agent directly
                CASE
                    WHEN user_agent LIKE '%iPad%' THEN 'tablet'
                    WHEN user_agent LIKE '%iPhone%' THEN 'mobile'
                    WHEN user_agent LIKE '%Android%' AND user_agent LIKE '%Mobile%' THEN 'mobile'
                    ELSE 'desktop'
                END
                ```

                **Browser detection note:** The transformation layer detects "Safari (in-app)" for iOS in-app browsers
                (Facebook, Instagram apps) where there's no `Safari/` in the User-Agent but the device is iOS with `Mobile/`.

                **Sources:**
                - [MDN: Sec-CH-UA header](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Sec-CH-UA) - "not Baseline because it does not work in some of the most widely-used browsers"
                - [Corbado: Client Hints in Chrome, Safari & Firefox](https://www.corbado.com/blog/client-hints-user-agent-chrome-safari-firefox) - "Firefox and Safari do not support this API"
                """)

            # Visual: Device breakdown
            device_chart_data = pd.DataFrame({
                "Device": ["Desktop", "Desktop", "Mobile", "Mobile"],
                "Property": ["Direct", "SST", "Direct", "SST"],
                "Sessions": [8932, 9101, 3582, 3508]
            })
            device_chart = alt.Chart(device_chart_data).mark_bar().encode(
                x=alt.X("Device:N", title="Device"),
                y=alt.Y("Sessions:Q", title="Sessions"),
                color=alt.Color("Property:N",
                    scale=alt.Scale(domain=["Direct", "SST"], range=["#3498db", "#2ecc71"]),
                    legend=alt.Legend(title="Property")
                ),
                xOffset="Property:N",
                tooltip=["Device", "Property", "Sessions"]
            ).properties(height=200).configure_axis(
                labelFontSize=CHART_LABEL_SIZE,
                titleFontSize=CHART_TITLE_SIZE
            ).configure_legend(
                labelFontSize=CHART_LABEL_SIZE,
                titleFontSize=CHART_TITLE_SIZE
            )
            st.altair_chart(device_chart, use_container_width=True)

            # Geographic Analysis
            st.markdown("---")
            st.markdown("#### 🌏 Geographic Analysis")
            st.markdown("*Session capture comparison by country*")

            # Country comparison table
            geo_comparison = pd.DataFrame({
                "Country": ["Australia", "China", "United States", "New Zealand", "India", "Vietnam", "United Kingdom", "Other"],
                "Direct": [9245, 1102, 312, 287, 198, 142, 89, 152],
                "SST": [8962, 1269, 308, 279, 169, 100, 83, 149],
                "Diff": ["-3.1%", "+15.2%", "-1.3%", "-2.8%", "-14.6%", "-29.6%", "-6.7%", "-2.0%"],
                "Winner": ["Direct", "SST", "≈ Parity", "Direct", "Direct", "Direct", "Direct", "≈ Parity"]
            })
            st.dataframe(geo_comparison, use_container_width=True, hide_index=True)

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                **Where Direct wins:**
                - 🇦🇺 Australia (-3.1%)
                - 🇻🇳 Vietnam (-29.6%)
                - 🇮🇳 India (-14.6%)
                - 🇳🇿 New Zealand (-2.8%)
                """)
            with col2:
                st.markdown("""
                **Where SST wins:**
                - 🇨🇳 China (+15.2%)
                """)

            # Visualize the discrepancy
            geo_chart_data = pd.DataFrame({
                "Country": ["Australia", "Australia", "China", "China", "Vietnam", "Vietnam", "India", "India"],
                "Property": ["Direct", "SST", "Direct", "SST", "Direct", "SST", "Direct", "SST"],
                "Sessions": [9245, 8962, 1102, 1269, 142, 100, 198, 169]
            })
            geo_chart = alt.Chart(geo_chart_data).mark_bar().encode(
                x=alt.X("Country:N", title="Country", sort=["Australia", "China", "Vietnam", "India"]),
                y=alt.Y("Sessions:Q", title="Sessions"),
                color=alt.Color("Property:N",
                    scale=alt.Scale(domain=["Direct", "SST"], range=["#3498db", "#2ecc71"]),
                    legend=alt.Legend(title="Property")
                ),
                xOffset="Property:N",
                tooltip=["Country", "Property", "Sessions"]
            ).properties(height=250).configure_axis(
                labelFontSize=CHART_LABEL_SIZE,
                titleFontSize=CHART_TITLE_SIZE
            ).configure_legend(
                labelFontSize=CHART_LABEL_SIZE,
                titleFontSize=CHART_TITLE_SIZE
            )
            st.altair_chart(geo_chart, use_container_width=True)

            # Why Different Sessions Section
            st.markdown("---")
            st.markdown("#### 🔬 Why Do SST and Direct Capture Different Sessions?")

            st.info("""
            **Understanding the comparison:** We categorize every session into three groups:
            - **"Both"** = Sessions captured by both SST and Direct (the overlap)
            - **"SST-only"** = Sessions captured only by SST (Direct missed these)
            - **"Direct-only"** = Sessions captured only by Direct (SST missed these)

            The **"Both" group is our baseline** - these are normal sessions where neither ad-blockers nor firewalls interfered.
            By comparing SST-only and Direct-only against this baseline, we can identify what makes each group different.
            """)

            st.markdown("##### ✅ Finding 1: SST-only Sessions are Ad-blocker Users")
            st.markdown("**Confidence: HIGH** — All three evidence criteria met")

            col1, col2 = st.columns(2)
            with col1:
                sst_profile = pd.DataFrame({
                    "Metric": ["Desktop", "Chrome", "Safari", "Extension-capable*"],
                    "SST-Only": ["81.7%", "73.3%", "15.7%", "83.3%"],
                    "Both (Baseline)": ["72.6%", "58.3%", "25.4%", "72.7%"],
                    "Difference": ["+9.0pp", "+14.9pp", "-9.7pp", "+10.6pp"]
                })
                st.dataframe(sst_profile, use_container_width=True, hide_index=True)
                st.caption("*Extension-capable = Chrome + Firefox + Edge (browsers that support ad-blocker extensions)")

            with col2:
                st.markdown("""
                **Why this profile confirms ad-blockers:**

                Ad-blocker extensions (uBlock Origin, Adblock Plus, Privacy Badger) run in desktop browsers.
                They block requests to `google-analytics.com` but allow first-party domains like `sst.warwick.com.au`.

                The SST-only profile matches this exactly:
                - **+9pp more desktop** — Ad-blockers are browser extensions, not mobile apps
                - **+15pp more Chrome** — Chrome Web Store has the richest ad-blocker ecosystem
                - **-10pp less Safari** — Safari has limited extension support; most Safari users don't have ad-blockers
                - **83% extension-capable** — These are the browsers where ad-blockers actually work
                """)

            st.markdown("---")
            st.markdown("##### ✅ Finding 2: Direct-only Sessions Concentrate in Business Hours")
            st.markdown("**Confidence: HIGH** — Strong time-of-day signal")

            col1, col2 = st.columns(2)
            with col1:
                time_data = pd.DataFrame({
                    "Time Period (Melbourne)": ["Business hours (9am-5pm)", "After hours (6pm-8am)"],
                    "Direct-Only": ["62.5%", "31.2%"],
                    "Both (Baseline)": ["54.2%", "39.2%"],
                    "Difference": ["+8.3pp", "-8.1pp"]
                })
                st.dataframe(time_data, use_container_width=True, hide_index=True)

            with col2:
                st.markdown("""
                **Why this points to corporate networks:**

                Corporate firewalls typically:
                - **Whitelist** well-known domains like `google-analytics.com`
                - **Block** unfamiliar domains like `sst.warwick.com.au`

                The +8.3pp business hours concentration tells us Direct-only sessions are
                disproportionately from users browsing during work hours — consistent with
                Warwick's B2B audience (architects, interior designers) on corporate networks.

                **Note:** Desktop/Windows shares are only slightly elevated (+0.8pp / +2.5pp),
                so corporate attribution is inferred primarily from the time pattern.
                """)

            st.markdown("---")
            st.markdown("##### ✅ Finding 3: SST Captures China Traffic Blocked by the Great Firewall")
            st.markdown("**Confidence: HIGH** — Dramatic over-representation in SST-only")

            col1, col2 = st.columns(2)
            with col1:
                china_data = pd.DataFrame({
                    "Session Category": ["SST-only", "Both (Baseline)", "Direct-only"],
                    "% from China": ["34.6%", "4.3%", "11.2%"],
                    "vs Baseline": ["+30.3pp ⬆️", "—", "+6.9pp"]
                })
                st.dataframe(china_data, use_container_width=True, hide_index=True)

            with col2:
                st.markdown("""
                **How the Great Firewall creates this pattern:**

                The GFW doesn't block 100% of the time — it blocks **intermittently** based on
                network conditions, ISP, and time of day.

                | GFW Status | What happens | Result |
                |------------|--------------|--------|
                | **Blocking** `google-analytics.com` | Only SST succeeds | SST-only session |
                | **Not blocking** | Both succeed | Both session |
                | Corporate VPN blocking `sst.warwick.com.au` | Only Direct succeeds | Direct-only session |

                The fact that **34.6% of SST-only sessions are from China** (vs 4.3% baseline)
                is strong evidence that SST captures traffic when the GFW blocks Direct.
                """)

            st.markdown("""
            **China traffic profile (applies to both SST-only and Direct-only China sessions):**
            - **99%+ Desktop Chrome** — Not casual mobile browsing; this is professional/B2B usage
            - **Peak hours: 3am, 11pm, 1-2am Melbourne time** — This equals 4-5pm Beijing time (China business hours)
            - **Interpretation:** Chinese architects, designers, or suppliers researching Australian fabrics during their workday
            """)

            st.markdown("---")
            st.markdown("##### 📊 Summary: Validated Causes of SST vs Direct Discrepancies")

            summary_data = pd.DataFrame({
                "Finding": [
                    "SST-only = Ad-blocker users",
                    "Direct-only = Business hours / corporate",
                    "SST captures GFW-blocked China traffic"
                ],
                "Confidence": ["HIGH", "HIGH", "HIGH"],
                "Key Evidence": [
                    "81.7% desktop, Chrome +15pp, Safari -10pp, 83% extension-capable browsers",
                    "+8.3pp during 9am-5pm Melbourne; corporate firewalls block unknown domains",
                    "34.6% of SST-only from China vs 4.3% baseline; 99% desktop Chrome at China business hours"
                ]
            })
            st.dataframe(summary_data, use_container_width=True, hide_index=True)

            with st.expander("🔍 Methodology & Alternative Hypotheses"):
                st.markdown("""
                **How we validated these findings:**

                1. **Categorized sessions** by matching `ga_session_id` between BigQuery (Direct) and Athena (SST)
                2. **Compared profiles** of SST-only, Direct-only, and Both groups across device, browser, OS, country, and time
                3. **Tested hypotheses** by looking for statistically significant deviations from the "Both" baseline

                **Alternative hypotheses ruled out:**

                | Alternative | Why Ruled Out |
                |-------------|---------------|
                | Random sampling noise | Patterns are consistent and coherent across multiple dimensions (device + browser + time) |
                | SST endpoint reliability issues | SST-only sessions have a coherent profile (ad-blocker users); random failures would produce random profiles |
                | Timezone artifacts | Both systems aligned to same UTC window; the differences are in session characteristics, not just counts |

                **Data sources:**
                - Analysis period: Jan 15-21, 2026
                - Direct: BigQuery `analytics_375839889.events_*`
                - SST: Athena `warwick_weave_sst_events.events` with User-Agent transformation (98%+ dimension match rate)
                - Script: `hypothesis_validation.py`
                """)

            # Conversion Events Parity
            st.markdown("---")
            st.markdown("#### 💰 Conversion Events")
            st.markdown("*Business-critical events are at parity - both systems track conversions reliably*")

            conversion_data = pd.DataFrame({
                "Event": ["purchase", "add_payment_info", "begin_checkout", "add_to_cart"],
                "Direct": [301, 301, 347, 1238],
                "SST": [300, 300, 340, 1231],
                "Match Rate": ["99.7%", "99.7%", "98.0%", "99.4%"],
                "Status": ["✅ Parity", "✅ Parity", "✅ Parity", "✅ Parity"]
            })
            st.dataframe(conversion_data, use_container_width=True, hide_index=True)
            st.success("**All conversion events at near-perfect parity.** Revenue and funnel data is accurate in both properties.")

            # New vs Returning Users
            st.markdown("---")
            st.markdown("#### 👤 New vs Returning User Paradox")
            st.markdown("*Why Direct shows more returning users than SST*")

            col1, col2 = st.columns(2)
            with col1:
                user_type_data = pd.DataFrame({
                    "User Type": ["New Users", "Returning Users"],
                    "Direct": [7409, 4661],
                    "SST": [7924, 4114],
                    "Difference": ["+7.0%", "-11.7%"]
                })
                st.dataframe(user_type_data, use_container_width=True, hide_index=True)

            with col2:
                st.markdown("""
                **This is NOT a bug.** It's a "cookie identity paradox" caused by different user pools:

                1. **Different cookies:** Direct uses `_ga` (JS-set), SST uses `FPID` (server-set)
                2. **Different user pools:** Ad-blocked users only appear in SST → skews "new" higher
                3. **Different identity scopes:** Each property has its own user definition
                """)

            with st.expander("📋 Long-term expectation"):
                st.markdown("""
                **Over 30+ days**, SST should show *improved* returning user identification for Safari users.

                | Cookie | Set By | Safari Lifetime | Expected Behavior |
                |--------|--------|-----------------|-------------------|
                | `_ga` | JavaScript | 7 days (ITP capped) | Safari users appear "new" after 1 week |
                | `FPID` | Server | 1 year (full) | Safari users recognized for full year |

                **To validate:** Compare returning user % for Safari-only traffic between properties after 30 days of data collection.
                """)

            # Safari ITP Benefits
            st.markdown("---")
            st.markdown("#### 🍎 Safari Cookie Benefits")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                **The Problem:**
                Safari's Intelligent Tracking Prevention (ITP) limits JavaScript-set cookies to **7 days**.
                This means returning Safari users are often counted as "new" after a week.

                **The SST Solution:**
                SST uses server-set `FPID` cookies with **1-year expiry** that bypass ITP restrictions.
                """)
            with col2:
                st.markdown("""
                **Cookie Comparison:**

                | Cookie | Set By | Safari Lifetime |
                |--------|--------|-----------------|
                | `_ga` (Direct) | JavaScript | 7 days (ITP capped) |
                | `FPID` (SST) | Server | 1 year (full) |

                *Long-term benefit: Better returning user identification for Safari traffic.*
                """)

            # Two-Layer Blocking Model
            st.markdown("---")
            st.markdown("#### 🔒 Two-Layer Blocking Model")
            st.markdown("*Why neither system captures 100% of traffic*")

            st.code("""
┌─────────────────────────────────────────────────────────────────────┐
│                           USER BROWSER                               │
├─────────────────────────────────────────────────────────────────────┤
│  LAYER 1: GTM Script Loading                                        │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  Source: googletagmanager.com/gtm.js                          │  │
│  │  ❌ Blocked by: Safari Private, Brave, Firefox strict         │  │
│  │  → If blocked: NO tracking (neither Direct nor SST)           │  │
│  │  → Impact: ~5% of traffic completely invisible                │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                              ↓ (if GTM loads)                        │
│  LAYER 2: Analytics Requests  ← SST BYPASSES THIS LAYER             │
│  ┌─────────────────────────────┐  ┌─────────────────────────────┐  │
│  │  Direct                     │  │  SST                        │  │
│  │  analytics.google.com       │  │  sst.warwick.com.au         │  │
│  │  ❌ Blocked by ad-blockers  │  │  ✅ First-party (allowed)   │  │
│  └─────────────────────────────┘  └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
            """, language=None)

            st.info("""
            **What SST captures that Direct doesn't:** Users with ad-blockers (uBlock Origin, Adblock Plus, Privacy Badger) that block `google-analytics.com` but allow first-party domains.

            **What neither captures:** Users with browsers that block the GTM script itself (Safari Private Browsing, Brave shields). This affects ~5% of traffic and would require a GTM script proxy to fix.
            """)

            # The Invisible ~5%
            st.markdown("---")
            st.markdown("#### 👻 The Invisible ~5%")
            st.markdown("*Traffic that neither SST nor Direct can see*")

            invisible_data = pd.DataFrame({
                "Browser/Mode": ["Safari Private Browsing", "Safari + Advanced Tracking Protection", "Brave Browser (default)", "Firefox strict ETP"],
                "Est. Traffic": ["3-4%", "1-2%", "<1%", "<0.5%"],
                "Blocks": ["GTM script", "GTM script", "GTM script", "Sometimes GTM"],
                "Can Fix?": ["GTM proxy", "GTM proxy", "GTM proxy", "GTM proxy"]
            })
            st.dataframe(invisible_data, use_container_width=True, hide_index=True)

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                **Why it's unfixable (without proxy):**

                These browsers block `googletagmanager.com/gtm.js` at the network level.
                If GTM never loads, there's nothing to send data anywhere.
                """)
            with col2:
                st.markdown("""
                **Should Warwick fix this?**

                **Probably not.** At ~$99/month for a GTM proxy service (Stape), the ROI for ~375 extra sessions/week is marginal for B2B traffic. Revisit if consumer traffic grows.
                """)

            st.markdown("---")
            st.markdown("#### 📈 Event Comparison (SST vs Direct)")
            st.markdown("*Detailed breakdown of event capture by type*")

            # Event comparison with aligned time ranges
            event_comparison = pd.DataFrame({
                "Event Type": [
                    "page_view", "view_item_list", "view_item", "scroll",
                    "user_engagement", "select_item", "form_start", "form_submit",
                    "add_to_cart", "begin_checkout", "add_payment_info", "purchase"
                ],
                "Direct": [
                    "76,758", "46,268", "27,688", "9,449",
                    "11,075", "13,038", "4,671", "2,085",
                    "1,238", "347", "301", "301"
                ],
                "SST": [
                    "76,476", "46,161", "28,122", "10,373",
                    "10,882", "12,989", "4,627", "2,040",
                    "1,231", "340", "300", "300"
                ],
                "Diff": [
                    "-0.4%", "-0.2%", "+1.6%", "+9.8%",
                    "-1.7%", "-0.4%", "-0.9%", "-2.2%",
                    "-0.6%", "-2.0%", "-0.3%", "-0.3%"
                ],
                "Interpretation": [
                    "✅ Parity", "✅ Parity", "✅ SST captures more (+434 views)", "🎯 SST +9.8% (server handles edge cases)",
                    "✅ Parity", "✅ Parity", "✅ Parity", "✅ Parity",
                    "✅ Parity", "✅ Parity", "✅ Parity", "✅ Parity"
                ]
            })
            st.dataframe(event_comparison, use_container_width=True, hide_index=True)

            st.markdown("""
            **Notable findings:**
            - **scroll events (+9.8%):** SST's server-side processing handles edge cases where the browser tab closes before Direct can fire
            - **view_item (+1.6%):** SST captures 434 extra product detail page views over 7 days
            - **Conversion events:** All at near-perfect parity (99%+), validating both systems track business outcomes accurately
            """)

            with st.expander("🔬 Deep Dive: Why SST Captures More Scroll Events"):
                st.markdown("""
                **The +9.8% scroll event difference is not random.** It reveals how SST handles browser edge cases better than Direct.

                **How scroll tracking works:**
                1. User scrolls down the page
                2. JavaScript detects scroll depth threshold (typically 90%)
                3. GTM fires the `scroll` event
                4. Browser sends HTTP request to analytics endpoint

                **Where Direct fails:**

                | Scenario | Direct | SST |
                |----------|--------|-----|
                | User scrolls, then immediately closes tab | ❌ Request aborted | ✅ Server receives partial request |
                | Slow network during page unload | ❌ Timeout | ✅ Server-side retry logic |
                | Browser aggressively kills background tabs | ❌ Lost | ✅ First-party domain gets priority |

                **Why this matters:**
                The +924 extra scroll events (over 7 days) represent real user engagement that would otherwise be invisible. These are users who:
                - Read to the bottom of product pages
                - Engaged deeply with content
                - May have converted later

                **Technical detail:** SST uses `navigator.sendBeacon()` with first-party cookies, which browsers prioritize during page unload. Direct requests to `google-analytics.com` are more likely to be deprioritized or blocked during the critical unload window.
                """)

            # Event totals
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Direct Total", "265,608")
            with col2:
                st.metric("SST Total", "266,447")
            with col3:
                st.metric("SST vs Direct", "+0.3%", help="SST captures slightly more events overall")

            st.markdown("---")
            st.markdown("#### 📋 Analysis Details")
            st.markdown("""
            **Methodology:**
            - Analysis period: `2026-01-10 00:00:00 UTC` to `2026-01-14 10:54:28 UTC`
            - Cutoff aligned to Athena's max timestamp for fair comparison
            - Excluded: `session_start`, `first_visit` (GA4 synthetic events, Direct-only by design)
            - Excluded: `add_to_cart_click_fallback` (Safari fallback tag)
            - Session matching: by `ga_session_id` (timestamp-based, ~2% collision rate)

            **Transformation Layer (v3.4):**
            - SST dimensions (device, browser, OS, country) transformed to match BigQuery schema
            - Verified 98%+ match rate across all dimensions via session-level comparison
            - User-Agent parsing (not client_hints) for device/browser/OS detection
            - ISO country codes mapped to full names (AU → Australia)
            - Bot detection matching BigQuery's IAB/ABC filtering behavior
            """)

            # Long-term monitoring
            st.markdown("---")
            st.markdown("#### 📆 Long-term Monitoring: What to Watch")

            col1, col2 = st.columns(2)
            with col1:
                st.markdown("""
                **FPID Cookie Benefits (30+ days)**

                The server-set FPID cookie should improve Safari user identification over time:

                | Metric | When to Check | Expected Result |
                |--------|---------------|-----------------|
                | Returning user % (Safari) | After 30 days | SST > Direct |
                | 30-day attribution | Ongoing | SST more accurate |
                | Multi-touch journeys | After 60 days | Longer paths in SST |
                """)
            with col2:
                st.markdown("""
                **Traffic Pattern Shifts**

                Monitor for changes that would increase SST value:

                | Signal | Implication |
                |--------|-------------|
                | Consumer traffic increase | Higher ad-blocker rates → more SST value |
                | Safari market share growth | More ITP bypass benefit |
                | Privacy regulation changes | First-party tracking more valuable |
                | Holiday traffic spikes | Personal devices = more ad-blockers |
                """)

            st.success("""
            **Current Status:** SST is working correctly. The +14.5% dual-property lift justifies running both systems.
            Continue monitoring quarterly to validate long-term benefits.
            """)

            # Data sources reference (collapsible)
            with st.expander("📚 Data Sources Reference"):
                st.markdown("""
                | Source | Location | Measurement ID |
                |--------|----------|----------------|
                | **Direct** | BigQuery `analytics_375839889` | `G-EP4KTC47K3` |
                | **SST** | Athena `warwick_weave_sst_events.events` | `G-Y0RSKRWP87` |

                **BigQuery (Direct):**
                - GCP Project: `376132452327`
                - Tables: `events_YYYYMMDD` (bucketed by Melbourne timezone)
                - Timestamp: `event_timestamp` (microseconds since epoch)

                **Athena (SST):**
                - S3 Bucket: `warwick-com-au-events`
                - Partitions: `year/month/day` (bucketed by UTC)
                - Timestamp: ISO 8601 format with `T` separator (e.g., `2026-01-10T00:00:00Z`)

                **Transformation Layer v3.4:**
                Use these views for reconciliation (match BigQuery dimensions):
                - `sst_events_transformed` - Event-level with parsed dimensions
                - `sst_sessions` - Session-level rollup for JOIN matching
                - `sst_sessions_daily` - Daily aggregates by dimension
                - `sst_comparison_ready` - Filtered for AU comparison

                **Critical Gotchas:**
                - Athena timestamp format must use `T` separator, not space
                - BigQuery tables use Melbourne timezone, Athena uses UTC
                - SST hashes `client_id` differently - use `ga_session_id` for matching
                - ~2% session ID collision rate (acceptable for aggregate analysis)
                - Always use transformation views for device/browser/OS/country - raw client_hints are incomplete
                """)

        with tab3:
            st.subheader("Event Details")

            # Event type filter
            query = f"""
            SELECT DISTINCT event_name
            FROM {ATHENA_TABLE}
            WHERE timestamp >= '{start_ts}'
              AND timestamp <= '{end_ts}'
            ORDER BY event_name
            """
            with st.spinner("Loading event types..."):
                event_types_df = run_athena_query(query)
                event_types = ["All"] + event_types_df["event_name"].tolist()

            selected_event = st.selectbox("Select Event Type", event_types)

            # Get events with optional filter
            event_filter = "" if selected_event == "All" else f"AND event_name = '{selected_event}'"
            query = f"""
            SELECT
                timestamp,
                event_name,
                {json_field('page_location')} as page_location,
                {json_field('client_id')} as client_id,
                {json_field('ga_session_id')} as ga_session_id
            FROM {ATHENA_TABLE}
            WHERE timestamp >= '{start_ts}'
              AND timestamp <= '{end_ts}'
              {event_filter}
            ORDER BY timestamp DESC
            LIMIT 100
            """
            with st.spinner("Loading events..."):
                df = run_athena_query(query)
                st.dataframe(df, use_container_width=True)

        with tab4:
            st.subheader("Raw Data Explorer")
            st.markdown("Run custom Athena queries against the SST events table.")

            # Default query - show how to extract fields from base64-encoded raw_payload
            default_query = f"""SELECT
    timestamp,
    event_name,
    ip_address,
    json_extract_scalar(json_parse(from_utf8(from_base64(raw_payload))), '$.client_id') as client_id,
    json_extract_scalar(json_parse(from_utf8(from_base64(raw_payload))), '$.ga_session_id') as ga_session_id,
    json_extract_scalar(json_parse(from_utf8(from_base64(raw_payload))), '$.page_location') as page_location
FROM {ATHENA_TABLE}
WHERE timestamp >= '{start_ts}'
  AND timestamp <= '{end_ts}'
LIMIT 10"""

            user_query = st.text_area(
                "SQL Query",
                value=default_query,
                height=150,
                help="Enter a valid Athena SQL query"
            )

            if st.button("Run Query", type="primary"):
                with st.spinner("Executing query..."):
                    try:
                        df = run_athena_query(user_query, timeout_seconds=120)
                        st.success(f"Query returned {len(df)} rows")
                        st.dataframe(df, use_container_width=True)

                        # Download button
                        csv = df.to_csv(index=False)
                        st.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name=f"athena_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    except Exception as e:
                        st.error(f"Query failed: {str(e)}")

    except Exception as e:
        st.error(f"Failed to connect to Athena: {str(e)}")
        st.markdown("""
        ### Troubleshooting

        **For local development:**
        1. Ensure AWS credentials are configured (`aws configure` or AWS_PROFILE)
        2. Use the Warwick AWS profile: `export AWS_PROFILE=warwick`
        3. Verify SSO login: `aws sso login --profile warwick`

        **For Streamlit Cloud:**
        1. Add AWS credentials to Streamlit secrets
        2. See `.streamlit/secrets.toml.example` for format
        """)

    # Footer
    st.markdown("---")
    brisbane_now = datetime.now(ZoneInfo("Australia/Brisbane"))
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        f"Warwick SST Dashboard | v{VERSION} | "
        f"Refreshed: {brisbane_now.strftime('%Y-%m-%d %H:%M:%S')} AEST"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
