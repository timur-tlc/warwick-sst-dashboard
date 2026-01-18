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
        return boto3.client(
            "athena",
            region_name=AWS_REGION,
            aws_access_key_id=st.secrets["aws"]["access_key_id"],
            aws_secret_access_key=st.secrets["aws"]["secret_access_key"],
        )
    except (KeyError, FileNotFoundError):
        # Fall back to environment/profile credentials (for local development)
        return boto3.client("athena", region_name=AWS_REGION)


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
            st.markdown("""
            This tab compares Server-Side Tracking (SST) data with Direct GA4 tracking.
            SST sends data through `sst.warwick.com.au` (first-party), while Direct sends to `google-analytics.com` (third-party).

            *Analysis: Jan 10-14, 2026 (UTC-aligned comparison, corrected 2026-01-18)*
            """)

            # Key findings from analysis
            st.markdown("#### Key Findings")

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Session Overlap", "68.6%", help="Sessions captured by BOTH SST and Direct")
            with col2:
                st.metric("Dual-Property Lift", "+13.8%", help="Additional unique sessions captured by running both SST and Direct vs Direct alone")
            with col3:
                st.metric("SST-Only Sessions", "12.1%", help="Sessions captured ONLY by SST (ad-blocker bypass)")
            with col4:
                st.metric("Direct-Only Sessions", "19.3%", help="Sessions captured ONLY by Direct (SST domain blocked by corporate firewalls/proxies)")

            st.markdown("---")
            st.markdown("#### Why Run Both?")

            st.markdown("""
            | Scenario | SST Captures | Direct Captures |
            |----------|--------------|-----------------|
            | Ad-blocker blocks `google-analytics.com` | ✅ Yes | ❌ No |
            | Corporate firewall blocks `sst.warwick.com.au` | ❌ No | ✅ Yes |
            | Safari ITP (7-day cookie limit) | ✅ First-party cookies (longer) | ⚠️ Limited |
            | Normal browsing | ✅ Yes | ✅ Yes |

            **Key Insight:** For Warwick's B2B audience, corporate firewalls blocking `sst.warwick.com.au` (19.3%) slightly outweigh ad-blockers blocking `google-analytics.com` (12.1%). Running both captures +13.8% more sessions than Direct alone.
            """)

            st.markdown("---")
            st.markdown("#### Event Comparison (SST vs Direct)")

            # Updated event comparison from corrected analysis (2026-01-18)
            event_comparison = pd.DataFrame({
                "Event Type": ["page_view", "view_item_list", "view_item", "scroll", "user_engagement", "add_to_cart"],
                "Direct Events": ["79,548", "47,424", "29,111", "8,231", "10,264", "1,362"],
                "SST Events": ["74,249", "49,685", "27,539", "7,930", "9,592", "1,316"],
                "Difference": ["-6.7%", "+4.8%", "-5.4%", "-3.7%", "-6.5%", "-3.4%"],
                "Note": [
                    "Near parity",
                    "SST captures more (ad-blocker bypass)",
                    "Near parity",
                    "Near parity",
                    "Near parity",
                    "Near parity"
                ]
            })
            st.dataframe(event_comparison, use_container_width=True, hide_index=True)

            st.markdown("---")
            st.markdown("#### Current SST Data (from Athena)")

            # Show SST-specific stats
            query = f"""
            SELECT
                COUNT(*) as total_events,
                COUNT(DISTINCT {json_field('ga_session_id')}) as unique_sessions,
                COUNT(DISTINCT {json_field('client_id')}) as unique_clients,
                COUNT(DISTINCT DATE(from_iso8601_timestamp(timestamp))) as days_of_data
            FROM {ATHENA_TABLE}
            WHERE timestamp >= '{start_ts}'
              AND timestamp <= '{end_ts}'
            """
            with st.spinner("Loading SST stats..."):
                df = run_athena_query(query)
                if not df.empty:
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("SST Events", f"{int(df['total_events'].iloc[0]):,}")
                    c2.metric("SST Sessions", f"{int(df['unique_sessions'].iloc[0]):,}")
                    c3.metric("SST Clients", f"{int(df['unique_clients'].iloc[0]):,}")
                    c4.metric("Days of Data", df['days_of_data'].iloc[0])

            st.info("💡 **Note:** This dashboard shows SST data only. For full Direct comparison, see the GA4 properties directly or the validation reports.")

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
