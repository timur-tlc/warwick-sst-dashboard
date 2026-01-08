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
from datetime import datetime, timedelta
import time

# Page configuration
st.set_page_config(
    page_title="Warwick SST Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
        tab1, tab2, tab3 = st.tabs(["📈 Overview", "🎯 Events", "🔍 Raw Data"])

        with tab1:
            st.subheader("Event Overview")

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
                        df = df.sort_values("count", ascending=False)
                        st.bar_chart(df.set_index("event_name")["count"])
                    else:
                        st.info("No events found for selected date range")

            with col2:
                # Events over time
                st.markdown("#### Events Over Time")
                query = f"""
                SELECT
                    DATE(from_iso8601_timestamp(timestamp)) as date,
                    COUNT(*) as count
                FROM {ATHENA_TABLE}
                WHERE timestamp >= '{start_ts}'
                  AND timestamp <= '{end_ts}'
                GROUP BY DATE(from_iso8601_timestamp(timestamp))
                ORDER BY date
                """
                with st.spinner("Loading daily counts..."):
                    df = run_athena_query(query)
                    if not df.empty:
                        df["count"] = pd.to_numeric(df["count"])
                        df["date"] = pd.to_datetime(df["date"])
                        st.line_chart(df.set_index("date")["count"])
                    else:
                        st.info("No events found for selected date range")

            # Key metrics
            st.markdown("---")
            st.markdown("#### Key Metrics")

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

        with tab3:
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
    st.markdown(
        "<div style='text-align: center; color: gray;'>"
        "Warwick SST Dashboard | Data from AWS Athena | "
        f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
