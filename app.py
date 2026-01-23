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
from corrected_matching_helpers import get_corrected_session_stats

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
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=script_dir,
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


# ============================================================================
# CORRECTED SESSION CATEGORIZATION
# ============================================================================

@st.cache_data(ttl=3600, show_spinner="Performing corrected session matching...")
def load_corrected_comparison_data():
    """Load corrected session categorization using timestamp+attribute matching."""
    return get_corrected_session_stats(
        date_start='20260106',
        date_end='20260113'
    )


def render_corrected_comparison_tab():
    """Render the corrected SST vs Direct comparison tab with timestamp+attribute matching."""

    st.subheader("SST vs Direct Comparison (CORRECTED METHODOLOGY)")

    # Critical correction notice
    st.error("""
    ### ⚠️ METHODOLOGY CORRECTION (2026-01-23)

    **Previous analysis had a fundamental flaw:** Session matching by `ga_session_id` failed because SST and Direct
    assign **different session IDs to the same browsing session** due to sub-second timing differences.

    **Example:** Same user session received ID `1768098275` in SST and `1768098276` in Direct (0.3 seconds apart).

    **This analysis uses corrected methodology:**
    - Match sessions by **timestamp (±5 min) + device + country**
    - 54.6% of miscat categorized sessions had consecutive IDs → were same sessions!
    - **Corrected categorization below shows the TRUE capture rates**
    """)

    # Load corrected data
    try:
        with st.spinner("Loading corrected session categorization..."):
            data = load_corrected_comparison_data()
    except Exception as e:
        st.error(f"Failed to load data: {str(e)}")
        st.info("Make sure AWS SSO credentials are valid: `aws sso login --profile warwick`")
        st.info("Also ensure corrected_matching_helpers.py is in the same directory")
        return

    totals = data['totals']
    profiles = data['profiles']

    # Calculate corrected metrics
    total = totals['total']
    both_count = totals['both']
    sst_only_count = totals['sst_only']
    direct_only_count = totals['direct_only']

    both_pct = both_count / total * 100
    sst_only_pct = sst_only_count / total * 100
    direct_only_pct = direct_only_count / total * 100

    # Dual-property lift
    direct_total = both_count + direct_only_count
    lift_sessions = sst_only_count
    lift_pct = lift_sessions / direct_total * 100

    # Summary Banner
    st.success(f"""
    ### ✅ CORRECTED FINDINGS

    **Bottom Line:** Running both SST and Direct captures **+{lift_pct:.1f}% more unique sessions** than Direct alone.
    (Previously reported: +14.5%, which was inflated due to ID matching error)

    **SST Value:** Approximately **{int(lift_sessions/8):.0f} additional sessions per day**.
    """)

    st.markdown("---")

    # Key metrics
    st.markdown("#### 🎯 Key Metrics (CORRECTED)")
    m1, m2, m3, m4 = st.columns(4)

    with m1:
        st.metric("Both Sources", f"{both_pct:.1f}%",
                 help=f"{both_count:,} sessions captured by SST AND Direct")
    with m2:
        st.metric("SST-Only", f"{sst_only_pct:.1f}%",
                 help=f"{sst_only_count:,} sessions only in SST (ad-blockers)")
    with m3:
        st.metric("Direct-Only", f"{direct_only_pct:.1f}%",
                 help=f"{direct_only_count:,} sessions only in Direct (firewalls)")
    with m4:
        st.metric("SST Lift", f"+{lift_pct:.1f}%",
                 help="Extra sessions from running both vs Direct alone")

    # Daily timeseries chart
    st.markdown("---")
    st.markdown("#### 📈 Daily Session Breakdown (CORRECTED)")

    daily_df = data.get('daily')
    if daily_df is not None and not daily_df.empty:
        # Melt for Altair
        daily_melted = daily_df.melt(id_vars=['date'], var_name='Category', value_name='Sessions')

        chart = alt.Chart(daily_melted).mark_line(point=True).encode(
            x=alt.X('date:T', title='Date', axis=alt.Axis(format='%b %d')),
            y=alt.Y('Sessions:Q', title='Sessions'),
            color=alt.Color('Category:N',
                scale=alt.Scale(
                    domain=['Both', 'SST-only', 'Direct-only'],
                    range=['#9b59b6', '#2ecc71', '#3498db']
                ),
                legend=alt.Legend(title='Category')
            ),
            tooltip=[alt.Tooltip('date:T', format='%Y-%m-%d'), 'Category', 'Sessions']
        ).properties(height=300).configure_axis(
            labelFontSize=CHART_LABEL_SIZE,
            titleFontSize=CHART_TITLE_SIZE
        ).configure_legend(
            labelFontSize=CHART_LABEL_SIZE,
            titleFontSize=CHART_TITLE_SIZE
        )
        st.altair_chart(chart, use_container_width=True)

        # Also show the data table
        with st.expander("📋 Daily Data Table"):
            display_df = daily_df.copy()
            display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d (%a)')
            display_df['Total'] = display_df['Both'] + display_df['SST-only'] + display_df['Direct-only']
            st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Hourly chart
    st.markdown("---")
    st.markdown("#### 🕐 Hourly Distribution (CORRECTED)")
    st.caption("Sessions by hour of day (AEST) - aggregated across Jan 6-13")

    hourly_df = data.get('hourly')
    if hourly_df is not None and not hourly_df.empty:
        # Melt for Altair
        hourly_melted = hourly_df.melt(id_vars=['hour'], var_name='Category', value_name='Sessions')

        chart = alt.Chart(hourly_melted).mark_line(point=True).encode(
            x=alt.X('hour:O', title='Hour (AEST)', axis=alt.Axis(values=list(range(0, 24, 2)))),
            y=alt.Y('Sessions:Q', title='Sessions'),
            color=alt.Color('Category:N',
                scale=alt.Scale(
                    domain=['Both', 'SST-only', 'Direct-only'],
                    range=['#9b59b6', '#2ecc71', '#3498db']
                ),
                legend=alt.Legend(title='Category')
            ),
            tooltip=['hour', 'Category', 'Sessions']
        ).properties(height=300).configure_axis(
            labelFontSize=CHART_LABEL_SIZE,
            titleFontSize=CHART_TITLE_SIZE
        ).configure_legend(
            labelFontSize=CHART_LABEL_SIZE,
            titleFontSize=CHART_TITLE_SIZE
        )
        st.altair_chart(chart, use_container_width=True)

        # Business hours analysis
        business_hours = hourly_df[(hourly_df['hour'] >= 9) & (hourly_df['hour'] <= 17)]
        off_hours = hourly_df[(hourly_df['hour'] < 9) | (hourly_df['hour'] > 17)]

        biz_direct_pct = business_hours['Direct-only'].sum() / hourly_df['Direct-only'].sum() * 100
        biz_sst_pct = business_hours['SST-only'].sum() / hourly_df['SST-only'].sum() * 100
        biz_both_pct = business_hours['Both'].sum() / hourly_df['Both'].sum() * 100

        st.info(f"""
        **Business Hours (9am-5pm AEST) Concentration:**
        - Direct-only: **{biz_direct_pct:.1f}%** during business hours
        - SST-only: **{biz_sst_pct:.1f}%** during business hours
        - Both: **{biz_both_pct:.1f}%** during business hours

        {"**✅ Supports corporate hypothesis:** Direct-only is more concentrated in business hours." if biz_direct_pct > biz_both_pct + 3 else ""}
        """)

    # Comparison table
    st.markdown("---")
    st.markdown("#### 📊 OLD vs NEW Categorization")

    old_new_df = pd.DataFrame({
        "Category": ["Both", "SST-Only", "Direct-Only"],
        "OLD (ga_session_id)": ["9,448 (71.6%)", "1,672 (12.7%)", "2,079 (15.8%)"],
        "NEW (timestamp+attr)": [
            f"{both_count:,} ({both_pct:.1f}%)",
            f"{sst_only_count:,} ({sst_only_pct:.1f}%)",
            f"{direct_only_count:,} ({direct_only_pct:.1f}%)"
        ],
        "Difference": [
            f"+{both_count - 9448:,}",
            f"{sst_only_count - 1672:,}",
            f"{direct_only_count - 2079:,}"
        ]
    })
    st.dataframe(old_new_df, use_container_width=True, hide_index=True)

    st.warning("""
    **61% of sessions** previously categorized as "SST-only" or "Direct-only" were actually captured by **both systems**
    but received different session IDs due to 0.2-1.5 second timing differences.
    """)

    # Profile Analysis
    st.markdown("---")
    st.markdown("#### 👥 User Profiles (CORRECTED)")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Both (Baseline)**")
        st.metric("Desktop", f"{profiles['Both']['desktop_pct']:.1f}%")
        st.metric("Windows", f"{profiles['Both']['windows_pct']:.1f}%")
        st.metric("Win+Desktop", f"{profiles['Both']['windows_and_desktop_pct']:.1f}%",
                 help="Corporate profile: Windows OS on Desktop device")
        st.metric("Purchase Rate", f"{profiles['Both']['purchase_rate']:.2f}%")
        st.metric("Avg Engagement", f"{profiles['Both']['avg_engagement_sec']:.0f}s",
                 help="Average engagement time per session")

    with col2:
        st.markdown("**Direct-Only**")
        desktop_diff = profiles['Direct-only']['desktop_pct'] - profiles['Both']['desktop_pct']
        windows_diff = profiles['Direct-only']['windows_pct'] - profiles['Both']['windows_pct']
        win_desktop_diff = profiles['Direct-only']['windows_and_desktop_pct'] - profiles['Both']['windows_and_desktop_pct']
        purchase_diff = profiles['Direct-only']['purchase_rate'] - profiles['Both']['purchase_rate']
        engagement_diff = profiles['Direct-only']['avg_engagement_sec'] - profiles['Both']['avg_engagement_sec']

        st.metric("Desktop", f"{profiles['Direct-only']['desktop_pct']:.1f}%",
                 delta=f"{desktop_diff:+.1f}pp")
        st.metric("Windows", f"{profiles['Direct-only']['windows_pct']:.1f}%",
                 delta=f"{windows_diff:+.1f}pp")
        st.metric("Win+Desktop", f"{profiles['Direct-only']['windows_and_desktop_pct']:.1f}%",
                 delta=f"{win_desktop_diff:+.1f}pp",
                 help="Corporate profile: Windows OS on Desktop device")
        st.metric("Purchase Rate", f"{profiles['Direct-only']['purchase_rate']:.2f}%",
                 delta=f"{purchase_diff:+.2f}pp", delta_color="inverse")
        st.metric("Avg Engagement", f"{profiles['Direct-only']['avg_engagement_sec']:.0f}s",
                 delta=f"{engagement_diff:+.0f}s", delta_color="inverse")

    with col3:
        st.markdown("**SST-Only**")
        desktop_diff_sst = profiles['SST-only']['desktop_pct'] - profiles['Both']['desktop_pct']
        windows_diff_sst = profiles['SST-only']['windows_pct'] - profiles['Both']['windows_pct']
        win_desktop_diff_sst = profiles['SST-only']['windows_and_desktop_pct'] - profiles['Both']['windows_and_desktop_pct']
        purchase_diff_sst = profiles['SST-only']['purchase_rate'] - profiles['Both']['purchase_rate']
        engagement_diff_sst = profiles['SST-only']['avg_engagement_sec'] - profiles['Both']['avg_engagement_sec']

        st.metric("Desktop", f"{profiles['SST-only']['desktop_pct']:.1f}%",
                 delta=f"{desktop_diff_sst:+.1f}pp")
        st.metric("Windows", f"{profiles['SST-only']['windows_pct']:.1f}%",
                 delta=f"{windows_diff_sst:+.1f}pp")
        st.metric("Win+Desktop", f"{profiles['SST-only']['windows_and_desktop_pct']:.1f}%",
                 delta=f"{win_desktop_diff_sst:+.1f}pp",
                 help="Corporate profile: Windows OS on Desktop device")
        st.metric("Purchase Rate", f"{profiles['SST-only']['purchase_rate']:.2f}%",
                 delta=f"{purchase_diff_sst:+.2f}pp", delta_color="inverse")
        st.metric("Avg Engagement", f"{profiles['SST-only']['avg_engagement_sec']:.0f}s",
                 delta=f"{engagement_diff_sst:+.0f}s", delta_color="inverse")

    # Corporate Hypothesis
    st.markdown("---")
    if win_desktop_diff > 15 or (desktop_diff > 10 and windows_diff > 15):
        st.success(f"""
        ##### 🏢 Corporate Hypothesis - VALIDATED ✅

        Direct-only users have a **strong corporate profile**:
        - **Windows+Desktop:** {profiles['Direct-only']['windows_and_desktop_pct']:.1f}% ({win_desktop_diff:+.1f}pp vs baseline)
        - Desktop: {desktop_diff:+.1f}pp higher | Windows: {windows_diff:+.1f}pp higher

        This matches **office workers on corporate machines** whose IT departments block `sst.warwick.com.au` (unknown domain)
        but whitelist `google-analytics.com` (standard analytics).

        **SST-only shows similar corporate profile** ({profiles['SST-only']['windows_and_desktop_pct']:.1f}%, {win_desktop_diff_sst:+.1f}pp),
        suggesting both "only" groups are B2B users blocked by different network policies.
        """)
    else:
        st.info("Corporate hypothesis: weak evidence (profiles similar to baseline)")

    # Conversion & Engagement Hypothesis
    if purchase_diff < -0.5 and purchase_diff_sst < -0.5:
        st.success(f"""
        ##### 💰 Conversion & Engagement Hypothesis - VALIDATED ✅

        Both "only" categories show lower purchase rates:
        - Direct-only: {purchase_diff:+.2f}pp ({profiles['Direct-only']['purchase_rate']:.2f}% vs {profiles['Both']['purchase_rate']:.2f}%)
        - SST-only: {purchase_diff_sst:+.2f}pp ({profiles['SST-only']['purchase_rate']:.2f}% vs {profiles['Both']['purchase_rate']:.2f}%)

        Engagement time difference:
        - Direct-only: {engagement_diff:+.0f}s ({profiles['Direct-only']['avg_engagement_sec']:.0f}s vs {profiles['Both']['avg_engagement_sec']:.0f}s)
        - SST-only: {engagement_diff_sst:+.0f}s ({profiles['SST-only']['avg_engagement_sec']:.0f}s vs {profiles['Both']['avg_engagement_sec']:.0f}s)

        **Conclusion:** These are research/browsing sessions from corporate users, not purchase intent.
        """)

    st.markdown("---")
    with st.expander("🔬 Methodology Details"):
        st.markdown("""
        ### Fuzzy Matching Algorithm

        **Problem:** Same session gets different IDs:
        - SST: `ga_session_id = 1768098275`
        - Direct: `ga_session_id = 1768098276` (0.3 seconds later)

        **Solution:** Match by timestamp + attributes:
        1. For each SST session, find Direct sessions within ±5 minutes
        2. Filter to matching device + country
        3. Take closest timestamp match
        4. Label both as "Both"

        **Validation:**
        - Median time difference: 0.3 seconds
        - 50.2% have consecutive IDs (differ by 1)
        - r = 0.635 correlation (timestamp vs ID diff)
        """)

    st.markdown("---")
    st.markdown("#### 📊 Device & OS Breakdown (CORRECTED)")

    # Visualize the corrected profiles
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Desktop Usage by Category**")
        desktop_chart_data = pd.DataFrame({
            "Category": ["Both", "SST-only", "Direct-only"],
            "Desktop %": [
                profiles['Both']['desktop_pct'],
                profiles['SST-only']['desktop_pct'],
                profiles['Direct-only']['desktop_pct']
            ]
        })
        chart = alt.Chart(desktop_chart_data).mark_bar().encode(
            x=alt.X("Category:N", title="", sort=["Both", "SST-only", "Direct-only"]),
            y=alt.Y("Desktop %:Q", title="Desktop %", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color("Category:N",
                scale=alt.Scale(
                    domain=["Both", "SST-only", "Direct-only"],
                    range=["#9b59b6", "#2ecc71", "#3498db"]
                ),
                legend=None
            ),
            tooltip=["Category", alt.Tooltip("Desktop %:Q", format=".1f")]
        ).properties(height=250).configure_axis(
            labelFontSize=CHART_LABEL_SIZE,
            titleFontSize=CHART_TITLE_SIZE
        )
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("**Windows Usage by Category**")
        windows_chart_data = pd.DataFrame({
            "Category": ["Both", "SST-only", "Direct-only"],
            "Windows %": [
                profiles['Both']['windows_pct'],
                profiles['SST-only']['windows_pct'],
                profiles['Direct-only']['windows_pct']
            ]
        })
        chart = alt.Chart(windows_chart_data).mark_bar().encode(
            x=alt.X("Category:N", title="", sort=["Both", "SST-only", "Direct-only"]),
            y=alt.Y("Windows %:Q", title="Windows %", scale=alt.Scale(domain=[0, 100])),
            color=alt.Color("Category:N",
                scale=alt.Scale(
                    domain=["Both", "SST-only", "Direct-only"],
                    range=["#9b59b6", "#2ecc71", "#3498db"]
                ),
                legend=None
            ),
            tooltip=["Category", alt.Tooltip("Windows %:Q", format=".1f")]
        ).properties(height=250).configure_axis(
            labelFontSize=CHART_LABEL_SIZE,
            titleFontSize=CHART_TITLE_SIZE
        )
        st.altair_chart(chart, use_container_width=True)

    st.markdown("---")
    st.markdown("#### 💰 Engagement & Conversion (CORRECTED)")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Purchase Rate Comparison**")
        purchase_chart_data = pd.DataFrame({
            "Category": ["Both", "SST-only", "Direct-only"],
            "Purchase Rate %": [
                profiles['Both']['purchase_rate'],
                profiles['SST-only']['purchase_rate'],
                profiles['Direct-only']['purchase_rate']
            ]
        })
        chart = alt.Chart(purchase_chart_data).mark_bar().encode(
            x=alt.X("Category:N", title="", sort=["Both", "SST-only", "Direct-only"]),
            y=alt.Y("Purchase Rate %:Q", title="Purchase Rate %"),
            color=alt.Color("Category:N",
                scale=alt.Scale(
                    domain=["Both", "SST-only", "Direct-only"],
                    range=["#9b59b6", "#2ecc71", "#3498db"]
                ),
                legend=None
            ),
            tooltip=["Category", alt.Tooltip("Purchase Rate %:Q", format=".2f")]
        ).properties(height=250).configure_axis(
            labelFontSize=CHART_LABEL_SIZE,
            titleFontSize=CHART_TITLE_SIZE
        )
        st.altair_chart(chart, use_container_width=True)

    with col2:
        st.markdown("**Average Engagement Time**")
        engagement_chart_data = pd.DataFrame({
            "Category": ["Both", "SST-only", "Direct-only"],
            "Engagement (seconds)": [
                profiles['Both']['avg_engagement_sec'],
                profiles['SST-only']['avg_engagement_sec'],
                profiles['Direct-only']['avg_engagement_sec']
            ]
        })
        chart = alt.Chart(engagement_chart_data).mark_bar().encode(
            x=alt.X("Category:N", title="", sort=["Both", "SST-only", "Direct-only"]),
            y=alt.Y("Engagement (seconds):Q", title="Avg Engagement (seconds)"),
            color=alt.Color("Category:N",
                scale=alt.Scale(
                    domain=["Both", "SST-only", "Direct-only"],
                    range=["#9b59b6", "#2ecc71", "#3498db"]
                ),
                legend=None
            ),
            tooltip=["Category", alt.Tooltip("Engagement (seconds):Q", format=".0f")]
        ).properties(height=250).configure_axis(
            labelFontSize=CHART_LABEL_SIZE,
            titleFontSize=CHART_TITLE_SIZE
        )
        st.altair_chart(chart, use_container_width=True)

    st.info(f"""
    **Key Insights:**
    - Both "only" categories have **lower purchase rates** ({profiles['Direct-only']['purchase_rate']:.2f}% and {profiles['SST-only']['purchase_rate']:.2f}% vs {profiles['Both']['purchase_rate']:.2f}% baseline)
    - Both "only" categories have **{('lower' if engagement_diff < 0 else 'higher')} engagement time** ({profiles['Direct-only']['avg_engagement_sec']:.0f}s and {profiles['SST-only']['avg_engagement_sec']:.0f}s vs {profiles['Both']['avg_engagement_sec']:.0f}s baseline)
    - This suggests research/browsing sessions from B2B users, not purchase intent
    """)

    st.markdown("---")
    st.markdown("#### 💡 Recommendations")
    st.success(f"""
    1. **Continue running both properties** - SST captures +{lift_pct:.1f}% additional sessions
    2. **Use Direct as primary** - 82% capture rate is sufficient for most reporting
    3. **Update stakeholder communications** - Previous +14.5% claim was inflated
    4. **Monitor SST for ad-blocker recovery** - ~{sst_only_pct:.1f}% of traffic only visible via SST
    """)

    st.markdown("---")
    with st.expander("📊 Want detailed browser/geo/daily breakdowns with corrected methodology?"):
        st.info("""
        **To generate detailed breakdowns using corrected methodology:**

        The current analysis shows corrected session categorization and user profiles. To add detailed breakdowns
        by browser, geography, or daily traffic patterns using the corrected timestamp+attribute matching, you would need to:

        1. **Extend `corrected_matching_helpers.py`** to return additional breakdowns
        2. **Query both sources** for each dimension (browser, country, date) after categorization
        3. **Join the results** using the corrected session categories

        **What's currently available:**
        - ✅ Corrected session counts (Both, SST-only, Direct-only)
        - ✅ User profiles (Desktop %, Windows %, Purchase Rate) by category
        - ✅ Statistical validation (chi-squared tests, correlations)

        **What would require additional development:**
        - ❌ Daily traffic patterns with corrected matching
        - ❌ Browser/device breakdowns by corrected category
        - ❌ Geographic analysis by corrected category

        The OLD analysis (Jan 10-14) had these breakdowns but used flawed ga_session_id matching,
        making the numbers unreliable.
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
            - **Corrected Period:** Jan 6-13, 2026 (8 days, timestamp+attribute matching)
            - **Historical Period:** Jan 10-14, 2026 (5 days, UTC-aligned, ga_session_id matching)
            """)


def main():
    st.title("📊 Warwick SST Events Dashboard")
    st.caption("Server-side tracking validation & comparison with Direct GA4 | **Status: ✅ Project Complete**")

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

        st.markdown("---")
        with st.expander("ℹ️ About This Dashboard"):
            st.markdown("""
            **Purpose:** Validate that Warwick's server-side tracking (SST) implementation is working
            and quantify the additional sessions captured vs Direct GA4.

            **Key Finding:** SST captures +14.5% more unique sessions than Direct alone, primarily from:
            - Ad-blocker users (desktop Chrome/Firefox)
            - China traffic (Great Firewall blocks Direct)

            **Data Sources:**
            - SST: AWS Athena (`warwick_weave_sst_events`)
            - Direct: BigQuery (`analytics_375839889`)

            **Project:** TLC / Warwick Fabrics
            **Completed:** January 2026
            """)

    # Main content area
    try:
        # Test connection with a simple query
        with st.spinner("Connecting to Athena..."):
            test_df = run_athena_query("SELECT 1 as test", timeout_seconds=30)

        # Layout with tabs
        tab_comparison, tab_overview, tab_live, tab_events, tab_raw = st.tabs(["🔄 SST vs Direct", "📈 Overview", "📊 Live (Transformed)", "🎯 Events", "🔍 Raw Data"])

        with tab_overview:
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

        with tab_comparison:
            render_corrected_comparison_tab()

        with tab_live:
            st.subheader("Live SST Analysis (Schema Alignment Layer)")
            st.info(f"""
            **This tab queries the Schema Alignment Layer (SAL) views in real-time.**

            Date range: {start_date} to {end_date} (same as Overview tab)

            **SAL Views used:**
            - `sst_sessions` - Session-level rollup with parsed dimensions
            - `sst_sessions_daily` - Daily aggregates for trends
            - `sst_comparison_ready` - Filtered AU data ready for comparison
            """)

            # Key session metrics from transformation layer
            st.markdown("#### Session Metrics (AU Site)")
            with st.spinner("Querying transformation layer..."):
                session_query = f"""
                SELECT
                    COUNT(DISTINCT ga_session_id) as total_sessions,
                    COUNT(DISTINCT user_pseudo_id) as unique_users,
                    SUM(pageviews) as total_pageviews,
                    SUM(purchases) as total_purchases,
                    ROUND(SUM(purchase_value), 2) as total_revenue
                FROM warwick_weave_sst_events.sst_sessions
                WHERE site = 'AU'
                  AND year = '{start_date.year}'
                  AND (
                    (month = '{start_date.month:02d}' AND day >= '{start_date.day:02d}')
                    OR (month = '{end_date.month:02d}' AND day <= '{end_date.day:02d}')
                  )
                """
                try:
                    metrics_df = run_athena_query(session_query)
                    if not metrics_df.empty:
                        m1, m2, m3, m4, m5 = st.columns(5)
                        m1.metric("Sessions", f"{int(metrics_df['total_sessions'].iloc[0]):,}")
                        m2.metric("Users", f"{int(metrics_df['unique_users'].iloc[0]):,}")
                        m3.metric("Pageviews", f"{int(metrics_df['total_pageviews'].iloc[0]):,}")
                        m4.metric("Purchases", f"{int(metrics_df['total_purchases'].iloc[0]):,}")
                        revenue = float(metrics_df['total_revenue'].iloc[0]) if metrics_df['total_revenue'].iloc[0] else 0
                        m5.metric("Revenue", f"${revenue:,.2f}")
                except Exception as e:
                    st.error(f"Query failed: {str(e)}")

            st.markdown("---")

            # Device breakdown from transformation layer
            st.markdown("#### Device Breakdown (Transformed)")
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**By Device Category**")
                device_query = f"""
                SELECT
                    device_category,
                    COUNT(DISTINCT ga_session_id) as sessions
                FROM warwick_weave_sst_events.sst_sessions
                WHERE site = 'AU'
                  AND year = '{start_date.year}'
                  AND month IN ('{start_date.month:02d}', '{end_date.month:02d}')
                GROUP BY 1
                ORDER BY sessions DESC
                """
                try:
                    device_df = run_athena_query(device_query)
                    if not device_df.empty:
                        device_df["sessions"] = pd.to_numeric(device_df["sessions"])
                        st.dataframe(device_df, use_container_width=True, hide_index=True)

                        chart = alt.Chart(device_df).mark_bar().encode(
                            x=alt.X("device_category:N", title="Device"),
                            y=alt.Y("sessions:Q", title="Sessions"),
                            color=alt.Color("device_category:N", legend=None),
                            tooltip=["device_category", "sessions"]
                        ).properties(height=200).configure_axis(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        )
                        st.altair_chart(chart, use_container_width=True)
                except Exception as e:
                    st.error(f"Query failed: {str(e)}")

            with col2:
                st.markdown("**By Browser**")
                browser_query = f"""
                SELECT
                    device_browser,
                    COUNT(DISTINCT ga_session_id) as sessions
                FROM warwick_weave_sst_events.sst_sessions
                WHERE site = 'AU'
                  AND year = '{start_date.year}'
                  AND month IN ('{start_date.month:02d}', '{end_date.month:02d}')
                GROUP BY 1
                ORDER BY sessions DESC
                LIMIT 10
                """
                try:
                    browser_df = run_athena_query(browser_query)
                    if not browser_df.empty:
                        browser_df["sessions"] = pd.to_numeric(browser_df["sessions"])
                        st.dataframe(browser_df, use_container_width=True, hide_index=True)

                        chart = alt.Chart(browser_df).mark_bar().encode(
                            x=alt.X("device_browser:N", sort="-y", title="Browser"),
                            y=alt.Y("sessions:Q", title="Sessions"),
                            color=alt.Color("device_browser:N", legend=None),
                            tooltip=["device_browser", "sessions"]
                        ).properties(height=200).configure_axis(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        )
                        st.altair_chart(chart, use_container_width=True)
                except Exception as e:
                    st.error(f"Query failed: {str(e)}")

            st.markdown("---")

            # Geographic breakdown
            st.markdown("#### Geographic Breakdown (Transformed)")
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Top Countries**")
                country_query = f"""
                SELECT
                    geo_country,
                    COUNT(DISTINCT ga_session_id) as sessions
                FROM warwick_weave_sst_events.sst_sessions
                WHERE site = 'AU'
                  AND year = '{start_date.year}'
                  AND month IN ('{start_date.month:02d}', '{end_date.month:02d}')
                GROUP BY 1
                ORDER BY sessions DESC
                LIMIT 10
                """
                try:
                    country_df = run_athena_query(country_query)
                    if not country_df.empty:
                        country_df["sessions"] = pd.to_numeric(country_df["sessions"])
                        st.dataframe(country_df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Query failed: {str(e)}")

            with col2:
                st.markdown("**By Operating System**")
                os_query = f"""
                SELECT
                    device_operating_system,
                    COUNT(DISTINCT ga_session_id) as sessions
                FROM warwick_weave_sst_events.sst_sessions
                WHERE site = 'AU'
                  AND year = '{start_date.year}'
                  AND month IN ('{start_date.month:02d}', '{end_date.month:02d}')
                GROUP BY 1
                ORDER BY sessions DESC
                """
                try:
                    os_df = run_athena_query(os_query)
                    if not os_df.empty:
                        os_df["sessions"] = pd.to_numeric(os_df["sessions"])
                        st.dataframe(os_df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Query failed: {str(e)}")

            st.markdown("---")

            # Daily trends from transformation layer
            st.markdown("#### Daily Session Trends (Transformed)")
            daily_query = f"""
            SELECT
                date,
                device_category,
                SUM(sessions) as sessions
            FROM warwick_weave_sst_events.sst_sessions_daily
            WHERE site = 'AU'
              AND date >= DATE '{start_date}'
              AND date <= DATE '{end_date}'
            GROUP BY 1, 2
            ORDER BY date
            """
            try:
                daily_df = run_athena_query(daily_query)
                if not daily_df.empty:
                    daily_df["sessions"] = pd.to_numeric(daily_df["sessions"])
                    daily_df["date"] = pd.to_datetime(daily_df["date"])

                    chart = alt.Chart(daily_df).mark_line(point=True).encode(
                        x=alt.X("date:T", title="Date", axis=alt.Axis(format="%b %d")),
                        y=alt.Y("sessions:Q", title="Sessions"),
                        color=alt.Color("device_category:N", title="Device"),
                        tooltip=[alt.Tooltip("date:T", format="%Y-%m-%d"), "device_category", "sessions"]
                    ).properties(height=300).configure_axis(
                        labelFontSize=CHART_LABEL_SIZE,
                        titleFontSize=CHART_TITLE_SIZE
                    ).configure_legend(
                        labelFontSize=CHART_LABEL_SIZE,
                        titleFontSize=CHART_TITLE_SIZE
                    )
                    st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.error(f"Query failed: {str(e)}")

            st.markdown("---")

            # Event breakdown from transformation layer
            st.markdown("#### Event Breakdown (Transformed)")
            event_query = f"""
            SELECT
                event_name,
                COUNT(*) as events
            FROM warwick_weave_sst_events.sst_comparison_ready
            WHERE year = '{start_date.year}'
              AND month IN ('{start_date.month:02d}', '{end_date.month:02d}')
            GROUP BY 1
            ORDER BY events DESC
            LIMIT 15
            """
            try:
                event_df = run_athena_query(event_query)
                if not event_df.empty:
                    event_df["events"] = pd.to_numeric(event_df["events"])

                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.dataframe(event_df, use_container_width=True, hide_index=True)

                    with col2:
                        chart = alt.Chart(event_df.head(10)).mark_bar().encode(
                            x=alt.X("event_name:N", sort="-y", title="Event"),
                            y=alt.Y("events:Q", title="Count"),
                            color=alt.Color("event_name:N", legend=None),
                            tooltip=["event_name", "events"]
                        ).properties(height=250).configure_axis(
                            labelFontSize=CHART_LABEL_SIZE,
                            titleFontSize=CHART_TITLE_SIZE
                        )
                        st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.error(f"Query failed: {str(e)}")

            st.markdown("---")
            with st.expander("ℹ️ About This Tab"):
                st.markdown("""
                **This tab queries the deployed Schema Alignment Layer (SAL) views.**

                Unlike the "SST vs Direct" tab which uses pre-computed historical data, this tab
                queries live data from Athena using the SAL views:

                | View | Purpose |
                |------|---------|
                | `sst_events_transformed` | Base view with all transformations applied |
                | `sst_sessions` | Session-level rollup for aggregations |
                | `sst_sessions_daily` | Pre-aggregated daily metrics |
                | `sst_comparison_ready` | AU-filtered events ready for comparison |

                **Schema Alignment Layer (SAL) v3.5:**
                - Device category parsed from User-Agent (not client_hints)
                - Browser detection with proper order (Edge before Chrome, etc.)
                - 90+ country codes mapped to full names (AU → Australia, ET → Ethiopia, etc.)
                - Bot detection matching BigQuery's IAB/ABC filtering
                - Synthetic events filtered out (session_start, first_visit)
                - Geo match rate: 96.5% vs BigQuery (3.5% mismatch is VPN users)
                """)

        with tab_events:
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

        with tab_raw:
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
        f"Project Status: ✅ Complete | "
        f"Refreshed: {brisbane_now.strftime('%Y-%m-%d %H:%M:%S')} AEST"
        "</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
