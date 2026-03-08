"""
SST Filtering Analysis: Check what SST receives vs what gets filtered

Queries raw SST data to find:
1. Sessions flagged as bots (is_bot = TRUE)
2. Sessions with missing/incomplete data
3. Patterns in filtered traffic
4. Compare filtered SST sessions to Direct-only profile
"""

import pandas as pd
import boto3
import time
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"


def safe_pct(numerator, denominator):
    """Calculate percentage safely."""
    return (numerator / denominator * 100) if denominator > 0 else 0.0


def run_athena_query(query, description=""):
    """Run an Athena query and return results as DataFrame."""
    print(f"\n{description}..." if description else "\nRunning query...")

    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'warwick_weave_sst_events'},
        ResultConfiguration={'OutputLocation': 's3://warwick-com-au-athena-results/'},
        WorkGroup='primary'
    )
    query_id = response['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(1)

    if state != 'SUCCEEDED':
        error = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
        raise Exception(f"Query failed: {error}")

    # Get results
    rows = []
    next_token = None
    while True:
        if next_token:
            results = athena.get_query_results(QueryExecutionId=query_id, NextToken=next_token, MaxResults=1000)
        else:
            results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1000)

        result_rows = results['ResultSet']['Rows']
        if not next_token and result_rows:
            # First row is header
            columns = [col.get('VarCharValue', '') for col in result_rows[0]['Data']]
            result_rows = result_rows[1:]

        for row in result_rows:
            rows.append([field.get('VarCharValue', '') for field in row['Data']])

        next_token = results.get('NextToken')
        if not next_token:
            break

    df = pd.DataFrame(rows, columns=columns)
    print(f"  Retrieved {len(df)} rows")
    return df


def analyze_bot_flagged_sessions():
    """Check how many SST sessions are flagged as bots."""
    print("=" * 70)
    print("BOT-FLAGGED SST SESSIONS")
    print("=" * 70)

    query = """
    SELECT
        is_bot,
        is_likely_human,
        COUNT(DISTINCT ga_session_id) as sessions,
        COUNT(*) as events
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE site = 'AU'
      AND year = '2026'
      AND month = '01'
      AND day BETWEEN '06' AND '13'
    GROUP BY 1, 2
    ORDER BY 3 DESC
    """

    df = run_athena_query(query, "Querying bot flags")

    print("\nSST sessions by bot/human flags:")
    print(df.to_string(index=False))

    return df


def get_bot_session_details():
    """Get details of sessions flagged as bots."""
    print("\n" + "=" * 70)
    print("BOT SESSION DETAILS")
    print("=" * 70)

    query = """
    SELECT
        ga_session_id,
        device_category,
        device_operating_system,
        device_browser,
        geo_country,
        COUNT(*) as events,
        MIN(timestamp) as first_event,
        MAX(timestamp) as last_event
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE site = 'AU'
      AND year = '2026'
      AND month = '01'
      AND day BETWEEN '06' AND '13'
      AND is_bot = TRUE
    GROUP BY 1, 2, 3, 4, 5
    LIMIT 100
    """

    df = run_athena_query(query, "Querying bot session details")

    if len(df) > 0:
        print("\nBot session profile:")
        for col in ['device_category', 'device_operating_system', 'device_browser', 'geo_country']:
            if col in df.columns:
                print(f"\n{col}:")
                print(df[col].value_counts().head(5).to_string())

    return df


def get_incomplete_sessions():
    """Find sessions with NULL/missing critical fields."""
    print("\n" + "=" * 70)
    print("INCOMPLETE SST SESSIONS (Missing Fields)")
    print("=" * 70)

    query = """
    SELECT
        CASE
            WHEN ga_session_id IS NULL OR ga_session_id = '' THEN 'missing_session_id'
            WHEN user_pseudo_id IS NULL OR user_pseudo_id = '' THEN 'missing_user_id'
            WHEN device_category IS NULL OR device_category = '' THEN 'missing_device'
            WHEN geo_country IS NULL OR geo_country = '' THEN 'missing_geo'
            ELSE 'complete'
        END as data_quality,
        COUNT(*) as events,
        COUNT(DISTINCT ga_session_id) as sessions
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE site = 'AU'
      AND year = '2026'
      AND month = '01'
      AND day BETWEEN '06' AND '13'
    GROUP BY 1
    ORDER BY 2 DESC
    """

    df = run_athena_query(query, "Checking data completeness")

    print("\nData quality breakdown:")
    print(df.to_string(index=False))

    return df


def compare_sst_filtered_to_direct_only():
    """Compare profile of SST-filtered sessions to Direct-only."""
    print("\n" + "=" * 70)
    print("COMPARING SST-FILTERED TO DIRECT-ONLY PROFILE")
    print("=" * 70)

    # Get SST sessions that would be filtered (is_likely_human = FALSE)
    query = """
    SELECT
        device_category,
        device_operating_system as device_os,
        device_browser as browser,
        geo_country,
        COUNT(DISTINCT ga_session_id) as sessions
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE site = 'AU'
      AND year = '2026'
      AND month = '01'
      AND day BETWEEN '06' AND '13'
      AND is_likely_human = FALSE
    GROUP BY 1, 2, 3, 4
    """

    filtered_df = run_athena_query(query, "Querying SST filtered sessions")

    if len(filtered_df) == 0:
        print("\nNo filtered sessions found in SST!")
        print("This means SST is NOT the one filtering - traffic never arrives.")
        return None

    # Compare to Direct-only profile from cache
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    direct_only = direct_df[direct_df['session_category'] == 'Direct-only']

    print(f"\nSST filtered sessions: {filtered_df['sessions'].astype(int).sum()}")
    print(f"Direct-only sessions:  {len(direct_only)}")

    # Compare dimensions
    for dim in ['device_category', 'device_os', 'browser', 'geo_country']:
        sst_col = dim
        direct_col = 'device_operating_system' if dim == 'device_os' else ('device_browser' if dim == 'browser' else dim)

        if sst_col in filtered_df.columns and direct_col in direct_only.columns:
            print(f"\n{dim.upper()}:")

            # SST filtered distribution
            sst_dist = filtered_df.groupby(sst_col)['sessions'].sum()
            sst_dist = (sst_dist / sst_dist.sum() * 100).sort_values(ascending=False).head(5)

            # Direct-only distribution
            do_dist = direct_only[direct_col].value_counts(normalize=True).head(5) * 100

            print(f"  {'Value':<25} {'SST-filtered %':>15} {'Direct-only %':>15}")
            print(f"  {'-'*55}")

            all_vals = set(sst_dist.index) | set(do_dist.index)
            for val in sorted(all_vals, key=lambda x: sst_dist.get(x, 0) + do_dist.get(x, 0), reverse=True)[:5]:
                sst_pct = sst_dist.get(val, 0)
                do_pct = do_dist.get(val, 0)
                val_str = str(val)[:23] if val else "(empty)"
                print(f"  {val_str:<25} {sst_pct:>14.1f}% {do_pct:>14.1f}%")

    return filtered_df


def check_raw_events_table():
    """Check the raw events table for any pre-filtering."""
    print("\n" + "=" * 70)
    print("RAW SST EVENTS (Before Transformation)")
    print("=" * 70)

    query = """
    SELECT
        COUNT(*) as total_events,
        COUNT(DISTINCT json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ga_session_id')) as unique_sessions,
        COUNT(CASE WHEN json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.user_agent') IS NULL
                   OR json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.user_agent') = ''
              THEN 1 END) as missing_ua,
        COUNT(CASE WHEN json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ga_session_id') IS NULL
                   OR json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ga_session_id') = ''
              THEN 1 END) as missing_session_id
    FROM warwick_weave_sst_events.events
    WHERE year = '2026'
      AND month = '01'
      AND day BETWEEN '06' AND '13'
    """

    df = run_athena_query(query, "Checking raw events table")

    print("\nRaw event statistics:")
    for col in df.columns:
        print(f"  {col}: {df[col].iloc[0]}")

    return df


def analyze_user_agent_patterns():
    """Look at User-Agent patterns in SST data."""
    print("\n" + "=" * 70)
    print("SST USER-AGENT PATTERNS")
    print("=" * 70)

    # Get sample of User-Agents from bot-flagged sessions
    query = """
    SELECT
        DISTINCT json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.user_agent') as user_agent,
        COUNT(*) as events
    FROM warwick_weave_sst_events.events
    WHERE year = '2026'
      AND month = '01'
      AND day BETWEEN '06' AND '13'
    GROUP BY 1
    HAVING COUNT(*) > 10
    ORDER BY 2 DESC
    LIMIT 30
    """

    df = run_athena_query(query, "Getting User-Agent distribution")

    print("\nTop User-Agents in SST data:")
    for _, row in df.iterrows():
        ua = row['user_agent'][:80] if row['user_agent'] else "(empty)"
        print(f"  {row['events']:>6} events: {ua}")

    return df


def check_session_timing():
    """Check timing patterns of SST sessions."""
    print("\n" + "=" * 70)
    print("SST SESSION TIMING PATTERNS")
    print("=" * 70)

    query = """
    WITH session_stats AS (
        SELECT
            ga_session_id,
            is_likely_human,
            COUNT(*) as events,
            MIN(from_iso8601_timestamp(timestamp)) as first_event,
            MAX(from_iso8601_timestamp(timestamp)) as last_event,
            date_diff('millisecond',
                      MIN(from_iso8601_timestamp(timestamp)),
                      MAX(from_iso8601_timestamp(timestamp))) as duration_ms
        FROM warwick_weave_sst_events.sst_events_transformed
        WHERE site = 'AU'
          AND year = '2026'
          AND month = '01'
          AND day BETWEEN '06' AND '13'
        GROUP BY 1, 2
    )
    SELECT
        is_likely_human,
        COUNT(*) as sessions,
        AVG(events) as avg_events,
        AVG(duration_ms) as avg_duration_ms,
        APPROX_PERCENTILE(duration_ms, 0.5) as median_duration_ms,
        COUNT(CASE WHEN duration_ms < 100 THEN 1 END) as instant_sessions
    FROM session_stats
    GROUP BY 1
    """

    df = run_athena_query(query, "Analyzing session timing")

    print("\nSession timing by human flag:")
    print(df.to_string(index=False))

    return df


def main():
    print("=" * 70)
    print("SST FILTERING ANALYSIS")
    print("What does SST receive? What gets filtered?")
    print("=" * 70)

    # 1. Check bot flagging
    bot_flags = analyze_bot_flagged_sessions()

    # 2. Check incomplete sessions
    incomplete = get_incomplete_sessions()

    # 3. Get details of bot sessions
    bot_details = get_bot_session_details()

    # 4. Compare to Direct-only profile
    comparison = compare_sst_filtered_to_direct_only()

    # 5. Check raw events
    raw_stats = check_raw_events_table()

    # 6. Session timing
    timing = check_session_timing()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    # Calculate totals
    if bot_flags is not None and len(bot_flags) > 0:
        total_sst = bot_flags['sessions'].astype(int).sum()
        bot_sessions = bot_flags[bot_flags['is_bot'] == 'true']['sessions'].astype(int).sum() if 'true' in bot_flags['is_bot'].values else 0
        human_sessions = bot_flags[bot_flags['is_likely_human'] == 'true']['sessions'].astype(int).sum() if 'true' in bot_flags['is_likely_human'].values else 0

        print(f"\nTotal SST sessions: {total_sst:,}")
        print(f"  Flagged as bot:     {bot_sessions:,} ({safe_pct(bot_sessions, total_sst):.1f}%)")
        print(f"  Likely human:       {human_sessions:,} ({safe_pct(human_sessions, total_sst):.1f}%)")
        print(f"  Filtered out:       {total_sst - human_sessions:,} ({safe_pct(total_sst - human_sessions, total_sst):.1f}%)")

    # Load Direct-only count for comparison
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    direct_only_count = len(direct_df[direct_df['session_category'] == 'Direct-only'])

    print(f"\nDirect-only sessions: {direct_only_count:,}")

    if bot_flags is not None and len(bot_flags) > 0:
        filtered_count = total_sst - human_sessions if 'human_sessions' in dir() else 0
        if filtered_count > 0:
            print(f"\nSST filtered ({filtered_count:,}) vs Direct-only ({direct_only_count:,})")
            if filtered_count < direct_only_count * 0.1:
                print("\n>>> SST filters very few sessions - traffic never arrives!")
                print(">>> Direct-only sessions are NOT being filtered by SST")
                print(">>> They simply never reach the SST endpoint")
            else:
                print("\n>>> SST filters a significant number - could explain Direct-only")

    print("\n" + "-" * 70)
    print("CONCLUSIONS")
    print("-" * 70)
    print("""
If SST filtered sessions << Direct-only sessions:
  - Traffic never reaches SST (prefetch, network issues, client-side blocking)
  - GTM Web Container tag doesn't fire for these sessions
  - Not a server-side filtering issue

If SST filtered sessions ~= Direct-only sessions:
  - SST is actively filtering this traffic
  - Check GTM Server Container settings
  - Check if is_likely_human filter matches Direct-only profile
""")


if __name__ == "__main__":
    main()
