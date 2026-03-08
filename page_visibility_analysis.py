"""
Page Visibility Analysis: Validate Prerender Hypothesis

Run this after a few hours/days of data collection to test whether
Direct-only sessions correlate with prerender/hidden page visibility.

Hypothesis: Direct-only sessions are prefetch/prerender traffic where:
- Client-side GA4 fires (Direct receives event)
- SST tag doesn't fire or complete (SST never receives event)

Expected findings if hypothesis is correct:
- Direct-only sessions should have high prerender/hidden rates
- Sessions with visible in Direct should mostly appear in SST (Both)
- Sessions with prerender in Direct should NOT appear in SST
"""

import pandas as pd
from google.cloud import bigquery
import boto3
import time
from datetime import datetime, timedelta

# Date range - adjust as needed
DATE_START = '20260125'  # Day of GTM publish
DATE_END = '20260126'    # Include today for more data


def safe_pct(numerator, denominator):
    return (numerator / denominator * 100) if denominator > 0 else 0.0


def query_direct_page_visibility():
    """Query BigQuery for page_visibility distribution in Direct."""
    print("=" * 70)
    print("DIRECT (BigQuery) - Page Visibility Distribution")
    print("=" * 70)

    bq_client = bigquery.Client(project="376132452327")

    query = f"""
    WITH session_data AS (
        SELECT
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_visibility') as page_visibility,
            MIN(event_timestamp) as session_start_ts,
            MAX(event_timestamp) as session_end_ts,
            COUNT(*) as event_count,
            ANY_VALUE(device.category) as device_category,
            ANY_VALUE(device.operating_system) as device_os,
            ANY_VALUE(geo.country) as geo_country,
            SUM(COALESCE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec'), 0)) as engagement_time_msec,
            ANY_VALUE((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number')) as session_number
        FROM `analytics_375839889.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{DATE_START}' AND '{DATE_END}'
          AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
        GROUP BY 1, 2
    )
    SELECT
        COALESCE(page_visibility, '(not set)') as page_visibility,
        COUNT(*) as sessions,

        -- Session characteristics
        ROUND(AVG((session_end_ts - session_start_ts) / 1000000.0), 2) as avg_duration_sec,
        COUNTIF((session_end_ts - session_start_ts) / 1000000.0 < 0.1) as instant_sessions,
        ROUND(AVG(event_count), 1) as avg_events,
        COUNTIF(engagement_time_msec = 0) as zero_engagement,
        COUNTIF(session_number = 1) as new_users,

        -- Device profile
        COUNTIF(device_category = 'desktop') as desktop,
        COUNTIF(device_os = 'Windows') as windows,
        COUNTIF(geo_country = 'Australia') as australia

    FROM session_data
    GROUP BY 1
    ORDER BY 2 DESC
    """

    df = bq_client.query(query).to_dataframe()

    print(f"\nPage Visibility Distribution (Direct):\n")
    print(df.to_string(index=False))

    # Calculate percentages
    total = df['sessions'].sum()
    print(f"\nTotal sessions: {total:,}")

    for _, row in df.iterrows():
        pv = row['page_visibility']
        sessions = row['sessions']
        pct = safe_pct(sessions, total)
        instant_pct = safe_pct(row['instant_sessions'], sessions)
        zero_eng_pct = safe_pct(row['zero_engagement'], sessions)
        new_user_pct = safe_pct(row['new_users'], sessions)
        desktop_pct = safe_pct(row['desktop'], sessions)

        print(f"\n{pv}:")
        print(f"  Sessions: {sessions:,} ({pct:.1f}%)")
        print(f"  Instant (<0.1s): {instant_pct:.1f}%")
        print(f"  Zero engagement: {zero_eng_pct:.1f}%")
        print(f"  New users: {new_user_pct:.1f}%")
        print(f"  Desktop: {desktop_pct:.1f}%")

    return df


def query_sst_page_visibility():
    """Query Athena for page_visibility distribution in SST."""
    print("\n" + "=" * 70)
    print("SST (Athena) - Page Visibility Distribution")
    print("=" * 70)

    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

    # First repair table to ensure today's partition is visible
    print("\nRepairing table partitions...")
    repair_query = "MSCK REPAIR TABLE warwick_weave_sst_events.events"
    response = athena.start_query_execution(
        QueryString=repair_query,
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
    print(f"  Partition repair: {state}")

    # Parse date range
    year = DATE_START[:4]
    month = DATE_START[4:6]
    day_start = DATE_START[6:8]
    day_end = DATE_END[6:8]

    query = f"""
    WITH session_data AS (
        SELECT
            ga_session_id,
            json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.page_visibility') as page_visibility,
            MIN(from_iso8601_timestamp(timestamp)) as session_start,
            MAX(from_iso8601_timestamp(timestamp)) as session_end,
            COUNT(*) as event_count,
            ARBITRARY(json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.device_category')) as device_category,
            ARBITRARY(json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.event_location.country')) as geo_country
        FROM warwick_weave_sst_events.events
        WHERE year = '{year}' AND month = '{month}' AND day BETWEEN '{day_start}' AND '{day_end}'
        GROUP BY 1, 2
    )
    SELECT
        COALESCE(page_visibility, '(not set)') as page_visibility,
        COUNT(*) as sessions,
        ROUND(AVG(date_diff('millisecond', session_start, session_end) / 1000.0), 2) as avg_duration_sec,
        COUNT(CASE WHEN date_diff('millisecond', session_start, session_end) < 100 THEN 1 END) as instant_sessions,
        ROUND(AVG(event_count), 1) as avg_events
    FROM session_data
    GROUP BY 1
    ORDER BY 2 DESC
    """

    print("\nQuerying SST data...")
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
        print(f"Query failed: {status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')}")
        return None

    results = athena.get_query_results(QueryExecutionId=query_id)
    rows = results['ResultSet']['Rows']

    if len(rows) <= 1:
        print("No data found in SST for this date range")
        return None

    # Parse results
    columns = [c.get('VarCharValue', '') for c in rows[0]['Data']]
    data = []
    for row in rows[1:]:
        data.append([c.get('VarCharValue', '') for c in row['Data']])

    df = pd.DataFrame(data, columns=columns)
    df['sessions'] = pd.to_numeric(df['sessions'])
    df['instant_sessions'] = pd.to_numeric(df['instant_sessions'])

    print(f"\nPage Visibility Distribution (SST):\n")
    print(df.to_string(index=False))

    total = df['sessions'].sum()
    print(f"\nTotal SST sessions: {total:,}")

    return df


def cross_reference_sessions():
    """Check if prerender/hidden sessions in Direct appear in SST."""
    print("\n" + "=" * 70)
    print("CROSS-REFERENCE: Do prerender/hidden Direct sessions appear in SST?")
    print("=" * 70)

    bq_client = bigquery.Client(project="376132452327")

    # Get Direct sessions with their page_visibility
    query = f"""
    SELECT
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
        ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_visibility')) as page_visibility,
        MIN(event_timestamp) as session_start_ts,
        ANY_VALUE(device.category) as device_category,
        ANY_VALUE(geo.country) as geo_country
    FROM `analytics_375839889.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{DATE_START}' AND '{DATE_END}'
      AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
    GROUP BY 1
    """

    print("\nFetching Direct sessions...")
    direct_df = bq_client.query(query).to_dataframe()
    print(f"  Direct sessions: {len(direct_df):,}")

    # Get SST session IDs
    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

    year = DATE_START[:4]
    month = DATE_START[4:6]
    day_start = DATE_START[6:8]
    day_end = DATE_END[6:8]

    query = f"""
    SELECT DISTINCT ga_session_id
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE site = 'AU'
      AND year = '{year}' AND month = '{month}' AND day BETWEEN '{day_start}' AND '{day_end}'
    """

    print("Fetching SST session IDs...")
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
        print(f"Query failed")
        return

    # Collect all SST session IDs
    sst_ids = set()
    next_token = None
    while True:
        if next_token:
            results = athena.get_query_results(QueryExecutionId=query_id, NextToken=next_token)
        else:
            results = athena.get_query_results(QueryExecutionId=query_id)

        rows = results['ResultSet']['Rows']
        start = 1 if not next_token else 0
        for row in rows[start:]:
            val = row['Data'][0].get('VarCharValue', '')
            if val:
                sst_ids.add(val)

        next_token = results.get('NextToken')
        if not next_token:
            break

    print(f"  SST sessions: {len(sst_ids):,}")

    # Cross-reference
    direct_df['in_sst'] = direct_df['ga_session_id'].isin(sst_ids)

    print("\n" + "-" * 70)
    print("Results by page_visibility:")
    print("-" * 70)

    for pv in direct_df['page_visibility'].unique():
        subset = direct_df[direct_df['page_visibility'] == pv]
        total = len(subset)
        in_sst = subset['in_sst'].sum()
        not_in_sst = total - in_sst

        pv_label = pv if pv else '(not set)'
        print(f"\n{pv_label}:")
        print(f"  Total in Direct: {total:,}")
        print(f"  Also in SST (Both): {in_sst:,} ({safe_pct(in_sst, total):.1f}%)")
        print(f"  Direct-only: {not_in_sst:,} ({safe_pct(not_in_sst, total):.1f}%)")

    # Summary
    print("\n" + "=" * 70)
    print("HYPOTHESIS TEST SUMMARY")
    print("=" * 70)

    visible = direct_df[direct_df['page_visibility'] == 'visible']
    hidden = direct_df[direct_df['page_visibility'] == 'hidden']
    prerender = direct_df[direct_df['page_visibility'] == 'prerender']
    not_set = direct_df[direct_df['page_visibility'].isna() | (direct_df['page_visibility'] == '')]

    print(f"""
If hypothesis is CORRECT:
- 'visible' sessions should mostly appear in SST (high Both %)
- 'prerender' sessions should NOT appear in SST (high Direct-only %)
- 'hidden' sessions are ambiguous (could be either)

Actual results:
- 'visible': {safe_pct(visible['in_sst'].sum(), len(visible)):.1f}% in SST
- 'hidden': {safe_pct(hidden['in_sst'].sum(), len(hidden)):.1f}% in SST
- 'prerender': {safe_pct(prerender['in_sst'].sum(), len(prerender)) if len(prerender) > 0 else 'N/A'}% in SST
- '(not set)': {safe_pct(not_set['in_sst'].sum(), len(not_set)):.1f}% in SST

Interpretation:
""")

    # Interpret results
    if len(prerender) > 0:
        prerender_in_sst_pct = safe_pct(prerender['in_sst'].sum(), len(prerender))
        if prerender_in_sst_pct < 20:
            print("✅ HYPOTHESIS SUPPORTED: prerender sessions rarely appear in SST")
        else:
            print("❌ HYPOTHESIS NOT SUPPORTED: prerender sessions DO appear in SST")
    else:
        print("⚠️  No 'prerender' sessions found yet - need more data or prerender may be rare")

    visible_in_sst_pct = safe_pct(visible['in_sst'].sum(), len(visible)) if len(visible) > 0 else 0
    if visible_in_sst_pct > 70:
        print("✅ 'visible' sessions mostly appear in Both (expected)")
    else:
        print(f"⚠️  Only {visible_in_sst_pct:.1f}% of 'visible' sessions in SST (lower than expected)")


def main():
    print("=" * 70)
    print("PAGE VISIBILITY ANALYSIS")
    print(f"Date range: {DATE_START} to {DATE_END}")
    print("=" * 70)

    # Query both sources
    direct_df = query_direct_page_visibility()
    sst_df = query_sst_page_visibility()

    # Cross-reference
    cross_reference_sessions()

    print("\n" + "=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    print("""
1. If 'prerender' values are rare/absent:
   - Prerender may not be common on Warwick site
   - Or Chrome speculation rules aren't triggering for this site

2. If '(not set)' is high:
   - Check GTM - variable may not be firing correctly
   - Or events from before GTM publish are in the data

3. If hypothesis not supported:
   - Direct-only may have different cause (network issues, SST endpoint blocks)
   - Consider other explanations for the 35% instant session rate
""")


if __name__ == "__main__":
    main()
