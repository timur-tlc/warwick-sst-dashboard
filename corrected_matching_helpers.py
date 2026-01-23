"""
Helper functions for corrected session categorization.

These replace the flawed ga_session_id-based matching with timestamp+attribute matching.
"""

import pandas as pd
import boto3
from google.cloud import bigquery
import time


def fuzzy_match_sessions(date_start='20260106', date_end='20260113', time_window_seconds=300):
    """
    Perform fuzzy matching between SST and Direct sessions.

    Returns:
        dict with keys:
            - 'both': list of (sst_id, direct_id) pairs
            - 'sst_only': list of SST session IDs with no match
            - 'direct_only': list of Direct session IDs with no match
            - 'sst_df': DataFrame of SST sessions with category
            - 'direct_df': DataFrame of Direct sessions with category
    """

    print("Loading session data from both sources...")

    # Load BigQuery (Direct) sessions
    bq_client = bigquery.Client(project="376132452327")
    bq_query = f"""
    SELECT
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
        MIN(event_timestamp) as session_start_ts,
        device.category as device_category,
        device.operating_system as device_operating_system,
        device.web_info.browser as device_browser,
        geo.country as geo_country,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_traffic_source_last_click_source') as traffic_source,
        COUNT(*) as event_count,
        MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as has_purchase,
        SUM((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec')) as engagement_time_msec
    FROM `analytics_375839889.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{date_start}' AND '{date_end}'
      AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
    GROUP BY 1, 3, 4, 5, 6, 7
    HAVING ga_session_id IS NOT NULL
    """

    bq_df = bq_client.query(bq_query).to_dataframe()
    bq_df = bq_df.drop_duplicates(subset='ga_session_id', keep='first')
    bq_df['session_start_ts'] = pd.to_numeric(bq_df['session_start_ts'])
    bq_df['source'] = 'Direct'

    print(f"  BigQuery: {len(bq_df):,} sessions")

    # Load Athena (SST) sessions
    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

    # Convert date strings to YYYY, MM, DD for Athena partitions
    year = date_start[:4]
    month = date_start[4:6]
    day_start = date_start[6:8]
    day_end = date_end[6:8]

    athena_query = f"""
    SELECT
        ga_session_id,
        MIN(CAST(to_unixtime(from_iso8601_timestamp(timestamp)) AS BIGINT) * 1000000) as session_start_ts,
        ARBITRARY(device_category) as device_category,
        ARBITRARY(device_operating_system) as device_operating_system,
        ARBITRARY(device_browser) as device_browser,
        ARBITRARY(geo_country) as geo_country,
        ARBITRARY(traffic_source) as traffic_source,
        COUNT(*) as event_count,
        MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as has_purchase,
        SUM(CAST(engagement_time_msec AS BIGINT)) as engagement_time_msec
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE site = 'AU'
      AND year = '{year}'
      AND month = '{month}'
      AND day BETWEEN '{day_start}' AND '{day_end}'
    GROUP BY 1
    """

    response = athena.start_query_execution(
        QueryString=athena_query,
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
        raise Exception("Athena query failed")

    athena_rows = []
    next_token = None
    while True:
        if next_token:
            results = athena.get_query_results(QueryExecutionId=query_id, NextToken=next_token, MaxResults=1000)
        else:
            results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1000)

        rows = results['ResultSet']['Rows']
        if not next_token:
            rows = rows[1:]

        for row in rows:
            athena_rows.append([field.get('VarCharValue', '') for field in row['Data']])

        next_token = results.get('NextToken')
        if not next_token:
            break

    sst_df = pd.DataFrame(athena_rows, columns=[
        'ga_session_id', 'session_start_ts', 'device_category', 'device_operating_system',
        'device_browser', 'geo_country', 'traffic_source', 'event_count', 'has_purchase', 'engagement_time_msec'
    ])
    sst_df = sst_df.drop_duplicates(subset='ga_session_id', keep='first')
    sst_df['session_start_ts'] = pd.to_numeric(sst_df['session_start_ts'])
    sst_df['source'] = 'SST'

    print(f"  Athena: {len(sst_df):,} sessions")

    # Perform fuzzy matching
    print("Performing fuzzy match...")

    bq_df['matched_to_sst'] = False
    sst_df['matched_to_direct'] = False
    bq_df['session_category'] = 'Direct-only'
    sst_df['session_category'] = 'SST-only'

    matches = []
    time_window_micros = time_window_seconds * 1000000

    for idx, sst_row in sst_df.iterrows():
        sst_ts = sst_row['session_start_ts']

        # Find Direct sessions within time window
        candidates = bq_df[
            (~bq_df['matched_to_sst']) &
            (bq_df['session_start_ts'] >= sst_ts - time_window_micros) &
            (bq_df['session_start_ts'] <= sst_ts + time_window_micros)
        ]

        if len(candidates) == 0:
            continue

        # Filter by device and country
        candidates = candidates[
            (candidates['device_category'] == sst_row['device_category']) &
            (candidates['geo_country'] == sst_row['geo_country'])
        ]

        if len(candidates) == 0:
            continue

        # Take closest timestamp match
        candidates = candidates.copy()
        candidates['ts_diff'] = abs(candidates['session_start_ts'] - sst_ts)
        best_match = candidates.nsmallest(1, 'ts_diff').iloc[0]

        # Record match
        matches.append((sst_row['ga_session_id'], best_match['ga_session_id']))

        # Mark as matched
        bq_df.loc[best_match.name, 'matched_to_sst'] = True
        bq_df.loc[best_match.name, 'session_category'] = 'Both'
        sst_df.loc[idx, 'matched_to_direct'] = True
        sst_df.loc[idx, 'session_category'] = 'Both'

    print(f"  Matched: {len(matches):,} session pairs")

    sst_only = sst_df[~sst_df['matched_to_direct']]['ga_session_id'].tolist()
    direct_only = bq_df[~bq_df['matched_to_sst']]['ga_session_id'].tolist()

    print(f"  Both:        {len(matches):,}")
    print(f"  SST-only:    {len(sst_only):,}")
    print(f"  Direct-only: {len(direct_only):,}")

    return {
        'both': matches,
        'sst_only': sst_only,
        'direct_only': direct_only,
        'sst_df': sst_df,
        'direct_df': bq_df
    }


def get_corrected_session_stats(date_start='20260106', date_end='20260113'):
    """
    Get session statistics using corrected matching.

    Returns dict with corrected categorization stats.
    """

    result = fuzzy_match_sessions(date_start, date_end)

    both_count = len(result['both'])
    sst_only_count = len(result['sst_only'])
    direct_only_count = len(result['direct_only'])
    total = both_count + sst_only_count + direct_only_count

    # Get profiles for each category
    sst_df = result['sst_df']
    direct_df = result['direct_df']

    both_sst = sst_df[sst_df['session_category'] == 'Both']
    sst_only_sessions = sst_df[sst_df['session_category'] == 'SST-only']
    direct_only_sessions = direct_df[direct_df['session_category'] == 'Direct-only']

    # Calculate engagement time (convert to seconds)
    both_direct = direct_df[direct_df['session_category'] == 'Both']

    # Calculate daily breakdown for timeseries
    # Use Direct timestamps for date extraction (microseconds since epoch)
    direct_df['date'] = pd.to_datetime(direct_df['session_start_ts'] // 1000000, unit='s').dt.date
    sst_df['date'] = pd.to_datetime(sst_df['session_start_ts'] // 1000000, unit='s').dt.date

    # Daily counts by category
    daily_both = direct_df[direct_df['session_category'] == 'Both'].groupby('date').size()
    daily_direct_only = direct_df[direct_df['session_category'] == 'Direct-only'].groupby('date').size()
    daily_sst_only = sst_df[sst_df['session_category'] == 'SST-only'].groupby('date').size()

    # Combine into a single dataframe
    daily_df = pd.DataFrame({
        'Both': daily_both,
        'Direct-only': daily_direct_only,
        'SST-only': daily_sst_only
    }).fillna(0).astype(int)
    daily_df.index = pd.to_datetime(daily_df.index)
    daily_df = daily_df.reset_index().rename(columns={'index': 'date'})

    return {
        'totals': {
            'both': both_count,
            'sst_only': sst_only_count,
            'direct_only': direct_only_count,
            'total': total
        },
        'daily': daily_df,
        'profiles': {
            'Both': {
                'desktop_pct': (both_sst['device_category'] == 'desktop').sum() / len(both_sst) * 100,
                'windows_pct': (both_sst['device_operating_system'] == 'Windows').sum() / len(both_sst) * 100,
                'windows_and_desktop_pct': ((both_sst['device_category'] == 'desktop') & (both_sst['device_operating_system'] == 'Windows')).sum() / len(both_sst) * 100,
                'purchase_rate': both_sst['has_purchase'].astype(int).sum() / len(both_sst) * 100,
                'avg_engagement_sec': pd.to_numeric(both_direct['engagement_time_msec'], errors='coerce').fillna(0).mean() / 1000
            },
            'SST-only': {
                'desktop_pct': (sst_only_sessions['device_category'] == 'desktop').sum() / len(sst_only_sessions) * 100,
                'windows_pct': (sst_only_sessions['device_operating_system'] == 'Windows').sum() / len(sst_only_sessions) * 100,
                'windows_and_desktop_pct': ((sst_only_sessions['device_category'] == 'desktop') & (sst_only_sessions['device_operating_system'] == 'Windows')).sum() / len(sst_only_sessions) * 100,
                'purchase_rate': sst_only_sessions['has_purchase'].astype(int).sum() / len(sst_only_sessions) * 100,
                'avg_engagement_sec': pd.to_numeric(sst_only_sessions['engagement_time_msec'], errors='coerce').fillna(0).mean() / 1000
            },
            'Direct-only': {
                'desktop_pct': (direct_only_sessions['device_category'] == 'desktop').sum() / len(direct_only_sessions) * 100,
                'windows_pct': (direct_only_sessions['device_operating_system'] == 'Windows').sum() / len(direct_only_sessions) * 100,
                'windows_and_desktop_pct': ((direct_only_sessions['device_category'] == 'desktop') & (direct_only_sessions['device_operating_system'] == 'Windows')).sum() / len(direct_only_sessions) * 100,
                'purchase_rate': direct_only_sessions['has_purchase'].astype(int).sum() / len(direct_only_sessions) * 100,
                'avg_engagement_sec': pd.to_numeric(direct_only_sessions['engagement_time_msec'], errors='coerce').fillna(0).mean() / 1000
            }
        },
        'dataframes': {
            'sst': sst_df,
            'direct': direct_df
        }
    }
