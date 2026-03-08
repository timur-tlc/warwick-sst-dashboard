"""
Compare event names between SST and Direct for matched sessions.
Uses fuzzy matching (timestamp + device + country) to find corresponding sessions.
"""

import pandas as pd
import boto3
from google.cloud import bigquery
from collections import Counter
import numpy as np

# Date range (13 normal days)
DATE_RANGES = [
    ('20260106', '20260113'),
    ('20260121', '20260125'),
]

TIME_WINDOW_SECONDS = 15


def get_direct_events():
    """Get event-level data from BigQuery Direct."""
    print("Querying BigQuery for Direct events...")
    client = bigquery.Client(project="376132452327")

    # Build UNION for date ranges
    unions = []
    for start, end in DATE_RANGES:
        unions.append(f"""
        SELECT
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
            event_timestamp,
            event_name,
            device.category as device_category,
            geo.country as geo_country
        FROM `analytics_375839889.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start}' AND '{end}'
          AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
        """)

    query = " UNION ALL ".join(unions)
    df = client.query(query).to_dataframe()
    print(f"  Direct: {len(df):,} events, {df['ga_session_id'].nunique():,} sessions")
    return df


def get_sst_events():
    """Get event-level data from Athena SST."""
    print("Querying Athena for SST events...")
    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

    # Build date filter
    date_conditions = []
    for start, end in DATE_RANGES:
        date_conditions.append(
            f"(CAST(CONCAT(year, month, day) AS INTEGER) BETWEEN {start} AND {end})"
        )

    date_filter = " OR ".join(date_conditions)

    query = f"""
    SELECT
        ga_session_id,
        timestamp as event_timestamp,
        event_name,
        device_category,
        geo_country
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE ({date_filter})
      AND site = 'AU'
      AND is_likely_human = TRUE
      AND ga_session_id IS NOT NULL
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'warwick_weave_sst_events'},
        ResultConfiguration={'OutputLocation': 's3://warwick-com-au-athena-results/'},
        WorkGroup='primary'
    )

    query_id = response['QueryExecutionId']

    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        import time
        time.sleep(2)

    if state != 'SUCCEEDED':
        raise Exception(f"Query failed: {state}")

    # Get results
    s3 = session.client('s3', region_name='ap-southeast-2')
    result_location = status['QueryExecution']['ResultConfiguration']['OutputLocation']
    bucket = result_location.split('/')[2]
    key = '/'.join(result_location.split('/')[3:])

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])

    print(f"  SST: {len(df):,} events, {df['ga_session_id'].nunique():,} sessions")
    return df


def aggregate_to_sessions(df, source_name):
    """Aggregate event-level data to session-level with event list."""
    print(f"  Aggregating {source_name} to sessions...")

    # For Direct, timestamp is in microseconds; for SST it's ISO string
    # Check for any integer type (int64 or Int64 nullable)
    if pd.api.types.is_integer_dtype(df['event_timestamp']):
        df['event_ts'] = pd.to_datetime(df['event_timestamp'], unit='us')
    else:
        df['event_ts'] = pd.to_datetime(df['event_timestamp'])
        # Remove timezone if present
        if df['event_ts'].dt.tz is not None:
            df['event_ts'] = df['event_ts'].dt.tz_localize(None)

    # Debug: print sample timestamps
    print(f"    Sample timestamps ({source_name}):")
    print(f"    {df['event_ts'].iloc[0]} (type: {type(df['event_ts'].iloc[0])})")

    sessions = df.groupby('ga_session_id').agg(
        session_start=('event_ts', 'min'),
        device_category=('device_category', 'first'),
        geo_country=('geo_country', 'first'),
        event_count=('event_name', 'count'),
        event_names=('event_name', list)
    ).reset_index()

    print(f"    Session start range: {sessions['session_start'].min()} to {sessions['session_start'].max()}")
    print(f"    Device categories: {sessions['device_category'].value_counts().head(3).to_dict()}")
    print(f"    Top countries: {sessions['geo_country'].value_counts().head(3).to_dict()}")

    return sessions


def fuzzy_match_sessions(direct_sessions, sst_sessions):
    """
    Match sessions using timestamp + device + country.
    Returns list of (direct_session_id, sst_session_id) pairs.
    """
    print(f"\nFuzzy matching sessions (±{TIME_WINDOW_SECONDS}s window)...")

    # Convert timestamps to numeric for fast comparison
    direct_sessions = direct_sessions.copy()
    sst_sessions = sst_sessions.copy()

    # Handle different datetime64 units (ns vs us)
    def to_unix_seconds(series):
        dtype_str = str(series.dtype)
        if 'datetime64[ns]' in dtype_str:
            return series.astype(np.int64) // 10**9
        elif 'datetime64[us]' in dtype_str:
            return series.astype(np.int64) // 10**6
        else:
            # Fallback: convert to ns first
            return series.astype('datetime64[ns]').astype(np.int64) // 10**9

    direct_sessions['ts_numeric'] = to_unix_seconds(direct_sessions['session_start'])
    sst_sessions['ts_numeric'] = to_unix_seconds(sst_sessions['session_start'])

    # Debug: print sample ts_numeric values
    print(f"  Direct ts_numeric sample: {direct_sessions['ts_numeric'].iloc[0]}")
    print(f"  SST ts_numeric sample: {sst_sessions['ts_numeric'].iloc[0]}")
    print(f"  Direct ts_numeric range: {direct_sessions['ts_numeric'].min()} to {direct_sessions['ts_numeric'].max()}")
    print(f"  SST ts_numeric range: {sst_sessions['ts_numeric'].min()} to {sst_sessions['ts_numeric'].max()}")

    # Check overlap in device/country combinations
    direct_combos = set(zip(direct_sessions['device_category'], direct_sessions['geo_country']))
    sst_combos = set(zip(sst_sessions['device_category'], sst_sessions['geo_country']))
    common_combos = direct_combos & sst_combos
    print(f"  Common device+country combos: {len(common_combos)}")
    print(f"  Sample combos: {list(common_combos)[:5]}")

    matches = []
    sst_matched = set()

    # Group by device + country for efficiency
    direct_grouped = direct_sessions.groupby(['device_category', 'geo_country'])
    sst_grouped = {k: v for k, v in sst_sessions.groupby(['device_category', 'geo_country'])}

    checked_groups = 0
    for (device, country), direct_group in direct_grouped:
        if (device, country) not in sst_grouped:
            continue

        checked_groups += 1
        sst_group = sst_grouped[(device, country)]

        for _, direct_row in direct_group.iterrows():
            direct_ts = direct_row['ts_numeric']

            # Find SST sessions within time window
            candidates = sst_group[
                (sst_group['ts_numeric'] >= direct_ts - TIME_WINDOW_SECONDS) &
                (sst_group['ts_numeric'] <= direct_ts + TIME_WINDOW_SECONDS) &
                (~sst_group['ga_session_id'].isin(sst_matched))
            ]

            if len(candidates) > 0:
                # Take closest match
                candidates = candidates.copy()
                candidates['time_diff'] = abs(candidates['ts_numeric'] - direct_ts)
                best_match = candidates.loc[candidates['time_diff'].idxmin()]

                matches.append((direct_row['ga_session_id'], best_match['ga_session_id']))
                sst_matched.add(best_match['ga_session_id'])

    print(f"  Checked {checked_groups} device+country groups")
    print(f"  Found {len(matches):,} matched session pairs")
    return matches


def compare_events_for_matched_sessions(direct_df, sst_df, matches):
    """
    For matched session pairs, compare which events each has.
    """
    print(f"\nComparing events for {len(matches):,} matched session pairs...")

    if len(matches) == 0:
        print("  No matches to compare!")
        return Counter(), Counter()

    # Create lookup from session ID to events
    direct_events_by_session = direct_df.groupby('ga_session_id')['event_name'].apply(list).to_dict()
    sst_events_by_session = sst_df.groupby('ga_session_id')['event_name'].apply(list).to_dict()

    # Count events across all matched sessions
    direct_event_counts = Counter()
    sst_event_counts = Counter()

    missing_from_sst = Counter()  # Events in Direct but not SST
    missing_from_direct = Counter()  # Events in SST but not Direct

    for direct_id, sst_id in matches:
        direct_events = direct_events_by_session.get(direct_id, [])
        sst_events = sst_events_by_session.get(sst_id, [])

        # Count total events
        for e in direct_events:
            direct_event_counts[e] += 1
        for e in sst_events:
            sst_event_counts[e] += 1

        # Find differences (as multisets, not sets)
        direct_counter = Counter(direct_events)
        sst_counter = Counter(sst_events)

        # Events with more occurrences in Direct
        for event in direct_counter:
            diff = direct_counter[event] - sst_counter.get(event, 0)
            if diff > 0:
                missing_from_sst[event] += diff

        # Events with more occurrences in SST
        for event in sst_counter:
            diff = sst_counter[event] - direct_counter.get(event, 0)
            if diff > 0:
                missing_from_direct[event] += diff

    # Print comparison table
    all_events = set(direct_event_counts.keys()) | set(sst_event_counts.keys())

    print("\n" + "="*80)
    print("EVENT COUNTS BY SOURCE (matched sessions only)")
    print("="*80)
    print(f"{'Event Name':<40} {'Direct':>10} {'SST':>10} {'Diff':>10} {'%':>8}")
    print("-"*80)

    rows = []
    for event in sorted(all_events):
        direct_count = direct_event_counts.get(event, 0)
        sst_count = sst_event_counts.get(event, 0)
        diff = direct_count - sst_count
        pct = (diff / direct_count * 100) if direct_count > 0 else 0
        rows.append((event, direct_count, sst_count, diff, pct))
        print(f"{event:<40} {direct_count:>10,} {sst_count:>10,} {diff:>+10,} {pct:>+7.1f}%")

    print("-"*80)
    total_direct = sum(direct_event_counts.values())
    total_sst = sum(sst_event_counts.values())
    total_diff = total_direct - total_sst
    total_pct = (total_diff / total_direct * 100) if total_direct > 0 else 0
    print(f"{'TOTAL':<40} {total_direct:>10,} {total_sst:>10,} {total_diff:>+10,} {total_pct:>+7.1f}%")

    print("\n" + "="*80)
    print("EVENTS MORE COMMON IN DIRECT (missing from SST)")
    print("="*80)
    for event, count in missing_from_sst.most_common(15):
        pct = count / len(matches) * 100
        print(f"  {event:<40} {count:>8,} occurrences ({pct:.1f}% of sessions)")

    print("\n" + "="*80)
    print("EVENTS MORE COMMON IN SST (missing from Direct)")
    print("="*80)
    for event, count in missing_from_direct.most_common(15):
        pct = count / len(matches) * 100
        print(f"  {event:<40} {count:>8,} occurrences ({pct:.1f}% of sessions)")

    return missing_from_sst, missing_from_direct


def main():
    # Get event data from both sources
    direct_df = get_direct_events()
    sst_df = get_sst_events()

    # Aggregate to sessions
    direct_sessions = aggregate_to_sessions(direct_df, "Direct")
    sst_sessions = aggregate_to_sessions(sst_df, "SST")

    # Fuzzy match sessions
    matches = fuzzy_match_sessions(direct_sessions, sst_sessions)

    # Compare events for matched sessions
    missing_sst, missing_direct = compare_events_for_matched_sessions(direct_df, sst_df, matches)

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    if missing_sst:
        print("\nThe event count gap is primarily caused by:")
        for event, count in missing_sst.most_common(3):
            print(f"  - {event}: {count:,} more in Direct than SST")
    else:
        print("\nNo event differences found (or no matches).")


if __name__ == "__main__":
    main()
