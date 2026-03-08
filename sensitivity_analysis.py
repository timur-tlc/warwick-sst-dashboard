"""
Sensitivity Analysis: Time Window Impact on Session Matching

Tests how different time windows affect the matching results.
Stable results across windows = confident methodology.
"""

import pandas as pd
import boto3
from google.cloud import bigquery
import time
import matplotlib.pyplot as plt
import numpy as np

DATE_START = '20260106'
DATE_END = '20260113'


def load_session_data():
    """Load raw session data from both sources."""
    print("Loading session data from both sources...")

    # Load BigQuery (Direct) sessions
    bq_client = bigquery.Client(project="376132452327")
    bq_query = f"""
    SELECT
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
        MIN(event_timestamp) as session_start_ts,
        ANY_VALUE(device.category) as device_category,
        ANY_VALUE(device.operating_system) as device_operating_system,
        ANY_VALUE(device.web_info.browser) as device_browser,
        ANY_VALUE(geo.country) as geo_country,
        COUNT(*) as event_count
    FROM `analytics_375839889.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{DATE_START}' AND '{DATE_END}'
      AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
    GROUP BY 1
    HAVING ga_session_id IS NOT NULL
    """

    bq_df = bq_client.query(bq_query).to_dataframe()
    bq_df = bq_df.drop_duplicates(subset='ga_session_id', keep='first')
    bq_df['session_start_ts'] = pd.to_numeric(bq_df['session_start_ts'])
    print(f"  BigQuery: {len(bq_df):,} sessions")

    # Load Athena (SST) sessions
    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

    year = DATE_START[:4]
    month = DATE_START[4:6]
    day_start = DATE_START[6:8]
    day_end = DATE_END[6:8]

    athena_query = f"""
    SELECT
        ga_session_id,
        MIN(CAST(to_unixtime(from_iso8601_timestamp(timestamp)) AS BIGINT) * 1000000) as session_start_ts,
        ARBITRARY(device_category) as device_category,
        ARBITRARY(device_operating_system) as device_operating_system,
        ARBITRARY(device_browser) as device_browser,
        ARBITRARY(geo_country) as geo_country,
        COUNT(*) as event_count
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
        'device_browser', 'geo_country', 'event_count'
    ])
    sst_df = sst_df.drop_duplicates(subset='ga_session_id', keep='first')
    sst_df['session_start_ts'] = pd.to_numeric(sst_df['session_start_ts'])
    print(f"  Athena: {len(sst_df):,} sessions")

    return sst_df, bq_df


def run_matching_with_window(sst_df, bq_df, time_window_seconds):
    """Run matching with a specific time window."""
    bq_df = bq_df.copy()
    sst_df = sst_df.copy()

    bq_df['matched_to_sst'] = False
    time_window_micros = time_window_seconds * 1000000

    match_count = 0

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
        best_match_idx = candidates['ts_diff'].idxmin()

        bq_df.loc[best_match_idx, 'matched_to_sst'] = True
        match_count += 1

    sst_only = len(sst_df) - match_count
    direct_only = len(bq_df) - match_count
    total = match_count + sst_only + direct_only

    if total == 0:
        return {
            'window_seconds': time_window_seconds,
            'both': 0, 'sst_only': 0, 'direct_only': 0,
            'both_pct': 0, 'sst_only_pct': 0, 'direct_only_pct': 0
        }

    return {
        'window_seconds': time_window_seconds,
        'both': match_count,
        'sst_only': sst_only,
        'direct_only': direct_only,
        'both_pct': match_count / total * 100,
        'sst_only_pct': sst_only / total * 100,
        'direct_only_pct': direct_only / total * 100
    }


def main():
    print("=" * 60)
    print("SENSITIVITY ANALYSIS: Time Window Impact")
    print("=" * 60)

    sst_df, bq_df = load_session_data()

    # Test different time windows
    windows = [30, 60, 120, 180, 300, 600]  # seconds
    results = []

    print("\nTesting time windows...")
    for window in windows:
        print(f"\n  Window: ±{window}s ({window/60:.1f} min)")
        result = run_matching_with_window(sst_df, bq_df, window)
        results.append(result)
        print(f"    Both: {result['both']:,} ({result['both_pct']:.1f}%)")
        print(f"    SST-only: {result['sst_only']:,} ({result['sst_only_pct']:.1f}%)")
        print(f"    Direct-only: {result['direct_only']:,} ({result['direct_only_pct']:.1f}%)")

    # Create results dataframe
    results_df = pd.DataFrame(results)

    # Calculate stability metrics
    print("\n" + "=" * 60)
    print("STABILITY ANALYSIS")
    print("=" * 60)

    # Check variance across windows
    both_std = results_df['both_pct'].std()
    sst_only_std = results_df['sst_only_pct'].std()
    direct_only_std = results_df['direct_only_pct'].std()

    print(f"\nStandard deviation of match rates across windows:")
    print(f"  Both %:        {both_std:.2f}% (lower = more stable)")
    print(f"  SST-only %:    {sst_only_std:.2f}%")
    print(f"  Direct-only %: {direct_only_std:.2f}%")

    # Check if 5-minute window (300s) is reasonable
    baseline = results_df[results_df['window_seconds'] == 300].iloc[0]
    print(f"\nBaseline (5-min window):")
    print(f"  Both: {baseline['both_pct']:.1f}%")
    print(f"  SST-only: {baseline['sst_only_pct']:.1f}%")
    print(f"  Direct-only: {baseline['direct_only_pct']:.1f}%")

    # Check for asymptotic behavior
    print("\nChange from previous window:")
    for i in range(1, len(results)):
        delta_both = results[i]['both'] - results[i-1]['both']
        delta_pct = delta_both / results[i-1]['both'] * 100
        print(f"  {results[i-1]['window_seconds']}s → {results[i]['window_seconds']}s: "
              f"+{delta_both} matches (+{delta_pct:.1f}%)")

    # Plot
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # Left plot: absolute counts
    ax1 = axes[0]
    window_labels = [f"±{w}s" for w in results_df['window_seconds']]
    x = np.arange(len(window_labels))
    width = 0.25

    ax1.bar(x - width, results_df['both'], width, label='Both', color='#9b59b6')
    ax1.bar(x, results_df['sst_only'], width, label='SST-only', color='#2ecc71')
    ax1.bar(x + width, results_df['direct_only'], width, label='Direct-only', color='#3498db')

    ax1.set_xlabel('Time Window')
    ax1.set_ylabel('Session Count')
    ax1.set_title('Session Counts by Time Window')
    ax1.set_xticks(x)
    ax1.set_xticklabels(window_labels)
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)

    # Right plot: percentages (line chart for trend)
    ax2 = axes[1]
    ax2.plot(results_df['window_seconds'], results_df['both_pct'],
             'o-', label='Both', color='#9b59b6', linewidth=2, markersize=8)
    ax2.plot(results_df['window_seconds'], results_df['sst_only_pct'],
             's-', label='SST-only', color='#2ecc71', linewidth=2, markersize=8)
    ax2.plot(results_df['window_seconds'], results_df['direct_only_pct'],
             '^-', label='Direct-only', color='#3498db', linewidth=2, markersize=8)

    ax2.set_xlabel('Time Window (seconds)')
    ax2.set_ylabel('Percentage of Total Sessions')
    ax2.set_title('Match Rate Stability Across Time Windows')
    ax2.legend()
    ax2.grid(alpha=0.3)
    ax2.axvline(x=300, color='gray', linestyle='--', alpha=0.5, label='Current (5 min)')

    plt.tight_layout()
    plt.savefig('docs/sensitivity_analysis.png', dpi=150, bbox_inches='tight')
    print(f"\nPlot saved to docs/sensitivity_analysis.png")

    # Save results to CSV
    results_df.to_csv('docs/sensitivity_analysis_results.csv', index=False)
    print(f"Results saved to docs/sensitivity_analysis_results.csv")

    # Verdict
    print("\n" + "=" * 60)
    print("VERDICT")
    print("=" * 60)

    if both_std < 2.0:
        print("STABLE: Match rates are consistent across time windows.")
        print("        This indicates the matching methodology is robust.")
    elif both_std < 5.0:
        print("MODERATE: Some variation across windows.")
        print("          Consider investigating sessions near window boundaries.")
    else:
        print("UNSTABLE: High variation across windows.")
        print("          The matching methodology may need refinement.")


if __name__ == "__main__":
    main()
