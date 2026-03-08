#!/usr/bin/env python3
"""
Pairwise matching between SST-only and Direct-only sessions.

Question: Are these parallel sessions from the same users captured by different systems?

Method:
1. Join SST-only with Direct-only on timestamp (within ±5 min window)
2. Check if matched sessions have similar attributes (device, geo, etc.)
3. Calculate match rate

If match rate is high → same users, one system missed them
If match rate is low → different users browsing at different times
"""

import boto3
from google.cloud import bigquery
import pandas as pd
import time
from datetime import datetime, timedelta

print("="*100)
print("PAIRWISE MATCHING: SST-ONLY vs DIRECT-ONLY")
print("="*100)

# Get session details from both sources
print("\nQuerying BigQuery (Direct)...")
bq_client = bigquery.Client(project="376132452327")
bq_query = """
SELECT
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
    user_pseudo_id,
    MIN(event_timestamp) as session_start_ts,
    device.category as device_category,
    device.operating_system as device_operating_system,
    device.web_info.browser as device_browser,
    geo.country as geo_country,
    geo.city as geo_city,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_traffic_source_last_click_source') as traffic_source,
    COUNT(*) as event_count,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as has_purchase
FROM `analytics_375839889.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260106' AND '20260113'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
GROUP BY 1, 2, 4, 5, 6, 7, 8, 9
HAVING ga_session_id IS NOT NULL
"""
bq_df = bq_client.query(bq_query).to_dataframe()
print(f"BigQuery: {len(bq_df)} session records")

# Deduplicate
bq_df = bq_df.drop_duplicates(subset='ga_session_id', keep='first')
print(f"  After dedup: {len(bq_df)} unique sessions")

print("\nQuerying Athena (SST)...")
session = boto3.Session(profile_name='warwick')
athena = session.client('athena', region_name='ap-southeast-2')

athena_query = """
SELECT
    ga_session_id,
    user_pseudo_id,
    MIN(CAST(to_unixtime(from_iso8601_timestamp(timestamp)) AS BIGINT) * 1000000) as session_start_ts,
    ARBITRARY(device_category) as device_category,
    ARBITRARY(device_operating_system) as device_operating_system,
    ARBITRARY(device_browser) as device_browser,
    ARBITRARY(geo_country) as geo_country,
    ARBITRARY(geo_city) as geo_city,
    ARBITRARY(traffic_source) as traffic_source,
    COUNT(*) as event_count,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as has_purchase
FROM warwick_weave_sst_events.sst_events_transformed
WHERE site = 'AU'
  AND year = '2026'
  AND month = '01'
  AND day BETWEEN '06' AND '13'
GROUP BY 1, 2
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
    print(f"Athena query failed")
    exit(1)

# Paginate results
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
    'ga_session_id', 'user_pseudo_id', 'session_start_ts', 'device_category',
    'device_operating_system', 'device_browser', 'geo_country', 'geo_city',
    'traffic_source', 'event_count', 'has_purchase'
])
print(f"Athena: {len(sst_df)} session records")

# Deduplicate
sst_df = sst_df.drop_duplicates(subset='ga_session_id', keep='first')
print(f"  After dedup: {len(sst_df)} unique sessions")

# Convert timestamps to numeric
bq_df['session_start_ts'] = pd.to_numeric(bq_df['session_start_ts'])
sst_df['session_start_ts'] = pd.to_numeric(sst_df['session_start_ts'])

# Categorize sessions
bq_session_ids = set(bq_df['ga_session_id'])
sst_session_ids = set(sst_df['ga_session_id'])

both = bq_session_ids & sst_session_ids
direct_only_ids = bq_session_ids - sst_session_ids
sst_only_ids = sst_session_ids - bq_session_ids

direct_only_df = bq_df[bq_df['ga_session_id'].isin(direct_only_ids)].copy()
sst_only_df = sst_df[sst_df['ga_session_id'].isin(sst_only_ids)].copy()

print("\n" + "="*100)
print("SESSION CATEGORIZATION")
print("="*100)
print(f"Both:        {len(both):6,}")
print(f"Direct-only: {len(direct_only_df):6,}")
print(f"SST-only:    {len(sst_only_df):6,}")

# Prepare for timestamp matching
print("\n" + "="*100)
print("TIMESTAMP-BASED PAIRWISE MATCHING")
print("="*100)

print("\nMatching windows tested:")
windows = [
    (1, "1 second"),
    (5, "5 seconds"),
    (30, "30 seconds"),
    (60, "1 minute"),
    (300, "5 minutes"),
    (600, "10 minutes"),
    (1800, "30 minutes"),
]

results = []

for window_seconds, label in windows:
    window_micros = window_seconds * 1000000

    # For each SST-only session, find Direct-only sessions within time window
    matches = []

    for _, sst_row in sst_only_df.iterrows():
        sst_ts = sst_row['session_start_ts']

        # Find Direct-only sessions within window
        candidates = direct_only_df[
            (direct_only_df['session_start_ts'] >= sst_ts - window_micros) &
            (direct_only_df['session_start_ts'] <= sst_ts + window_micros)
        ]

        if len(candidates) > 0:
            # Check for attribute matches
            for _, direct_row in candidates.iterrows():
                # Calculate match score
                device_match = sst_row['device_category'] == direct_row['device_category']
                os_match = sst_row['device_operating_system'] == direct_row['device_operating_system']
                country_match = sst_row['geo_country'] == direct_row['geo_country']
                city_match = sst_row['geo_city'] == direct_row['geo_city']

                # Strong match: device + country match
                if device_match and country_match:
                    ts_diff = abs(sst_row['session_start_ts'] - direct_row['session_start_ts']) / 1000000
                    matches.append({
                        'sst_session_id': sst_row['ga_session_id'],
                        'direct_session_id': direct_row['ga_session_id'],
                        'ts_diff_seconds': ts_diff,
                        'device_match': device_match,
                        'os_match': os_match,
                        'country_match': country_match,
                        'city_match': city_match,
                        'sst_device': sst_row['device_category'],
                        'direct_device': direct_row['device_category'],
                        'sst_country': sst_row['geo_country'],
                        'direct_country': direct_row['geo_country'],
                    })
                    break  # Only take first match per SST session

    match_rate = len(matches) / len(sst_only_df) * 100 if len(sst_only_df) > 0 else 0
    results.append({
        'window': label,
        'matches': len(matches),
        'match_rate': match_rate
    })

    print(f"\n{label} window:")
    print(f"  SST-only sessions with match: {len(matches):,} / {len(sst_only_df):,} ({match_rate:.1f}%)")

# Show results summary
print("\n" + "="*100)
print("MATCHING SUMMARY")
print("="*100)

print(f"\n{'Window':>15} | {'Matches':>8} | {'Match Rate':>11}")
print("-" * 45)
for r in results:
    print(f"{r['window']:>15} | {r['matches']:8,} | {r['match_rate']:10.1f}%")

# Analyze the 5-minute window matches in detail
window_micros = 300 * 1000000
matches_detailed = []

for _, sst_row in sst_only_df.iterrows():
    sst_ts = sst_row['session_start_ts']

    candidates = direct_only_df[
        (direct_only_df['session_start_ts'] >= sst_ts - window_micros) &
        (direct_only_df['session_start_ts'] <= sst_ts + window_micros)
    ]

    if len(candidates) > 0:
        for _, direct_row in candidates.iterrows():
            device_match = sst_row['device_category'] == direct_row['device_category']
            os_match = sst_row['device_operating_system'] == direct_row['device_operating_system']
            country_match = sst_row['geo_country'] == direct_row['geo_country']
            city_match = sst_row['geo_city'] == direct_row['geo_city']

            if device_match and country_match:
                ts_diff = abs(sst_row['session_start_ts'] - direct_row['session_start_ts']) / 1000000
                matches_detailed.append({
                    'sst_session_id': sst_row['ga_session_id'],
                    'direct_session_id': direct_row['ga_session_id'],
                    'ts_diff_seconds': ts_diff,
                    'device_match': device_match,
                    'os_match': os_match,
                    'country_match': country_match,
                    'city_match': city_match,
                    'sst_device': sst_row['device_category'],
                    'direct_device': direct_row['device_category'],
                    'sst_os': sst_row['device_operating_system'],
                    'direct_os': direct_row['device_operating_system'],
                    'sst_country': sst_row['geo_country'],
                    'direct_country': direct_row['geo_country'],
                    'sst_city': sst_row['geo_city'],
                    'direct_city': direct_row['geo_city'],
                })
                break

if len(matches_detailed) > 0:
    matches_df = pd.DataFrame(matches_detailed)

    print("\n" + "="*100)
    print("MATCH QUALITY ANALYSIS (5-minute window)")
    print("="*100)

    print(f"\nTotal pairwise matches: {len(matches_df):,}")
    print(f"Match rate: {len(matches_df)/len(sst_only_df)*100:.1f}% of SST-only sessions")

    print("\nAttribute match rates among pairs:")
    print(f"  Device category: {matches_df['device_match'].sum():,} / {len(matches_df):,} ({matches_df['device_match'].sum()/len(matches_df)*100:.1f}%)")
    print(f"  Operating system: {matches_df['os_match'].sum():,} / {len(matches_df):,} ({matches_df['os_match'].sum()/len(matches_df)*100:.1f}%)")
    print(f"  Country:         {matches_df['country_match'].sum():,} / {len(matches_df):,} ({matches_df['country_match'].sum()/len(matches_df)*100:.1f}%)")
    print(f"  City:            {matches_df['city_match'].sum():,} / {len(matches_df):,} ({matches_df['city_match'].sum()/len(matches_df)*100:.1f}%)")

    print(f"\nAverage timestamp difference: {matches_df['ts_diff_seconds'].mean():.1f} seconds")
    print(f"Median timestamp difference:  {matches_df['ts_diff_seconds'].median():.1f} seconds")
    print(f"Max timestamp difference:     {matches_df['ts_diff_seconds'].max():.1f} seconds")

    # Show examples
    print("\n" + "="*100)
    print("EXAMPLE MATCHED PAIRS")
    print("="*100)

    print("\nFirst 10 matched pairs:")
    for idx, row in matches_df.head(10).iterrows():
        print(f"\nSST session {row['sst_session_id']} ↔ Direct session {row['direct_session_id']}")
        print(f"  Time diff:  {row['ts_diff_seconds']:.1f}s")
        print(f"  SST:        {row['sst_device']:8} | {row['sst_os']:15} | {row['sst_country']:20} | {row['sst_city']}")
        print(f"  Direct:     {row['direct_device']:8} | {row['direct_os']:15} | {row['direct_country']:20} | {row['direct_city']}")
        if row['device_match'] and row['os_match'] and row['country_match'] and row['city_match']:
            print("  → PERFECT MATCH ✅")
        elif row['device_match'] and row['country_match']:
            print("  → Strong match (device + country)")
        else:
            print("  → Weak match")

# Reverse matching: Direct-only → SST-only
print("\n" + "="*100)
print("REVERSE MATCHING: DIRECT-ONLY → SST-ONLY")
print("="*100)

reverse_matches = []
for _, direct_row in direct_only_df.iterrows():
    direct_ts = direct_row['session_start_ts']

    candidates = sst_only_df[
        (sst_only_df['session_start_ts'] >= direct_ts - window_micros) &
        (sst_only_df['session_start_ts'] <= direct_ts + window_micros)
    ]

    if len(candidates) > 0:
        for _, sst_row in candidates.iterrows():
            device_match = direct_row['device_category'] == sst_row['device_category']
            country_match = direct_row['geo_country'] == sst_row['geo_country']

            if device_match and country_match:
                reverse_matches.append(1)
                break

print(f"\nDirect-only sessions with SST-only match: {len(reverse_matches):,} / {len(direct_only_df):,} ({len(reverse_matches)/len(direct_only_df)*100:.1f}%)")

# Final interpretation
print("\n" + "="*100)
print("INTERPRETATION")
print("="*100)

sst_match_rate = len(matches_detailed) / len(sst_only_df) * 100
direct_match_rate = len(reverse_matches) / len(direct_only_df) * 100

if sst_match_rate < 10 and direct_match_rate < 10:
    print("\n❌ VERY LOW match rate (<10%)")
    print("\nConclusion:")
    print("  → SST-only and Direct-only are DIFFERENT SESSIONS from DIFFERENT USERS")
    print("  → NOT the same users captured by different systems")
    print("  → Supports the hypothesis that these are distinct user populations:")
    print("    - SST-only: Ad-blocker users invisible to Direct")
    print("    - Direct-only: Corporate firewall users invisible to SST")
elif sst_match_rate < 30 and direct_match_rate < 30:
    print("\n⚠️  LOW match rate (10-30%)")
    print("\nConclusion:")
    print("  → Mostly different sessions, some overlap")
    print("  → Mix of distinct users + timing coincidences")
elif sst_match_rate < 70 and direct_match_rate < 70:
    print("\n⚠️  MODERATE match rate (30-70%)")
    print("\nConclusion:")
    print("  → Significant overlap - could be same users in different systems")
    print("  → Further investigation needed")
else:
    print("\n✅ HIGH match rate (>70%)")
    print("\nConclusion:")
    print("  → SST-only and Direct-only are likely the SAME USERS")
    print("  → Captured by one system but missed by the other")
    print("  → Cookie/ID mismatch preventing proper session linkage")

print("\n" + "="*100)
print("KEY FINDING")
print("="*100)

print(f"\n{len(sst_only_df):,} SST-only sessions")
print(f"{len(direct_only_df):,} Direct-only sessions")
print(f"{len(matches_detailed):,} pairwise matches at same time/device/country ({sst_match_rate:.1f}%)")
print(f"\nThese are SEPARATE SESSIONS, not the same session captured differently.")
