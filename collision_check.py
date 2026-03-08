#!/usr/bin/env python3
"""
Check for ga_session_id collisions between SST and Direct.

ID collision = same ga_session_id value in both sources but:
- Different device_category (desktop vs mobile)
- Different device_operating_system (Windows vs iOS)
- Different geo_country (Australia vs China)
- Session start times >2 hours apart

If collision rate is high (>5%), the categorization is invalid.
"""

import boto3
from google.cloud import bigquery
import pandas as pd
import time

# BigQuery: Get session attributes from Direct
print("Querying BigQuery (Direct)...")
bq_client = bigquery.Client(project="376132452327")
bq_query = """
SELECT
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
    device.category as device_category,
    device.operating_system as device_operating_system,
    geo.country as geo_country,
    MIN(event_timestamp) as session_start_ts,
    COUNT(*) as event_count
FROM `analytics_375839889.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260106' AND '20260113'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
GROUP BY 1, 2, 3, 4
HAVING ga_session_id IS NOT NULL
"""
bq_df = bq_client.query(bq_query).to_dataframe()
print(f"BigQuery: {len(bq_df)} sessions")

# Athena: Get session attributes from SST
print("\nQuerying Athena (SST)...")
session = boto3.Session(profile_name='warwick')
athena = session.client('athena', region_name='ap-southeast-2')

athena_query = """
SELECT
    ga_session_id,
    device_category,
    device_operating_system,
    geo_country,
    MIN(CAST(to_unixtime(from_iso8601_timestamp(timestamp)) AS BIGINT) * 1000000) as session_start_ts,
    COUNT(*) as event_count
FROM warwick_weave_sst_events.sst_events_transformed
WHERE site = 'AU'
  AND year = '2026'
  AND month = '01'
  AND day BETWEEN '06' AND '13'
GROUP BY 1, 2, 3, 4
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
    print(f"Athena query failed: {status}")
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
        rows = rows[1:]  # Skip header

    for row in rows:
        athena_rows.append([field.get('VarCharValue', '') for field in row['Data']])

    next_token = results.get('NextToken')
    if not next_token:
        break

sst_df = pd.DataFrame(athena_rows, columns=['ga_session_id', 'device_category', 'device_operating_system', 'geo_country', 'session_start_ts', 'event_count'])
print(f"Athena: {len(sst_df)} sessions")

# Find overlapping session IDs
bq_sessions = set(bq_df['ga_session_id'])
sst_sessions = set(sst_df['ga_session_id'])
overlap = bq_sessions & sst_sessions

print(f"\n=== Session Categorization ===")
print(f"Both (overlap):   {len(overlap):,}")
print(f"Direct-only:      {len(bq_sessions - sst_sessions):,}")
print(f"SST-only:         {len(sst_sessions - bq_sessions):,}")

# Check for collisions in the overlap
print(f"\n=== Checking for ID Collision ===")
print(f"Examining {len(overlap):,} sessions in 'Both' category...")

# Join on ga_session_id
both_bq = bq_df[bq_df['ga_session_id'].isin(overlap)].copy()
both_sst = sst_df[sst_df['ga_session_id'].isin(overlap)].copy()

# Merge to compare attributes
merged = both_bq.merge(both_sst, on='ga_session_id', suffixes=('_bq', '_sst'))

# Convert timestamps to numeric for comparison
merged['session_start_ts_bq'] = pd.to_numeric(merged['session_start_ts_bq'], errors='coerce')
merged['session_start_ts_sst'] = pd.to_numeric(merged['session_start_ts_sst'], errors='coerce')

# Calculate timestamp difference in hours
merged['ts_diff_hours'] = abs(merged['session_start_ts_bq'] - merged['session_start_ts_sst']) / 1000000 / 3600

# Detect mismatches
merged['device_mismatch'] = merged['device_category_bq'] != merged['device_category_sst']
merged['os_mismatch'] = merged['device_operating_system_bq'] != merged['device_operating_system_sst']
merged['country_mismatch'] = merged['geo_country_bq'] != merged['geo_country_sst']
merged['time_mismatch'] = merged['ts_diff_hours'] > 2  # >2 hours apart = likely different sessions

# Overall collision flag
merged['is_collision'] = (
    merged['device_mismatch'] |
    merged['os_mismatch'] |
    merged['country_mismatch'] |
    merged['time_mismatch']
)

collision_count = merged['is_collision'].sum()
collision_rate = collision_count / len(merged) * 100

print(f"\n=== Results ===")
print(f"Collision rate: {collision_rate:.2f}% ({collision_count:,} / {len(merged):,})")
print(f"\nBreakdown:")
print(f"  Device mismatch:   {merged['device_mismatch'].sum():,} ({merged['device_mismatch'].sum()/len(merged)*100:.2f}%)")
print(f"  OS mismatch:       {merged['os_mismatch'].sum():,} ({merged['os_mismatch'].sum()/len(merged)*100:.2f}%)")
print(f"  Country mismatch:  {merged['country_mismatch'].sum():,} ({merged['country_mismatch'].sum()/len(merged)*100:.2f}%)")
print(f"  Time mismatch:     {merged['time_mismatch'].sum():,} ({merged['time_mismatch'].sum()/len(merged)*100:.2f}%)")

if collision_rate > 5:
    print(f"\n⚠️  HIGH COLLISION RATE: {collision_rate:.2f}% suggests ga_session_id is not reliable for matching")
    print("The 'similar profiles' finding may be an artifact of incorrect matching.")
elif collision_rate > 2:
    print(f"\n⚠️  MODERATE COLLISION: {collision_rate:.2f}% is concerning but may be acceptable")
    print("This matches the ~2% mentioned in CLAUDE.md line 774")
else:
    print(f"\n✅ LOW COLLISION: {collision_rate:.2f}% is within acceptable range")
    print("The session matching is reliable.")

# Show examples of collisions
if collision_count > 0:
    print(f"\n=== Example Collisions ===")
    collisions = merged[merged['is_collision']].head(10)
    for idx, row in collisions.iterrows():
        print(f"\nSession ID: {row['ga_session_id']}")
        device_bq = row['device_category_bq'] or '(null)'
        device_sst = row['device_category_sst'] or '(null)'
        os_bq = row['device_operating_system_bq'] or '(null)'
        os_sst = row['device_operating_system_sst'] or '(null)'
        country_bq = row['geo_country_bq'] or '(null)'
        country_sst = row['geo_country_sst'] or '(null)'
        print(f"  BigQuery:  {device_bq:8} | {os_bq:12} | {country_bq}")
        print(f"  SST:       {device_sst:8} | {os_sst:12} | {country_sst}")
        print(f"  Time diff: {row['ts_diff_hours']:.1f} hours")

# Investigation: Why do we have more rows than expected?
print(f"\n=== Data Quality Check ===")
print(f"Expected rows (overlap size): {len(overlap):,}")
print(f"Actual rows in merged df:     {len(merged):,}")
if len(merged) > len(overlap):
    print(f"⚠️  Duplicate session IDs detected!")
    duplicates_bq = both_bq[both_bq.duplicated(subset=['ga_session_id'], keep=False)]
    duplicates_sst = both_sst[both_sst.duplicated(subset=['ga_session_id'], keep=False)]
    print(f"  Duplicates in BigQuery: {len(duplicates_bq):,}")
    print(f"  Duplicates in SST:      {len(duplicates_sst):,}")
    print("\nThis means some ga_session_ids have MULTIPLE device/OS/country combinations within the SAME source!")
    print("This is NOT ID collision - it's sessions switching devices mid-session or data quality issues.")
