#!/usr/bin/env python3
"""
Diagnose User-Agent parsing issues causing device/OS misdetections.

Fetches raw User-Agent strings for sessions with classification mismatches
to identify patterns in SAL parsing bugs.
"""

import boto3
from google.cloud import bigquery
import pandas as pd
import time

# Get sessions with significant mismatches (mobile vs desktop)
print("Querying sessions with device category mismatches...")

# BigQuery: Get sessions with their User-Agent and classification
bq_client = bigquery.Client(project="376132452327")
bq_query = """
SELECT
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
    device.category as device_category,
    device.operating_system as device_operating_system,
    device.web_info.browser as device_browser,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location
FROM `analytics_375839889.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260106' AND '20260113'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
  AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
LIMIT 50000
"""
bq_df = bq_client.query(bq_query).to_dataframe()
print(f"BigQuery: {len(bq_df)} events")

# Athena: Get same sessions with User-Agent
print("\nQuerying Athena for User-Agent strings...")
session = boto3.Session(profile_name='warwick')
athena = session.client('athena', region_name='ap-southeast-2')

athena_query = """
SELECT
    ga_session_id,
    device_category,
    device_operating_system,
    device_browser,
    user_agent,
    page_location
FROM warwick_weave_sst_events.sst_events_transformed
WHERE site = 'AU'
  AND year = '2026'
  AND month = '01'
  AND day BETWEEN '06' AND '13'
  AND user_agent IS NOT NULL
LIMIT 50000
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

sst_df = pd.DataFrame(athena_rows, columns=['ga_session_id', 'device_category', 'device_operating_system', 'device_browser', 'user_agent', 'page_location'])
print(f"Athena: {len(sst_df)} events")

# Group by session and take first value (to compare session-level)
bq_sessions = bq_df.groupby('ga_session_id').first().reset_index()
sst_sessions = sst_df.groupby('ga_session_id').first().reset_index()

# Find sessions in both
both = set(bq_sessions['ga_session_id']) & set(sst_sessions['ga_session_id'])
print(f"\n{len(both):,} sessions in both sources")

# Merge to compare
merged = bq_sessions[bq_sessions['ga_session_id'].isin(both)].merge(
    sst_sessions[sst_sessions['ga_session_id'].isin(both)],
    on='ga_session_id',
    suffixes=('_bq', '_sst')
)

# Find mismatches
merged['device_mismatch'] = merged['device_category_bq'] != merged['device_category_sst']
merged['os_mismatch'] = merged['device_operating_system_bq'] != merged['device_operating_system_sst']

print(f"\n=== Mismatch Rates ===")
print(f"Device category mismatch: {merged['device_mismatch'].sum():,} / {len(merged):,} ({merged['device_mismatch'].sum()/len(merged)*100:.2f}%)")
print(f"OS mismatch:              {merged['os_mismatch'].sum():,} / {len(merged):,} ({merged['os_mismatch'].sum()/len(merged)*100:.2f}%)")

# Focus on the serious mismatches: mobile→desktop or desktop→mobile
serious = merged[merged['device_mismatch']].copy()
print(f"\n=== Device Category Mismatches ===")
print(f"Total device mismatches: {len(serious):,}")
print(f"Available columns in merged df: {list(merged.columns)}")

# Count mismatch patterns
serious['mismatch_pattern'] = serious['device_category_bq'] + ' → ' + serious['device_category_sst']
pattern_counts = serious['mismatch_pattern'].value_counts()
print("\nMismatch patterns:")
for pattern, count in pattern_counts.items():
    print(f"  {pattern:20} {count:4,} ({count/len(serious)*100:.1f}%)")

# Show examples of each major pattern
print(f"\n{'='*100}")
print("EXAMPLE USER-AGENTS FOR MAJOR MISMATCH PATTERNS")
print('='*100)

for pattern in pattern_counts.head(5).index:
    print(f"\n{'='*100}")
    print(f"PATTERN: {pattern}")
    print('='*100)

    examples = serious[serious['mismatch_pattern'] == pattern].head(10)

    for idx, row in examples.iterrows():
        print(f"\nSession ID: {row['ga_session_id']}")
        print(f"  BigQuery Detection:  {row['device_category_bq']:8} | {row['device_operating_system_bq'] or '(null)':15} | {row['device_browser_bq'] or '(null)'}")
        print(f"  SST Detection:       {row['device_category_sst']:8} | {row['device_operating_system_sst'] or '(null)':15} | {row['device_browser_sst'] or '(null)'}")
        ua = row.get('user_agent', row.get('user_agent_sst', 'N/A'))
        print(f"  User-Agent: {ua[:150]}")
        if len(ua) > 150:
            print(f"              {ua[150:]}")

# Also check OS mismatches within same device category
print(f"\n{'='*100}")
print("OS MISMATCHES (same device category, different OS)")
print('='*100)

os_only = merged[~merged['device_mismatch'] & merged['os_mismatch']].copy()
os_only['mismatch_pattern'] = os_only['device_operating_system_bq'].fillna('(null)') + ' → ' + os_only['device_operating_system_sst'].fillna('(null)')
os_patterns = os_only['mismatch_pattern'].value_counts()

print(f"\nTotal OS-only mismatches: {len(os_only):,}")
print("\nOS mismatch patterns:")
for pattern, count in os_patterns.head(10).items():
    print(f"  {pattern:30} {count:4,}")

print(f"\n{'='*100}")
print("EXAMPLE USER-AGENTS FOR OS MISMATCHES")
print('='*100)

for pattern in os_patterns.head(3).index:
    print(f"\n{pattern}:")
    examples = os_only[os_only['mismatch_pattern'] == pattern].head(3)
    for idx, row in examples.iterrows():
        ua = row.get('user_agent', row.get('user_agent_sst', 'N/A'))
        print(f"  Session {row['ga_session_id']}: {ua[:120]}")
