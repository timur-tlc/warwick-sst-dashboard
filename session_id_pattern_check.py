#!/usr/bin/env python3
"""
Analyze the pattern of ga_session_id differences between matched pairs.

If they're truly the same sessions, we'd expect:
1. Consecutive or near-consecutive session IDs (differ by 1-2)
2. Consistent timing patterns
3. Strong correlation between ID difference and timestamp difference
"""

import boto3
from google.cloud import bigquery
import pandas as pd
import time

print("="*100)
print("SESSION ID PATTERN ANALYSIS")
print("="*100)

# Get matched pairs (reusing pairwise matching logic)
print("\nQuerying both sources...")

bq_client = bigquery.Client(project="376132452327")
bq_query = """
SELECT
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
    MIN(event_timestamp) as session_start_ts,
    device.category as device_category,
    geo.country as geo_country
FROM `analytics_375839889.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260106' AND '20260113'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
GROUP BY 1, 3, 4
HAVING ga_session_id IS NOT NULL
"""
bq_df = bq_client.query(bq_query).to_dataframe()
bq_df = bq_df.drop_duplicates(subset='ga_session_id', keep='first')

session = boto3.Session(profile_name='warwick')
athena = session.client('athena', region_name='ap-southeast-2')

athena_query = """
SELECT
    ga_session_id,
    MIN(CAST(to_unixtime(from_iso8601_timestamp(timestamp)) AS BIGINT) * 1000000) as session_start_ts,
    ARBITRARY(device_category) as device_category,
    ARBITRARY(geo_country) as geo_country
FROM warwick_weave_sst_events.sst_events_transformed
WHERE site = 'AU'
  AND year = '2026'
  AND month = '01'
  AND day BETWEEN '06' AND '13'
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

sst_df = pd.DataFrame(athena_rows, columns=['ga_session_id', 'session_start_ts', 'device_category', 'geo_country'])
sst_df = sst_df.drop_duplicates(subset='ga_session_id', keep='first')

# Convert to numeric
bq_df['session_start_ts'] = pd.to_numeric(bq_df['session_start_ts'])
sst_df['session_start_ts'] = pd.to_numeric(sst_df['session_start_ts'])
bq_df['ga_session_id_int'] = pd.to_numeric(bq_df['ga_session_id'])
sst_df['ga_session_id_int'] = pd.to_numeric(sst_df['ga_session_id'])

# Categorize
bq_session_ids = set(bq_df['ga_session_id'])
sst_session_ids = set(sst_df['ga_session_id'])
both = bq_session_ids & sst_session_ids
direct_only_ids = bq_session_ids - sst_session_ids
sst_only_ids = sst_session_ids - bq_session_ids

direct_only_df = bq_df[bq_df['ga_session_id'].isin(direct_only_ids)].copy()
sst_only_df = sst_df[sst_df['ga_session_id'].isin(sst_only_ids)].copy()

print(f"SST-only: {len(sst_only_df):,}")
print(f"Direct-only: {len(direct_only_df):,}")

# Find matches within 5 minutes
window_micros = 300 * 1000000
matches = []

for _, sst_row in sst_only_df.iterrows():
    sst_ts = sst_row['session_start_ts']

    candidates = direct_only_df[
        (direct_only_df['session_start_ts'] >= sst_ts - window_micros) &
        (direct_only_df['session_start_ts'] <= sst_ts + window_micros)
    ]

    if len(candidates) > 0:
        for _, direct_row in candidates.iterrows():
            if (sst_row['device_category'] == direct_row['device_category'] and
                sst_row['geo_country'] == direct_row['geo_country']):

                ts_diff = abs(sst_row['session_start_ts'] - direct_row['session_start_ts']) / 1000000
                id_diff = abs(sst_row['ga_session_id_int'] - direct_row['ga_session_id_int'])

                matches.append({
                    'sst_id': sst_row['ga_session_id_int'],
                    'direct_id': direct_row['ga_session_id_int'],
                    'id_diff': id_diff,
                    'ts_diff_seconds': ts_diff,
                    'device': sst_row['device_category'],
                    'country': sst_row['geo_country']
                })
                break

matches_df = pd.DataFrame(matches)

print(f"\nMatched pairs: {len(matches_df):,}")

# Analyze session ID differences
print("\n" + "="*100)
print("SESSION ID DIFFERENCE ANALYSIS")
print("="*100)

print(f"\nSession ID differences:")
print(f"  Mean:   {matches_df['id_diff'].mean():,.1f}")
print(f"  Median: {matches_df['id_diff'].median():,.0f}")
print(f"  Min:    {matches_df['id_diff'].min():,.0f}")
print(f"  Max:    {matches_df['id_diff'].max():,.0f}")

# Distribution of ID differences
print(f"\nDistribution of session ID differences:")
print(f"  Differ by 1:      {len(matches_df[matches_df['id_diff'] == 1]):,} ({len(matches_df[matches_df['id_diff'] == 1])/len(matches_df)*100:.1f}%)")
print(f"  Differ by 2-10:   {len(matches_df[(matches_df['id_diff'] >= 2) & (matches_df['id_diff'] <= 10)]):,} ({len(matches_df[(matches_df['id_diff'] >= 2) & (matches_df['id_diff'] <= 10)])/len(matches_df)*100:.1f}%)")
print(f"  Differ by 11-100: {len(matches_df[(matches_df['id_diff'] >= 11) & (matches_df['id_diff'] <= 100)]):,} ({len(matches_df[(matches_df['id_diff'] >= 11) & (matches_df['id_diff'] <= 100)])/len(matches_df)*100:.1f}%)")
print(f"  Differ by >100:   {len(matches_df[matches_df['id_diff'] > 100]):,} ({len(matches_df[matches_df['id_diff'] > 100])/len(matches_df)*100:.1f}%)")

# Correlation between timestamp difference and ID difference
correlation = matches_df['ts_diff_seconds'].corr(matches_df['id_diff'])
print(f"\nCorrelation between timestamp diff and ID diff: r = {correlation:.3f}")

if correlation > 0.5:
    print("  ✅ Strong positive correlation - session IDs track with time")
    print("  → Suggests ga_session_id is generated from timestamp")
else:
    print("  ⚠️  Weak correlation - ID differences not explained by time alone")

# Show examples of consecutive IDs
consecutive = matches_df[matches_df['id_diff'] <= 2]
print(f"\n{len(consecutive):,} pairs with consecutive IDs (differ by 1-2):")
print("\nExamples:")
for idx, row in consecutive.head(20).iterrows():
    print(f"  SST {int(row['sst_id'])} ↔ Direct {int(row['direct_id'])} | Δ={int(row['id_diff'])} | {row['ts_diff_seconds']:.1f}s | {row['device']}/{row['country']}")

# Check if SST or Direct consistently gets higher/lower IDs
matches_df['sst_higher'] = matches_df['sst_id'] > matches_df['direct_id']
sst_higher_pct = matches_df['sst_higher'].sum() / len(matches_df) * 100

print(f"\n" + "="*100)
print("ID ORDERING PATTERN")
print("="*100)

print(f"\nSST session ID > Direct session ID: {matches_df['sst_higher'].sum():,} / {len(matches_df):,} ({sst_higher_pct:.1f}%)")

if sst_higher_pct > 60:
    print("  → SST consistently gets HIGHER session IDs")
    print("  → Suggests SST events arrive LATER than Direct events")
elif sst_higher_pct < 40:
    print("  → Direct consistently gets HIGHER session IDs")
    print("  → Suggests Direct events arrive LATER than SST events")
else:
    print("  → No consistent ordering (50/50 split)")

print(f"\n" + "="*100)
print("CONCLUSION")
print("="*100)

if correlation > 0.5 and len(consecutive) / len(matches_df) > 0.05:
    print("\n✅ STRONG EVIDENCE these are the SAME sessions with different IDs")
    print("\nEvidence:")
    print(f"  • {len(consecutive)/len(matches_df)*100:.1f}% have consecutive IDs (differ by 1-2)")
    print(f"  • r = {correlation:.3f} correlation between time and ID differences")
    print(f"  • Median timestamp difference: {matches_df['ts_diff_seconds'].median():.1f}s")
    print("\nInterpretation:")
    print("  → ga_session_id is generated from Unix timestamp")
    print("  → SST and Direct fire at nearly same time but get different IDs")
    print("  → These are DUPLICATE sessions, not distinct user populations")
    print("\n⚠️  THIS MEANS THE 'BOTH' CATEGORY IS UNDERCOUNTED!")
else:
    print("\n⚠️  MIXED EVIDENCE")
    print(f"\n  • Only {len(consecutive)/len(matches_df)*100:.1f}% have consecutive IDs")
    print(f"  • Correlation: r = {correlation:.3f}")
    print("\n  → Some matches are likely same sessions, others are coincidental")
