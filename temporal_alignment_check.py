#!/usr/bin/env python3
"""
Analyze temporal alignment between SST-only, Direct-only, and Both sessions.

Questions:
1. Do SST-only and Direct-only sessions happen at the same times of day?
2. Are there hourly/daily patterns that distinguish the groups?
3. Is there temporal clustering that suggests behavioral differences?
"""

import boto3
from google.cloud import bigquery
import pandas as pd
import time
from datetime import datetime

print("="*100)
print("TEMPORAL ALIGNMENT ANALYSIS")
print("="*100)

# Get session timestamps from both sources
print("\nQuerying BigQuery (Direct)...")
bq_client = bigquery.Client(project="376132452327")
bq_query = """
SELECT
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
    MIN(event_timestamp) as session_start_ts,
    EXTRACT(HOUR FROM TIMESTAMP_MICROS(MIN(event_timestamp))) as hour_of_day,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP_MICROS(MIN(event_timestamp))) as day_of_week,
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_MICROS(MIN(event_timestamp))) as date
FROM `analytics_375839889.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260106' AND '20260113'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
GROUP BY 1
HAVING ga_session_id IS NOT NULL
"""
bq_df = bq_client.query(bq_query).to_dataframe()
print(f"BigQuery: {len(bq_df)} sessions")

# Deduplicate on ga_session_id (take first if multiple user_pseudo_ids)
bq_df = bq_df.drop_duplicates(subset='ga_session_id', keep='first')
print(f"  After dedup: {len(bq_df)} unique sessions")

print("\nQuerying Athena (SST)...")
session = boto3.Session(profile_name='warwick')
athena = session.client('athena', region_name='ap-southeast-2')

athena_query = """
SELECT
    ga_session_id,
    MIN(from_iso8601_timestamp(timestamp)) as session_start_ts,
    HOUR(MIN(from_iso8601_timestamp(timestamp))) as hour_of_day,
    DAY_OF_WEEK(MIN(from_iso8601_timestamp(timestamp))) as day_of_week,
    CAST(DATE(MIN(from_iso8601_timestamp(timestamp))) AS VARCHAR) as date
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

sst_df = pd.DataFrame(athena_rows, columns=['ga_session_id', 'session_start_ts', 'hour_of_day', 'day_of_week', 'date'])
print(f"Athena: {len(sst_df)} sessions")

# Deduplicate on ga_session_id
sst_df = sst_df.drop_duplicates(subset='ga_session_id', keep='first')
print(f"  After dedup: {len(sst_df)} unique sessions")

# Categorize sessions
bq_session_ids = set(bq_df['ga_session_id'])
sst_session_ids = set(sst_df['ga_session_id'])

both = bq_session_ids & sst_session_ids
direct_only = bq_session_ids - sst_session_ids
sst_only = sst_session_ids - bq_session_ids

print("\n" + "="*100)
print("SESSION CATEGORIZATION")
print("="*100)
print(f"Both:        {len(both):6,}  ({len(both)/(len(both)+len(direct_only)+len(sst_only))*100:5.1f}%)")
print(f"Direct-only: {len(direct_only):6,}  ({len(direct_only)/(len(both)+len(direct_only)+len(sst_only))*100:5.1f}%)")
print(f"SST-only:    {len(sst_only):6,}  ({len(sst_only)/(len(both)+len(direct_only)+len(sst_only))*100:5.1f}%)")

# Add category labels
bq_df['category'] = bq_df['ga_session_id'].apply(
    lambda x: 'Both' if x in both else 'Direct-only'
)
sst_df['category'] = sst_df['ga_session_id'].apply(
    lambda x: 'Both' if x in both else 'SST-only'
)

# Combine dataframes
bq_df['hour_of_day'] = pd.to_numeric(bq_df['hour_of_day'])
bq_df['day_of_week'] = pd.to_numeric(bq_df['day_of_week'])
sst_df['hour_of_day'] = pd.to_numeric(sst_df['hour_of_day'])
sst_df['day_of_week'] = pd.to_numeric(sst_df['day_of_week'])

# Create analysis dataframes for each category
both_bq = bq_df[bq_df['category'] == 'Both']
direct_only_df = bq_df[bq_df['category'] == 'Direct-only']
sst_only_df = sst_df[sst_df['category'] == 'SST-only']

# Hour of day analysis
print("\n" + "="*100)
print("HOUR OF DAY DISTRIBUTION")
print("="*100)

print("\nSessions by hour (UTC):")
print(f"{'Hour':>5} | {'Both':>7} | {'Direct':>7} | {'SST':>7} | {'Both %':>7} | {'Direct %':>9} | {'SST %':>7}")
print("-" * 80)

for hour in range(24):
    both_count = len(both_bq[both_bq['hour_of_day'] == hour])
    direct_count = len(direct_only_df[direct_only_df['hour_of_day'] == hour])
    sst_count = len(sst_only_df[sst_only_df['hour_of_day'] == hour])

    both_pct = both_count / len(both_bq) * 100 if len(both_bq) > 0 else 0
    direct_pct = direct_count / len(direct_only_df) * 100 if len(direct_only_df) > 0 else 0
    sst_pct = sst_count / len(sst_only_df) * 100 if len(sst_only_df) > 0 else 0

    print(f"{hour:5d} | {both_count:7,} | {direct_count:7,} | {sst_count:7,} | {both_pct:6.2f}% | {direct_pct:8.2f}% | {sst_pct:6.2f}%")

# Statistical similarity test
print("\n" + "="*100)
print("TEMPORAL SIMILARITY ANALYSIS")
print("="*100)

# Compare hourly distributions
both_hourly = both_bq.groupby('hour_of_day').size()
direct_hourly = direct_only_df.groupby('hour_of_day').size()
sst_hourly = sst_only_df.groupby('hour_of_day').size()

# Normalize to percentages
both_hourly_pct = (both_hourly / both_hourly.sum() * 100)
direct_hourly_pct = (direct_hourly / direct_hourly.sum() * 100)
sst_hourly_pct = (sst_hourly / sst_hourly.sum() * 100)

# Calculate correlation between distributions
from scipy.stats import pearsonr

# Ensure all hours are present (0-23)
all_hours = range(24)
both_dist = [both_hourly_pct.get(h, 0) for h in all_hours]
direct_dist = [direct_hourly_pct.get(h, 0) for h in all_hours]
sst_dist = [sst_hourly_pct.get(h, 0) for h in all_hours]

corr_both_direct, p_both_direct = pearsonr(both_dist, direct_dist)
corr_both_sst, p_both_sst = pearsonr(both_dist, sst_dist)
corr_direct_sst, p_direct_sst = pearsonr(direct_dist, sst_dist)

print("\nPearson correlation of hourly distributions:")
print(f"  Both vs Direct-only:  r = {corr_both_direct:.3f}  (p = {p_both_direct:.4f})")
print(f"  Both vs SST-only:     r = {corr_both_sst:.3f}  (p = {p_both_sst:.4f})")
print(f"  Direct vs SST-only:   r = {corr_direct_sst:.3f}  (p = {p_direct_sst:.4f})")

if corr_direct_sst > 0.9:
    print("\n✅ STRONG temporal alignment (r > 0.9)")
    print("   SST-only and Direct-only sessions happen at similar times")
    print("   → Suggests TECHNICAL BLOCKING, not behavioral differences")
elif corr_direct_sst > 0.7:
    print("\n⚠️  MODERATE temporal alignment (0.7 < r < 0.9)")
    print("   Some temporal patterns differ between groups")
    print("   → Mix of technical blocking and behavioral differences")
else:
    print("\n❌ WEAK temporal alignment (r < 0.7)")
    print("   SST-only and Direct-only sessions happen at different times")
    print("   → Suggests BEHAVIORAL DIFFERENCES, not just technical blocking")

# Day of week analysis
print("\n" + "="*100)
print("DAY OF WEEK DISTRIBUTION")
print("="*100)

day_names = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 5: 'Thursday', 6: 'Friday', 7: 'Saturday'}

print("\nSessions by day of week:")
print(f"{'Day':>10} | {'Both':>7} | {'Direct':>7} | {'SST':>7} | {'Both %':>7} | {'Direct %':>9} | {'SST %':>7}")
print("-" * 80)

for day in sorted(day_names.keys()):
    both_count = len(both_bq[both_bq['day_of_week'] == day])
    direct_count = len(direct_only_df[direct_only_df['day_of_week'] == day])
    sst_count = len(sst_only_df[sst_only_df['day_of_week'] == day])

    both_pct = both_count / len(both_bq) * 100 if len(both_bq) > 0 else 0
    direct_pct = direct_count / len(direct_only_df) * 100 if len(direct_only_df) > 0 else 0
    sst_pct = sst_count / len(sst_only_df) * 100 if len(sst_only_df) > 0 else 0

    print(f"{day_names[day]:>10} | {both_count:7,} | {direct_count:7,} | {sst_count:7,} | {both_pct:6.2f}% | {direct_pct:8.2f}% | {sst_pct:6.2f}%")

# Weekday vs Weekend
print("\n" + "="*100)
print("WEEKDAY vs WEEKEND")
print("="*100)

both_weekday = len(both_bq[both_bq['day_of_week'].isin([2,3,4,5,6])])
both_weekend = len(both_bq[both_bq['day_of_week'].isin([1,7])])
direct_weekday = len(direct_only_df[direct_only_df['day_of_week'].isin([2,3,4,5,6])])
direct_weekend = len(direct_only_df[direct_only_df['day_of_week'].isin([1,7])])
sst_weekday = len(sst_only_df[sst_only_df['day_of_week'].isin([2,3,4,5,6])])
sst_weekend = len(sst_only_df[sst_only_df['day_of_week'].isin([1,7])])

print(f"\n{'Category':>12} | {'Weekday':>8} | {'Weekend':>8} | {'Weekday %':>10}")
print("-" * 50)
print(f"{'Both':>12} | {both_weekday:8,} | {both_weekend:8,} | {both_weekday/(both_weekday+both_weekend)*100:9.1f}%")
print(f"{'Direct-only':>12} | {direct_weekday:8,} | {direct_weekend:8,} | {direct_weekday/(direct_weekday+direct_weekend)*100:9.1f}%")
print(f"{'SST-only':>12} | {sst_weekday:8,} | {sst_weekend:8,} | {sst_weekday/(sst_weekday+sst_weekend)*100:9.1f}%")

weekday_diff = (direct_weekday/(direct_weekday+direct_weekend)*100) - (sst_weekday/(sst_weekday+sst_weekend)*100)
if abs(weekday_diff) > 5:
    print(f"\n⚠️  Weekday difference: {weekday_diff:+.1f} percentage points")
    if weekday_diff > 0:
        print("   Direct-only sessions skew more toward weekdays")
        print("   → Suggests corporate/work-hour blocking")
    else:
        print("   SST-only sessions skew more toward weekdays")
else:
    print(f"\n✅ Similar weekday/weekend distribution (diff: {weekday_diff:+.1f}pp)")

# Peak hour analysis
print("\n" + "="*100)
print("PEAK HOURS")
print("="*100)

both_peak = both_hourly_pct.nlargest(3)
direct_peak = direct_hourly_pct.nlargest(3)
sst_peak = sst_hourly_pct.nlargest(3)

print("\nTop 3 peak hours for each category:")
print(f"\nBoth:")
for hour, pct in both_peak.items():
    print(f"  {int(hour):02d}:00 - {pct:.1f}%")

print(f"\nDirect-only:")
for hour, pct in direct_peak.items():
    print(f"  {int(hour):02d}:00 - {pct:.1f}%")

print(f"\nSST-only:")
for hour, pct in sst_peak.items():
    print(f"  {int(hour):02d}:00 - {pct:.1f}%")

# Check if peak hours overlap
both_peak_hours = set(both_peak.index)
direct_peak_hours = set(direct_peak.index)
sst_peak_hours = set(sst_peak.index)

overlap = direct_peak_hours & sst_peak_hours
print(f"\nPeak hour overlap between Direct-only and SST-only: {len(overlap)}/3 hours")
if len(overlap) >= 2:
    print("✅ Peak hours align - groups happen at same times")
else:
    print("⚠️  Peak hours differ - groups have different temporal patterns")

print("\n" + "="*100)
print("CONCLUSION")
print("="*100)

if corr_direct_sst > 0.9 and abs(weekday_diff) < 5:
    print("\n✅ STRONG temporal alignment between SST-only and Direct-only")
    print("\nEvidence:")
    print(f"  • High hourly correlation (r = {corr_direct_sst:.3f})")
    print(f"  • Similar weekday/weekend split (Δ = {weekday_diff:+.1f}pp)")
    print(f"  • {len(overlap)}/3 peak hours overlap")
    print("\nInterpretation:")
    print("  → SST-only and Direct-only sessions occur at the SAME TIMES")
    print("  → This supports TECHNICAL BLOCKING hypothesis (ad-blockers, firewalls)")
    print("  → NOT behavioral differences (different user types browsing at different times)")
else:
    print("\n⚠️  MIXED temporal alignment between SST-only and Direct-only")
    print("\nEvidence:")
    print(f"  • Hourly correlation: r = {corr_direct_sst:.3f}")
    print(f"  • Weekday/weekend difference: {weekday_diff:+.1f}pp")
    print(f"  • {len(overlap)}/3 peak hours overlap")
    print("\nInterpretation:")
    print("  → Some temporal differences exist")
    print("  → Mix of technical blocking AND behavioral patterns")
