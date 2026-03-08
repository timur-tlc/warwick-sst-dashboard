#!/usr/bin/env python3
"""
CORRECTED SESSION CATEGORIZATION

Problem: ga_session_id matching fails because SST and Direct generate different
session IDs for the same browsing session due to timestamp differences.

Solution: Match sessions based on:
- Timestamp proximity (within ±5 minutes)
- Device category match
- Country match
- User fingerprint similarity

This will correctly identify which sessions are truly in both sources vs only one.
"""

import boto3
from google.cloud import bigquery
import pandas as pd
import time
import numpy as np

print("="*100)
print("CORRECTED SESSION CATEGORIZATION USING TIMESTAMP+ATTRIBUTE MATCHING")
print("="*100)

# ============================================================================
# STEP 1: Load session data from both sources
# ============================================================================

print("\nSTEP 1: Loading session data...")
print("-" * 100)

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
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_traffic_source_last_click_medium') as traffic_medium,
    COUNT(*) as event_count,
    COUNT(DISTINCT event_name) as unique_events,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as has_purchase,
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) as has_add_to_cart
FROM `analytics_375839889.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260106' AND '20260113'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
GROUP BY 1, 2, 4, 5, 6, 7, 8, 9, 10
HAVING ga_session_id IS NOT NULL
"""
print("Querying BigQuery (Direct)...")
bq_df = bq_client.query(bq_query).to_dataframe()
print(f"  Loaded: {len(bq_df)} session records")

# Deduplicate on ga_session_id
bq_df = bq_df.drop_duplicates(subset='ga_session_id', keep='first')
bq_df['session_start_ts'] = pd.to_numeric(bq_df['session_start_ts'])
bq_df['source'] = 'Direct'
print(f"  Unique sessions: {len(bq_df):,}")

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
    ARBITRARY(traffic_medium) as traffic_medium,
    COUNT(*) as event_count,
    COUNT(DISTINCT event_name) as unique_events,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) as has_purchase,
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) as has_add_to_cart
FROM warwick_weave_sst_events.sst_events_transformed
WHERE site = 'AU'
  AND year = '2026'
  AND month = '01'
  AND day BETWEEN '06' AND '13'
GROUP BY 1, 2
"""

print("Querying Athena (SST)...")
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
    print("Athena query failed")
    exit(1)

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
    'traffic_source', 'traffic_medium', 'event_count', 'unique_events',
    'has_purchase', 'has_add_to_cart'
])
print(f"  Loaded: {len(sst_df)} session records")

sst_df = sst_df.drop_duplicates(subset='ga_session_id', keep='first')
sst_df['session_start_ts'] = pd.to_numeric(sst_df['session_start_ts'])
sst_df['source'] = 'SST'
print(f"  Unique sessions: {len(sst_df):,}")

# ============================================================================
# STEP 2: Fuzzy matching based on timestamp + attributes
# ============================================================================

print("\n" + "="*100)
print("STEP 2: Fuzzy matching (timestamp + device + country)")
print("-" * 100)

# Matching parameters
TIME_WINDOW_SECONDS = 300  # ±5 minutes

print(f"\nMatching criteria:")
print(f"  • Timestamp within ±{TIME_WINDOW_SECONDS} seconds")
print(f"  • Device category matches")
print(f"  • Country matches")

# Add unique session identifiers
bq_df['session_index'] = range(len(bq_df))
sst_df['session_index'] = range(len(sst_df))

# Track which sessions have been matched
bq_df['matched_to_sst'] = False
sst_df['matched_to_direct'] = False
bq_df['match_type'] = 'Direct-only'
sst_df['match_type'] = 'SST-only'

matches = []
time_window_micros = TIME_WINDOW_SECONDS * 1000000

print(f"\nSearching for matches (this may take a minute)...")

for idx, sst_row in sst_df.iterrows():
    if idx % 500 == 0:
        print(f"  Progress: {idx:,} / {len(sst_df):,} SST sessions processed...")

    sst_ts = sst_row['session_start_ts']

    # Find Direct sessions within time window
    candidates = bq_df[
        (~bq_df['matched_to_sst']) &  # Not already matched
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

    # Take the closest timestamp match
    candidates = candidates.copy()
    candidates['ts_diff'] = abs(candidates['session_start_ts'] - sst_ts)
    best_match = candidates.nsmallest(1, 'ts_diff').iloc[0]

    # Record the match
    matches.append({
        'sst_session_id': sst_row['ga_session_id'],
        'direct_session_id': best_match['ga_session_id'],
        'sst_index': idx,
        'direct_index': best_match['session_index'],
        'ts_diff_seconds': best_match['ts_diff'] / 1000000,
        'device': sst_row['device_category'],
        'country': sst_row['geo_country']
    })

    # Mark as matched
    bq_df.loc[best_match.name, 'matched_to_sst'] = True
    bq_df.loc[best_match.name, 'match_type'] = 'Both'
    sst_df.loc[idx, 'matched_to_direct'] = True
    sst_df.loc[idx, 'match_type'] = 'Both'

print(f"  Complete!")

matches_df = pd.DataFrame(matches)

print(f"\n✅ Found {len(matches_df):,} matched session pairs")
print(f"   Average time difference: {matches_df['ts_diff_seconds'].mean():.1f} seconds")
print(f"   Median time difference:  {matches_df['ts_diff_seconds'].median():.1f} seconds")

# ============================================================================
# STEP 3: Categorize all sessions
# ============================================================================

print("\n" + "="*100)
print("STEP 3: Session categorization (CORRECTED)")
print("-" * 100)

both_count = len(matches_df)
direct_only_count = (~bq_df['matched_to_sst']).sum()
sst_only_count = (~sst_df['matched_to_direct']).sum()
total = both_count + direct_only_count + sst_only_count

print(f"\nCorrected categorization:")
print(f"  Both (captured by SST and Direct): {both_count:6,}  ({both_count/total*100:5.1f}%)")
print(f"  Direct-only (firewalls):            {direct_only_count:6,}  ({direct_only_count/total*100:5.1f}%)")
print(f"  SST-only (ad-blockers):             {sst_only_count:6,}  ({sst_only_count/total*100:5.1f}%)")
print(f"  Total unique sessions:              {total:6,}")

# Compare to old (wrong) categorization
bq_session_ids = set(bq_df['ga_session_id'])
sst_session_ids = set(sst_df['ga_session_id'])
both_old = len(bq_session_ids & sst_session_ids)
direct_only_old = len(bq_session_ids - sst_session_ids)
sst_only_old = len(sst_session_ids - bq_session_ids)
total_old = both_old + direct_only_old + sst_only_old

print(f"\nOLD (incorrect) categorization based on ga_session_id:")
print(f"  Both:        {both_old:6,}  ({both_old/total_old*100:5.1f}%)")
print(f"  Direct-only: {direct_only_old:6,}  ({direct_only_old/total_old*100:5.1f}%)")
print(f"  SST-only:    {sst_only_old:6,}  ({sst_only_old/total_old*100:5.1f}%)")

print(f"\nDifference:")
print(f"  Both:        {both_count - both_old:+6,}  ({(both_count - both_old)/both_old*100:+5.1f}%)")
print(f"  Direct-only: {direct_only_count - direct_only_old:+6,}  ({(direct_only_count - direct_only_old)/direct_only_old*100:+5.1f}%)")
print(f"  SST-only:    {sst_only_count - sst_only_old:+6,}  ({(sst_only_count - sst_only_old)/sst_only_old*100:+5.1f}%)")

# ============================================================================
# STEP 4: Analyze the TRUE "only" groups
# ============================================================================

print("\n" + "="*100)
print("STEP 4: Profile analysis of TRUE Direct-only vs SST-only sessions")
print("-" * 100)

direct_only_sessions = bq_df[~bq_df['matched_to_sst']].copy()
sst_only_sessions = sst_df[~sst_df['matched_to_direct']].copy()
both_sessions_direct = bq_df[bq_df['matched_to_sst']].copy()

# Device analysis
print("\n📱 DEVICE CATEGORY")
print("-" * 100)

def calc_distribution(df, column, total):
    dist = df[column].value_counts()
    result = []
    for value in ['desktop', 'mobile', 'tablet']:
        count = dist.get(value, 0)
        pct = count / total * 100 if total > 0 else 0
        result.append((value, count, pct))
    return result

both_device = calc_distribution(both_sessions_direct, 'device_category', len(both_sessions_direct))
direct_device = calc_distribution(direct_only_sessions, 'device_category', len(direct_only_sessions))
sst_device = calc_distribution(sst_only_sessions, 'device_category', len(sst_only_sessions))

print(f"\n{'Device':>10} | {'Both':>15} | {'Direct-only':>15} | {'SST-only':>15}")
print("-" * 70)
for i, device in enumerate(['desktop', 'mobile', 'tablet']):
    print(f"{device:>10} | {both_device[i][1]:6,} ({both_device[i][2]:5.1f}%) | {direct_device[i][1]:6,} ({direct_device[i][2]:5.1f}%) | {sst_device[i][1]:6,} ({sst_device[i][2]:5.1f}%)")

# OS analysis
print("\n💻 OPERATING SYSTEM")
print("-" * 100)

def calc_os_dist(df, total):
    dist = df['device_operating_system'].value_counts()
    result = []
    for os in ['Windows', 'iOS', 'Macintosh', 'Android', 'Linux']:
        count = dist.get(os, 0)
        pct = count / total * 100 if total > 0 else 0
        result.append((os, count, pct))
    return result

both_os = calc_os_dist(both_sessions_direct, len(both_sessions_direct))
direct_os = calc_os_dist(direct_only_sessions, len(direct_only_sessions))
sst_os = calc_os_dist(sst_only_sessions, len(sst_only_sessions))

print(f"\n{'OS':>12} | {'Both':>15} | {'Direct-only':>15} | {'SST-only':>15}")
print("-" * 75)
for i, os in enumerate(['Windows', 'iOS', 'Macintosh', 'Android', 'Linux']):
    print(f"{os:>12} | {both_os[i][1]:6,} ({both_os[i][2]:5.1f}%) | {direct_os[i][1]:6,} ({direct_os[i][2]:5.1f}%) | {sst_os[i][1]:6,} ({sst_os[i][2]:5.1f}%)")

# Traffic source analysis
print("\n🔗 TRAFFIC SOURCE")
print("-" * 100)

def calc_traffic_dist(df, total):
    # Clean up traffic source
    df_clean = df.copy()
    df_clean['traffic_clean'] = df_clean['traffic_source'].fillna('(direct)')
    df_clean.loc[df_clean['traffic_clean'] == '', 'traffic_clean'] = '(direct)'

    dist = df_clean['traffic_clean'].value_counts()
    result = []
    for source in ['(direct)', 'google', 'bing', '(not set)']:
        count = dist.get(source, 0)
        pct = count / total * 100 if total > 0 else 0
        result.append((source, count, pct))
    # Add "other"
    other = total - sum(r[1] for r in result)
    other_pct = other / total * 100 if total > 0 else 0
    result.append(('other', other, other_pct))
    return result

both_traffic = calc_traffic_dist(both_sessions_direct, len(both_sessions_direct))
direct_traffic = calc_traffic_dist(direct_only_sessions, len(direct_only_sessions))
sst_traffic = calc_traffic_dist(sst_only_sessions, len(sst_only_sessions))

print(f"\n{'Source':>12} | {'Both':>15} | {'Direct-only':>15} | {'SST-only':>15}")
print("-" * 75)
for i, source in enumerate(['(direct)', 'google', 'bing', '(not set)', 'other']):
    print(f"{source:>12} | {both_traffic[i][1]:6,} ({both_traffic[i][2]:5.1f}%) | {direct_traffic[i][1]:6,} ({direct_traffic[i][2]:5.1f}%) | {sst_traffic[i][1]:6,} ({sst_traffic[i][2]:5.1f}%)")

# Conversion analysis
print("\n💰 CONVERSION RATES")
print("-" * 100)

both_purchases = both_sessions_direct['has_purchase'].astype(int).sum()
direct_purchases = direct_only_sessions['has_purchase'].astype(int).sum()
sst_purchases = sst_only_sessions['has_purchase'].astype(int).sum()

both_purchase_rate = both_purchases / len(both_sessions_direct) * 100
direct_purchase_rate = direct_purchases / len(direct_only_sessions) * 100
sst_purchase_rate = sst_purchases / len(sst_only_sessions) * 100

print(f"\n{'Category':>15} | {'Sessions':>8} | {'Purchases':>10} | {'Rate':>10}")
print("-" * 60)
print(f"{'Both':>15} | {len(both_sessions_direct):8,} | {both_purchases:10,} | {both_purchase_rate:9.2f}%")
print(f"{'Direct-only':>15} | {len(direct_only_sessions):8,} | {direct_purchases:10,} | {direct_purchase_rate:9.2f}%")
print(f"{'SST-only':>15} | {len(sst_only_sessions):8,} | {sst_purchases:10,} | {sst_purchase_rate:9.2f}%")

# ============================================================================
# STEP 5: Statistical significance tests
# ============================================================================

print("\n" + "="*100)
print("STEP 5: Statistical significance")
print("-" * 100)

from scipy.stats import chi2_contingency

# Desktop vs mobile comparison
both_desktop = both_device[0][1]
both_mobile = both_device[1][1]
direct_desktop = direct_device[0][1]
direct_mobile = direct_device[1][1]
sst_desktop = sst_device[0][1]
sst_mobile = sst_device[1][1]

# Chi-square test: Both vs Direct-only
contingency_table_bd = [[both_desktop, both_mobile], [direct_desktop, direct_mobile]]
chi2_bd, p_bd, _, _ = chi2_contingency(contingency_table_bd)

# Chi-square test: Both vs SST-only
contingency_table_bs = [[both_desktop, both_mobile], [sst_desktop, sst_mobile]]
chi2_bs, p_bs, _, _ = chi2_contingency(contingency_table_bs)

# Chi-square test: Direct-only vs SST-only
contingency_table_ds = [[direct_desktop, direct_mobile], [sst_desktop, sst_mobile]]
chi2_ds, p_ds, _, _ = chi2_contingency(contingency_table_ds)

print("\nDesktop vs Mobile distribution:")
print(f"  Both vs Direct-only:   χ² = {chi2_bd:6.2f}, p = {p_bd:.4f} {'***' if p_bd < 0.001 else '**' if p_bd < 0.01 else '*' if p_bd < 0.05 else 'ns'}")
print(f"  Both vs SST-only:      χ² = {chi2_bs:6.2f}, p = {p_bs:.4f} {'***' if p_bs < 0.001 else '**' if p_bs < 0.01 else '*' if p_bs < 0.05 else 'ns'}")
print(f"  Direct vs SST-only:    χ² = {chi2_ds:6.2f}, p = {p_ds:.4f} {'***' if p_ds < 0.001 else '**' if p_ds < 0.01 else '*' if p_ds < 0.05 else 'ns'}")

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("\n" + "="*100)
print("FINAL SUMMARY")
print("="*100)

print(f"\n📊 CORRECTED SESSION BREAKDOWN:")
print(f"   Both:        {both_count:6,} sessions ({both_count/total*100:5.1f}%)")
print(f"   Direct-only: {direct_only_count:6,} sessions ({direct_only_count/total*100:5.1f}%)")
print(f"   SST-only:    {sst_only_count:6,} sessions ({sst_only_count/total*100:5.1f}%)")

print(f"\n📈 KEY FINDINGS:")

# Check if Direct-only has higher desktop %
direct_desktop_pct = direct_device[0][2]
both_desktop_pct = both_device[0][2]
desktop_diff = direct_desktop_pct - both_desktop_pct

if desktop_diff > 5:
    print(f"   ✅ Direct-only DOES have more desktop users (+{desktop_diff:.1f}pp)")
    print(f"      → Supports corporate firewall hypothesis")
else:
    print(f"   ❌ Direct-only does NOT have significantly more desktop users ({desktop_diff:+.1f}pp)")
    print(f"      → Corporate hypothesis NOT supported")

# Check Windows %
direct_windows_pct = direct_os[0][2]
both_windows_pct = both_os[0][2]
windows_diff = direct_windows_pct - both_windows_pct

if windows_diff > 5:
    print(f"   ✅ Direct-only DOES have more Windows users (+{windows_diff:.1f}pp)")
    print(f"      → Supports corporate firewall hypothesis")
else:
    print(f"   ❌ Direct-only does NOT have significantly more Windows users ({windows_diff:+.1f}pp)")
    print(f"      → Corporate hypothesis NOT supported")

# Check direct traffic
direct_direct_traffic_pct = direct_traffic[0][2]
both_direct_traffic_pct = both_traffic[0][2]
traffic_diff = direct_direct_traffic_pct - both_direct_traffic_pct

if traffic_diff > 5:
    print(f"   ✅ Direct-only DOES have more direct traffic (+{traffic_diff:.1f}pp)")
    print(f"      → Supports corporate hypothesis (bookmarks/typed URLs)")
else:
    print(f"   ⚠️  Direct-only direct traffic similar to baseline ({traffic_diff:+.1f}pp)")

print(f"\n💡 INTERPRETATION:")
if desktop_diff > 5 and windows_diff > 5:
    print("   Strong evidence for corporate firewall blocking SST")
elif desktop_diff > 3 or windows_diff > 3:
    print("   Moderate evidence for corporate firewall blocking SST")
else:
    print("   Weak evidence for corporate hypothesis - groups look similar")

print("\n" + "="*100)
