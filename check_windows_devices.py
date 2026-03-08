#!/usr/bin/env python3
"""
Check if Windows sessions have any mobile/tablet devices.
"""

import boto3
from google.cloud import bigquery
import pandas as pd
import time

print("="*100)
print("WINDOWS DEVICE DISTRIBUTION CHECK")
print("="*100)

# Check BigQuery (Direct)
print("\nQuerying BigQuery (Direct)...")
bq_client = bigquery.Client(project="376132452327")
bq_query = """
SELECT
    device.category as device_category,
    device.operating_system as device_operating_system,
    COUNT(*) as session_count
FROM `analytics_375839889.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20260106' AND '20260113'
  AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
  AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
GROUP BY 1, 2
ORDER BY 2, 1
"""
bq_df = bq_client.query(bq_query).to_dataframe()

print("\nBigQuery - All OS/Device combinations:")
print(bq_df.to_string())

# Filter to Windows
windows_bq = bq_df[bq_df['device_operating_system'] == 'Windows']
print("\n" + "="*100)
print("BigQuery - WINDOWS sessions by device category:")
print("="*100)
if len(windows_bq) > 0:
    print(windows_bq.to_string())

    total_windows = windows_bq['session_count'].astype(int).sum()
    desktop_windows = windows_bq[windows_bq['device_category'] == 'desktop']['session_count'].astype(int).sum()
    mobile_windows = windows_bq[windows_bq['device_category'] == 'mobile']['session_count'].astype(int).sum()
    tablet_windows = windows_bq[windows_bq['device_category'] == 'tablet']['session_count'].astype(int).sum()

    print(f"\nTotal Windows sessions: {total_windows:,}")
    print(f"  Desktop: {desktop_windows:,} ({desktop_windows/total_windows*100:.2f}%)")
    print(f"  Mobile:  {mobile_windows:,} ({mobile_windows/total_windows*100:.2f}%)")
    print(f"  Tablet:  {tablet_windows:,} ({tablet_windows/total_windows*100:.2f}%)")

    if desktop_windows == total_windows:
        print("\n✅ CONFIRMED: 100% of Windows sessions are Desktop")
        print("   This explains why Windows% == Windows+Desktop%")
else:
    print("No Windows sessions found")

# Check Athena (SST)
print("\n" + "="*100)
print("Querying Athena (SST)...")
session = boto3.Session(profile_name='warwick')
athena = session.client('athena', region_name='ap-southeast-2')

athena_query = """
SELECT
    device_category,
    device_operating_system,
    COUNT(*) as session_count
FROM warwick_weave_sst_events.sst_events_transformed
WHERE site = 'AU'
  AND year = '2026'
  AND month = '01'
  AND day BETWEEN '06' AND '13'
GROUP BY 1, 2
ORDER BY 2, 1
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
    print("Athena query failed")
    exit(1)

results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=100)
rows = results['ResultSet']['Rows'][1:]

sst_data = []
for row in rows:
    fields = [field.get('VarCharValue', '') for field in row['Data']]
    sst_data.append(fields)

sst_df = pd.DataFrame(sst_data, columns=['device_category', 'device_operating_system', 'session_count'])

print("\nAthena - All OS/Device combinations:")
print(sst_df.to_string())

# Filter to Windows
windows_sst = sst_df[sst_df['device_operating_system'] == 'Windows']
print("\n" + "="*100)
print("Athena - WINDOWS sessions by device category:")
print("="*100)
if len(windows_sst) > 0:
    print(windows_sst.to_string())

    total_windows = windows_sst['session_count'].astype(int).sum()
    desktop_windows = windows_sst[windows_sst['device_category'] == 'desktop']['session_count'].astype(int).sum()

    mobile_windows = 0
    if len(windows_sst[windows_sst['device_category'] == 'mobile']) > 0:
        mobile_windows = windows_sst[windows_sst['device_category'] == 'mobile']['session_count'].astype(int).sum()

    tablet_windows = 0
    if len(windows_sst[windows_sst['device_category'] == 'tablet']) > 0:
        tablet_windows = windows_sst[windows_sst['device_category'] == 'tablet']['session_count'].astype(int).sum()

    print(f"\nTotal Windows sessions: {total_windows:,}")
    print(f"  Desktop: {desktop_windows:,} ({desktop_windows/total_windows*100:.2f}%)")
    print(f"  Mobile:  {mobile_windows:,} ({mobile_windows/total_windows*100:.2f}%)")
    print(f"  Tablet:  {tablet_windows:,} ({tablet_windows/total_windows*100:.2f}%)")

    if desktop_windows == total_windows:
        print("\n✅ CONFIRMED: 100% of Windows sessions are Desktop")
        print("   This explains why Windows% == Windows+Desktop%")
else:
    print("No Windows sessions found")

print("\n" + "="*100)
print("CONCLUSION")
print("="*100)
print("""
If Windows % == Windows+Desktop %, it means ALL Windows traffic is from desktop/laptop computers.

This is expected because:
1. Windows Phone is a dead platform (discontinued 2017)
2. Windows tablets (Surface) are typically classified as 'desktop' by User-Agent parsing
3. The SAL's device detection doesn't have specific patterns for Windows tablets

This is NOT a bug - it reflects the reality that Windows usage is primarily desktop/laptop machines.

**Recommendation:** The Windows+Desktop metric is redundant. Consider replacing it with:
- "Apple devices" (iOS + Macintosh) to contrast with Windows
- "Mobile" % to show mobile vs desktop split
- "Direct traffic %" to show bookmark/typed URL usage
""")
