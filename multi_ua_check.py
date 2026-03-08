#!/usr/bin/env python3
"""
Check if mismatched sessions have multiple User-Agents within the same session.

This would explain why BigQuery and SST classify the same session differently -
they're looking at different events within the session.
"""

import boto3
import time

# Check specific mismatched sessions
mismatched_sessions = [
    '1767939810',  # BQ: mobile/iOS, SAT: desktop/Windows
    '1767931302',  # BQ: desktop/Windows, SAT: mobile/iOS
    '1767944021',  # BQ: desktop/Linux, SAT: tablet/Android
    '1767962859',  # BQ: mobile/iOS, SAT: tablet/Android
]

session = boto3.Session(profile_name='warwick')
athena = session.client('athena', region_name='ap-southeast-2')

for session_id in mismatched_sessions:
    print(f"\n{'='*100}")
    print(f"Session ID: {session_id}")
    print('='*100)

    # Query all events in this session to see User-Agent variations
    query = f"""
    SELECT
        timestamp,
        event_name,
        user_agent,
        device_category,
        device_operating_system,
        device_browser
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE ga_session_id = '{session_id}'
      AND year = '2026'
      AND month = '01'
    ORDER BY timestamp
    """

    response = athena.start_query_execution(
        QueryString=query,
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
        time.sleep(0.5)

    if state != 'SUCCEEDED':
        print(f"  Query failed: {status}")
        continue

    results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=100)
    rows = results['ResultSet']['Rows'][1:]  # Skip header

    print(f"Total events: {len(rows)}")

    # Check for unique User-Agents
    user_agents = set()
    device_combos = set()

    print("\nEvents in session:")
    for i, row in enumerate(rows, 1):
        fields = [f.get('VarCharValue', '') for f in row['Data']]
        timestamp, event_name, ua, device_cat, device_os, device_browser = fields
        user_agents.add(ua)
        device_combos.add(f"{device_cat}/{device_os}/{device_browser}")

        # Show first 3 and last 3 events
        if i <= 3 or i > len(rows) - 3:
            print(f"  {i:2}. {event_name:20} | {device_cat:8} | {device_os:12} | {device_browser:20}")
            if i <= 3:
                print(f"      UA: {ua[:100]}")

    print(f"\n  Unique User-Agents in session:      {len(user_agents)}")
    print(f"  Unique device/OS/browser combos:    {len(device_combos)}")

    if len(user_agents) > 1:
        print("\n  ⚠️  MULTIPLE USER-AGENTS IN SAME SESSION!")
        print("  This explains the mismatch - BigQuery and SST are using different events.")
        print("\n  All unique User-Agents:")
        for i, ua in enumerate(sorted(user_agents), 1):
            print(f"    {i}. {ua[:120]}")
    else:
        print("\n  ✓ Only ONE User-Agent in session (consistent)")
