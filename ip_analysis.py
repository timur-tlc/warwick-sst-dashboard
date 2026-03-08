#!/usr/bin/env python3
"""
IP Analysis: Classify SST sessions by IP type (business vs residential).

BigQuery/GA4 doesn't store IP addresses, so we use SST IPs and categorize
sessions as "Both" or "SST-only" by matching ga_session_id with BigQuery.
"""

import boto3
import pandas as pd
from google.cloud import bigquery
import requests
import time
from collections import defaultdict
import json

# IPinfo.io free tier: 50k lookups/month
IPINFO_TOKEN = None  # Set to your token for higher limits, or None for anonymous (1k/day)

def get_sst_sessions_with_ip():
    """Get SST sessions with IP addresses."""
    print("Querying Athena for SST sessions with IPs...")

    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

    query = """
    SELECT
        ga_session_id,
        ip_address,
        device_category,
        device_browser,
        geo_country
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE site = 'AU'
      AND year = '2026'
      AND month = '01'
      AND day BETWEEN '15' AND '21'
      AND ip_address IS NOT NULL
      AND ip_address != ''
      AND is_likely_human = true
      AND ga_session_id IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
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
        time.sleep(1)

    if state != 'SUCCEEDED':
        raise Exception(f"Athena query failed: {state}")

    # Paginate results
    rows = []
    next_token = None
    while True:
        if next_token:
            results = athena.get_query_results(QueryExecutionId=query_id, NextToken=next_token, MaxResults=1000)
        else:
            results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1000)

        result_rows = results['ResultSet']['Rows']
        if not next_token:
            result_rows = result_rows[1:]  # Skip header

        for row in result_rows:
            rows.append([field.get('VarCharValue', '') for field in row['Data']])

        next_token = results.get('NextToken')
        if not next_token:
            break

    df = pd.DataFrame(rows, columns=['ga_session_id', 'ip_address', 'device_category', 'device_browser', 'geo_country'])
    print(f"  Got {len(df)} SST sessions with IPs")
    return df


def get_bigquery_session_ids():
    """Get session IDs from BigQuery to identify 'Both' sessions."""
    print("Querying BigQuery for Direct session IDs...")

    bq_client = bigquery.Client(project="376132452327")
    query = """
    SELECT DISTINCT
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id
    FROM `analytics_375839889.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20260115' AND '20260121'
      AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
    HAVING ga_session_id IS NOT NULL
    """

    df = bq_client.query(query).to_dataframe()
    print(f"  Got {len(df)} Direct session IDs")
    return set(df['ga_session_id'].tolist())


def lookup_ip_batch(ips, batch_size=100):
    """Look up IP information using ipinfo.io batch API."""
    print(f"Looking up {len(ips)} unique IPs via ipinfo.io...")

    results = {}
    ip_list = list(ips)

    for i in range(0, len(ip_list), batch_size):
        batch = ip_list[i:i+batch_size]

        # IPinfo.io batch endpoint
        url = "https://ipinfo.io/batch"
        headers = {"Content-Type": "application/json"}
        if IPINFO_TOKEN:
            headers["Authorization"] = f"Bearer {IPINFO_TOKEN}"

        try:
            response = requests.post(url, json=batch, headers=headers, timeout=30)
            if response.status_code == 200:
                batch_results = response.json()
                for ip, info in batch_results.items():
                    if isinstance(info, dict):
                        results[ip] = {
                            'org': info.get('org', 'Unknown'),
                            'asn': info.get('asn', {}).get('asn', 'Unknown') if isinstance(info.get('asn'), dict) else 'Unknown',
                            'type': info.get('asn', {}).get('type', 'Unknown') if isinstance(info.get('asn'), dict) else 'Unknown',
                            'company': info.get('company', {}).get('name', '') if isinstance(info.get('company'), dict) else '',
                            'company_type': info.get('company', {}).get('type', '') if isinstance(info.get('company'), dict) else '',
                        }
            elif response.status_code == 429:
                print(f"  Rate limited at batch {i//batch_size + 1}")
                break
            else:
                print(f"  Error {response.status_code} at batch {i//batch_size + 1}")
        except Exception as e:
            print(f"  Exception at batch {i//batch_size + 1}: {e}")

        # Rate limiting
        time.sleep(0.5)

        if (i // batch_size + 1) % 10 == 0:
            print(f"  Processed {i + len(batch)} IPs...")

    print(f"  Got info for {len(results)} IPs")
    return results


def classify_org(org_string):
    """Classify organization string into broad categories."""
    org_lower = org_string.lower() if org_string else ''

    # ISP patterns (residential)
    isp_patterns = ['telstra', 'optus', 'tpg', 'vodafone', 'aussie broadband', 'iinet',
                    'internode', 'dodo', 'belong', 'exetel', 'superloop', 'pentanet',
                    'vocus', 'amaysim', 'boost mobile', 'kogan', 'mate', 'tangerine']

    # Cloud/hosting patterns
    cloud_patterns = ['amazon', 'aws', 'google', 'microsoft', 'azure', 'digitalocean',
                      'linode', 'vultr', 'cloudflare', 'akamai', 'fastly', 'ovh',
                      'hetzner', 'oracle cloud', 'alibaba cloud']

    # VPN patterns
    vpn_patterns = ['nordvpn', 'expressvpn', 'surfshark', 'cyberghost', 'private internet',
                    'mullvad', 'protonvpn', 'ipvanish', 'purevpn', 'hotspot shield']

    # University/education
    edu_patterns = ['university', 'college', 'school', 'education', 'academic',
                    'unsw', 'usyd', 'unimelb', 'uq.edu', 'anu.edu', 'monash']

    # Government
    gov_patterns = ['government', 'council', 'department of', 'ministry', '.gov.au']

    for pattern in isp_patterns:
        if pattern in org_lower:
            return 'Residential ISP'

    for pattern in cloud_patterns:
        if pattern in org_lower:
            return 'Cloud/Hosting'

    for pattern in vpn_patterns:
        if pattern in org_lower:
            return 'VPN'

    for pattern in edu_patterns:
        if pattern in org_lower:
            return 'Education'

    for pattern in gov_patterns:
        if pattern in org_lower:
            return 'Government'

    # If none of the above, likely business
    if org_string and org_string != 'Unknown':
        return 'Business/Corporate'

    return 'Unknown'


def main():
    # Get SST sessions
    sst_df = get_sst_sessions_with_ip()

    # Get BigQuery session IDs
    bq_session_ids = get_bigquery_session_ids()

    # Categorize SST sessions
    sst_df['category'] = sst_df['ga_session_id'].apply(
        lambda x: 'Both' if x in bq_session_ids else 'SST-only'
    )

    print(f"\nSession categories:")
    print(sst_df['category'].value_counts())

    # Get unique IPs per category
    both_ips = set(sst_df[sst_df['category'] == 'Both']['ip_address'].unique())
    sst_only_ips = set(sst_df[sst_df['category'] == 'SST-only']['ip_address'].unique())

    print(f"\nUnique IPs: Both={len(both_ips)}, SST-only={len(sst_only_ips)}")

    # Sample IPs for lookup (to stay within rate limits)
    # Prioritize SST-only since that's what we're investigating
    sample_both = list(both_ips)[:500]
    sample_sst_only = list(sst_only_ips)[:500]
    all_sample_ips = set(sample_both + sample_sst_only)

    print(f"Sampling {len(all_sample_ips)} IPs for lookup...")

    # Look up IPs
    ip_info = lookup_ip_batch(all_sample_ips)

    # Classify IPs
    ip_classification = {}
    for ip, info in ip_info.items():
        ip_classification[ip] = classify_org(info.get('org', ''))

    # Add classification to dataframe
    sst_df['ip_type'] = sst_df['ip_address'].map(ip_classification).fillna('Not looked up')

    # Analyze by category
    print("\n" + "="*60)
    print("IP TYPE BREAKDOWN BY SESSION CATEGORY")
    print("="*60)

    for category in ['Both', 'SST-only']:
        cat_df = sst_df[sst_df['category'] == category]
        cat_df = cat_df[cat_df['ip_type'] != 'Not looked up']

        print(f"\n{category} sessions ({len(cat_df)} with IP lookup):")
        type_counts = cat_df['ip_type'].value_counts()
        type_pcts = cat_df['ip_type'].value_counts(normalize=True) * 100

        for ip_type in type_counts.index:
            print(f"  {ip_type}: {type_counts[ip_type]} ({type_pcts[ip_type]:.1f}%)")

    # Compare percentages
    print("\n" + "="*60)
    print("COMPARISON: SST-only vs Both")
    print("="*60)

    both_types = sst_df[(sst_df['category'] == 'Both') & (sst_df['ip_type'] != 'Not looked up')]['ip_type'].value_counts(normalize=True) * 100
    sst_only_types = sst_df[(sst_df['category'] == 'SST-only') & (sst_df['ip_type'] != 'Not looked up')]['ip_type'].value_counts(normalize=True) * 100

    all_types = set(both_types.index) | set(sst_only_types.index)

    print(f"\n{'IP Type':<25} {'Both':>10} {'SST-only':>10} {'Diff':>10}")
    print("-" * 55)
    for ip_type in sorted(all_types):
        both_pct = both_types.get(ip_type, 0)
        sst_pct = sst_only_types.get(ip_type, 0)
        diff = sst_pct - both_pct
        diff_str = f"+{diff:.1f}pp" if diff > 0 else f"{diff:.1f}pp"
        print(f"{ip_type:<25} {both_pct:>9.1f}% {sst_pct:>9.1f}% {diff_str:>10}")

    # Save detailed results
    output_file = 'ip_analysis_results.csv'
    sst_df.to_csv(output_file, index=False)
    print(f"\nDetailed results saved to {output_file}")

    # Save IP lookup cache
    with open('ip_lookup_cache.json', 'w') as f:
        json.dump(ip_info, f, indent=2)
    print("IP lookup cache saved to ip_lookup_cache.json")

    # Show top organizations for SST-only
    print("\n" + "="*60)
    print("TOP ORGANIZATIONS IN SST-ONLY SESSIONS")
    print("="*60)

    sst_only_df = sst_df[sst_df['category'] == 'SST-only']
    sst_only_df['org'] = sst_only_df['ip_address'].map(lambda ip: ip_info.get(ip, {}).get('org', 'Unknown'))

    print("\nTop 20 orgs in SST-only sessions:")
    org_counts = sst_only_df['org'].value_counts().head(20)
    for org, count in org_counts.items():
        print(f"  {org}: {count}")


if __name__ == '__main__':
    main()
