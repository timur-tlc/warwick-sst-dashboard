#!/usr/bin/env python3
"""
Verify SST transformation logic against BigQuery dimension values.

This script compares dimension values (device_category, browser, OS, country)
between BigQuery and SST for the same sessions (matched by ga_session_id).

The goal is to verify that our User-Agent parsing and country code mapping
produces the same values as BigQuery, without any information leakage.

NO BigQuery values are used to inform SST transformations - we independently
parse the raw SST data and compare results.
"""

import os
import re
import time
from datetime import datetime

import boto3
import pandas as pd
from google.cloud import bigquery

# Configuration
BQ_PROJECT = "376132452327"
BQ_DATASET = "analytics_375839889"
AWS_PROFILE = "warwick"
ATHENA_DATABASE = "warwick_weave_sst_events"
ATHENA_OUTPUT = "s3://warwick-com-au-events/athena-results/"

# Sample size for verification
SAMPLE_SIZE = 1000


def get_bigquery_sessions(session_ids):
    """Get BigQuery dimension values for specific sessions."""
    print("[2/4] Querying BigQuery for matching sessions...")

    client = bigquery.Client(project=BQ_PROJECT)

    # Take sample to avoid query size limits
    sample_ids = session_ids[:500]
    ids_str = ','.join([str(sid) for sid in sample_ids])

    # Use ga_session_id for matching (accepts ~2% collision rate)
    query = f"""
    SELECT
        ga_session_id,
        ANY_VALUE(device_category) as bq_device_category,
        ANY_VALUE(browser) as bq_browser,
        ANY_VALUE(os) as bq_os,
        ANY_VALUE(country) as bq_country
    FROM (
        SELECT
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as ga_session_id,
            device.category as device_category,
            device.web_info.browser as browser,
            device.operating_system as os,
            geo.country as country
        FROM `{BQ_PROJECT}.{BQ_DATASET}.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '20260115' AND '20260121'
          AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
    )
    WHERE ga_session_id IN ({ids_str})
    GROUP BY ga_session_id
    """

    df = client.query(query).to_dataframe()
    print(f"   Got {len(df)} sessions from BigQuery")
    return df


def run_athena_query(query, session):
    """Execute Athena query and return results with pagination."""
    athena = session.client('athena', region_name='ap-southeast-2')

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )

    query_id = response['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if state != 'SUCCEEDED':
        error_msg = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        raise Exception(f"Query failed: {state} - {error_msg}")

    # Get results with pagination
    columns = None
    data = []
    next_token = None

    while True:
        if next_token:
            results = athena.get_query_results(QueryExecutionId=query_id, NextToken=next_token, MaxResults=1000)
        else:
            results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1000)

        rows = results['ResultSet']['Rows']

        if columns is None:
            columns = [col.get('VarCharValue', '') for col in rows[0]['Data']]
            rows = rows[1:]

        for row in rows:
            data.append([col.get('VarCharValue', '') for col in row['Data']])

        next_token = results.get('NextToken')
        if not next_token:
            break

    return pd.DataFrame(data, columns=columns) if columns else pd.DataFrame()


def get_athena_raw_sessions():
    """Get sample of raw SST sessions with user_agent and country code."""
    print("[1/4] Querying Athena for SST sessions...")

    session = boto3.Session(profile_name=AWS_PROFILE)

    # Use ga_session_id for matching (accepts ~2% collision rate)
    # Use ARBITRARY to pick one user_agent per session
    query = f"""
    SELECT
        ga_session_id,
        ARBITRARY(user_agent) as user_agent,
        ARBITRARY(geo_country_code) as geo_country_code
    FROM (
        SELECT
            json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ga_session_id') as ga_session_id,
            json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.user_agent') as user_agent,
            json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.event_location.country') as geo_country_code
        FROM warwick_weave_sst_events.events
        WHERE year = '2026' AND month = '01'
          AND day IN ('15', '16', '17', '18', '19', '20', '21')
          AND json_extract_scalar(from_utf8(from_base64(raw_payload)), '$.ga_session_id') IS NOT NULL
          AND json_extract_scalar(from_utf8(from_base64(raw_payload)), '$["x-ga-measurement_id"]') = 'G-Y0RSKRWP87'
    )
    GROUP BY ga_session_id
    LIMIT {SAMPLE_SIZE}
    """

    df = run_athena_query(query, session)
    print(f"   Got {len(df)} sessions from Athena")
    return df


# =============================================================================
# TRANSFORMATION LOGIC (must match athena_transformation_layer.sql exactly)
# =============================================================================

def transform_device_category(user_agent):
    """Transform user_agent to device_category."""
    if not user_agent:
        return 'desktop'

    ua = user_agent

    # Tablets first
    if 'iPad' in ua:
        return 'tablet'
    if 'Android' in ua and 'Mobile' not in ua:
        return 'tablet'

    # Mobile phones
    if 'iPhone' in ua:
        return 'mobile'
    if 'iPod' in ua:
        return 'mobile'
    if 'Android' in ua and 'Mobile' in ua:
        return 'mobile'
    if 'Windows Phone' in ua:
        return 'mobile'
    if 'BlackBerry' in ua:
        return 'mobile'
    if 'Opera Mini' in ua:
        return 'mobile'
    if 'IEMobile' in ua:
        return 'mobile'

    # Desktop (default)
    return 'desktop'


def transform_os(user_agent):
    """Transform user_agent to operating_system."""
    if not user_agent:
        return '(not set)'

    ua = user_agent

    if 'iPhone' in ua or 'iPad' in ua or 'iPod' in ua:
        return 'iOS'
    if 'Android' in ua:
        return 'Android'
    if 'Windows Phone' in ua:
        return 'Windows Phone'
    if 'CrOS' in ua:
        return 'Chrome OS'
    if 'Windows' in ua:
        return 'Windows'
    if 'Macintosh' in ua:
        return 'Macintosh'
    if 'Linux' in ua:
        return 'Linux'

    return '(not set)'


def transform_browser(user_agent):
    """Transform user_agent to browser."""
    if not user_agent:
        return '(not set)'

    ua = user_agent

    # Order matters - check specific browsers before generic patterns
    if 'Edg/' in ua or 'Edge/' in ua:
        return 'Edge'
    if 'OPR/' in ua or 'Opera' in ua:
        return 'Opera'
    if 'SamsungBrowser' in ua:
        return 'Samsung Internet'
    if 'Firefox/' in ua:
        return 'Firefox'
    if 'CriOS' in ua:
        return 'Chrome'
    if 'Chrome/' in ua:
        return 'Chrome'
    if 'Safari/' in ua:
        return 'Safari'
    if 'MSIE' in ua or 'Trident' in ua:
        return 'Internet Explorer'

    # In-app browsers on iOS (Facebook, Instagram, etc.)
    if ('iPhone' in ua or 'iPad' in ua) and 'Mobile/' in ua and 'Safari/' not in ua:
        return 'Safari (in-app)'

    return '(not set)'


COUNTRY_MAP = {
    'AU': 'Australia', 'NZ': 'New Zealand', 'FJ': 'Fiji', 'NC': 'New Caledonia',
    'PG': 'Papua New Guinea', 'CN': 'China', 'HK': 'Hong Kong', 'TW': 'Taiwan',
    'JP': 'Japan', 'KR': 'South Korea', 'IN': 'India', 'PK': 'Pakistan',
    'BD': 'Bangladesh', 'LK': 'Sri Lanka', 'VN': 'Vietnam', 'TH': 'Thailand',
    'MY': 'Malaysia', 'SG': 'Singapore', 'ID': 'Indonesia', 'PH': 'Philippines',
    'GB': 'United Kingdom', 'IE': 'Ireland', 'FR': 'France', 'DE': 'Germany',
    'NL': 'Netherlands', 'BE': 'Belgium', 'CH': 'Switzerland', 'AT': 'Austria',
    'SE': 'Sweden', 'NO': 'Norway', 'DK': 'Denmark', 'FI': 'Finland',
    'ES': 'Spain', 'PT': 'Portugal', 'IT': 'Italy', 'GR': 'Greece',
    'PL': 'Poland', 'CZ': 'Czechia', 'RU': 'Russia', 'UA': 'Ukraine',
    'US': 'United States', 'CA': 'Canada', 'MX': 'Mexico', 'BR': 'Brazil',
    'AR': 'Argentina', 'CL': 'Chile', 'CO': 'Colombia', 'PE': 'Peru',
    'AE': 'United Arab Emirates', 'SA': 'Saudi Arabia', 'IL': 'Israel',
    'TR': 'Turkey', 'ZA': 'South Africa', 'EG': 'Egypt', 'NG': 'Nigeria',
    'KE': 'Kenya', 'ZW': 'Zimbabwe', 'MG': 'Madagascar',
}


def transform_country(geo_country_code):
    """Transform ISO country code to full name."""
    if not geo_country_code:
        return '(not set)'
    return COUNTRY_MAP.get(geo_country_code, geo_country_code)


def apply_transformations(df):
    """Apply all transformations to raw SST data."""
    df = df.copy()
    df['sst_device_category'] = df['user_agent'].apply(transform_device_category)
    df['sst_os'] = df['user_agent'].apply(transform_os)
    df['sst_browser'] = df['user_agent'].apply(transform_browser)
    df['sst_country'] = df['geo_country_code'].apply(transform_country)
    return df


def compare_dimensions(merged_df):
    """Compare dimension values and report mismatches."""
    print("\n[4/4] Comparing dimension values...")
    print("=" * 70)

    dimensions = [
        ('device_category', 'bq_device_category', 'sst_device_category'),
        ('browser', 'bq_browser', 'sst_browser'),
        ('operating_system', 'bq_os', 'sst_os'),
        ('country', 'bq_country', 'sst_country'),
    ]

    for name, bq_col, sst_col in dimensions:
        # Normalize to lowercase for comparison (BigQuery might use different casing)
        bq_vals = merged_df[bq_col].fillna('(not set)').str.lower()
        sst_vals = merged_df[sst_col].fillna('(not set)').str.lower()

        matches = (bq_vals == sst_vals).sum()
        total = len(merged_df)
        match_rate = matches / total * 100 if total > 0 else 0

        print(f"\n{name.upper()}")
        print(f"  Match rate: {matches}/{total} ({match_rate:.1f}%)")

        # Show mismatches
        mismatches = merged_df[bq_vals != sst_vals]
        if len(mismatches) > 0:
            print(f"  Mismatches ({len(mismatches)}):")

            # Group by mismatch pattern
            mismatch_patterns = mismatches.groupby([bq_col, sst_col]).size().reset_index(name='count')
            mismatch_patterns = mismatch_patterns.sort_values('count', ascending=False).head(10)

            for _, row in mismatch_patterns.iterrows():
                print(f"    BigQuery='{row[bq_col]}' vs SST='{row[sst_col]}' ({row['count']} sessions)")

            # Show sample user agents for investigation
            if name in ['device_category', 'browser', 'operating_system']:
                print(f"\n  Sample user agents for mismatches:")
                for _, row in mismatches.head(3).iterrows():
                    print(f"    BQ={row[bq_col]}, SST={row[sst_col]}")
                    print(f"    UA: {row['user_agent'][:100]}...")


def main():
    print("SST Transformation Verification")
    print("=" * 70)
    print(f"Timestamp: {datetime.now()}")
    print(f"Sample size: {SAMPLE_SIZE}")
    print()

    # Get SST sessions first (with raw user_agent)
    sst_df = get_athena_raw_sessions()

    if len(sst_df) == 0:
        print("No sessions found in Athena")
        return

    # Get matching BigQuery sessions using ga_session_id
    session_ids = sst_df['ga_session_id'].astype(int).tolist()
    bq_df = get_bigquery_sessions(session_ids)

    if len(bq_df) == 0:
        print("No matching sessions found in BigQuery")
        return

    # Apply transformations to SST data
    print("\n[3/4] Applying transformations to SST data...")
    sst_df = apply_transformations(sst_df)

    # Merge on ga_session_id
    sst_df['ga_session_id'] = sst_df['ga_session_id'].astype(int)
    bq_df['ga_session_id'] = bq_df['ga_session_id'].astype(int)

    merged_df = pd.merge(
        bq_df,
        sst_df,
        on='ga_session_id',
        how='inner'
    )
    print(f"   Matched {len(merged_df)} sessions between BigQuery and SST")

    if len(merged_df) == 0:
        print("No matching sessions found")
        return

    # Compare dimensions
    compare_dimensions(merged_df)

    # Save mismatches for investigation
    output_dir = os.path.dirname(os.path.abspath(__file__))
    merged_df.to_csv(f"{output_dir}/transformation_verification.csv", index=False)
    print(f"\nFull data saved to: {output_dir}/transformation_verification.csv")


if __name__ == "__main__":
    main()
