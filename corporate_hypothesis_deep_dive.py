#!/usr/bin/env python3
"""
Deep dive into corporate user hypothesis for Direct-only sessions.

Analyzes multiple signals beyond just business hours:
- Day of week patterns
- Screen resolution (corporate monitors vs consumer)
- Page behavior (product specs vs casual browsing)
- Session characteristics
"""

import boto3
import pandas as pd
from google.cloud import bigquery
import time

def query_athena(query, description=""):
    """Run Athena query and return DataFrame."""
    print(f"Athena: {description}...")

    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

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
        time.sleep(1)

    if state != 'SUCCEEDED':
        error = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        raise Exception(f"Query failed: {error}")

    rows = []
    next_token = None
    while True:
        if next_token:
            results = athena.get_query_results(QueryExecutionId=query_id, NextToken=next_token, MaxResults=1000)
        else:
            results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1000)

        result_rows = results['ResultSet']['Rows']
        if not next_token:
            columns = [col['VarCharValue'] for col in result_rows[0]['Data']]
            result_rows = result_rows[1:]

        for row in result_rows:
            rows.append([field.get('VarCharValue', '') for field in row['Data']])

        next_token = results.get('NextToken')
        if not next_token:
            break

    return pd.DataFrame(rows, columns=columns)


def query_bigquery(query, description=""):
    """Run BigQuery query and return DataFrame."""
    print(f"BigQuery: {description}...")
    bq_client = bigquery.Client(project="376132452327")
    return bq_client.query(query).to_dataframe()


def main():
    print("="*70)
    print("CORPORATE HYPOTHESIS DEEP DIVE")
    print("="*70)

    # Get session IDs from both sources with detailed attributes

    # BigQuery: Get Direct sessions with day of week, hour, screen resolution
    bq_query = """
    WITH sessions AS (
        SELECT
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
            EXTRACT(DAYOFWEEK FROM TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE 'Australia/Melbourne') as day_of_week,
            EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE 'Australia/Melbourne') as hour_melbourne,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'screen_resolution') as screen_resolution,
            device.category as device_category,
            device.operating_system as os,
            geo.city as city
        FROM `analytics_375839889.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '20260115' AND '20260121'
          AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%warwick.com.au%'
    )
    SELECT
        ga_session_id,
        ANY_VALUE(day_of_week) as day_of_week,
        ANY_VALUE(hour_melbourne) as hour_melbourne,
        ANY_VALUE(screen_resolution) as screen_resolution,
        ANY_VALUE(device_category) as device_category,
        ANY_VALUE(os) as os,
        ANY_VALUE(city) as city
    FROM sessions
    WHERE ga_session_id IS NOT NULL
    GROUP BY ga_session_id
    """
    bq_df = query_bigquery(bq_query, "Getting Direct sessions with attributes")
    bq_sessions = set(bq_df['ga_session_id'].tolist())
    print(f"  Got {len(bq_df)} Direct sessions")

    # Athena: Get SST sessions
    athena_query = """
    SELECT
        ga_session_id,
        EXTRACT(DOW FROM from_iso8601_timestamp(session_start) AT TIME ZONE 'Australia/Melbourne') as day_of_week,
        EXTRACT(HOUR FROM from_iso8601_timestamp(session_start) AT TIME ZONE 'Australia/Melbourne') as hour_melbourne,
        device_category,
        device_operating_system as os,
        geo_country
    FROM warwick_weave_sst_events.sst_sessions
    WHERE site = 'AU'
      AND year = '2026'
      AND month = '01'
      AND day BETWEEN '15' AND '21'
    """
    sst_df = query_athena(athena_query, "Getting SST sessions with attributes")
    sst_sessions = set(sst_df['ga_session_id'].tolist())
    print(f"  Got {len(sst_df)} SST sessions")

    # Categorize
    both = bq_sessions & sst_sessions
    direct_only = bq_sessions - sst_sessions
    sst_only = sst_sessions - bq_sessions

    print(f"\nSession categories: Both={len(both)}, Direct-only={len(direct_only)}, SST-only={len(sst_only)}")

    # Add category to BigQuery dataframe
    bq_df['category'] = bq_df['ga_session_id'].apply(
        lambda x: 'Both' if x in both else 'Direct-only'
    )

    # =========================================================================
    # ANALYSIS 1: Day of Week
    # =========================================================================
    print("\n" + "="*70)
    print("ANALYSIS 1: DAY OF WEEK")
    print("Corporate hypothesis: Direct-only should drop significantly on weekends")
    print("="*70)

    # Day of week: 1=Sunday, 2=Monday, ..., 7=Saturday (BigQuery DAYOFWEEK)
    day_names = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday',
                 5: 'Thursday', 6: 'Friday', 7: 'Saturday'}

    bq_df['day_of_week'] = pd.to_numeric(bq_df['day_of_week'])
    bq_df['day_name'] = bq_df['day_of_week'].map(day_names)
    bq_df['is_weekend'] = bq_df['day_of_week'].isin([1, 7])  # Sunday or Saturday

    # Calculate weekend vs weekday percentages by category
    for category in ['Both', 'Direct-only']:
        cat_df = bq_df[bq_df['category'] == category]
        weekend_pct = cat_df['is_weekend'].mean() * 100
        weekday_pct = 100 - weekend_pct
        print(f"\n{category}:")
        print(f"  Weekday (Mon-Fri): {weekday_pct:.1f}%")
        print(f"  Weekend (Sat-Sun): {weekend_pct:.1f}%")

    # Compare
    both_weekend = bq_df[bq_df['category'] == 'Both']['is_weekend'].mean() * 100
    direct_weekend = bq_df[bq_df['category'] == 'Direct-only']['is_weekend'].mean() * 100
    diff = direct_weekend - both_weekend
    print(f"\nDirect-only weekend % vs Both: {diff:+.1f}pp")
    print("(Negative = fewer weekend sessions = supports corporate hypothesis)")

    # Day-by-day breakdown
    both_total = bq_df[bq_df['category'] == 'Both'].shape[0]
    direct_total = bq_df[bq_df['category'] == 'Direct-only'].shape[0]
    print(f"\nDaily breakdown (Both: {both_total:,} sessions, Direct-only: {direct_total:,} sessions):")
    print(f"{'Day':<12} {'Both':>16} {'Direct-only':>18} {'Diff':>10}")
    print("-" * 58)

    for day_num in [2, 3, 4, 5, 6, 7, 1]:  # Mon-Sun
        day_name = day_names[day_num]
        both_count = bq_df[(bq_df['category'] == 'Both') & (bq_df['day_of_week'] == day_num)].shape[0]
        direct_count = bq_df[(bq_df['category'] == 'Direct-only') & (bq_df['day_of_week'] == day_num)].shape[0]
        both_pct = both_count / both_total * 100
        direct_pct = direct_count / direct_total * 100
        diff = direct_pct - both_pct
        diff_str = f"{diff:+.1f}pp"
        print(f"{day_name:<12} {both_pct:>6.1f}% ({both_count:>4}) {direct_pct:>6.1f}% ({direct_count:>4}) {diff_str:>10}")

    # =========================================================================
    # ANALYSIS 2: Screen Resolution
    # =========================================================================
    print("\n" + "="*70)
    print("ANALYSIS 2: SCREEN RESOLUTION")
    print("Corporate hypothesis: More standard corporate monitor resolutions")
    print("(1920x1080, 1366x768 are common corporate; varied sizes for consumer)")
    print("="*70)

    # Common corporate resolutions
    corporate_resolutions = ['1920x1080', '1366x768', '1536x864', '1440x900', '1280x720']

    bq_df['is_corporate_resolution'] = bq_df['screen_resolution'].isin(corporate_resolutions)

    for category in ['Both', 'Direct-only']:
        cat_df = bq_df[(bq_df['category'] == category) & (bq_df['device_category'] == 'desktop')]
        if len(cat_df) > 0:
            corp_res_count = cat_df['is_corporate_resolution'].sum()
            corp_res_pct = cat_df['is_corporate_resolution'].mean() * 100
            print(f"\n{category} (desktop only, {len(cat_df):,} sessions):")
            print(f"  Standard corporate resolutions: {corp_res_pct:.1f}% ({corp_res_count:,} sessions)")
            print(f"  Top resolutions:")
            for res, count in cat_df['screen_resolution'].value_counts().head(5).items():
                pct = count / len(cat_df) * 100
                print(f"    {res}: {pct:.1f}% ({count:,})")

    # =========================================================================
    # ANALYSIS 3: Geographic - Business Districts
    # =========================================================================
    print("\n" + "="*70)
    print("ANALYSIS 3: CITY DISTRIBUTION")
    print("Corporate hypothesis: More concentration in business districts/CBDs")
    print("="*70)

    # Major Australian business cities
    for category in ['Both', 'Direct-only']:
        cat_df = bq_df[bq_df['category'] == category]
        print(f"\n{category} ({len(cat_df):,} sessions) - Top 10 cities:")
        for city, count in cat_df['city'].value_counts().head(10).items():
            pct = count / len(cat_df) * 100
            print(f"  {city}: {pct:.1f}% ({count:,})")

    # =========================================================================
    # ANALYSIS 4: Hour Distribution (more detail)
    # =========================================================================
    print("\n" + "="*70)
    print("ANALYSIS 4: HOURLY DISTRIBUTION (Melbourne Time)")
    print("Corporate hypothesis: Strong 9-5 pattern with lunch dip")
    print("="*70)

    bq_df['hour_melbourne'] = pd.to_numeric(bq_df['hour_melbourne'])

    print(f"\n{'Hour':<8} {'Both':>16} {'Direct-only':>18} {'Diff':>10}")
    print("-" * 56)

    for hour in range(24):
        both_count = bq_df[(bq_df['category'] == 'Both') & (bq_df['hour_melbourne'] == hour)].shape[0]
        direct_count = bq_df[(bq_df['category'] == 'Direct-only') & (bq_df['hour_melbourne'] == hour)].shape[0]
        both_pct = both_count / both_total * 100
        direct_pct = direct_count / direct_total * 100
        diff = direct_pct - both_pct

        # Highlight business hours
        marker = "  <-- business" if 9 <= hour <= 17 else ""
        if hour == 12:
            marker = "  <-- lunch"

        print(f"{hour:02d}:00    {both_pct:>5.1f}% ({both_count:>4}) {direct_pct:>5.1f}% ({direct_count:>4}) {diff:>+7.1f}pp{marker}")

    # =========================================================================
    # ANALYSIS 5: Weekday Business Hours vs Other
    # =========================================================================
    print("\n" + "="*70)
    print("ANALYSIS 5: WEEKDAY BUSINESS HOURS CONCENTRATION")
    print("Corporate hypothesis: High concentration in weekday 9-5")
    print("="*70)

    bq_df['is_weekday_business'] = (
        (~bq_df['is_weekend']) &
        (bq_df['hour_melbourne'] >= 9) &
        (bq_df['hour_melbourne'] <= 17)
    )

    for category in ['Both', 'Direct-only']:
        cat_df = bq_df[bq_df['category'] == category]
        weekday_biz_count = cat_df['is_weekday_business'].sum()
        other_count = len(cat_df) - weekday_biz_count
        weekday_biz_pct = cat_df['is_weekday_business'].mean() * 100
        print(f"\n{category} ({len(cat_df):,} sessions):")
        print(f"  Weekday 9am-5pm: {weekday_biz_pct:.1f}% ({weekday_biz_count:,} sessions)")
        print(f"  Other times:     {100-weekday_biz_pct:.1f}% ({other_count:,} sessions)")

    both_wbh = bq_df[bq_df['category'] == 'Both']['is_weekday_business'].mean() * 100
    direct_wbh = bq_df[bq_df['category'] == 'Direct-only']['is_weekday_business'].mean() * 100
    diff = direct_wbh - both_wbh
    print(f"\nDirect-only weekday-business-hours vs Both: {diff:+.1f}pp")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "="*70)
    print("SUMMARY OF FINDINGS")
    print("="*70)

    # Recalculate key metrics with absolute numbers
    both_df = bq_df[bq_df['category'] == 'Both']
    direct_df = bq_df[bq_df['category'] == 'Direct-only']

    both_weekend_count = both_df['is_weekend'].sum()
    direct_weekend_count = direct_df['is_weekend'].sum()
    both_weekend = both_df['is_weekend'].mean() * 100
    direct_weekend = direct_df['is_weekend'].mean() * 100
    weekend_diff = direct_weekend - both_weekend

    both_wbh_count = both_df['is_weekday_business'].sum()
    direct_wbh_count = direct_df['is_weekday_business'].sum()
    both_wbh = both_df['is_weekday_business'].mean() * 100
    direct_wbh = direct_df['is_weekday_business'].mean() * 100
    wbh_diff = direct_wbh - both_wbh

    both_desktop_count = (both_df['device_category'] == 'desktop').sum()
    direct_desktop_count = (direct_df['device_category'] == 'desktop').sum()
    both_desktop = both_df['device_category'].eq('desktop').mean() * 100
    direct_desktop = direct_df['device_category'].eq('desktop').mean() * 100
    desktop_diff = direct_desktop - both_desktop

    print(f"""
Session counts: Both = {len(both_df):,}, Direct-only = {len(direct_df):,}

Signal                              Direct-only              Both                  Diff        Corporate?
----------------------------------------------------------------------------------------------------------
Weekend sessions                    {direct_weekend:>5.1f}% ({direct_weekend_count:>4})        {both_weekend:>5.1f}% ({both_weekend_count:>5})        {weekend_diff:>+6.1f}pp     {"YES ✓" if weekend_diff < -2 else "WEAK" if weekend_diff < 0 else "NO"}
Weekday business hours (9-5)        {direct_wbh:>5.1f}% ({direct_wbh_count:>4})        {both_wbh:>5.1f}% ({both_wbh_count:>5})        {wbh_diff:>+6.1f}pp     {"YES ✓" if wbh_diff > 3 else "WEAK" if wbh_diff > 0 else "NO"}
Desktop device share                {direct_desktop:>5.1f}% ({direct_desktop_count:>4})        {both_desktop:>5.1f}% ({both_desktop_count:>5})        {desktop_diff:>+6.1f}pp     {"YES ✓" if desktop_diff > 3 else "WEAK" if desktop_diff > 0 else "NO"}
""")


if __name__ == '__main__':
    main()
