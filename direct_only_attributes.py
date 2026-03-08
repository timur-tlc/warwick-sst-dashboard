"""
Direct-only Attribute Analysis: Why don't these sessions appear in SST?

Compares attributes between Direct-only and Both sessions to find
systematic differences that might explain SST filtering.

Hypotheses to test:
1. Missing/suspicious User-Agent
2. Missing cookies (_ga, _gid)
3. Prefetch/prerender signals in headers
4. Geographic patterns (certain regions blocked?)
5. Device/browser patterns
"""

import pandas as pd
from google.cloud import bigquery
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"

DATE_START = '20260106'
DATE_END = '20260113'


def safe_pct(numerator, denominator):
    """Calculate percentage safely."""
    return (numerator / denominator * 100) if denominator > 0 else 0.0


def load_cached_categories():
    """Load session categories from cache."""
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    return direct_df


def query_session_details(session_ids, label):
    """Query detailed attributes for a sample of sessions."""
    print(f"\nQuerying {label} session details from BigQuery...")

    # Sample if too many
    sample_size = min(1000, len(session_ids))
    sample_ids = list(session_ids)[:sample_size]

    ids_clause = ','.join([f"'{sid}'" for sid in sample_ids])

    bq_client = bigquery.Client(project="376132452327")

    # Query for detailed session attributes
    query = f"""
    WITH session_events AS (
        SELECT
            CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
            event_name,
            event_timestamp,
            user_pseudo_id,

            -- Device info
            device.category as device_category,
            device.operating_system as device_os,
            device.operating_system_version as device_os_version,
            device.web_info.browser as browser,
            device.web_info.browser_version as browser_version,
            device.web_info.hostname as hostname,
            device.language as language,

            -- Geo
            geo.country as geo_country,
            geo.region as geo_region,
            geo.city as geo_city,

            -- Traffic source
            traffic_source.source as traffic_source,
            traffic_source.medium as traffic_medium,

            -- Page info
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') as page_referrer,

            -- Engagement
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engagement_time_msec') as engagement_time_msec,
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engaged_session_event') as engaged_session,

            -- Session info
            (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') as session_number,

            -- Entropy source (indicates cookie presence)
            (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'entropy_source') as entropy_source

        FROM `analytics_375839889.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{DATE_START}' AND '{DATE_END}'
          AND CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) IN ({ids_clause})
    )
    SELECT
        ga_session_id,

        -- Aggregates
        COUNT(*) as event_count,
        COUNT(DISTINCT event_name) as unique_events,
        MIN(event_timestamp) as first_event_ts,
        MAX(event_timestamp) as last_event_ts,
        (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000.0 as session_duration_sec,

        -- Device (first value)
        ANY_VALUE(device_category) as device_category,
        ANY_VALUE(device_os) as device_os,
        ANY_VALUE(device_os_version) as device_os_version,
        ANY_VALUE(browser) as browser,
        ANY_VALUE(browser_version) as browser_version,
        ANY_VALUE(hostname) as hostname,
        ANY_VALUE(language) as language,

        -- Geo
        ANY_VALUE(geo_country) as geo_country,
        ANY_VALUE(geo_region) as geo_region,
        ANY_VALUE(geo_city) as geo_city,

        -- Traffic
        ANY_VALUE(traffic_source) as traffic_source,
        ANY_VALUE(traffic_medium) as traffic_medium,

        -- First page
        MIN_BY(page_location, event_timestamp) as landing_page,
        MIN_BY(page_referrer, event_timestamp) as first_referrer,

        -- Engagement
        SUM(COALESCE(engagement_time_msec, 0)) as total_engagement_ms,
        MAX(COALESCE(engaged_session, 0)) as is_engaged,

        -- Session number (1 = new user)
        ANY_VALUE(session_number) as session_number,

        -- Entropy (cookie indicator)
        ANY_VALUE(entropy_source) as entropy_source,

        -- Event sequence
        STRING_AGG(event_name, ' -> ' ORDER BY event_timestamp LIMIT 10) as event_sequence

    FROM session_events
    GROUP BY 1
    """

    df = bq_client.query(query).to_dataframe()
    print(f"  Retrieved {len(df)} sessions")
    return df


def compare_distributions(both_df, direct_only_df, column, top_n=10):
    """Compare distribution of a column between Both and Direct-only."""
    both_dist = both_df[column].value_counts(normalize=True).head(top_n) * 100
    do_dist = direct_only_df[column].value_counts(normalize=True).head(top_n) * 100

    all_values = set(both_dist.index) | set(do_dist.index)

    print(f"\n{column.upper()}:")
    print(f"{'Value':<30} {'Both %':>10} {'Dir-only %':>12} {'Diff':>8}")
    print("-" * 62)

    for val in sorted(all_values, key=lambda x: both_dist.get(x, 0) + do_dist.get(x, 0), reverse=True)[:top_n]:
        both_pct = both_dist.get(val, 0)
        do_pct = do_dist.get(val, 0)
        diff = do_pct - both_pct
        flag = "***" if abs(diff) > 10 else "**" if abs(diff) > 5 else "*" if abs(diff) > 2 else ""
        val_str = str(val)[:28] if val else "(empty)"
        print(f"{val_str:<30} {both_pct:>9.1f}% {do_pct:>11.1f}% {diff:>+7.1f} {flag}")


def analyze_timing_patterns(both_df, direct_only_df):
    """Analyze session duration and event timing."""
    print("\n" + "=" * 70)
    print("TIMING PATTERNS")
    print("=" * 70)

    # Session duration
    print("\nSession duration (seconds):")
    print(f"{'Metric':<20} {'Both':>15} {'Direct-only':>15}")
    print("-" * 52)

    for metric, func in [('Mean', 'mean'), ('Median', 'median'), ('Std', 'std'), ('Max', 'max')]:
        both_val = getattr(both_df['session_duration_sec'], func)()
        do_val = getattr(direct_only_df['session_duration_sec'], func)()
        print(f"{metric:<20} {both_val:>14.2f}s {do_val:>14.2f}s")

    # Zero-duration sessions (all events in same second)
    both_zero = (both_df['session_duration_sec'] < 0.1).sum()
    do_zero = (direct_only_df['session_duration_sec'] < 0.1).sum()

    print(f"\n{'<0.1s duration':<20} {both_zero:>10} ({safe_pct(both_zero, len(both_df)):.1f}%) "
          f"{do_zero:>10} ({safe_pct(do_zero, len(direct_only_df)):.1f}%)")

    if safe_pct(do_zero, len(direct_only_df)) > safe_pct(both_zero, len(both_df)) * 2:
        print("\n  *** SIGNIFICANT: Direct-only has much higher rate of near-instant sessions")
        print("  *** This strongly suggests prefetch/prerender traffic")


def analyze_event_sequences(both_df, direct_only_df):
    """Analyze event sequence patterns."""
    print("\n" + "=" * 70)
    print("EVENT SEQUENCE PATTERNS")
    print("=" * 70)

    # Most common sequences
    both_seqs = both_df['event_sequence'].value_counts().head(10)
    do_seqs = direct_only_df['event_sequence'].value_counts().head(10)

    print("\nTop sequences in Direct-only:")
    for seq, count in do_seqs.items():
        pct = safe_pct(count, len(direct_only_df))
        print(f"  {pct:>5.1f}% ({count:>4}): {seq[:70]}")

    print("\nTop sequences in Both:")
    for seq, count in both_seqs.head(5).items():
        pct = safe_pct(count, len(both_df))
        print(f"  {pct:>5.1f}% ({count:>4}): {seq[:70]}")


def analyze_new_vs_returning(both_df, direct_only_df):
    """Analyze session number (new vs returning users)."""
    print("\n" + "=" * 70)
    print("NEW VS RETURNING USERS")
    print("=" * 70)

    both_new = (both_df['session_number'] == 1).sum()
    do_new = (direct_only_df['session_number'] == 1).sum()

    print(f"\nSession number = 1 (new users):")
    print(f"  Both:        {both_new:>6} / {len(both_df):>6} ({safe_pct(both_new, len(both_df)):.1f}%)")
    print(f"  Direct-only: {do_new:>6} / {len(direct_only_df):>6} ({safe_pct(do_new, len(direct_only_df)):.1f}%)")

    if safe_pct(do_new, len(direct_only_df)) > safe_pct(both_new, len(both_df)) + 10:
        print("\n  *** Direct-only has more 'new' users - could indicate missing cookies")


def analyze_referrer_patterns(both_df, direct_only_df):
    """Analyze referrer patterns."""
    print("\n" + "=" * 70)
    print("REFERRER PATTERNS")
    print("=" * 70)

    # Empty referrer
    both_empty = both_df['first_referrer'].isna() | (both_df['first_referrer'] == '')
    do_empty = direct_only_df['first_referrer'].isna() | (direct_only_df['first_referrer'] == '')

    print(f"\nEmpty/missing referrer:")
    print(f"  Both:        {both_empty.sum():>6} / {len(both_df):>6} ({safe_pct(both_empty.sum(), len(both_df)):.1f}%)")
    print(f"  Direct-only: {do_empty.sum():>6} / {len(direct_only_df):>6} ({safe_pct(do_empty.sum(), len(direct_only_df)):.1f}%)")

    # Referrer domains
    def extract_domain(url):
        if pd.isna(url) or url == '':
            return '(none)'
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc or '(none)'
        except:
            return '(invalid)'

    both_df = both_df.copy()
    direct_only_df = direct_only_df.copy()
    both_df['referrer_domain'] = both_df['first_referrer'].apply(extract_domain)
    direct_only_df['referrer_domain'] = direct_only_df['first_referrer'].apply(extract_domain)

    compare_distributions(both_df, direct_only_df, 'referrer_domain', top_n=8)


def analyze_entropy_source(both_df, direct_only_df):
    """Analyze entropy source (indicates cookie presence)."""
    print("\n" + "=" * 70)
    print("ENTROPY SOURCE (Cookie Indicator)")
    print("=" * 70)

    compare_distributions(both_df, direct_only_df, 'entropy_source', top_n=5)

    # If entropy_source differs significantly, it indicates cookie issues
    both_cookie = both_df['entropy_source'].value_counts(normalize=True)
    do_cookie = direct_only_df['entropy_source'].value_counts(normalize=True)

    print("\nNote: 'cookie' entropy means first-party cookies present")
    print("      Other values may indicate cookieless/incognito traffic")


def main():
    print("=" * 70)
    print("DIRECT-ONLY ATTRIBUTE ANALYSIS")
    print("Why don't these sessions appear in SST?")
    print("=" * 70)

    # Load cached categories
    direct_df = load_cached_categories()

    both_ids = set(direct_df[direct_df['session_category'] == 'Both']['ga_session_id'])
    direct_only_ids = set(direct_df[direct_df['session_category'] == 'Direct-only']['ga_session_id'])

    print(f"\nSession counts from cache:")
    print(f"  Both:        {len(both_ids):,}")
    print(f"  Direct-only: {len(direct_only_ids):,}")

    # Query detailed attributes
    both_details = query_session_details(both_ids, "Both")
    do_details = query_session_details(direct_only_ids, "Direct-only")

    # Run analyses
    print("\n" + "=" * 70)
    print("DIMENSION COMPARISONS")
    print("=" * 70)

    compare_distributions(both_details, do_details, 'device_category')
    compare_distributions(both_details, do_details, 'device_os')
    compare_distributions(both_details, do_details, 'browser')
    compare_distributions(both_details, do_details, 'geo_country')
    compare_distributions(both_details, do_details, 'traffic_source')
    compare_distributions(both_details, do_details, 'traffic_medium')

    analyze_timing_patterns(both_details, do_details)
    analyze_event_sequences(both_details, do_details)
    analyze_new_vs_returning(both_details, do_details)
    analyze_referrer_patterns(both_details, do_details)
    analyze_entropy_source(both_details, do_details)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY: Potential SST Filtering Signals")
    print("=" * 70)

    # Check for key differences
    signals = []

    # Timing
    do_instant = (do_details['session_duration_sec'] < 0.1).sum() / len(do_details) * 100
    both_instant = (both_details['session_duration_sec'] < 0.1).sum() / len(both_details) * 100
    if do_instant > both_instant * 1.5:
        signals.append(f"Instant sessions: {do_instant:.1f}% vs {both_instant:.1f}% (prefetch signal)")

    # New users
    do_new = (do_details['session_number'] == 1).sum() / len(do_details) * 100
    both_new = (both_details['session_number'] == 1).sum() / len(both_details) * 100
    if do_new > both_new + 10:
        signals.append(f"New users: {do_new:.1f}% vs {both_new:.1f}% (missing cookies)")

    # Desktop
    do_desktop = (do_details['device_category'] == 'desktop').sum() / len(do_details) * 100
    both_desktop = (both_details['device_category'] == 'desktop').sum() / len(both_details) * 100
    if do_desktop > both_desktop + 10:
        signals.append(f"Desktop: {do_desktop:.1f}% vs {both_desktop:.1f}% (corporate/bot profile)")

    # Zero engagement
    do_zero_eng = (do_details['total_engagement_ms'] == 0).sum() / len(do_details) * 100
    both_zero_eng = (both_details['total_engagement_ms'] == 0).sum() / len(both_details) * 100
    if do_zero_eng > both_zero_eng * 1.5:
        signals.append(f"Zero engagement: {do_zero_eng:.1f}% vs {both_zero_eng:.1f}%")

    if signals:
        print("\nKey differences found:")
        for s in signals:
            print(f"  - {s}")
    else:
        print("\nNo strong differentiating signals found.")

    print("\n" + "-" * 70)
    print("HYPOTHESIS EVALUATION")
    print("-" * 70)

    print("""
Based on the data:

1. PREFETCH/PRERENDER: Check 'instant sessions' rate above
   - If Direct-only has many more <0.1s sessions, prefetch is likely
   - SST might not fire for speculative page loads

2. MISSING COOKIES: Check 'new users' and 'entropy_source' above
   - If Direct-only has more new users, cookies might be missing
   - Prefetch often doesn't send/receive cookies properly

3. GTM SERVER FILTERING: Can't detect from this data
   - Would need to check GTM Server Container settings
   - Or compare raw SST logs before any filtering

4. CORPORATE BOTS: Check device/browser profile above
   - High desktop + Chrome + Windows = corporate profile
   - Could be internal tools, monitoring, SEO crawlers
""")

    return both_details, do_details


if __name__ == "__main__":
    both_df, do_df = main()
