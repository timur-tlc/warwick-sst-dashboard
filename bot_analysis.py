"""
Bot Analysis: Investigate Direct-only Sessions for Bot Patterns

Checks for:
1. Zero-engagement sessions (strong bot indicator)
2. Shallow sessions (1-2 events only)
3. Known bot User-Agent patterns (requires live query)
4. Prefetch/prerender signals
"""

import pandas as pd
from google.cloud import bigquery
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"

DATE_START = '20260106'
DATE_END = '20260113'

# Known bot UA patterns
BOT_UA_PATTERNS = [
    'googlebot',
    'bingbot',
    'slurp',  # Yahoo
    'duckduckbot',
    'baiduspider',
    'yandexbot',
    'sogou',
    'exabot',
    'facebot',
    'ia_archiver',  # Alexa
    'mj12bot',
    'semrushbot',
    'ahrefsbot',
    'dotbot',
    'rogerbot',
    'gigabot',
    'scrapy',
    'wget',
    'curl',
    'python-requests',
    'python-urllib',
    'headless',
    'phantom',
    'selenium',
    'chrome-lighthouse',
    'lighthouse',
    'pagespeed',
    'prerender',
    'prefetch',
    'crawl',
    'spider',
    'bot',  # Generic catch-all
]


def load_cached_data():
    """Load session data from cache."""
    sst_df = pd.read_parquet(CACHE_DIR / "sst_sessions.parquet")
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    return sst_df, direct_df


def safe_pct(numerator, denominator):
    """Calculate percentage safely, returning 0 if denominator is 0."""
    return (numerator / denominator * 100) if denominator > 0 else 0.0


def analyze_engagement_patterns(direct_df):
    """Analyze engagement patterns in Direct-only vs Both sessions."""
    both = direct_df[direct_df['session_category'] == 'Both'].copy()
    direct_only = direct_df[direct_df['session_category'] == 'Direct-only'].copy()

    if len(both) == 0 or len(direct_only) == 0:
        print("\nWARNING: One or more categories are empty, skipping engagement analysis")
        return None

    print("\n" + "-" * 70)
    print("ENGAGEMENT ANALYSIS")
    print("-" * 70)

    # Zero engagement
    both_zero = (pd.to_numeric(both['engagement_time_msec'], errors='coerce').fillna(0) == 0).sum()
    direct_only_zero = (pd.to_numeric(direct_only['engagement_time_msec'], errors='coerce').fillna(0) == 0).sum()

    print(f"\nZero engagement sessions:")
    print(f"  Both:        {both_zero:>6} / {len(both):>6} ({safe_pct(both_zero, len(both)):>5.1f}%)")
    print(f"  Direct-only: {direct_only_zero:>6} / {len(direct_only):>6} ({safe_pct(direct_only_zero, len(direct_only)):>5.1f}%)")

    if safe_pct(direct_only_zero, len(direct_only)) > safe_pct(both_zero, len(both)) * 2:
        print("\n  SIGNIFICANT: Direct-only has >2x the zero-engagement rate of Both")

    # Event count distribution
    both['event_count'] = pd.to_numeric(both['event_count'], errors='coerce')
    direct_only['event_count'] = pd.to_numeric(direct_only['event_count'], errors='coerce')

    print("\n" + "-" * 70)
    print("EVENT COUNT DISTRIBUTION")
    print("-" * 70)

    def categorize_events(count):
        if count == 1:
            return '1 event (bounce)'
        elif count <= 3:
            return '2-3 events'
        elif count <= 5:
            return '4-5 events'
        elif count <= 10:
            return '6-10 events'
        else:
            return '11+ events'

    both_events = both['event_count'].apply(categorize_events).value_counts()
    direct_only_events = direct_only['event_count'].apply(categorize_events).value_counts()

    categories = ['1 event (bounce)', '2-3 events', '4-5 events', '6-10 events', '11+ events']

    print(f"\n{'Category':<20} {'Both':>10} {'Both %':>8} {'Direct-only':>12} {'Dir-only %':>10}")
    print("-" * 65)

    for cat in categories:
        both_count = both_events.get(cat, 0)
        direct_only_count = direct_only_events.get(cat, 0)
        both_pct = safe_pct(both_count, len(both))
        direct_only_pct = safe_pct(direct_only_count, len(direct_only))
        print(f"{cat:<20} {both_count:>10} {both_pct:>7.1f}% {direct_only_count:>12} {direct_only_pct:>9.1f}%")

    # Single-event (bounce) comparison
    both_bounces = (both['event_count'] == 1).sum()
    direct_only_bounces = (direct_only['event_count'] == 1).sum()

    print(f"\nBounce rate (single event sessions):")
    print(f"  Both:        {safe_pct(both_bounces, len(both)):.1f}%")
    print(f"  Direct-only: {safe_pct(direct_only_bounces, len(direct_only)):.1f}%")

    return {
        'both_zero_engagement_pct': safe_pct(both_zero, len(both)),
        'direct_only_zero_engagement_pct': safe_pct(direct_only_zero, len(direct_only)),
        'both_bounce_pct': safe_pct(both_bounces, len(both)),
        'direct_only_bounce_pct': safe_pct(direct_only_bounces, len(direct_only)),
    }


def query_user_agents_for_direct_only(direct_only_session_ids):
    """Query BigQuery for User-Agent strings of Direct-only sessions."""
    print("\n" + "-" * 70)
    print("QUERYING USER-AGENT STRINGS FROM BIGQUERY")
    print("-" * 70)

    # Sample if too many
    sample_size = min(500, len(direct_only_session_ids))
    sample_ids = direct_only_session_ids[:sample_size]

    # Build IN clause
    ids_clause = ','.join([f"'{sid}'" for sid in sample_ids])

    bq_client = bigquery.Client(project="376132452327")

    query = f"""
    SELECT
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
        ANY_VALUE(device.web_info.browser) as browser,
        ANY_VALUE(device.operating_system) as os,
        ANY_VALUE(device.category) as device_category,
        ANY_VALUE(user_pseudo_id) as user_pseudo_id,
        COUNT(*) as events
    FROM `analytics_375839889.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{DATE_START}' AND '{DATE_END}'
      AND CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) IN ({ids_clause})
    GROUP BY 1
    """

    print(f"\nQuerying {sample_size} Direct-only sessions...")
    df = bq_client.query(query).to_dataframe()
    print(f"  Retrieved {len(df)} session records")

    return df


def analyze_browser_patterns(direct_only_df, both_df):
    """Compare browser patterns between Direct-only and Both."""
    print("\n" + "-" * 70)
    print("BROWSER DISTRIBUTION")
    print("-" * 70)

    both_browsers = both_df['device_browser'].value_counts(normalize=True).head(10) * 100
    direct_only_browsers = direct_only_df['device_browser'].value_counts(normalize=True).head(10) * 100

    print(f"\n{'Browser':<20} {'Both %':>10} {'Direct-only %':>14}")
    print("-" * 45)

    all_browsers = set(both_browsers.index) | set(direct_only_browsers.index)
    for browser in sorted(all_browsers, key=lambda x: both_browsers.get(x, 0) + direct_only_browsers.get(x, 0), reverse=True)[:10]:
        both_pct = both_browsers.get(browser, 0)
        direct_only_pct = direct_only_browsers.get(browser, 0)
        print(f"{browser:<20} {both_pct:>9.1f}% {direct_only_pct:>13.1f}%")


def estimate_bot_percentage(direct_only_df):
    """Estimate what percentage of Direct-only sessions are likely bots."""
    print("\n" + "=" * 70)
    print("BOT ESTIMATION")
    print("=" * 70)

    total = len(direct_only_df)

    if total == 0:
        print("\nWARNING: No Direct-only sessions to analyze")
        return {'total_direct_only': 0, 'likely_bots': 0, 'likely_bot_pct': 0, 'zero_engagement_pct': 0}

    # Zero engagement = strong bot signal
    zero_engagement = (pd.to_numeric(direct_only_df['engagement_time_msec'], errors='coerce').fillna(0) == 0).sum()
    zero_engagement_pct = safe_pct(zero_engagement, total)

    # Single event = possible bot
    single_event = (pd.to_numeric(direct_only_df['event_count'], errors='coerce') == 1).sum()
    single_event_pct = safe_pct(single_event, total)

    # Both zero engagement AND single event = very likely bot
    likely_bots = (
        (pd.to_numeric(direct_only_df['engagement_time_msec'], errors='coerce').fillna(0) == 0) &
        (pd.to_numeric(direct_only_df['event_count'], errors='coerce') == 1)
    ).sum()
    likely_bot_pct = safe_pct(likely_bots, total)

    print(f"\nDirect-only session breakdown:")
    print(f"  Total:                        {total:>6}")
    print(f"  Zero engagement:              {zero_engagement:>6} ({zero_engagement_pct:.1f}%)")
    print(f"  Single event:                 {single_event:>6} ({single_event_pct:.1f}%)")
    print(f"  Both (high-confidence bots):  {likely_bots:>6} ({likely_bot_pct:.1f}%)")

    # Conservative estimate
    conservative_bots = likely_bots
    aggressive_bots = zero_engagement

    print(f"\nBot estimates:")
    print(f"  Conservative (zero-eng + single-event): {likely_bot_pct:.1f}%")
    print(f"  Aggressive (all zero-engagement):       {zero_engagement_pct:.1f}%")

    return {
        'total_direct_only': total,
        'likely_bots': likely_bots,
        'likely_bot_pct': likely_bot_pct,
        'zero_engagement_pct': zero_engagement_pct,
    }


def main():
    print("=" * 70)
    print("BOT ANALYSIS: Direct-only Session Investigation")
    print("=" * 70)

    sst_df, direct_df = load_cached_data()

    both_df = direct_df[direct_df['session_category'] == 'Both']
    direct_only_df = direct_df[direct_df['session_category'] == 'Direct-only']

    print(f"\nSession counts:")
    print(f"  Both:        {len(both_df):,}")
    print(f"  Direct-only: {len(direct_only_df):,}")

    # Analyze engagement patterns
    engagement_stats = analyze_engagement_patterns(direct_df)

    # Browser patterns
    analyze_browser_patterns(direct_only_df, both_df)

    # Bot estimation
    bot_stats = estimate_bot_percentage(direct_only_df)

    # Query UA strings for deeper analysis (optional - uses live query)
    try:
        print("\n" + "-" * 70)
        print("DETAILED USER-AGENT ANALYSIS (Live Query)")
        print("-" * 70)

        direct_only_ids = direct_only_df['ga_session_id'].tolist()
        ua_df = query_user_agents_for_direct_only(direct_only_ids)

        if len(ua_df) > 0:
            # Check for bot-like patterns in browser field
            ua_df['browser_lower'] = ua_df['browser'].fillna('').str.lower()

            bot_count = 0
            for pattern in BOT_UA_PATTERNS:
                matches = ua_df['browser_lower'].str.contains(pattern, na=False).sum()
                if matches > 0:
                    bot_count += matches
                    print(f"  Pattern '{pattern}': {matches} matches")

            print(f"\n  Total sessions with bot-like UA patterns: {bot_count} / {len(ua_df)}")

    except Exception as e:
        print(f"\n  Skipping live query: {e}")

    # Verdict
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    if bot_stats['likely_bot_pct'] > 30:
        print(f"\nHIGH BOT RATE: {bot_stats['likely_bot_pct']:.1f}% of Direct-only sessions are likely bots")
        print("This explains why these sessions appear only in Direct (no GTM server-side)")
        print("SST naturally filters bots because they don't execute JavaScript properly.")
    elif bot_stats['likely_bot_pct'] > 10:
        print(f"\nMODERATE BOT RATE: {bot_stats['likely_bot_pct']:.1f}% of Direct-only are likely bots")
        print("Some Direct-only traffic is bots, but other factors may contribute.")
    else:
        print(f"\nLOW BOT RATE: Only {bot_stats['likely_bot_pct']:.1f}% of Direct-only are obvious bots")
        print("Other explanations for Direct-only sessions should be considered:")
        print("  - Corporate proxies/firewalls blocking SST domain")
        print("  - Cached/stale pages with old tracking code")
        print("  - Edge cases in the matching algorithm")

    # Adjusted totals
    print("\n" + "-" * 70)
    print("ADJUSTED CATEGORIZATION (Excluding Likely Bots)")
    print("-" * 70)

    adjusted_direct_only = bot_stats['total_direct_only'] - bot_stats['likely_bots']
    total_sessions = len(sst_df[sst_df['session_category'] == 'Both']) + \
                     len(sst_df[sst_df['session_category'] == 'SST-only']) + \
                     adjusted_direct_only

    print(f"\nIf likely bots are excluded from Direct-only:")
    print(f"  Original Direct-only: {bot_stats['total_direct_only']}")
    print(f"  Likely bots:          {bot_stats['likely_bots']}")
    print(f"  Adjusted Direct-only: {adjusted_direct_only}")
    print(f"\n  Adjusted Direct-only %: {adjusted_direct_only/total_sessions*100:.1f}% (was {bot_stats['total_direct_only']/(bot_stats['total_direct_only']+total_sessions-adjusted_direct_only)*100:.1f}%)")


if __name__ == "__main__":
    main()
