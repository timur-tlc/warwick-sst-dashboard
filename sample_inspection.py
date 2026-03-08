"""
Sample Inspection: Manual Review of Random Sessions

Pulls 30 random sessions from each category (Both, SST-only, Direct-only)
with full details for manual plausibility review.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

CACHE_DIR = Path(__file__).parent / "cache"

SAMPLE_SIZE = 30


def load_cached_data():
    """Load session data from cache."""
    sst_df = pd.read_parquet(CACHE_DIR / "sst_sessions.parquet")
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    return sst_df, direct_df


def format_timestamp(ts_micros):
    """Convert microseconds timestamp to readable format."""
    if pd.isna(ts_micros) or ts_micros == 0:
        return "N/A"
    try:
        ts_seconds = int(ts_micros) // 1000000
        # Use timezone-aware datetime
        from datetime import timezone, timedelta
        dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        # Convert to AEST (UTC+10)
        aest = timezone(timedelta(hours=10))
        dt_aest = dt.astimezone(aest)
        return dt_aest.strftime('%Y-%m-%d %H:%M:%S AEST')
    except:
        return str(ts_micros)


def format_engagement(msec):
    """Convert engagement milliseconds to readable format."""
    try:
        msec = int(float(msec))
        if msec == 0:
            return "0s (no engagement)"
        elif msec < 1000:
            return f"{msec}ms"
        elif msec < 60000:
            return f"{msec/1000:.1f}s"
        else:
            return f"{msec/60000:.1f}min"
    except:
        return str(msec)


def sample_category(df, category, source_name, n=SAMPLE_SIZE):
    """Sample sessions from a specific category."""
    category_df = df[df['session_category'] == category]

    if len(category_df) < n:
        sample = category_df
    else:
        sample = category_df.sample(n=n, random_state=42)

    return sample


def print_session_details(row, source_name):
    """Print detailed information about a session."""
    print(f"    Session ID:     {row.get('ga_session_id', 'N/A')}")
    print(f"    Timestamp:      {format_timestamp(row.get('session_start_ts', 0))}")
    print(f"    Device:         {row.get('device_category', 'N/A')}")
    print(f"    OS:             {row.get('device_operating_system', 'N/A')}")
    print(f"    Browser:        {row.get('device_browser', 'N/A')}")
    print(f"    Country:        {row.get('geo_country', 'N/A')}")
    print(f"    Events:         {row.get('event_count', 'N/A')}")
    print(f"    Engagement:     {format_engagement(row.get('engagement_time_msec', 0))}")
    print(f"    Purchase:       {'Yes' if row.get('has_purchase') in [1, '1', True] else 'No'}")
    print(f"    Source:         {source_name}")
    print()


def safe_pct(numerator, denominator):
    """Calculate percentage safely, returning 0 if denominator is 0."""
    return (numerator / denominator * 100) if denominator > 0 else 0.0


def analyze_sample(df, category_name):
    """Analyze a sample for plausibility indicators."""
    if len(df) == 0:
        return {'total': 0, 'zero_engagement': 0, 'single_event': 0,
                'desktop': 0, 'mobile': 0, 'australia': 0, 'with_purchase': 0}

    # Convert to numeric
    df['event_count_num'] = pd.to_numeric(df['event_count'], errors='coerce')
    df['engagement_num'] = pd.to_numeric(df['engagement_time_msec'], errors='coerce').fillna(0)

    stats = {
        'total': len(df),
        'zero_engagement': (df['engagement_num'] == 0).sum(),
        'single_event': (df['event_count_num'] == 1).sum(),
        'desktop': (df['device_category'] == 'desktop').sum(),
        'mobile': (df['device_category'] == 'mobile').sum(),
        'australia': (df['geo_country'] == 'Australia').sum(),
        'with_purchase': df['has_purchase'].apply(lambda x: x in [1, '1', True]).sum(),
    }

    return stats


def main():
    print("=" * 70)
    print("SAMPLE INSPECTION: Random Session Review")
    print("=" * 70)

    np.random.seed(42)  # For reproducibility

    sst_df, direct_df = load_cached_data()

    # Sample from each category
    print(f"\nSampling {SAMPLE_SIZE} sessions from each category...")

    # Both - sample from SST side (has the matched data)
    both_sample = sample_category(sst_df, 'Both', 'SST')
    sst_only_sample = sample_category(sst_df, 'SST-only', 'SST')
    direct_only_sample = sample_category(direct_df, 'Direct-only', 'Direct')

    # Category 1: Both
    print("\n" + "=" * 70)
    print(f"CATEGORY: BOTH (Overlap) - {SAMPLE_SIZE} random samples")
    print("=" * 70)
    print("\nThese sessions appeared in BOTH SST and Direct.")
    print("Expected: Normal user sessions with real engagement.\n")

    for i, (_, row) in enumerate(both_sample.head(10).iterrows(), 1):
        print(f"  Sample {i}:")
        print_session_details(row, 'SST (matched)')

    both_stats = analyze_sample(both_sample, 'Both')
    print("-" * 50)
    print("Sample Summary:")
    print(f"  Zero engagement: {both_stats['zero_engagement']}/{both_stats['total']} ({safe_pct(both_stats['zero_engagement'], both_stats['total']):.0f}%)")
    print(f"  Single event:    {both_stats['single_event']}/{both_stats['total']} ({safe_pct(both_stats['single_event'], both_stats['total']):.0f}%)")
    print(f"  Desktop:         {both_stats['desktop']}/{both_stats['total']} ({safe_pct(both_stats['desktop'], both_stats['total']):.0f}%)")
    print(f"  Australia:       {both_stats['australia']}/{both_stats['total']} ({safe_pct(both_stats['australia'], both_stats['total']):.0f}%)")
    print(f"  With purchase:   {both_stats['with_purchase']}/{both_stats['total']}")

    # Category 2: SST-only
    print("\n" + "=" * 70)
    print(f"CATEGORY: SST-ONLY - {SAMPLE_SIZE} random samples")
    print("=" * 70)
    print("\nThese sessions appeared ONLY in SST (ad-blocked or geo-blocked).")
    print("Expected: Ad-blocker users or users from China (Great Firewall).\n")

    for i, (_, row) in enumerate(sst_only_sample.head(10).iterrows(), 1):
        print(f"  Sample {i}:")
        print_session_details(row, 'SST')

    sst_only_stats = analyze_sample(sst_only_sample, 'SST-only')
    print("-" * 50)
    print("Sample Summary:")
    print(f"  Zero engagement: {sst_only_stats['zero_engagement']}/{sst_only_stats['total']} ({safe_pct(sst_only_stats['zero_engagement'], sst_only_stats['total']):.0f}%)")
    print(f"  Single event:    {sst_only_stats['single_event']}/{sst_only_stats['total']} ({safe_pct(sst_only_stats['single_event'], sst_only_stats['total']):.0f}%)")
    print(f"  Desktop:         {sst_only_stats['desktop']}/{sst_only_stats['total']} ({safe_pct(sst_only_stats['desktop'], sst_only_stats['total']):.0f}%)")
    print(f"  Australia:       {sst_only_stats['australia']}/{sst_only_stats['total']} ({safe_pct(sst_only_stats['australia'], sst_only_stats['total']):.0f}%)")
    print(f"  With purchase:   {sst_only_stats['with_purchase']}/{sst_only_stats['total']}")

    # Category 3: Direct-only
    print("\n" + "=" * 70)
    print(f"CATEGORY: DIRECT-ONLY - {SAMPLE_SIZE} random samples")
    print("=" * 70)
    print("\nThese sessions appeared ONLY in Direct (bots, prefetch, or corporate).")
    print("Expected: High zero-engagement, possibly bot patterns.\n")

    for i, (_, row) in enumerate(direct_only_sample.head(10).iterrows(), 1):
        print(f"  Sample {i}:")
        print_session_details(row, 'Direct')

    direct_only_stats = analyze_sample(direct_only_sample, 'Direct-only')
    print("-" * 50)
    print("Sample Summary:")
    print(f"  Zero engagement: {direct_only_stats['zero_engagement']}/{direct_only_stats['total']} ({safe_pct(direct_only_stats['zero_engagement'], direct_only_stats['total']):.0f}%)")
    print(f"  Single event:    {direct_only_stats['single_event']}/{direct_only_stats['total']} ({safe_pct(direct_only_stats['single_event'], direct_only_stats['total']):.0f}%)")
    print(f"  Desktop:         {direct_only_stats['desktop']}/{direct_only_stats['total']} ({safe_pct(direct_only_stats['desktop'], direct_only_stats['total']):.0f}%)")
    print(f"  Australia:       {direct_only_stats['australia']}/{direct_only_stats['total']} ({safe_pct(direct_only_stats['australia'], direct_only_stats['total']):.0f}%)")
    print(f"  With purchase:   {direct_only_stats['with_purchase']}/{direct_only_stats['total']}")

    # Comparison summary
    print("\n" + "=" * 70)
    print("CATEGORY COMPARISON SUMMARY")
    print("=" * 70)

    print(f"\n{'Metric':<25} {'Both':>10} {'SST-only':>12} {'Direct-only':>14}")
    print("-" * 65)
    print(f"{'Zero engagement %':<25} {safe_pct(both_stats['zero_engagement'], both_stats['total']):>9.0f}% {safe_pct(sst_only_stats['zero_engagement'], sst_only_stats['total']):>11.0f}% {safe_pct(direct_only_stats['zero_engagement'], direct_only_stats['total']):>13.0f}%")
    print(f"{'Single event %':<25} {safe_pct(both_stats['single_event'], both_stats['total']):>9.0f}% {safe_pct(sst_only_stats['single_event'], sst_only_stats['total']):>11.0f}% {safe_pct(direct_only_stats['single_event'], direct_only_stats['total']):>13.0f}%")
    print(f"{'Desktop %':<25} {safe_pct(both_stats['desktop'], both_stats['total']):>9.0f}% {safe_pct(sst_only_stats['desktop'], sst_only_stats['total']):>11.0f}% {safe_pct(direct_only_stats['desktop'], direct_only_stats['total']):>13.0f}%")
    print(f"{'Australia %':<25} {safe_pct(both_stats['australia'], both_stats['total']):>9.0f}% {safe_pct(sst_only_stats['australia'], sst_only_stats['total']):>11.0f}% {safe_pct(direct_only_stats['australia'], direct_only_stats['total']):>13.0f}%")

    # Save full samples to CSV
    both_sample['category'] = 'Both'
    sst_only_sample['category'] = 'SST-only'
    direct_only_sample['category'] = 'Direct-only'

    all_samples = pd.concat([both_sample, sst_only_sample, direct_only_sample])
    all_samples.to_csv('docs/sample_inspection.csv', index=False)
    print(f"\n\nFull samples saved to docs/sample_inspection.csv")

    # Verdict
    print("\n" + "=" * 70)
    print("PLAUSIBILITY ASSESSMENT")
    print("=" * 70)

    issues = []

    # Check if Direct-only has expected bot characteristics
    if safe_pct(direct_only_stats['zero_engagement'], direct_only_stats['total']) > 40:
        print("\n  EXPECTED: Direct-only has high zero-engagement rate (likely bots/prefetch)")
    else:
        issues.append("Direct-only does NOT show expected high zero-engagement rate")

    # Check if SST-only has expected characteristics
    if safe_pct(sst_only_stats['australia'], sst_only_stats['total']) < 60:
        print("  EXPECTED: SST-only has lower Australia rate (international/blocked users)")
    else:
        issues.append("SST-only has unexpectedly high Australia rate")

    # Check if Both looks normal
    if safe_pct(both_stats['zero_engagement'], both_stats['total']) < 30:
        print("  EXPECTED: Both category has low zero-engagement (real users)")
    else:
        issues.append("Both category has unexpectedly high zero-engagement")

    if issues:
        print("\n  ISSUES DETECTED:")
        for issue in issues:
            print(f"    - {issue}")
    else:
        print("\n  ALL PATTERNS MATCH EXPECTATIONS")
        print("  The session categorization appears plausible.")


if __name__ == "__main__":
    main()
