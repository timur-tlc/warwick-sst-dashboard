#!/usr/bin/env python3
"""
SST vs Direct Quality Comparison

Compare ALL sessions from each source (with overlap):
- SST = Both + SST-only (all sessions received by SST)
- Direct = Both + Direct-only (all sessions received by Direct)

This is different from the existing segmented analysis which looks at
mutually exclusive categories (Both, SST-only, Direct-only).

IMPORTANT: event_count is NOT comparable across sources because the same
session may have different event counts in SST vs Direct (SST often receives
fewer events). We only compare metrics that are consistent: engagement_time,
has_purchase, device/geo attributes.
"""

import pandas as pd
import json
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"


def safe_pct(numerator, denominator):
    """Safe percentage calculation with division guard."""
    return (numerator / denominator * 100) if denominator > 0 else 0.0


def load_session_data():
    """Load session data from cache."""
    with open(CACHE_DIR / "metadata.json") as f:
        metadata = json.load(f)

    sst_df = pd.read_parquet(CACHE_DIR / "sst_sessions.parquet")
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")

    return sst_df, direct_df, metadata


def calculate_quality_metrics(df, source_name):
    """
    Calculate quality metrics for a session dataframe.

    NOTE: We exclude event_count-based metrics (deep/shallow/single-event)
    because event_count measures source receipt, not session quality.
    The same session has different event counts in SST vs Direct.
    """
    n = len(df)
    if n == 0:
        return {}

    # Convert numeric columns
    df = df.copy()
    df['engagement_time_msec'] = pd.to_numeric(df['engagement_time_msec'], errors='coerce').fillna(0)
    df['has_purchase'] = pd.to_numeric(df['has_purchase'], errors='coerce').fillna(0)

    # Engagement time in seconds
    df['engagement_sec'] = df['engagement_time_msec'] / 1000

    # Quality indicators (engagement-based only - comparable across sources)
    zero_engagement = (df['engagement_sec'] == 0).sum()
    engaged = (df['engagement_sec'] > 0).sum()
    purchases = df['has_purchase'].sum()

    # Calculate quality score (higher = better)
    # Based on engagement rate and purchase rate only
    engagement_rate = safe_pct(engaged, n)
    purchase_rate = safe_pct(purchases, n)

    # Weighted quality score (0-100)
    # 70% engagement, 30% purchase (capped at 30 to avoid purchase dominating)
    quality_score = (engagement_rate * 0.7) + min(purchase_rate * 10, 30)

    return {
        'source': source_name,
        'sessions': n,
        'zero_engagement': zero_engagement,
        'zero_engagement_pct': safe_pct(zero_engagement, n),
        'engaged': engaged,
        'engaged_pct': safe_pct(engaged, n),
        'purchases': int(purchases),
        'purchase_rate': safe_pct(purchases, n),
        'mean_engagement_sec': df['engagement_sec'].mean(),
        'median_engagement_sec': df['engagement_sec'].median(),
        'quality_score': quality_score,
        # Device breakdown
        'desktop_pct': safe_pct((df['device_category'] == 'desktop').sum(), n),
        'mobile_pct': safe_pct((df['device_category'] == 'mobile').sum(), n),
        'tablet_pct': safe_pct((df['device_category'] == 'tablet').sum(), n),
        # OS breakdown
        'windows_pct': safe_pct((df['device_operating_system'] == 'Windows').sum(), n),
        'ios_pct': safe_pct((df['device_operating_system'] == 'iOS').sum(), n),
        'macos_pct': safe_pct((df['device_operating_system'] == 'Macintosh').sum(), n),
        'android_pct': safe_pct((df['device_operating_system'] == 'Android').sum(), n),
        # Geo breakdown
        'australia_pct': safe_pct((df['geo_country'] == 'Australia').sum(), n),
        'china_pct': safe_pct((df['geo_country'] == 'China').sum(), n),
        'nz_pct': safe_pct((df['geo_country'] == 'New Zealand').sum(), n),
        'us_pct': safe_pct((df['geo_country'] == 'United States').sum(), n),
    }


def main():
    print("=" * 100)
    print("SST vs DIRECT QUALITY COMPARISON")
    print("All sessions from each source (with overlap in 'Both' counted to each)")
    print("=" * 100)

    sst_df, direct_df, metadata = load_session_data()

    print(f"\nDate range: {metadata['date_start']} to {metadata['date_end']}")
    print(f"\nSession counts:")
    print(f"  SST total:    {len(sst_df):,} sessions (Both + SST-only)")
    print(f"  Direct total: {len(direct_df):,} sessions (Both + Direct-only)")
    print(f"  Overlap:      {metadata['totals']['both']:,} sessions appear in both")

    # Calculate metrics for each source
    sst_metrics = calculate_quality_metrics(sst_df, 'SST')
    direct_metrics = calculate_quality_metrics(direct_df, 'Direct')

    # Print comparison
    print("\n" + "=" * 100)
    print("QUALITY COMPARISON: SST vs DIRECT")
    print("(event_count excluded - not comparable across sources)")
    print("=" * 100)

    print(f"\n{'Metric':<30} | {'SST':>18} | {'Direct':>18} | {'Diff':>12}")
    print("-" * 85)

    # Quality score
    diff = sst_metrics['quality_score'] - direct_metrics['quality_score']
    print(f"{'Quality Score':<30} | {sst_metrics['quality_score']:>17.1f} | {direct_metrics['quality_score']:>17.1f} | {diff:>+11.1f}")

    # Engagement metrics
    print(f"\n--- Engagement ---")
    diff = sst_metrics['zero_engagement_pct'] - direct_metrics['zero_engagement_pct']
    print(f"{'Zero Engagement %':<30} | {sst_metrics['zero_engagement_pct']:>16.1f}% | {direct_metrics['zero_engagement_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['engaged_pct'] - direct_metrics['engaged_pct']
    print(f"{'Engaged Sessions %':<30} | {sst_metrics['engaged_pct']:>16.1f}% | {direct_metrics['engaged_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['mean_engagement_sec'] - direct_metrics['mean_engagement_sec']
    print(f"{'Mean Engagement (sec)':<30} | {sst_metrics['mean_engagement_sec']:>17.1f} | {direct_metrics['mean_engagement_sec']:>17.1f} | {diff:>+11.1f}")

    diff = sst_metrics['median_engagement_sec'] - direct_metrics['median_engagement_sec']
    print(f"{'Median Engagement (sec)':<30} | {sst_metrics['median_engagement_sec']:>17.1f} | {direct_metrics['median_engagement_sec']:>17.1f} | {diff:>+11.1f}")

    # Conversions
    print(f"\n--- Conversions ---")
    print(f"{'Purchases':<30} | {sst_metrics['purchases']:>17,} | {direct_metrics['purchases']:>17,} | {sst_metrics['purchases'] - direct_metrics['purchases']:>+11,}")

    diff = sst_metrics['purchase_rate'] - direct_metrics['purchase_rate']
    print(f"{'Purchase Rate %':<30} | {sst_metrics['purchase_rate']:>16.2f}% | {direct_metrics['purchase_rate']:>16.2f}% | {diff:>+10.2f}pp")

    # Device breakdown
    print(f"\n--- Device Category ---")
    diff = sst_metrics['desktop_pct'] - direct_metrics['desktop_pct']
    print(f"{'Desktop %':<30} | {sst_metrics['desktop_pct']:>16.1f}% | {direct_metrics['desktop_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['mobile_pct'] - direct_metrics['mobile_pct']
    print(f"{'Mobile %':<30} | {sst_metrics['mobile_pct']:>16.1f}% | {direct_metrics['mobile_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['tablet_pct'] - direct_metrics['tablet_pct']
    print(f"{'Tablet %':<30} | {sst_metrics['tablet_pct']:>16.1f}% | {direct_metrics['tablet_pct']:>16.1f}% | {diff:>+10.1f}pp")

    # OS breakdown
    print(f"\n--- Operating System ---")
    diff = sst_metrics['windows_pct'] - direct_metrics['windows_pct']
    print(f"{'Windows %':<30} | {sst_metrics['windows_pct']:>16.1f}% | {direct_metrics['windows_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['ios_pct'] - direct_metrics['ios_pct']
    print(f"{'iOS %':<30} | {sst_metrics['ios_pct']:>16.1f}% | {direct_metrics['ios_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['macos_pct'] - direct_metrics['macos_pct']
    print(f"{'macOS %':<30} | {sst_metrics['macos_pct']:>16.1f}% | {direct_metrics['macos_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['android_pct'] - direct_metrics['android_pct']
    print(f"{'Android %':<30} | {sst_metrics['android_pct']:>16.1f}% | {direct_metrics['android_pct']:>16.1f}% | {diff:>+10.1f}pp")

    # Geo breakdown
    print(f"\n--- Geography ---")
    diff = sst_metrics['australia_pct'] - direct_metrics['australia_pct']
    print(f"{'Australia %':<30} | {sst_metrics['australia_pct']:>16.1f}% | {direct_metrics['australia_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['china_pct'] - direct_metrics['china_pct']
    print(f"{'China %':<30} | {sst_metrics['china_pct']:>16.1f}% | {direct_metrics['china_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['nz_pct'] - direct_metrics['nz_pct']
    print(f"{'New Zealand %':<30} | {sst_metrics['nz_pct']:>16.1f}% | {direct_metrics['nz_pct']:>16.1f}% | {diff:>+10.1f}pp")

    diff = sst_metrics['us_pct'] - direct_metrics['us_pct']
    print(f"{'United States %':<30} | {sst_metrics['us_pct']:>16.1f}% | {direct_metrics['us_pct']:>16.1f}% | {diff:>+10.1f}pp")

    # Summary
    print("\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)

    quality_diff = sst_metrics['quality_score'] - direct_metrics['quality_score']
    quality_ratio = sst_metrics['quality_score'] / direct_metrics['quality_score'] if direct_metrics['quality_score'] > 0 else 0

    print(f"\nSST Quality Score: {sst_metrics['quality_score']:.1f}")
    print(f"Direct Quality Score: {direct_metrics['quality_score']:.1f}")

    # Key differences
    print(f"\nKey Differences:")

    session_diff = sst_metrics['sessions'] - direct_metrics['sessions']
    print(f"  • SST captures {session_diff:,} MORE sessions (+{session_diff/direct_metrics['sessions']*100:.1f}%)")

    eng_diff = sst_metrics['engaged_pct'] - direct_metrics['engaged_pct']
    if eng_diff > 0:
        print(f"  • SST has {eng_diff:.1f}pp HIGHER engagement rate")
    else:
        print(f"  • Direct has {-eng_diff:.1f}pp HIGHER engagement rate")

    purchase_diff = sst_metrics['purchases'] - direct_metrics['purchases']
    if purchase_diff > 0:
        print(f"  • SST captures {purchase_diff:,} MORE purchases")
    elif purchase_diff < 0:
        print(f"  • Direct captures {-purchase_diff:,} MORE purchases")
    else:
        print(f"  • Both capture the same number of purchases")

    china_diff = sst_metrics['china_pct'] - direct_metrics['china_pct']
    if china_diff > 1:
        print(f"  • SST captures {china_diff:.1f}pp MORE China traffic (Great Firewall bypass)")

    # Interpretation
    both = metadata['totals']['both']
    total_unique = metadata['totals']['total']
    overlap_pct = both / total_unique * 100
    print(f"\nInterpretation:")
    print(f"  {overlap_pct:.1f}% of unique sessions appear in both sources ({both:,} / {total_unique:,})")
    print(f"  Quality scores are similar because overlap dominates")
    print(f"  The marginal differences come from:")
    print(f"    - SST-only: +{metadata['totals']['sst_only']:,} sessions (ad-blockers, China, privacy browsers)")
    print(f"    - Direct-only: +{metadata['totals']['direct_only']:,} sessions (prefetch/prerender, automated traffic)")

    print("\n" + "=" * 100)

    return sst_metrics, direct_metrics


if __name__ == "__main__":
    sst_metrics, direct_metrics = main()
