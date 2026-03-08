"""
Timestamp Drift Analysis: Timing Differences Between Matched Pairs

For matched "Both" sessions, analyzes the timestamp difference between
SST and Direct events. Should be centered near zero with small variance.

Systematic drift would indicate pipeline issues.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"


def load_cached_data():
    """Load session data from cache."""
    sst_df = pd.read_parquet(CACHE_DIR / "sst_sessions.parquet")
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    return sst_df, direct_df


def main():
    print("=" * 70)
    print("TIMESTAMP DRIFT ANALYSIS: Matched Pair Timing")
    print("=" * 70)

    sst_df, direct_df = load_cached_data()

    # Get "Both" sessions from each source
    both_sst = sst_df[sst_df['session_category'] == 'Both'].copy()
    both_direct = direct_df[direct_df['session_category'] == 'Both'].copy()

    print(f"\nMatched pairs: {len(both_sst):,}")

    # Create lookup by ga_session_id
    # Note: These are different session IDs, so we need to match by other attributes
    # Since we don't store the match pairs directly, we'll re-match using timestamp proximity

    print("\nRe-matching to calculate timestamp differences...")

    # Sort by timestamp for efficient matching
    both_sst_sorted = both_sst.sort_values('session_start_ts').reset_index(drop=True)
    both_direct_sorted = both_direct.sort_values('session_start_ts').reset_index(drop=True)

    # For each SST session, find the closest matching Direct session
    diffs = []
    matched_pairs = []

    time_window_micros = 300 * 1000000  # 5 minutes

    for _, sst_row in both_sst_sorted.iterrows():
        sst_ts = sst_row['session_start_ts']

        # Find Direct sessions within time window
        candidates = both_direct_sorted[
            (both_direct_sorted['session_start_ts'] >= sst_ts - time_window_micros) &
            (both_direct_sorted['session_start_ts'] <= sst_ts + time_window_micros) &
            (both_direct_sorted['device_category'] == sst_row['device_category']) &
            (both_direct_sorted['geo_country'] == sst_row['geo_country'])
        ]

        if len(candidates) == 0:
            continue

        # Take closest match
        candidates = candidates.copy()
        candidates['ts_diff'] = candidates['session_start_ts'] - sst_ts
        best_idx = candidates['ts_diff'].abs().idxmin()
        best_match = candidates.loc[best_idx]

        # Store the difference (Direct - SST) in seconds
        diff_seconds = best_match['ts_diff'] / 1000000  # microseconds to seconds
        diffs.append(diff_seconds)

        matched_pairs.append({
            'sst_ts': sst_ts,
            'direct_ts': best_match['session_start_ts'],
            'diff_seconds': diff_seconds,
            'device': sst_row['device_category'],
            'country': sst_row['geo_country']
        })

    pairs_df = pd.DataFrame(matched_pairs)

    print(f"\nMatched {len(pairs_df):,} pairs for analysis")

    if len(pairs_df) == 0:
        print("\nNo pairs found for timestamp drift analysis.")
        print("This could indicate:")
        print("  - Empty cache files")
        print("  - No 'Both' category sessions")
        print("  - Matching parameters too strict")
        return

    # Calculate statistics
    diffs_array = np.array(diffs)

    print("\n" + "-" * 70)
    print("TIMESTAMP DIFFERENCE STATISTICS (Direct - SST)")
    print("-" * 70)
    print(f"  Mean:     {np.mean(diffs_array):>8.3f} seconds")
    print(f"  Median:   {np.median(diffs_array):>8.3f} seconds")
    print(f"  Std Dev:  {np.std(diffs_array):>8.3f} seconds")
    print(f"  Min:      {np.min(diffs_array):>8.3f} seconds")
    print(f"  Max:      {np.max(diffs_array):>8.3f} seconds")

    # Percentiles
    print("\n  Percentiles:")
    for pct in [1, 5, 25, 50, 75, 95, 99]:
        val = np.percentile(diffs_array, pct)
        print(f"    {pct:>2}th: {val:>8.3f} seconds")

    # Count by direction
    direct_first = np.sum(diffs_array > 0)
    sst_first = np.sum(diffs_array < 0)
    same_time = np.sum(diffs_array == 0)

    print("\n" + "-" * 70)
    print("ARRIVAL ORDER")
    print("-" * 70)
    print(f"  Direct first (positive):  {direct_first:>6} ({direct_first/len(diffs_array)*100:.1f}%)")
    print(f"  SST first (negative):     {sst_first:>6} ({sst_first/len(diffs_array)*100:.1f}%)")
    print(f"  Same timestamp:           {same_time:>6} ({same_time/len(diffs_array)*100:.1f}%)")

    # Check for systematic drift
    print("\n" + "-" * 70)
    print("DRIFT ASSESSMENT")
    print("-" * 70)

    if abs(np.mean(diffs_array)) < 1.0:
        print("  GOOD: Mean drift is near zero (< 1 second)")
        print("        No systematic timing offset detected.")
    else:
        direction = "Direct arrives later" if np.mean(diffs_array) > 0 else "SST arrives later"
        print(f"  WARNING: Mean drift of {abs(np.mean(diffs_array)):.2f}s detected")
        print(f"           {direction}")

    if np.std(diffs_array) < 5.0:
        print("  GOOD: Low variance (< 5 seconds)")
        print("        Consistent timing between sources.")
    else:
        print(f"  NOTE: High variance ({np.std(diffs_array):.2f}s)")
        print("        Some sessions have large timing differences.")

    # Analyze by device type
    print("\n" + "-" * 70)
    print("DRIFT BY DEVICE TYPE")
    print("-" * 70)

    for device in pairs_df['device'].unique():
        device_diffs = pairs_df[pairs_df['device'] == device]['diff_seconds']
        print(f"\n  {device}:")
        print(f"    Mean:   {device_diffs.mean():>7.3f}s (n={len(device_diffs):,})")
        print(f"    Median: {device_diffs.median():>7.3f}s")

    # Plot
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    # Histogram of all differences
    ax1 = axes[0]
    ax1.hist(diffs_array, bins=100, color='#9b59b6', alpha=0.7, edgecolor='black')
    ax1.axvline(x=0, color='red', linestyle='--', linewidth=2, label='Zero')
    ax1.axvline(x=np.mean(diffs_array), color='green', linestyle='-', linewidth=2, label=f'Mean ({np.mean(diffs_array):.2f}s)')
    ax1.set_xlabel('Timestamp Difference (seconds)')
    ax1.set_ylabel('Count')
    ax1.set_title('Distribution of Timestamp Differences\n(Direct - SST)')
    ax1.legend()

    # Zoomed histogram (±5 seconds)
    ax2 = axes[1]
    zoomed_diffs = diffs_array[(diffs_array >= -5) & (diffs_array <= 5)]
    if len(zoomed_diffs) > 0:
        ax2.hist(zoomed_diffs, bins=50, color='#3498db', alpha=0.7, edgecolor='black')
        ax2.axvline(x=0, color='red', linestyle='--', linewidth=2)
        ax2.axvline(x=np.mean(zoomed_diffs), color='green', linestyle='-', linewidth=2)
        zoomed_pct = len(zoomed_diffs) / len(diffs_array) * 100
    else:
        zoomed_pct = 0
    ax2.set_xlabel('Timestamp Difference (seconds)')
    ax2.set_ylabel('Count')
    ax2.set_title(f'Zoomed View (±5s)\n{zoomed_pct:.1f}% of data')

    # Q-Q plot to check normality
    ax3 = axes[2]
    sorted_diffs = np.sort(diffs_array)
    n = len(sorted_diffs)
    theoretical_quantiles = np.array([(i - 0.5) / n for i in range(1, n + 1)])

    try:
        from scipy.stats import norm
        theoretical_values = norm.ppf(theoretical_quantiles) * np.std(diffs_array) + np.mean(diffs_array)

        # Sample for plotting (full data too dense)
        sample_idx = np.linspace(0, n-1, min(1000, n), dtype=int)
        ax3.scatter(theoretical_values[sample_idx], sorted_diffs[sample_idx], alpha=0.5, s=10)
        min_val = min(theoretical_values.min(), sorted_diffs.min())
        max_val = max(theoretical_values.max(), sorted_diffs.max())
        ax3.plot([min_val, max_val], [min_val, max_val], 'r--', linewidth=2, label='Normal')
        ax3.set_xlabel('Theoretical Quantiles (Normal)')
        ax3.set_ylabel('Observed Quantiles')
        ax3.set_title('Q-Q Plot\n(Check for Normality)')
        ax3.legend()
    except ImportError:
        ax3.text(0.5, 0.5, 'scipy not installed\n(pip install scipy)',
                 ha='center', va='center', transform=ax3.transAxes)
        ax3.set_title('Q-Q Plot (scipy required)')

    plt.tight_layout()
    plt.savefig('docs/timestamp_drift.png', dpi=150, bbox_inches='tight')
    print(f"\nPlot saved to docs/timestamp_drift.png")

    # Save detailed results
    pairs_df.to_csv('docs/timestamp_drift_pairs.csv', index=False)
    print(f"Pair data saved to docs/timestamp_drift_pairs.csv")

    # Verdict
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    if abs(np.mean(diffs_array)) < 1.0 and np.std(diffs_array) < 10.0:
        print("\nTIMING VALIDATION PASSED:")
        print("  - Timestamp differences are centered near zero")
        print("  - Variance is within acceptable range")
        print("  - This confirms the matching algorithm is working correctly")
    else:
        print("\nTIMING REQUIRES REVIEW:")
        issues = []
        if abs(np.mean(diffs_array)) >= 1.0:
            issues.append(f"  - Systematic offset of {np.mean(diffs_array):.2f}s detected")
        if np.std(diffs_array) >= 10.0:
            issues.append(f"  - High variance ({np.std(diffs_array):.2f}s) suggests some mismatches")
        for issue in issues:
            print(issue)


if __name__ == "__main__":
    main()
