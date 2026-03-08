"""
Bootstrap Analysis: Confidence Intervals for Match Rates

Resamples the data 1000x to compute 95% confidence intervals for:
- Both (overlap) percentage
- SST-only percentage
- Direct-only percentage

Narrow CIs = high confidence in the point estimates.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

CACHE_DIR = Path(__file__).parent / "cache"

N_BOOTSTRAP = 1000
CONFIDENCE_LEVEL = 0.95


def load_cached_data():
    """Load session data from cache."""
    sst_df = pd.read_parquet(CACHE_DIR / "sst_sessions.parquet")
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    return sst_df, direct_df


def compute_rates(sst_df, direct_df):
    """Compute category rates from dataframes."""
    both = len(sst_df[sst_df['session_category'] == 'Both'])
    sst_only = len(sst_df[sst_df['session_category'] == 'SST-only'])
    direct_only = len(direct_df[direct_df['session_category'] == 'Direct-only'])
    total = both + sst_only + direct_only

    if total == 0:
        return {'both_pct': 0, 'sst_only_pct': 0, 'direct_only_pct': 0, 'total': 0}

    return {
        'both_pct': both / total * 100,
        'sst_only_pct': sst_only / total * 100,
        'direct_only_pct': direct_only / total * 100,
        'total': total
    }


def bootstrap_resample(sst_df, direct_df, n_samples=N_BOOTSTRAP):
    """
    Perform bootstrap resampling to estimate confidence intervals.

    Uses stratified resampling to maintain category proportions in each resample.
    """
    results = []

    n_sst = len(sst_df)
    n_direct = len(direct_df)

    for i in range(n_samples):
        # Resample with replacement from each dataframe
        sst_sample = sst_df.sample(n=n_sst, replace=True)
        direct_sample = direct_df.sample(n=n_direct, replace=True)

        rates = compute_rates(sst_sample, direct_sample)
        results.append(rates)

        if (i + 1) % 100 == 0:
            print(f"  Completed {i+1}/{n_samples} samples")

    return pd.DataFrame(results)


def compute_ci(values, confidence=CONFIDENCE_LEVEL):
    """Compute confidence interval using percentile method."""
    alpha = 1 - confidence
    lower = np.percentile(values, alpha / 2 * 100)
    upper = np.percentile(values, (1 - alpha / 2) * 100)
    return lower, upper


def main():
    print("=" * 70)
    print("BOOTSTRAP ANALYSIS: Confidence Intervals for Match Rates")
    print("=" * 70)

    sst_df, direct_df = load_cached_data()

    # Compute point estimates
    point_estimates = compute_rates(sst_df, direct_df)

    print(f"\nPoint Estimates (from cached data):")
    print(f"  Both:        {point_estimates['both_pct']:.2f}%")
    print(f"  SST-only:    {point_estimates['sst_only_pct']:.2f}%")
    print(f"  Direct-only: {point_estimates['direct_only_pct']:.2f}%")
    print(f"  Total:       {point_estimates['total']:,} sessions")

    # Perform bootstrap
    print(f"\nPerforming {N_BOOTSTRAP} bootstrap resamples...")
    bootstrap_df = bootstrap_resample(sst_df, direct_df)

    # Compute confidence intervals
    print(f"\n" + "-" * 70)
    print(f"95% CONFIDENCE INTERVALS")
    print("-" * 70)

    ci_results = {}
    for metric in ['both_pct', 'sst_only_pct', 'direct_only_pct']:
        values = bootstrap_df[metric]
        lower, upper = compute_ci(values)
        point = point_estimates[metric]
        ci_width = upper - lower

        metric_name = metric.replace('_pct', '').replace('_', '-').title()
        ci_results[metric] = {
            'point': point,
            'lower': lower,
            'upper': upper,
            'width': ci_width,
            'std': np.std(values)
        }

        print(f"\n  {metric_name}:")
        print(f"    Point estimate: {point:.2f}%")
        print(f"    95% CI:         [{lower:.2f}%, {upper:.2f}%]")
        print(f"    CI width:       {ci_width:.2f} percentage points")
        print(f"    Bootstrap SE:   {np.std(values):.3f}%")

    # Assess CI quality
    print("\n" + "-" * 70)
    print("CONFIDENCE INTERVAL QUALITY ASSESSMENT")
    print("-" * 70)

    for metric, data in ci_results.items():
        metric_name = metric.replace('_pct', '').replace('_', '-').title()
        # Relative precision: CI width as % of point estimate
        if data['point'] > 0:
            relative_precision = data['width'] / data['point'] * 100
        else:
            relative_precision = float('inf')

        print(f"\n  {metric_name}:")
        if data['width'] < 1.0:
            print(f"    EXCELLENT: CI width < 1 percentage point ({data['width']:.2f})")
        elif data['width'] < 2.0:
            print(f"    GOOD: CI width < 2 percentage points ({data['width']:.2f})")
        elif data['width'] < 5.0:
            print(f"    ACCEPTABLE: CI width < 5 percentage points ({data['width']:.2f})")
        else:
            print(f"    WIDE: CI width >= 5 percentage points ({data['width']:.2f})")

        print(f"    Relative precision: ±{relative_precision/2:.1f}% of estimate")

    # Plot bootstrap distributions
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    metrics = ['both_pct', 'sst_only_pct', 'direct_only_pct']
    colors = ['#9b59b6', '#2ecc71', '#3498db']
    titles = ['Both (Overlap)', 'SST-only', 'Direct-only']

    for ax, metric, color, title in zip(axes, metrics, colors, titles):
        values = bootstrap_df[metric]
        point = point_estimates[metric]
        lower, upper = ci_results[metric]['lower'], ci_results[metric]['upper']

        # Histogram
        ax.hist(values, bins=50, color=color, alpha=0.7, edgecolor='black')

        # Point estimate
        ax.axvline(x=point, color='red', linestyle='-', linewidth=2, label=f'Point: {point:.2f}%')

        # CI bounds
        ax.axvline(x=lower, color='black', linestyle='--', linewidth=1.5, label=f'95% CI: [{lower:.2f}, {upper:.2f}]')
        ax.axvline(x=upper, color='black', linestyle='--', linewidth=1.5)

        # Shade CI region
        ax.axvspan(lower, upper, alpha=0.2, color='gray')

        ax.set_xlabel('Percentage')
        ax.set_ylabel('Bootstrap Count')
        ax.set_title(f'{title}\n({point:.1f}% ± {ci_results[metric]["width"]/2:.2f}%)')
        ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig('docs/bootstrap_analysis.png', dpi=150, bbox_inches='tight')
    print(f"\nPlot saved to docs/bootstrap_analysis.png")

    # Save bootstrap results
    summary_df = pd.DataFrame([
        {
            'Metric': metric.replace('_pct', '').replace('_', '-').title(),
            'Point Estimate (%)': ci_results[metric]['point'],
            'Lower 95% CI (%)': ci_results[metric]['lower'],
            'Upper 95% CI (%)': ci_results[metric]['upper'],
            'CI Width (pp)': ci_results[metric]['width'],
            'Bootstrap SE (%)': ci_results[metric]['std']
        }
        for metric in metrics
    ])
    summary_df.to_csv('docs/bootstrap_results.csv', index=False)
    print(f"Results saved to docs/bootstrap_results.csv")

    # Verdict
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    avg_width = np.mean([ci_results[m]['width'] for m in metrics])

    if avg_width < 1.5:
        print("\nHIGH CONFIDENCE: All CIs are narrow (avg width < 1.5 pp)")
        print("The match rate estimates are statistically precise.")
        print("\nRecommended reporting format:")
        for metric in metrics:
            name = metric.replace('_pct', '').replace('_', '-').title()
            point = ci_results[metric]['point']
            lower = ci_results[metric]['lower']
            upper = ci_results[metric]['upper']
            print(f"  {name}: {point:.1f}% (95% CI: {lower:.1f}%-{upper:.1f}%)")
    elif avg_width < 3.0:
        print("\nMODERATE CONFIDENCE: CIs are reasonably narrow")
        print("Estimates are reliable but could benefit from more data.")
    else:
        print("\nLOW CONFIDENCE: Wide confidence intervals detected")
        print("Consider collecting more data or investigating variance sources.")


if __name__ == "__main__":
    main()
