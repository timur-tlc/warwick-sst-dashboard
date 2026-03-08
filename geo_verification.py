"""
Geo Verification: China Hypothesis and Google-Blocking Countries

Validates that:
1. China sessions are predominantly SST-only (Great Firewall blocks google-analytics.com)
2. Other countries known to block Google show similar patterns
3. Countries with Google access show expected Both/Direct patterns
"""

import pandas as pd
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"

# Countries known to block or heavily restrict Google services
GOOGLE_BLOCKED_COUNTRIES = [
    'China',
    'North Korea',  # Unlikely to have any traffic
    'Iran',         # Partial blocking
    'Russia',       # Partial/intermittent blocking
]

# Countries with reliable Google access (for comparison)
GOOGLE_ACCESSIBLE_COUNTRIES = [
    'Australia',
    'United States',
    'United Kingdom',
    'New Zealand',
    'Canada',
]


def load_cached_data():
    """Load session data from cache."""
    sst_df = pd.read_parquet(CACHE_DIR / "sst_sessions.parquet")
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    return sst_df, direct_df


def analyze_country(sst_df, direct_df, country):
    """Analyze session distribution for a specific country."""
    sst_country = sst_df[sst_df['geo_country'] == country]
    direct_country = direct_df[direct_df['geo_country'] == country]

    both_sst = len(sst_country[sst_country['session_category'] == 'Both'])
    sst_only = len(sst_country[sst_country['session_category'] == 'SST-only'])
    direct_only = len(direct_country[direct_country['session_category'] == 'Direct-only'])

    total = both_sst + sst_only + direct_only

    return {
        'country': country,
        'both': both_sst,
        'sst_only': sst_only,
        'direct_only': direct_only,
        'total': total,
        'both_pct': both_sst / total * 100 if total > 0 else 0,
        'sst_only_pct': sst_only / total * 100 if total > 0 else 0,
        'direct_only_pct': direct_only / total * 100 if total > 0 else 0,
    }


def main():
    print("=" * 70)
    print("GEO VERIFICATION: China Hypothesis & Google-Blocking Countries")
    print("=" * 70)

    sst_df, direct_df = load_cached_data()

    # Get all countries with data
    all_countries = set(sst_df['geo_country'].unique()) | set(direct_df['geo_country'].unique())
    all_countries = [c for c in all_countries if c and c != '' and c != 'None']

    print(f"\nTotal countries in data: {len(all_countries)}")

    # Analyze all countries
    results = []
    for country in all_countries:
        result = analyze_country(sst_df, direct_df, country)
        if result['total'] > 0:
            results.append(result)

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('total', ascending=False)

    # Print summary table
    print("\n" + "-" * 70)
    print("TOP 20 COUNTRIES BY SESSION VOLUME")
    print("-" * 70)
    print(f"{'Country':<25} {'Both':>8} {'SST-only':>10} {'Direct-only':>12} {'Total':>8}")
    print("-" * 70)

    for _, row in results_df.head(20).iterrows():
        print(f"{row['country']:<25} {row['both']:>8} "
              f"{row['sst_only']:>10} {row['direct_only']:>12} {row['total']:>8}")

    # China-specific analysis
    print("\n" + "=" * 70)
    print("CHINA HYPOTHESIS VERIFICATION")
    print("=" * 70)

    china = results_df[results_df['country'] == 'China'].iloc[0] if 'China' in results_df['country'].values else None

    if china is not None:
        print(f"\nChina sessions: {china['total']}")
        print(f"  Both:        {china['both']:>6} ({china['both_pct']:>5.1f}%)")
        print(f"  SST-only:    {china['sst_only']:>6} ({china['sst_only_pct']:>5.1f}%)")
        print(f"  Direct-only: {china['direct_only']:>6} ({china['direct_only_pct']:>5.1f}%)")

        # Check the hypothesis
        if china['sst_only_pct'] > 70 and china['direct_only_pct'] < 10:
            print("\n  HYPOTHESIS CONFIRMED: China is predominantly SST-only")
            print("  Great Firewall blocks google-analytics.com as expected.")
        elif china['sst_only_pct'] > china['direct_only_pct']:
            print("\n  HYPOTHESIS PARTIALLY CONFIRMED: More SST-only than Direct-only")
            print(f"  SST captures {china['sst_only_pct']:.1f}% of China traffic that would otherwise be lost.")
        else:
            print("\n  HYPOTHESIS NOT CONFIRMED: Unexpected distribution")
            print("  This warrants further investigation.")
    else:
        print("\nNo China sessions found in data.")

    # Australia comparison (expected baseline)
    print("\n" + "-" * 70)
    print("AUSTRALIA COMPARISON (Expected: Mostly 'Both')")
    print("-" * 70)

    au = results_df[results_df['country'] == 'Australia'].iloc[0] if 'Australia' in results_df['country'].values else None

    if au is not None:
        print(f"\nAustralia sessions: {au['total']}")
        print(f"  Both:        {au['both']:>6} ({au['both_pct']:>5.1f}%)")
        print(f"  SST-only:    {au['sst_only']:>6} ({au['sst_only_pct']:>5.1f}%)")
        print(f"  Direct-only: {au['direct_only']:>6} ({au['direct_only_pct']:>5.1f}%)")

        if au['both_pct'] > 80:
            print("\n  EXPECTED: Australia shows mostly 'Both' sessions.")
            print("  This validates that matching works correctly for accessible regions.")
        else:
            print(f"\n  NOTE: Both percentage ({au['both_pct']:.1f}%) is lower than expected.")
            print("  This may indicate ad-blocker usage or corporate networks in AU.")

    # Other Google-blocked countries
    print("\n" + "-" * 70)
    print("OTHER GOOGLE-RESTRICTED COUNTRIES")
    print("-" * 70)

    for country in GOOGLE_BLOCKED_COUNTRIES:
        if country == 'China':
            continue
        row = results_df[results_df['country'] == country]
        if len(row) > 0:
            row = row.iloc[0]
            print(f"\n{country}: {row['total']} sessions")
            print(f"  SST-only: {row['sst_only_pct']:.1f}% | Direct-only: {row['direct_only_pct']:.1f}%")
        else:
            print(f"\n{country}: No sessions in data")

    # SST-only dominant countries
    print("\n" + "=" * 70)
    print("COUNTRIES WITH HIGH SST-ONLY PERCENTAGE (>40%)")
    print("=" * 70)

    high_sst_only = results_df[
        (results_df['sst_only_pct'] > 40) &
        (results_df['total'] >= 10)
    ].sort_values('sst_only_pct', ascending=False)

    print(f"\n{'Country':<25} {'SST-only %':>12} {'Direct-only %':>14} {'Total':>8}")
    print("-" * 60)

    for _, row in high_sst_only.iterrows():
        print(f"{row['country']:<25} {row['sst_only_pct']:>11.1f}% "
              f"{row['direct_only_pct']:>13.1f}% {row['total']:>8}")

    # Direct-only dominant countries (interesting cases)
    print("\n" + "=" * 70)
    print("COUNTRIES WITH HIGH DIRECT-ONLY PERCENTAGE (>20%)")
    print("=" * 70)

    high_direct_only = results_df[
        (results_df['direct_only_pct'] > 20) &
        (results_df['total'] >= 10)
    ].sort_values('direct_only_pct', ascending=False)

    print(f"\n{'Country':<25} {'SST-only %':>12} {'Direct-only %':>14} {'Total':>8}")
    print("-" * 60)

    for _, row in high_direct_only.iterrows():
        print(f"{row['country']:<25} {row['sst_only_pct']:>11.1f}% "
              f"{row['direct_only_pct']:>13.1f}% {row['total']:>8}")

    print("\n(High Direct-only may indicate bots, crawlers, or prefetch traffic)")

    # Save full results
    results_df.to_csv('docs/geo_verification_results.csv', index=False)
    print(f"\nFull results saved to docs/geo_verification_results.csv")

    # Verdict
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    # China hypothesis: SST-only should be significantly higher than Direct-only
    # (Great Firewall blocks google-analytics.com, so Direct fails but SST works)
    china_confirmed = china is not None and china['sst_only_pct'] > china['direct_only_pct'] * 3
    au_normal = au is not None and au['both_pct'] > 70

    if china_confirmed and au_normal:
        print("\nGEO PATTERNS VALIDATED:")
        print(f"  - China: SST-only ({china['sst_only_pct']:.1f}%) >> Direct-only ({china['direct_only_pct']:.1f}%)")
        print("  - Great Firewall impact confirmed (SST captures blocked traffic)")
        print(f"  - Australia: {au['both_pct']:.1f}% Both (normal matching)")
        print("  - This validates the matching methodology is working correctly")
    elif china_confirmed:
        print("\nCHINA HYPOTHESIS CONFIRMED but Australia pattern needs review.")
        print(f"  China SST-only: {china['sst_only_pct']:.1f}% vs Direct-only: {china['direct_only_pct']:.1f}%")
    elif au_normal:
        print("\nAUSTRALIA NORMAL but China hypothesis not fully confirmed.")
        if china is not None:
            print(f"  China SST-only: {china['sst_only_pct']:.1f}% (expected higher)")
    else:
        print("\nUNEXPECTED PATTERNS - further investigation recommended.")


if __name__ == "__main__":
    main()
