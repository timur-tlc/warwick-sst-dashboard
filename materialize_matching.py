"""
Pre-compute and materialize the fuzzy session matching results.

Run this script to generate cached parquet files that the dashboard loads instantly.
"""

import pandas as pd
from corrected_matching_helpers import fuzzy_match_sessions, get_corrected_session_stats
from pathlib import Path

CACHE_DIR = Path(__file__).parent / "cache"


def materialize_session_data(date_start='20260106', date_end='20260113'):
    """
    Run fuzzy matching and save results to parquet files.
    """
    print(f"Materializing session data for {date_start} to {date_end}...")

    # Create cache directory
    CACHE_DIR.mkdir(exist_ok=True)

    # Run the full matching
    stats = get_corrected_session_stats(date_start, date_end)

    # Save the dataframes
    sst_df = stats['dataframes']['sst']
    direct_df = stats['dataframes']['direct']

    sst_df.to_parquet(CACHE_DIR / "sst_sessions.parquet", index=False)
    direct_df.to_parquet(CACHE_DIR / "direct_sessions.parquet", index=False)

    # Save timeseries data
    stats['daily'].to_parquet(CACHE_DIR / "daily.parquet", index=False)
    stats['hourly'].to_parquet(CACHE_DIR / "hourly.parquet", index=False)
    stats['hourly_weekday'].to_parquet(CACHE_DIR / "hourly_weekday.parquet", index=False)
    stats['hourly_weekend'].to_parquet(CACHE_DIR / "hourly_weekend.parquet", index=False)

    # Save totals and profiles as JSON
    import json
    metadata = {
        'totals': stats['totals'],
        'profiles': stats['profiles'],
        'date_start': date_start,
        'date_end': date_end
    }
    with open(CACHE_DIR / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\nMaterialized data saved to {CACHE_DIR}/")
    print(f"  - sst_sessions.parquet: {len(sst_df):,} rows")
    print(f"  - direct_sessions.parquet: {len(direct_df):,} rows")
    print(f"  - daily.parquet: {len(stats['daily']):,} rows")
    print(f"  - hourly.parquet: {len(stats['hourly']):,} rows")
    print(f"  - hourly_weekday.parquet: {len(stats['hourly_weekday']):,} rows")
    print(f"  - hourly_weekend.parquet: {len(stats['hourly_weekend']):,} rows")
    print(f"  - metadata.json")

    return stats


if __name__ == "__main__":
    materialize_session_data()
